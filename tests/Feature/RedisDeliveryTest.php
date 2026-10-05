<?php

namespace ProcessHub\Logs\Tests\Feature;

use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\Psr7\Request as GuzzleRequest;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Queue;
use Illuminate\Support\Facades\Redis;
use ProcessHub\Logs\Jobs\SendLogBatchJob;
use ProcessHub\Logs\Tests\Concerns\UsesRedis;
use ProcessHub\Logs\Tests\TestCase;

/**
 * Real Redis queue + in-process worker: retry semantics and backlog repacking.
 */
class RedisDeliveryTest extends TestCase
{
    use UsesRedis;

    protected function setUp(): void
    {
        parent::setUp();
        $this->setUpRedis();
    }

    public function test_rate_limit_streak_does_not_exhaust_the_job(): void
    {
        Http::fakeSequence('ph.test/*')
            ->push('', 429, ['Retry-After' => '1'])
            ->push('', 429, ['Retry-After' => '1'])
            ->push('', 429, ['Retry-After' => '1'])
            ->push('', 429, ['Retry-After' => '1'])
            ->push('', 429, ['Retry-After' => '1'])
            ->push(['accepted' => 1]);
        $failed = 0;
        Event::listen(JobFailed::class, function () use (&$failed) {
            $failed++;
        });

        SendLogBatchJob::enqueue([['level' => 'ERROR', 'message' => 'survives 429']]);

        // The old `$tries = 3` would have failed this on the 4th attempt.
        for ($i = 0; $i < 6; $i++) {
            $this->artisan('queue:work', [
                'connection' => 'redis', '--queue' => 'logs', '--once' => true, '--tries' => 3,
            ])->assertSuccessful();
            // Past Retry-After and the global pause.
            $this->travel(2)->seconds();
        }

        $this->assertSame(0, $failed);
        Http::assertSentCount(6);
        $this->assertSame(0, Queue::connection('redis')->size('logs'));
        $this->assertFileDoesNotExist(config('processhub.fallback_path'));
    }

    /**
     * Laravel 11 lets a reset connection (cURL 56), TLS or HTTP/2 error out
     * of the HTTP client as a raw Guzzle RequestException. If it isn't
     * recognised as our own failure, the worker's report() logs it into the
     * processhub channel — a new job per failed attempt, 1 → 2 → 4 → …
     */
    public function test_transport_error_does_not_multiply_jobs(): void
    {
        config()->set('logging.default', 'processhub');
        $posts = 0;
        Http::fake(function ($request) use (&$posts) {
            $posts++;

            throw new RequestException(
                'cURL error 56: Recv failure: Connection reset by peer',
                new GuzzleRequest('POST', $request->url()),
            );
        });

        SendLogBatchJob::enqueue([['level' => 'ERROR', 'message' => 'reset']]);

        for ($i = 0; $i < 4; $i++) {
            $this->artisan('queue:work', ['connection' => 'redis', '--queue' => 'logs', '--once' => true]);
            $this->travel(301)->seconds();
        }

        $this->assertSame(1, Queue::connection('redis')->size('logs'));
        $this->assertSame(4, $posts);
    }

    public function test_rebatch_queue_repacks_backlog_into_full_batches(): void
    {
        $backlog = Queue::connection('redis');
        for ($i = 0; $i < 250; $i++) {
            $backlog->pushOn('logs-backlog', new SendLogBatchJob([['level' => 'ERROR', 'message' => 'm-' . $i]]));
        }
        $backlog->pushOn('logs-backlog', new ForeignTestJob());

        $this->artisan('processhub:rebatch-queue', ['--from' => 'logs-backlog', '--chunk' => 100, '--rate' => 0])
            ->expectsOutputToContain('Done: 251 jobs, 250 entries repacked into 3 batches')
            ->assertSuccessful();

        $batches = $this->drainQueue('logs');
        $this->assertSame([100, 100, 50], array_map('count', $batches));
        $this->assertSame(
            array_map(fn ($i) => 'm-' . $i, range(0, 249)),
            array_column(array_merge(...$batches), 'message'),
        );

        // Foreign job is returned untouched and not looped over.
        $left = Redis::connection()->lrange('queues:logs-backlog', 0, -1);
        $this->assertCount(1, $left);
        $this->assertSame(ForeignTestJob::class, json_decode($left[0], true)['displayName']);
        $this->assertSame(0, (int) Redis::connection()->llen('queues:logs-backlog:rebatching'));
    }

    public function test_rebatch_queue_recovers_chunk_claimed_by_crashed_run(): void
    {
        $backlog = Queue::connection('redis');
        for ($i = 0; $i < 3; $i++) {
            $backlog->pushOn('logs-backlog', new SendLogBatchJob([['message' => 'claimed-' . $i]]));
        }
        // Simulate a run that claimed the chunk and died before enqueueing.
        $redis = Redis::connection();
        $redis->rpush('queues:logs-backlog:rebatching', ...$redis->lrange('queues:logs-backlog', 0, -1));
        $redis->del('queues:logs-backlog');
        $backlog->pushOn('logs-backlog', new SendLogBatchJob([['message' => 'ready']]));

        $this->artisan('processhub:rebatch-queue', ['--from' => 'logs-backlog', '--rate' => 0])->assertSuccessful();

        $messages = array_column(array_merge(...$this->drainQueue('logs')), 'message');
        $this->assertSame(['claimed-0', 'claimed-1', 'claimed-2', 'ready'], $messages);
        $this->assertSame(0, (int) $redis->llen('queues:logs-backlog'));
        $this->assertSame(0, (int) $redis->llen('queues:logs-backlog:rebatching'));
        $this->assertSame(0, (int) $redis->exists('queues:logs-backlog:notify'), 'drained backlog leaves no :notify list');
    }

    public function test_rebatch_queue_paces_batches_behind_existing_queue(): void
    {
        config()->set('processhub.retry_window_sec', 3600);
        $this->freezeTime();
        $backlog = Queue::connection('redis');
        for ($i = 0; $i < 250; $i++) {
            $backlog->pushOn('logs-backlog', new SendLogBatchJob([['message' => 'm-' . $i]]));
        }

        $this->artisan('processhub:rebatch-queue', ['--from' => 'logs-backlog', '--chunk' => 100, '--rate' => 2])
            ->expectsOutputToContain('Paced at 2 batches/min: the last batch is due in 1 min.')
            ->assertSuccessful();

        // 2 per minute, evenly spaced.
        $this->assertSame([0, 30, 60], array_keys($this->scheduledBatches()));

        // The next run queues up behind the 3 batches already waiting.
        for ($i = 0; $i < 100; $i++) {
            $backlog->pushOn('logs-backlog', new SendLogBatchJob([['message' => 'n-' . $i]]));
        }
        $this->artisan('processhub:rebatch-queue', ['--from' => 'logs-backlog', '--rate' => 2])->assertSuccessful();

        $scheduled = $this->scheduledBatches();
        $this->assertSame([0, 30, 60, 90], array_keys($scheduled));
        // Each batch gets the full retry window counted from its due time.
        foreach ($scheduled as $dueIn => $payload) {
            $this->assertSame(now()->getTimestamp() + $dueIn + 3600, $payload['retryUntil']);
        }
        $this->assertSame(0, (int) Redis::connection()->exists('queues:logs-backlog:notify'));
    }

    public function test_rejected_batch_goes_to_fallback_not_to_failed_jobs(): void
    {
        Http::fake(['ph.test/*' => Http::response(['error' => 'bad token'], 401)]);
        $failed = $this->countFailedJobs();

        SendLogBatchJob::enqueue([['level' => 'ERROR', 'message' => 'rejected']]);
        $this->artisan('queue:work', ['connection' => 'redis', '--queue' => 'logs', '--once' => true]);

        $this->assertSame(0, $failed->count);
        $this->assertSame(0, Queue::connection('redis')->size('logs'));
        $this->assertStringContainsString('rejected', (string) file_get_contents(config('processhub.fallback_path')));
    }

    public function test_exhausted_retry_window_goes_to_fallback_not_to_failed_jobs(): void
    {
        config()->set('processhub.retry_window_sec', 60);
        Http::fake(['ph.test/*' => Http::response('', 503)]);
        $failed = $this->countFailedJobs();

        SendLogBatchJob::enqueue([['level' => 'ERROR', 'message' => 'outage']]);
        // 503 → backoff 10 s; on the 2nd 503 the 30 s backoff no longer fits the 60 s window.
        for ($i = 0; $i < 3; $i++) {
            $this->artisan('queue:work', ['connection' => 'redis', '--queue' => 'logs', '--once' => true]);
            $this->travel(11)->seconds();
        }

        Http::assertSentCount(2);
        $this->assertSame(0, $failed->count);
        $this->assertSame(0, Queue::connection('redis')->size('logs'));
        $this->assertStringContainsString('outage', (string) file_get_contents(config('processhub.fallback_path')));
    }

    public function test_rebatch_queue_refuses_working_queue_as_source(): void
    {
        $this->artisan('processhub:rebatch-queue', ['--from' => 'logs'])->assertFailed();
    }

    /**
     * Payloads on `logs` keyed by seconds until they are due.
     *
     * @return array<int, array<string, mixed>>
     */
    private function scheduledBatches(): array
    {
        $redis = Redis::connection();
        $scheduled = [];
        foreach ($redis->lrange('queues:logs', 0, -1) as $payload) {
            $scheduled[0] = json_decode($payload, true);
        }
        foreach ($redis->zrange('queues:logs:delayed', 0, -1, ['withscores' => true]) as $payload => $at) {
            $scheduled[(int) $at - now()->getTimestamp()] = json_decode($payload, true);
        }
        ksort($scheduled);

        return $scheduled;
    }

    private function countFailedJobs(): \stdClass
    {
        $failed = new \stdClass();
        $failed->count = 0;
        Event::listen(JobFailed::class, function () use ($failed) {
            $failed->count++;
        });

        return $failed;
    }

    /**
     * @return array<int, array<int, array<string, mixed>>>
     */
    private function drainQueue(string $queue): array
    {
        $batches = [];
        foreach (Redis::connection()->lrange('queues:' . $queue, 0, -1) as $payload) {
            $command = unserialize(json_decode($payload, true)['data']['command']);
            $batches[] = $command->entries;
        }

        return $batches;
    }
}

class ForeignTestJob implements ShouldQueue
{
    public function handle(): void {}
}
