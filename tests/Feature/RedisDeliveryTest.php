<?php

namespace ProcessHub\Logs\Tests\Feature;

use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\Psr7\Request as GuzzleRequest;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Queue\MaxAttemptsExceededException;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Queue;
use Illuminate\Support\Facades\Redis;
use Illuminate\Support\Str;
use ProcessHub\Logs\Jobs\SendLogBatchJob;
use ProcessHub\Logs\Support\IngestThrottle;
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
            $this->assertSame(now()->getTimestamp() + $dueIn + 3600, unserialize($payload['data']['command'])->retryDeadline);
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

    /**
     * Under a backlog a job may be picked up after its deadline. The worker
     * used to fail it by `retryUntil` (MaxAttemptsExceeded → failed_jobs,
     * Horizon «failed», a report) — tens of thousands in an overload.
     */
    public function test_job_picked_up_after_deadline_goes_to_fallback_not_to_failed_jobs(): void
    {
        Http::fake();
        $failed = $this->countFailedJobs();

        SendLogBatchJob::enqueue([['level' => 'ERROR', 'message' => 'late']]);
        $this->travel(3601)->seconds();
        $this->work();

        $this->assertSame(0, $failed->count);
        Http::assertNothingSent();
        $this->assertSame(0, Queue::connection('redis')->size('logs'));
        $lines = file(config('processhub.fallback_path'), FILE_IGNORE_NEW_LINES);
        $this->assertCount(1, $lines);
        $this->assertSame('late', json_decode($lines[0], true)['entries'][0]['message']);
    }

    /**
     * A job queued by 0.3 (`maxTries` 3, no `retryUntil`) must not run out of
     * attempts while the limiter keeps it waiting.
     */
    public function test_job_queued_by_v03_survives_local_rate_limit(): void
    {
        Http::fake(['ph.test/*' => Http::response(['accepted' => 1])]);
        $failed = $this->countFailedJobs();
        $command = preg_replace('/s:13:"retryDeadline";i:\d+;/', 's:5:"tries";i:3;', serialize(
            (new SendLogBatchJob([['level' => 'WARNING', 'message' => 'legacy']]))->onConnection('redis')->onQueue('logs'),
        ), 1, $replaced);
        $this->assertSame(1, $replaced);
        Queue::connection('redis')->pushRaw((string) json_encode([
            'uuid' => (string) Str::uuid(), 'displayName' => SendLogBatchJob::class,
            'job' => 'Illuminate\\Queue\\CallQueuedHandler@call', 'maxTries' => 3, 'maxExceptions' => null,
            'failOnTimeout' => false, 'backoff' => null, 'timeout' => 30, 'retryUntil' => null,
            'data' => ['commandName' => SendLogBatchJob::class, 'command' => $command], 'attempts' => 0,
        ]), 'logs');

        for ($i = 0; $i < 5; $i++) {
            IngestThrottle::make()->pauseFor(5);
            $this->work();
            $this->travel(6)->seconds();
        }
        $this->work();

        $this->assertSame(0, $failed->count);
        Http::assertSentCount(1);
        $this->assertSame(0, Queue::connection('redis')->size('logs'));
        $this->assertFileDoesNotExist(config('processhub.fallback_path'));
    }

    /**
     * What the worker throws when it gives up on the job is recognised as the
     * job's own failure (dontReportWhen/reportable hook), the batch is parked.
     */
    public function test_worker_giving_up_on_the_job_is_its_own_failure(): void
    {
        Http::fake();
        $exceptions = [];
        Event::listen(JobFailed::class, function (JobFailed $event) use (&$exceptions) {
            $exceptions[] = $event->exception;
        });

        SendLogBatchJob::enqueue([['level' => 'ERROR', 'message' => 'abandoned']]);
        $this->travel(3600 + 86_400 + 1)->seconds();
        $this->work();

        $this->assertCount(1, $exceptions);
        $this->assertInstanceOf(MaxAttemptsExceededException::class, $exceptions[0]);
        $this->assertTrue(SendLogBatchJob::isOwnFailure($exceptions[0]));
        $this->assertStringContainsString('abandoned', (string) file_get_contents(config('processhub.fallback_path')));
    }

    public function test_batch_with_broken_utf8_is_queued_and_delivered(): void
    {
        Http::fake(['ph.test/*' => Http::response(['accepted' => 2])]);

        SendLogBatchJob::enqueue([['level' => 'WARNING', 'message' => "bad \xB1 byte"], ['level' => 'WARNING', 'message' => 'ok']]);
        $this->assertSame(1, Queue::connection('redis')->size('logs'));
        $this->work();

        Http::assertSent(fn ($request) => array_column($request['logs'], 'message') === ["bad \u{FFFD} byte", 'ok']);
    }

    public function test_batch_the_fallback_file_cannot_take_stays_in_failed_jobs(): void
    {
        config()->set('processhub.fallback_path', '/nonexistent-dir/processhub-fallback.log');
        Http::fake(['ph.test/*' => Http::response(['error' => 'bad token'], 401)]);
        $failed = [];
        Event::listen(JobFailed::class, function (JobFailed $event) use (&$failed) {
            $failed[] = $event->job->payload();
        });

        SendLogBatchJob::enqueue([['level' => 'ERROR', 'message' => 'precious']]);
        $this->work();

        $this->assertCount(1, $failed);
        $this->assertSame('precious', unserialize($failed[0]['data']['command'])->entries[0]['message']);
        $this->assertSame(0, Queue::connection('redis')->size('logs'));
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

    private function work(): void
    {
        $this->artisan('queue:work', ['connection' => 'redis', '--queue' => 'logs', '--once' => true, '--tries' => 3]);
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
