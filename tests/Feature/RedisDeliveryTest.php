<?php

namespace ProcessHub\Logs\Tests\Feature;

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
            ->push('', 429, ['Retry-After' => '0'])
            ->push('', 429, ['Retry-After' => '0'])
            ->push('', 429, ['Retry-After' => '0'])
            ->push('', 429, ['Retry-After' => '0'])
            ->push('', 429, ['Retry-After' => '0'])
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
        }

        $this->assertSame(0, $failed);
        Http::assertSentCount(6);
        $this->assertSame(0, Queue::connection('redis')->size('logs'));
        $this->assertFileDoesNotExist(config('processhub.fallback_path'));
    }

    public function test_rebatch_queue_repacks_backlog_into_full_batches(): void
    {
        $backlog = Queue::connection('redis');
        for ($i = 0; $i < 250; $i++) {
            $backlog->pushOn('logs-backlog', new SendLogBatchJob([['level' => 'ERROR', 'message' => 'm-' . $i]]));
        }
        $backlog->pushOn('logs-backlog', new ForeignTestJob());

        $this->artisan('processhub:rebatch-queue', ['--from' => 'logs-backlog', '--chunk' => 100])
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

        $this->artisan('processhub:rebatch-queue', ['--from' => 'logs-backlog'])->assertSuccessful();

        $messages = array_column(array_merge(...$this->drainQueue('logs')), 'message');
        $this->assertSame(['claimed-0', 'claimed-1', 'claimed-2', 'ready'], $messages);
        $this->assertSame(0, (int) $redis->llen('queues:logs-backlog'));
        $this->assertSame(0, (int) $redis->llen('queues:logs-backlog:rebatching'));
    }

    public function test_rebatch_queue_refuses_working_queue_as_source(): void
    {
        $this->artisan('processhub:rebatch-queue', ['--from' => 'logs'])->assertFailed();
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
