<?php

namespace ProcessHub\Logs\Tests\Feature;

use Illuminate\Queue\Events\JobQueued;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Queue;
use Illuminate\Support\Facades\Redis;
use ProcessHub\Logs\Jobs\SendLogBatchJob;
use ProcessHub\Logs\Tests\Concerns\UsesRedis;
use ProcessHub\Logs\Tests\TestCase;

/**
 * processhub:rebatch-queue against a real Redis: crash recovery, locking,
 * :notify bookkeeping, schedule continuation and --max-ahead.
 */
class RebatchQueueCommandTest extends TestCase
{
    use UsesRedis;

    protected function setUp(): void
    {
        parent::setUp();
        $this->setUpRedis();
    }

    public function test_crash_after_a_pushed_batch_resends_only_that_batch(): void
    {
        $this->backlog(250);
        $pushed = 0;
        Event::listen(JobQueued::class, function () use (&$pushed): void {
            // Падение сразу после второго push, до подтверждения в claimed-списке.
            if (++$pushed === 2) {
                throw new \RuntimeException('killed');
            }
        });

        try {
            Artisan::call('processhub:rebatch-queue', ['--from' => 'logs-backlog', '--rate' => 0]);
            $this->fail('the run was expected to crash');
        } catch (\RuntimeException $e) {
            $this->assertSame('killed', $e->getMessage());
        }
        $this->assertSame(150, (int) Redis::connection()->llen('queues:logs-backlog:rebatching'));

        $this->artisan('processhub:rebatch-queue', ['--from' => 'logs-backlog', '--rate' => 0])
            ->expectsOutputToContain('(recovered)')
            ->assertSuccessful();

        $messages = array_column(array_merge(...$this->batches('logs')), 'message');
        $expected = $this->messages(0, 250);
        $this->assertSame($expected, array_values(array_unique($messages)));
        // Повторно ушла только неподтверждённая пачка.
        $this->assertSame([...$this->messages(0, 200), ...$this->messages(100, 250)], $messages);
        $this->assertSame(0, (int) Redis::connection()->exists('queues:logs-backlog:rebatching'));
    }

    public function test_foreign_jobs_are_returned_once_and_notify_follows_the_backlog(): void
    {
        $redis = Redis::connection();
        $this->backlog(150);
        Queue::connection('redis')->pushOn('logs-backlog', new ForeignTestJob());
        $this->backlog(100, 150);
        $this->assertSame(251, (int) $redis->llen('queues:logs-backlog:notify'));

        $this->artisan('processhub:rebatch-queue', ['--from' => 'logs-backlog', '--rate' => 0, '--chunk' => 200, '--limit' => 1])
            ->assertSuccessful();

        // Взято 200 задач — из :notify ушло столько же, чужая вернулась в хвост со своим уведомлением.
        $this->assertSame(52, (int) $redis->llen('queues:logs-backlog'));
        $this->assertSame(52, (int) $redis->llen('queues:logs-backlog:notify'));
        $this->assertSame(ForeignTestJob::class, json_decode($redis->lindex('queues:logs-backlog', -1), true)['displayName']);

        $this->artisan('processhub:rebatch-queue', ['--from' => 'logs-backlog', '--rate' => 0])->assertSuccessful();

        $this->assertSame($this->messages(0, 250), array_column(array_merge(...$this->batches('logs')), 'message'));
        $this->assertSame(1, (int) $redis->llen('queues:logs-backlog'));
        $this->assertSame(1, (int) $redis->llen('queues:logs-backlog:notify'));
    }

    public function test_concurrent_run_is_refused(): void
    {
        $this->backlog(10);
        Redis::connection()->set('queues:logs-backlog:rebatch-lock', 'other-run');

        $this->artisan('processhub:rebatch-queue', ['--from' => 'logs-backlog', '--rate' => 0])
            ->expectsOutputToContain('Another processhub:rebatch-queue run holds')
            ->assertFailed();

        $this->assertSame(10, (int) Redis::connection()->llen('queues:logs-backlog'));
        $this->assertSame(0, Queue::connection('redis')->size('logs'));
    }

    public function test_lock_is_released_after_the_run(): void
    {
        $this->backlog(10);

        $this->artisan('processhub:rebatch-queue', ['--from' => 'logs-backlog', '--rate' => 0])->assertSuccessful();

        $this->assertSame(0, (int) Redis::connection()->exists('queues:logs-backlog:rebatch-lock'));
    }

    public function test_schedule_continues_after_the_last_delayed_batch_at_a_new_rate(): void
    {
        $this->freezeTime();
        config()->set('processhub.batch_size', 10);
        $this->backlog(30);
        $this->artisan('processhub:rebatch-queue', ['--from' => 'logs-backlog', '--rate' => 1])->assertSuccessful();
        $this->assertSame([0, 60, 120], array_keys($this->schedule()));

        $this->backlog(20, 30);
        $this->artisan('processhub:rebatch-queue', ['--from' => 'logs-backlog', '--rate' => 6])->assertSuccessful();

        // 3 задачи при 6/мин «стекли» бы за 30 с, но последняя назначена на 120 с.
        $this->assertSame([0, 60, 120, 130, 140], array_keys($this->schedule()));
    }

    /** By default the backlog gets 90 % of the limit — the rest is left to live traffic. */
    public function test_default_rate_leaves_a_tenth_of_the_limit_to_live_traffic(): void
    {
        $this->freezeTime();
        config()->set('processhub.batch_size', 10);
        config()->set('processhub.rate_limit_per_minute', 50);
        $this->backlog(60);

        $this->artisan('processhub:rebatch-queue', ['--from' => 'logs-backlog'])
            ->expectsOutputToContain('Paced at 45 batches/min')
            ->assertSuccessful();

        $this->assertSame([0, 1, 2, 4, 5, 6], array_keys($this->schedule()));
    }

    public function test_max_ahead_leaves_the_rest_for_the_next_run(): void
    {
        $this->freezeTime();
        config()->set('processhub.batch_size', 10);
        config()->set('processhub.retry_window_sec', 3600);
        $this->backlog(100);
        $options = ['--from' => 'logs-backlog', '--rate' => 2, '--chunk' => 10, '--max-ahead' => 2];

        $this->artisan('processhub:rebatch-queue', $options)
            ->expectsOutputToContain('--max-ahead')
            ->assertSuccessful();

        $this->assertSame([0, 30, 60, 90, 120], array_keys($this->schedule()));
        $this->assertSame(50, (int) Redis::connection()->llen('queues:logs-backlog'));
        $this->assertSame(50, (int) Redis::connection()->llen('queues:logs-backlog:notify'));

        // Прошла минута, воркеры разобрали всё наступившее.
        $this->travel(60)->seconds();
        Redis::connection()->del('queues:logs');
        Redis::connection()->zremrangebyscore('queues:logs:delayed', '-inf', (string) (now()->getTimestamp() - 1));
        $this->artisan('processhub:rebatch-queue', $options)->assertSuccessful();

        $schedule = $this->schedule();
        // Было: 0, 30, 60 (сдвинулись на минуту); добавлено 90 и 120 — сразу после последней.
        $this->assertSame([0, 30, 60, 90, 120], array_keys($schedule));
        $this->assertSame('m-50', $schedule[90]['entries'][0]['message']);
        foreach ($schedule as $dueIn => $payload) {
            $this->assertGreaterThanOrEqual(now()->getTimestamp() + $dueIn + 3600, $payload['retryUntil']);
        }
        $this->assertSame(30, (int) Redis::connection()->llen('queues:logs-backlog'));
    }

    public function test_reads_jobs_serialized_by_v0_3(): void
    {
        $redis = Redis::connection();
        for ($i = 0; $i < 3; $i++) {
            $redis->rpush('queues:logs-backlog', $this->legacyPayload(['level' => 'ERROR', 'message' => 'old-' . $i]));
            $redis->rpush('queues:logs-backlog:notify', 1);
        }

        $this->artisan('processhub:rebatch-queue', ['--from' => 'logs-backlog', '--rate' => 0])
            ->expectsOutputToContain('Done: 3 jobs, 3 entries repacked into 1 batches')
            ->assertSuccessful();

        $this->assertSame(['old-0', 'old-1', 'old-2'], array_column($this->batches('logs')[0], 'message'));
        $this->assertSame(0, (int) $redis->exists('queues:logs-backlog:notify'));
    }

    private function backlog(int $count, int $from = 0): void
    {
        $queue = Queue::connection('redis');
        for ($i = $from; $i < $from + $count; $i++) {
            $queue->pushOn('logs-backlog', new SendLogBatchJob([['level' => 'ERROR', 'message' => 'm-' . $i]]));
        }
    }

    /**
     * @return array<int, string>
     */
    private function messages(int $from, int $to): array
    {
        return array_map(fn ($i) => 'm-' . $i, range($from, $to - 1));
    }

    /**
     * Payload as queued by 0.3: public $entries and $tries, maxTries 3, no retryUntil.
     *
     * @param  array<string, mixed>  $entry
     */
    private function legacyPayload(array $entry): string
    {
        $entries = serialize([$entry]);
        $command = sprintf(
            'O:%d:"%s":4:{s:7:"entries";%ss:5:"tries";i:3;s:7:"timeout";i:30;s:5:"queue";s:4:"logs";}',
            strlen(SendLogBatchJob::class),
            SendLogBatchJob::class,
            $entries,
        );

        return json_encode([
            'uuid' => (string) \Illuminate\Support\Str::uuid(),
            'displayName' => SendLogBatchJob::class,
            'job' => 'Illuminate\Queue\CallQueuedHandler@call',
            'maxTries' => 3,
            'maxExceptions' => null,
            'failOnTimeout' => false,
            'backoff' => null,
            'timeout' => 30,
            'retryUntil' => null,
            'data' => ['commandName' => SendLogBatchJob::class, 'command' => $command],
            'id' => 'legacy',
            'attempts' => 0,
        ]);
    }

    /**
     * Payloads on `logs` keyed by seconds until they are due (ready = 0).
     *
     * @return array<int, array<string, mixed>>
     */
    private function schedule(): array
    {
        $redis = Redis::connection();
        $scheduled = [];
        foreach ($redis->lrange('queues:logs', 0, -1) as $payload) {
            $scheduled[0] = $this->decode($payload);
        }
        foreach ($redis->zrange('queues:logs:delayed', 0, -1, ['withscores' => true]) as $payload => $at) {
            $scheduled[(int) $at - now()->getTimestamp()] = $this->decode($payload);
        }
        ksort($scheduled);

        return $scheduled;
    }

    /**
     * @return array<string, mixed>
     */
    private function decode(string $payload): array
    {
        $decoded = json_decode($payload, true);
        $decoded['entries'] = unserialize($decoded['data']['command'])->entries;

        return $decoded;
    }

    /**
     * @return array<int, array<int, array<string, mixed>>>
     */
    private function batches(string $queue): array
    {
        return array_map(
            fn (string $payload) => $this->decode($payload)['entries'],
            Redis::connection()->lrange('queues:' . $queue, 0, -1),
        );
    }
}
