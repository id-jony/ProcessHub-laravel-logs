<?php

namespace ProcessHub\Logs\Tests\Feature;

use Illuminate\Cache\ArrayStore;
use Illuminate\Contracts\Cache\Repository;
use Illuminate\Contracts\Cache\Store;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Redis;
use Illuminate\Support\Facades\Schema;
use Illuminate\Support\Sleep;
use Mockery;
use PHPUnit\Framework\Attributes\DataProvider;
use ProcessHub\Logs\Support\IngestThrottle;
use ProcessHub\Logs\Tests\TestCase;

class IngestThrottleTest extends TestCase
{
    private ?string $tmp = null;

    protected function setUp(): void
    {
        parent::setUp();
        config()->set('processhub.rate_limit_store', 'array');
        config()->set('processhub.rate_limit_per_minute', 50);
        $this->travelTo(Carbon::createFromTimestamp(intdiv(time(), 60) * 60 + 60));
    }

    protected function tearDown(): void
    {
        if ($this->tmp !== null) {
            exec('rm -rf ' . escapeshellarg($this->tmp));
        }
        parent::tearDown();
    }

    public function test_requests_are_spaced_with_a_small_burst(): void
    {
        $throttle = IngestThrottle::make();

        // 50/min → one per 1.2 s, burst of 5.
        $this->assertSame([0, 0, 0, 0, 0], array_map(fn () => $throttle->acquire(), range(1, 5)));
        $this->assertGreaterThan(0, $throttle->acquire());

        $this->travel(1200)->milliseconds();
        $this->assertSame(0, $throttle->acquire());
        $this->assertGreaterThan(0, $throttle->acquire());
    }

    public function test_any_60_second_window_gets_at_most_limit_plus_burst(): void
    {
        $throttle = IngestThrottle::make();
        $granted = [];

        // Ненасытный спрос 10 минут: шаг 100 мс, на каждом шаге — пока дают.
        for ($ms = 0; $ms < 600_000; $ms += 100) {
            while ($throttle->acquire() === 0) {
                $granted[] = $ms;
            }
            $this->travel(100)->milliseconds();
        }

        $max = 0;
        $from = 0;
        foreach ($granted as $i => $at) {
            while ($granted[$from] < $at - 60_000) {
                $from++;
            }
            $max = max($max, $i - $from + 1);
        }
        $this->assertSame(55, $max);
        $this->assertEqualsWithDelta(500, count($granted), 6);
    }

    public function test_refused_callers_get_successive_turns_and_are_served_on_return(): void
    {
        $throttle = IngestThrottle::make();
        while ($throttle->acquire() === 0) {
            // Выбираем burst.
        }
        $start = now()->getTimestamp();

        // Первый отказ выше уже занял ход через 2 с; дальше — по одному на 1.2 с.
        $waits = array_map(fn () => $throttle->acquire(), range(1, 9));
        $this->assertSame([3, 4, 5, 6, 8, 9, 10, 11, 12], $waits);

        foreach ($waits as $wait) {
            $this->travelTo(Carbon::createFromTimestamp($start + $wait));
            $this->assertSame(0, $throttle->acquire(), "turn at +{$wait}s");
        }
    }

    public function test_newcomer_lines_up_behind_callers_waiting_for_their_turn(): void
    {
        $throttle = IngestThrottle::make();
        while ($throttle->acquire() === 0) {
            // Выбираем burst; отказ занял ход через 2 с.
        }
        $start = now()->getTimestamp();
        $this->assertSame(3, $throttle->acquire());

        $this->travelTo(Carbon::createFromTimestamp($start + 2));
        $this->assertSame(2, $throttle->acquire(newcomer: true), 'newcomer must not take the turn of a waiting caller');
        $this->assertSame(0, $throttle->acquire(), 'the caller whose turn it is');
    }

    public function test_turns_left_by_callers_who_never_came_back_go_to_newcomers(): void
    {
        $throttle = IngestThrottle::make();
        while ($throttle->acquire() === 0) {
            // Выбираем burst.
        }
        for ($i = 0; $i < 10; $i++) {
            $throttle->acquire();
        }

        // Хозяева ходов не пришли: мощность простаивает дольше интервала.
        $this->travel(5)->seconds();

        $this->assertSame(0, $throttle->acquire(newcomer: true));
    }

    public function test_turn_beyond_max_wait_is_not_queued(): void
    {
        $throttle = IngestThrottle::make();
        while ($throttle->acquire() === 0) {
            // Выбираем burst; отказ занял ход через 2 с.
        }

        $this->assertSame(3, $throttle->acquire(maxWait: 2));
        // Ход не занят — следующему достаётся тот же.
        $this->assertSame(3, $throttle->acquire());
    }

    public function test_pause_stops_all_senders_and_is_never_shortened(): void
    {
        IngestThrottle::make()->pauseFor(30);
        IngestThrottle::make()->pauseFor(5);

        $this->assertSame(30, IngestThrottle::make()->acquire());
        // Отказанные во время паузы выходят после неё по очереди, а не толпой.
        $this->assertSame(32, IngestThrottle::make()->acquire());
        $this->travel(31)->seconds();
        $this->assertSame(0, IngestThrottle::make()->acquire());
    }

    public function test_budget_is_shared_between_instances(): void
    {
        config()->set('processhub.rate_limit_per_minute', 6);

        $this->assertSame(0, IngestThrottle::make()->acquire());
        $this->assertSame(10, IngestThrottle::make()->acquire());
    }

    /**
     * 300 queued jobs, each released until the turn it was given: every job
     * is attempted at most twice (the slotted limiter retried each one every
     * 10 s — thousands of idle pops).
     */
    public function test_backlog_is_served_in_at_most_two_attempts_per_job(): void
    {
        $throttle = IngestThrottle::make();
        $start = now()->getTimestamp();
        $due = [0 => array_fill(0, 300, true)];
        $attempts = 0;
        $sent = 0;

        for ($second = 0; $second < 600 && $sent < 300; $second++) {
            $this->travelTo(Carbon::createFromTimestamp($start + $second));
            foreach ($due[$second] ?? [] as $newcomer) {
                $attempts++;
                $wait = $throttle->acquire(3570, $newcomer);
                if ($wait === 0) {
                    $sent++;
                } else {
                    $due[$second + $wait][] = false;
                }
            }
            unset($due[$second]);
        }

        $this->assertSame(300, $sent);
        $this->assertLessThanOrEqual(600, $attempts);
    }

    public function test_zero_limit_disables_throttling(): void
    {
        config()->set('processhub.rate_limit_per_minute', 0);
        $throttle = IngestThrottle::make();

        for ($i = 0; $i < 100; $i++) {
            $this->assertSame(0, $throttle->acquire());
        }
    }

    public function test_unknown_store_does_not_break_delivery(): void
    {
        config()->set('processhub.rate_limit_store', 'no-such-store');
        $throttle = IngestThrottle::make();

        $throttle->pauseFor(30);
        $this->assertSame(0, $throttle->acquire());
    }

    public function test_failing_store_does_not_break_delivery(): void
    {
        $cache = Mockery::mock(Repository::class);
        $cache->allows('getStore')->andReturn(new ArrayStore());
        $cache->allows('get')->andThrow(new \RuntimeException('Connection refused'));
        $throttle = new IngestThrottle($cache, 50);

        $throttle->pauseFor(30);
        $this->assertSame(0, $throttle->acquire());
    }

    public function test_store_without_atomic_locks_does_not_break_delivery(): void
    {
        $throttle = new IngestThrottle(new \Illuminate\Cache\Repository(Mockery::mock(Store::class)), 50);

        $throttle->pauseFor(30);
        $this->assertSame(0, $throttle->acquire());
    }

    public function test_lock_held_by_a_stuck_process_refuses_instead_of_granting(): void
    {
        Sleep::fake(syncWithCarbon: true);
        Cache::store('array')->lock('processhub:ingest-throttle:lock', 60)->get();

        $this->assertSame(2, IngestThrottle::make()->acquire());
    }

    public function test_limiter_stepping_aside_is_reported_once_per_process(): void
    {
        (new \ReflectionProperty(IngestThrottle::class, 'warned'))->setValue(null, false);
        Log::spy();
        $throttle = new IngestThrottle(new \Illuminate\Cache\Repository(Mockery::mock(Store::class)), 50);

        $throttle->acquire();
        $throttle->pauseFor(30);
        IngestThrottle::make()->acquire();

        Log::shouldHaveReceived('warning')->once()->withArgs(fn (string $message) => str_contains($message, 'atomic locks'));
    }

    /**
     * The slotted limiter left a cache file per 10 s slot behind (8640 a day
     * on the file store, nothing collects expired files).
     */
    public function test_limiter_keeps_a_single_entry_in_the_store(): void
    {
        $dir = $this->tmpDir() . '/cache';
        config()->set('cache.stores.file.path', $dir);
        config()->set('processhub.rate_limit_store', 'file');

        for ($i = 0; $i < 360; $i++) {
            IngestThrottle::make()->acquire();
            $this->travel(10)->seconds();
        }

        $files = iterator_to_array(new \RecursiveIteratorIterator(new \RecursiveDirectoryIterator($dir, \FilesystemIterator::SKIP_DOTS)));
        $this->assertCount(1, $files);
    }

    /**
     * @return array<string, array{string}>
     */
    public static function sharedStores(): array
    {
        return ['file' => ['file'], 'database' => ['database'], 'redis' => ['redis']];
    }

    /**
     * Concurrent processes at one frozen moment get exactly the burst. The
     * slotted limiter's add + increment isn't atomic on the file store (12
     * of 8 granted) and fails open under SQLite contention.
     */
    #[DataProvider('sharedStores')]
    public function test_concurrent_processes_never_exceed_the_budget(string $store): void
    {
        if (! function_exists('pcntl_fork')) {
            $this->markTestSkipped('pcntl is required.');
        }
        $dir = $this->tmpDir();
        $this->useStore($store, $dir);
        $processes = 6;

        $children = [];
        for ($p = 0; $p < $processes; $p++) {
            $pid = pcntl_fork();
            if ($pid === 0) {
                $this->grantInChild("{$dir}/granted.{$p}");
            }
            $children[] = $pid;
        }
        foreach ($children as $pid) {
            pcntl_waitpid($pid, $status);
        }

        $granted = 0;
        for ($p = 0; $p < $processes; $p++) {
            $this->assertFileExists("{$dir}/granted.{$p}");
            $granted += (int) file_get_contents("{$dir}/granted.{$p}");
        }
        $this->assertSame(5, $granted);
    }

    private function grantInChild(string $result): never
    {
        $granted = 0;
        try {
            Redis::purge();
            DB::purge();
            Cache::forgetDriver(config('processhub.rate_limit_store'));
            for ($i = 0; $i < 20; $i++) {
                $granted += IngestThrottle::make()->acquire() === 0 ? 1 : 0;
                usleep(random_int(0, 300));
            }
            file_put_contents($result, (string) $granted);
        } finally {
            // Без teardown PHPUnit и деструкторов родителя.
            posix_kill(getmypid(), SIGKILL);
        }
        exit(1);
    }

    private function useStore(string $store, string $dir): void
    {
        config()->set('processhub.rate_limit_store', $store);
        if ($store === 'file') {
            config()->set('cache.stores.file.path', $dir . '/cache');
        }
        if ($store === 'database') {
            touch($dir . '/cache.sqlite');
            config()->set('database.connections.throttle', [
                'driver' => 'sqlite', 'database' => $dir . '/cache.sqlite', 'prefix' => '', 'busy_timeout' => 5000,
            ]);
            config()->set('database.default', 'throttle');
            config()->set('cache.stores.database', [
                'driver' => 'database', 'connection' => 'throttle', 'table' => 'cache',
                'lock_connection' => 'throttle', 'lock_table' => 'cache_locks',
            ]);
            Schema::connection('throttle')->create('cache', function ($table) {
                $table->string('key')->primary();
                $table->mediumText('value');
                $table->integer('expiration');
            });
            Schema::connection('throttle')->create('cache_locks', function ($table) {
                $table->string('key')->primary();
                $table->string('owner');
                $table->integer('expiration');
            });
            DB::purge('throttle');
        }
        if ($store === 'redis') {
            if (! env('PROCESSHUB_TEST_REDIS_HOST')) {
                $this->markTestSkipped('Set PROCESSHUB_TEST_REDIS_HOST to run Redis-backed tests.');
            }
            config()->set('cache.stores.redis', ['driver' => 'redis', 'connection' => 'default', 'lock_connection' => 'default']);
            Redis::connection()->flushdb();
            Redis::purge();
        }
    }

    private function tmpDir(): string
    {
        $this->tmp = sys_get_temp_dir() . '/processhub-throttle-' . uniqid();
        mkdir($this->tmp);

        return $this->tmp;
    }
}
