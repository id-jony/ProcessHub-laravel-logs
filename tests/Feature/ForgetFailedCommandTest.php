<?php

namespace ProcessHub\Logs\Tests\Feature;

use Illuminate\Contracts\Console\Kernel;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Queue\Failed\FailedJobProviderInterface;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use ProcessHub\Logs\Commands\ForgetFailedCommand;
use ProcessHub\Logs\Jobs\SendLogBatchJob;
use ProcessHub\Logs\Tests\TestCase;

class ForgetFailedCommandTest extends TestCase
{
    private const HORIZON_REPOSITORY = 'Laravel\Horizon\Contracts\JobRepository';

    protected function defineEnvironment($app): void
    {
        parent::defineEnvironment($app);
        $app['config']->set('database.default', 'testing');
        $app['config']->set('database.connections.testing', ['driver' => 'sqlite', 'database' => ':memory:', 'prefix' => '']);
        $app['config']->set('queue.failed', ['driver' => 'database-uuids', 'database' => 'testing', 'table' => 'failed_jobs']);
    }

    protected function setUp(): void
    {
        parent::setUp();
        // Провайдер пакета регистрирует команду отдельно; здесь — вручную.
        $this->app[Kernel::class]->registerCommand($this->app->make(ForgetFailedCommand::class));

        Schema::create('failed_jobs', function (Blueprint $table): void {
            $table->id();
            $table->string('uuid')->unique();
            $table->text('connection');
            $table->text('queue');
            $table->longText('payload');
            $table->longText('exception');
            $table->timestamp('failed_at')->useCurrent();
        });
    }

    public function test_deletes_only_send_log_batch_job_rows_in_chunks(): void
    {
        $failer = $this->app['queue.failer'];
        for ($i = 0; $i < 1500; $i++) {
            $failer->log('redis', 'logs', self::payload(SendLogBatchJob::class), new \RuntimeException('own'));
            if ($i % 500 === 0) {
                $failer->log('redis', 'default', self::payload('App\Jobs\ChargeCard'), new \RuntimeException('foreign'));
            }
        }
        // Чужая задача, в данных которой встречается имя нашего класса.
        $failer->log('redis', 'logs', self::payload('App\Jobs\Audit', SendLogBatchJob::class), new \RuntimeException(SendLogBatchJob::class));

        $this->artisan('processhub:forget-failed', ['--dry-run' => true])
            ->expectsOutputToContain('1504 records scanned, 1500 of ' . SendLogBatchJob::class . ', none deleted (dry run)')
            ->doesntExpectOutputToContain('Horizon')
            ->assertSuccessful();
        $this->assertSame(1504, DB::table('failed_jobs')->count());

        $this->artisan('processhub:forget-failed')
            ->expectsOutputToContain('1500 of ' . SendLogBatchJob::class . ', 1500 deleted')
            ->assertSuccessful();

        $left = DB::table('failed_jobs')->pluck('payload')->map(fn ($p) => json_decode($p, true)['displayName'])->all();
        $this->assertSame(['App\Jobs\ChargeCard', 'App\Jobs\ChargeCard', 'App\Jobs\ChargeCard', 'App\Jobs\Audit'], $left);
    }

    public function test_other_failed_job_providers_go_through_the_interface(): void
    {
        $failer = new InMemoryFailedJobProvider();
        foreach ([SendLogBatchJob::class, 'App\Jobs\Other', SendLogBatchJob::class] as $i => $class) {
            $failer->jobs["id-{$i}"] = (object) ['id' => "id-{$i}", 'payload' => self::payload($class)];
        }
        $this->app->instance('queue.failer', $failer);

        $this->artisan('processhub:forget-failed', ['--dry-run' => true])->assertSuccessful();
        $this->assertCount(3, $failer->jobs);

        $this->artisan('processhub:forget-failed')
            ->expectsOutputToContain('3 records scanned, 2 of ' . SendLogBatchJob::class . ', 2 deleted')
            ->assertSuccessful();
        $this->assertSame(['id-1'], array_keys($failer->jobs));
    }

    public function test_horizon_failed_list_is_paged_and_only_own_records_deleted(): void
    {
        $horizon = new FakeHorizonJobRepository();
        for ($i = 0; $i < 230; $i++) {
            $class = $i % 3 === 0 ? 'App\Jobs\Other' : SendLogBatchJob::class;
            // Хэш части записей уже истёк — Horizon такие не отдаёт.
            $horizon->add("job-{$i}", $class, expired: $i % 7 === 0);
        }
        $this->app->instance(self::HORIZON_REPOSITORY, $horizon);
        $own = count(array_filter($horizon->jobs, fn ($job) => $job !== null && $job->name === SendLogBatchJob::class));

        $this->artisan('processhub:forget-failed', ['--dry-run' => true])
            ->expectsOutputToContain("Horizon: 197 records scanned, {$own} of " . SendLogBatchJob::class . ', none deleted (dry run)')
            ->assertSuccessful();
        $this->assertCount(230, $horizon->ids);

        $this->artisan('processhub:forget-failed')->assertSuccessful();

        $left = array_map(fn ($id) => $horizon->jobs[$id]?->name ?? 'expired', $horizon->ids);
        $this->assertNotContains(SendLogBatchJob::class, $left);
        $this->assertCount(230 - $own, $left);
    }

    public function test_to_fallback_exports_entries_once_per_job_before_deleting(): void
    {
        $failer = $this->app['queue.failer'];
        $horizon = new FakeHorizonJobRepository();
        foreach (['a', 'b'] as $uuid) {
            $payload = self::jobPayload($uuid, [['level' => 'ERROR', 'message' => "entry-{$uuid}"]]);
            $failer->log('redis', 'logs', $payload, new \RuntimeException('x'));
            // Тот же сбой Horizon хранит у себя под тем же uuid.
            $horizon->add($uuid, SendLogBatchJob::class, payload: $payload);
        }
        $horizon->add('c', SendLogBatchJob::class, payload: self::jobPayload('c', [['level' => 'ERROR', 'message' => 'entry-c']]));
        $failer->log('redis', 'logs', self::payload(SendLogBatchJob::class), new \RuntimeException('unreadable'));
        $this->app->instance(self::HORIZON_REPOSITORY, $horizon);

        $this->artisan('processhub:forget-failed', ['--to-fallback' => true])
            ->expectsOutputToContain("Kept 1 records whose entries couldn't be read.")
            ->assertSuccessful();

        $lines = array_map(fn ($line) => json_decode($line, true), file(config('processhub.fallback_path'), FILE_IGNORE_NEW_LINES));
        $this->assertSame(['entry-a', 'entry-b', 'entry-c'], array_map(fn ($line) => $line['entries'][0]['message'], $lines));
        $this->assertSame('failed job a', $lines[0]['reason']);
        $this->assertSame(1, DB::table('failed_jobs')->count(), 'the unreadable record stays');
        $this->assertSame([], $horizon->ids);
    }

    public function test_to_fallback_stops_without_deleting_when_the_file_is_not_writable(): void
    {
        config()->set('processhub.fallback_path', sys_get_temp_dir() . '/processhub-missing-dir-' . uniqid() . '/fallback.log');
        $failer = $this->app['queue.failer'];
        foreach (['a', 'b'] as $uuid) {
            $failer->log('redis', 'logs', self::jobPayload($uuid, [['message' => $uuid]]), new \RuntimeException('x'));
        }

        $this->artisan('processhub:forget-failed', ['--to-fallback' => true])
            ->expectsOutputToContain('Failed to write')
            ->assertFailed();

        $this->assertSame(2, DB::table('failed_jobs')->count());
    }

    /**
     * @param  array<int, array<string, mixed>>  $entries
     */
    private static function jobPayload(string $uuid, array $entries): string
    {
        return json_encode([
            'uuid' => $uuid,
            'displayName' => SendLogBatchJob::class,
            'job' => 'Illuminate\Queue\CallQueuedHandler@call',
            'data' => ['commandName' => SendLogBatchJob::class, 'command' => serialize(new SendLogBatchJob($entries))],
        ]);
    }

    private static function payload(string $class, string $mention = ''): string
    {
        return json_encode([
            'uuid' => (string) \Illuminate\Support\Str::uuid(),
            'displayName' => $class,
            'job' => 'Illuminate\Queue\CallQueuedHandler@call',
            'data' => ['commandName' => $class, 'command' => 'O:1:"x":1:{s:4:"note";s:' . strlen($mention) . ':"' . $mention . '";}'],
        ]);
    }
}

final class InMemoryFailedJobProvider implements FailedJobProviderInterface
{
    /** @var array<string, object> */
    public array $jobs = [];

    public function log($connection, $queue, $payload, $exception)
    {
        return null;
    }

    /**
     * @return array<int, string>
     */
    public function ids($queue = null): array
    {
        return array_keys($this->jobs);
    }

    public function all()
    {
        return array_values($this->jobs);
    }

    public function find($id)
    {
        return $this->jobs[$id] ?? null;
    }

    public function forget($id)
    {
        $found = isset($this->jobs[$id]);
        unset($this->jobs[$id]);

        return $found;
    }

    public function flush($hours = null)
    {
        $this->jobs = [];
    }
}

/**
 * Horizon's RedisJobRepository semantics that matter here: a sorted list of
 * ids, pages of 50 by rank, records with an expired hash filtered out.
 */
final class FakeHorizonJobRepository
{
    /** @var array<int, string> */
    public array $ids = [];

    /** @var array<string, object|null> */
    public array $jobs = [];

    public function add(string $id, string $class, bool $expired = false, ?string $payload = null): void
    {
        $this->ids[] = $id;
        $this->jobs[$id] = $expired ? null : (object) [
            'id' => $id,
            'name' => $class,
            'status' => 'failed',
            'payload' => $payload ?? json_encode(['displayName' => $class]),
        ];
    }

    public function totalFailed(): int
    {
        return count($this->ids);
    }

    public function getFailed($afterIndex = null): Collection
    {
        $from = $afterIndex === null ? 0 : $afterIndex + 1;

        return collect(array_slice($this->ids, $from, 50))
            ->map(fn (string $id) => $this->jobs[$id])
            ->filter()
            ->values();
    }

    public function deleteFailed($id): int
    {
        $rank = array_search($id, $this->ids, true);
        if ($rank === false) {
            return 0;
        }
        array_splice($this->ids, $rank, 1);
        unset($this->jobs[$id]);

        return 1;
    }
}
