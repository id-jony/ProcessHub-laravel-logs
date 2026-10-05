<?php

namespace ProcessHub\Logs\Tests\Feature;

use Illuminate\Console\Events\CommandFinished;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Queue;
use Illuminate\Support\Facades\Schema;
use ProcessHub\Logs\Jobs\SendLogBatchJob;
use ProcessHub\Logs\Logging\LogBuffer;
use ProcessHub\Logs\Tests\TestCase;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\NullOutput;

/**
 * Real worker over the database queue (sqlite in memory): a worker running
 * many jobs that log one line each must ship full batches, not one per job.
 */
class WorkerBatchingTest extends TestCase
{
    protected function defineEnvironment($app): void
    {
        parent::defineEnvironment($app);

        $app['config']->set('database.default', 'jobs');
        $app['config']->set('database.connections.jobs', ['driver' => 'sqlite', 'database' => ':memory:', 'prefix' => '']);
        $app['config']->set('queue.connections.database', [
            'driver' => 'database', 'connection' => 'jobs', 'table' => 'jobs', 'queue' => 'default', 'retry_after' => 90,
        ]);
        $app['config']->set('processhub.connection', 'database');
        $app['config']->set('processhub.queue', 'logs');
        $app['config']->set('logging.default', 'processhub');
    }

    protected function setUp(): void
    {
        parent::setUp();

        Schema::create('jobs', function (Blueprint $table) {
            $table->id();
            $table->string('queue')->index();
            $table->longText('payload');
            $table->unsignedTinyInteger('attempts');
            $table->unsignedInteger('reserved_at')->nullable();
            $table->unsignedInteger('available_at');
            $table->unsignedInteger('created_at');
        });
    }

    public function test_one_log_line_per_job_is_shipped_in_full_batches(): void
    {
        Carbon::setTestNow('2026-10-05 12:00:00');
        for ($i = 0; $i < 200; $i++) {
            Queue::connection('database')->push(new LogOneLineJob($i));
        }

        $this->artisan('queue:work', [
            'connection' => 'database', '--queue' => 'default', '--stop-when-empty' => true,
        ])->assertSuccessful();

        $batches = $this->queuedBatches();
        $this->assertLessThanOrEqual(3, count($batches));
        $this->assertSame(
            array_map(fn ($i) => 'job-' . $i, range(0, 199)),
            array_column(array_merge(...$batches), 'message'),
        );
    }

    public function test_worker_drops_the_report_of_a_failed_delivery_job_and_unmutes_when_done(): void
    {
        // A payload that can't be decoded: the worker releases the job and
        // reports a generic exception, not one of isOwnFailure().
        DB::table('jobs')->insert([
            'queue' => 'logs',
            'payload' => json_encode([
                'uuid' => 'broken', 'displayName' => SendLogBatchJob::class, 'job' => 'Illuminate\\Queue\\CallQueuedHandler@call',
                'maxTries' => null, 'timeout' => null, 'data' => ['commandName' => SendLogBatchJob::class, 'command' => 'garbage'],
            ]),
            'attempts' => 0, 'reserved_at' => null, 'available_at' => time(), 'created_at' => time(),
        ]);

        $this->artisan('queue:work', [
            'connection' => 'database', '--queue' => 'logs', '--once' => true, '--tries' => 0,
        ])->assertSuccessful();

        $buffer = app(LogBuffer::class);
        $this->assertSame(0, $buffer->pending());
        $this->assertSame(1, DB::table('jobs')->where('queue', 'logs')->count());

        // Artisan в тестах не шлёт CommandFinished — то же, что в конце `php artisan queue:work --once`.
        event(new CommandFinished('queue:work', new ArrayInput([]), new NullOutput(), 0));
        $this->assertFalse($buffer->isMuted());
        Log::warning('after the worker');
        $this->assertSame(1, $buffer->pending());
    }

    /**
     * @return array<int, array<int, array<string, mixed>>>
     */
    private function queuedBatches(): array
    {
        return DB::table('jobs')->where('queue', 'logs')->orderBy('id')->pluck('payload')
            ->map(fn (string $payload) => unserialize(json_decode($payload, true)['data']['command']))
            ->each(fn ($job) => $this->assertInstanceOf(SendLogBatchJob::class, $job))
            ->map(fn (SendLogBatchJob $job) => $job->entries)
            ->values()
            ->all();
    }
}

class LogOneLineJob implements ShouldQueue
{
    public function __construct(public int $n) {}

    public function handle(): void
    {
        Log::warning('job-' . $this->n);
    }
}
