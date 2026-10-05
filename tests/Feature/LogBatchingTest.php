<?php

namespace ProcessHub\Logs\Tests\Feature;

use Illuminate\Console\Events\CommandFinished;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Illuminate\Contracts\Queue\Job;
use Illuminate\Contracts\Queue\Queue;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Queue\Events\JobProcessed;
use Illuminate\Queue\Events\JobProcessing;
use Illuminate\Queue\Events\Looping;
use Illuminate\Queue\Events\WorkerStopping;
use Illuminate\Queue\MaxAttemptsExceededException;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Queue as QueueFacade;
use Mockery;
use ProcessHub\Logs\Exceptions\DeliveryFailedException;
use ProcessHub\Logs\Jobs\SendLogBatchJob;
use ProcessHub\Logs\Logging\LogBuffer;
use ProcessHub\Logs\Tests\TestCase;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\NullOutput;

class LogBatchingTest extends TestCase
{
    protected function tearDown(): void
    {
        // Иначе деструктор обработчика отправит остаток уже в следующий тест.
        app(LogBuffer::class)->flush();
        parent::tearDown();
    }

    public function test_records_are_packed_into_batches_of_batch_size(): void
    {
        QueueFacade::fake();

        $this->logMany(250);

        QueueFacade::assertPushed(SendLogBatchJob::class, 2);
        $this->assertSame(50, app(LogBuffer::class)->pending());

        $this->app->terminate();

        QueueFacade::assertPushed(SendLogBatchJob::class, 3);
        $sizes = QueueFacade::pushed(SendLogBatchJob::class)->map(fn ($job) => count($job->entries))->all();
        $this->assertSame([100, 100, 50], $sizes);
    }

    public function test_batch_size_config_is_honoured_and_capped_at_contract_limit(): void
    {
        QueueFacade::fake();

        config()->set('processhub.batch_size', 10);
        $this->logMany(25);
        $this->app->terminate();
        $this->assertSame([10, 10, 5], $this->pushedSizes());

        config()->set('processhub.batch_size', 500);
        $this->logMany(150);
        $this->app->terminate();
        $this->assertSame([10, 10, 5, 100, 50], $this->pushedSizes());
    }

    public function test_batches_respect_byte_budget(): void
    {
        QueueFacade::fake();
        config()->set('processhub.batch_max_bytes', 4096);

        $this->logMany(10, str_repeat('x', 1000));
        $this->app->terminate();

        $sizes = $this->pushedSizes();
        $this->assertSame(10, array_sum($sizes));
        $this->assertGreaterThan(1, count($sizes));
    }

    public function test_worker_ships_partial_batch_only_once_oldest_entry_is_stale(): void
    {
        QueueFacade::fake();
        Carbon::setTestNow('2026-10-05 12:00:00');
        config()->set('processhub.flush_interval_sec', 10);

        $this->logMany(5);
        event(new JobProcessed('redis', Mockery::mock(Job::class)));
        Carbon::setTestNow('2026-10-05 12:00:09');
        event(new Looping('redis', 'default'));
        QueueFacade::assertNothingPushed();

        Carbon::setTestNow('2026-10-05 12:00:10');
        event(new Looping('redis', 'default'));
        $this->assertSame([5], $this->pushedSizes());
    }

    public function test_stale_entry_is_shipped_on_next_push(): void
    {
        QueueFacade::fake();
        Carbon::setTestNow('2026-10-05 12:00:00');
        config()->set('processhub.flush_interval_sec', 10);

        $this->logMany(1);
        Carbon::setTestNow('2026-10-05 12:00:30');
        $this->logMany(1);

        $this->assertSame([2], $this->pushedSizes());
    }

    public function test_age_is_counted_from_the_oldest_entry_of_the_current_batch(): void
    {
        QueueFacade::fake();
        Carbon::setTestNow('2026-10-05 12:00:00');
        config()->set('processhub.batch_size', 2);
        config()->set('processhub.flush_interval_sec', 10);

        $this->logMany(3);
        $this->assertSame([2], $this->pushedSizes());

        Carbon::setTestNow('2026-10-05 12:00:09');
        $this->logMany(1);
        $this->assertSame([2, 2], $this->pushedSizes());
        event(new Looping('redis', 'default'));
        $this->assertSame([2, 2], $this->pushedSizes());
    }

    public function test_worker_stop_flushes_everything(): void
    {
        QueueFacade::fake();
        $this->logMany(5);

        event(new WorkerStopping(0));

        $this->assertSame([5], $this->pushedSizes());
    }

    public function test_buffer_is_muted_while_worker_handles_delivery_job(): void
    {
        QueueFacade::fake();
        $deliveryJob = $this->queueJob(SendLogBatchJob::class);
        $businessJob = $this->queueJob('App\\Jobs\\SendReceipt');

        event(new JobProcessing('redis', $deliveryJob));
        Log::channel('processhub')->error('during delivery');
        event(new JobFailed('redis', $deliveryJob, new \RuntimeException('boom')));
        // Worker::runJob() reports the exception after JobFailed.
        Log::channel('processhub')->error('worker reports boom');
        $this->assertSame(0, app(LogBuffer::class)->pending());

        event(new Looping('redis', 'default'));
        Log::channel('processhub')->error('next tick');
        $this->assertSame(1, app(LogBuffer::class)->pending());

        event(new JobProcessing('redis', $deliveryJob));
        event(new JobProcessing('redis', $businessJob));
        Log::channel('processhub')->error('business job');
        $this->assertSame(2, app(LogBuffer::class)->pending());
    }

    public function test_sync_delivery_job_does_not_leave_buffer_muted(): void
    {
        config()->set('queue.default', 'sync');
        config()->set('processhub.url', null);
        SendLogBatchJob::enqueue([['message' => 'delivered inline']]);
        config()->set('processhub.url', 'https://ph.test');

        QueueFacade::fake();
        Log::channel('processhub')->error('after');
        $this->assertSame(1, app(LogBuffer::class)->pending());
    }

    public function test_forked_child_does_not_ship_parent_entries(): void
    {
        if (! function_exists('pcntl_fork') || ! function_exists('posix_kill')) {
            $this->markTestSkipped('pcntl/posix are required.');
        }
        QueueFacade::fake();
        $this->logMany(3);
        $result = tempnam(sys_get_temp_dir(), 'processhub-fork-');

        $pid = pcntl_fork();
        if ($pid === 0) {
            Log::channel('processhub')->error('child');
            app(LogBuffer::class)->flush();
            file_put_contents($result, json_encode(
                QueueFacade::pushed(SendLogBatchJob::class)->map(fn ($job) => array_column($job->entries, 'message'))->values()->all(),
            ));
            // Без shutdown-функций и деструкторов PHPUnit.
            posix_kill(getmypid(), SIGKILL);
        }
        pcntl_waitpid($pid, $status);

        $this->assertSame([['child']], json_decode((string) file_get_contents($result), true));
        @unlink($result);
        $this->assertSame(3, app(LogBuffer::class)->pending());
    }

    public function test_delivery_failures_are_not_reported_on_every_attempt(): void
    {
        $handler = app(ExceptionHandler::class);

        $this->assertFalse($handler->shouldReport(DeliveryFailedException::status(503)));
        $this->assertTrue($handler->shouldReport(new \RuntimeException('business failure')));

        if (method_exists($handler, 'dontReportWhen')) {
            $deliveryJob = Mockery::mock(Job::class);
            $deliveryJob->allows('resolveName')->andReturn(SendLogBatchJob::class);
            $this->assertFalse($handler->shouldReport(MaxAttemptsExceededException::forJob($deliveryJob)));
        }
    }

    public function test_buffer_is_flushed_after_console_command(): void
    {
        QueueFacade::fake();
        $this->logMany(3);

        event(new CommandFinished('some:command', new ArrayInput([]), new NullOutput(), 0));

        $this->assertSame([3], $this->pushedSizes());
    }

    public function test_handler_close_flushes(): void
    {
        QueueFacade::fake();
        $this->logMany(4);

        Log::channel('processhub')->getLogger()->close();

        $this->assertSame([4], $this->pushedSizes());
    }

    public function test_failing_queue_push_goes_to_fallback_without_recursion(): void
    {
        $pushes = 0;
        $queue = Mockery::mock(Queue::class);
        $queue->allows('pushOn')->andReturnUsing(function () use (&$pushes) {
            $pushes++;
            // The queue backend logging its own failure must not re-enter.
            Log::channel('processhub')->error('redis is down');
            throw new \RuntimeException('Connection refused');
        });
        $factory = Mockery::mock(QueueFactory::class);
        $factory->allows('connection')->andReturn($queue);
        $this->app->instance('queue', $factory);

        config()->set('processhub.batch_size', 2);
        $this->logMany(3);
        $this->app->terminate();

        $this->assertSame(2, $pushes);
        $lines = file(config('processhub.fallback_path'), FILE_IGNORE_NEW_LINES);
        $this->assertCount(2, $lines);
        $this->assertStringContainsString('Queue push failed: Connection refused', $lines[0]);
        $entries = array_merge(...array_map(fn ($l) => json_decode($l, true)['entries'], $lines));
        $this->assertSame(['m-0', 'm-1', 'm-2'], array_column($entries, 'message'));
    }

    public function test_own_delivery_failures_are_not_logged(): void
    {
        QueueFacade::fake();
        $deliveryJob = Mockery::mock(Job::class);
        $deliveryJob->allows('resolveName')->andReturn(SendLogBatchJob::class);

        Log::channel('processhub')->error('x', ['exception' => DeliveryFailedException::status(503)]);
        Log::channel('processhub')->error('y', ['exception' => MaxAttemptsExceededException::forJob($deliveryJob)]);
        app(LogBuffer::class)->mute(fn () => Log::channel('processhub')->error('z'));

        $this->assertSame(0, app(LogBuffer::class)->pending());
    }

    private function logMany(int $count, string $suffix = ''): void
    {
        for ($i = 0; $i < $count; $i++) {
            Log::channel('processhub')->error('m-' . $i . $suffix);
        }
    }

    private function queueJob(string $class): Job
    {
        $job = Mockery::mock(Job::class);
        $job->allows('resolveName')->andReturn($class);
        $job->allows('payload')->andReturn(['displayName' => $class]);

        return $job;
    }

    /**
     * @return array<int, int>
     */
    private function pushedSizes(): array
    {
        return QueueFacade::pushed(SendLogBatchJob::class)->map(fn ($job) => count($job->entries))->values()->all();
    }
}
