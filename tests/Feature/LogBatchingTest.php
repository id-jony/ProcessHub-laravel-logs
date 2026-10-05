<?php

namespace ProcessHub\Logs\Tests\Feature;

use Illuminate\Console\Events\CommandFinished;
use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Illuminate\Contracts\Queue\Job;
use Illuminate\Contracts\Queue\Queue;
use Illuminate\Queue\Events\JobProcessed;
use Illuminate\Queue\MaxAttemptsExceededException;
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

    public function test_buffer_is_flushed_after_each_queue_job(): void
    {
        QueueFacade::fake();
        $this->logMany(5);
        QueueFacade::assertNothingPushed();

        event(new JobProcessed('redis', Mockery::mock(Job::class)));

        $this->assertSame([5], $this->pushedSizes());
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

    /**
     * @return array<int, int>
     */
    private function pushedSizes(): array
    {
        return QueueFacade::pushed(SendLogBatchJob::class)->map(fn ($job) => count($job->entries))->values()->all();
    }
}
