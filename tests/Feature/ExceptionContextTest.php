<?php

namespace ProcessHub\Logs\Tests\Feature;

use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Queue;
use ProcessHub\Logs\Jobs\SendLogBatchJob;
use ProcessHub\Logs\Tests\TestCase;

class ExceptionContextTest extends TestCase
{
    public function test_exception_in_context_is_shipped_with_stack_trace(): void
    {
        Queue::fake();
        $e = new \DomainException('User john@example.com not found');

        Log::channel('processhub')->error('lookup failed', [
            'exception' => $e,
            'request_id' => 'req-1',
            'password' => 'secret',
            'order_id' => 42,
        ]);
        $this->app->terminate();

        $entry = Queue::pushed(SendLogBatchJob::class)->first()->entries[0];
        $this->assertSame('exception', $entry['contextType']);
        $this->assertSame('req-1', $entry['requestId']);

        $context = $entry['context'];
        $this->assertSame(\DomainException::class, $context['class']);
        $this->assertSame('User [EMAIL] not found', $context['message']);
        $this->assertSame(__FILE__, $context['file']);
        $this->assertSame($e->getLine(), $context['line']);
        $this->assertNotEmpty($context['trace']);
        $this->assertStringStartsWith('#0 ', $context['trace'][0]);
        $this->assertSame('[REDACTED]', $context['password']);
        $this->assertSame(42, $context['order_id']);
        $this->assertArrayNotHasKey('exception', $context);
    }

    public function test_reported_exception_is_shipped_once_when_processhub_is_in_default_stack(): void
    {
        Queue::fake();
        config()->set('logging.channels.stack', ['driver' => 'stack', 'channels' => ['processhub']]);
        config()->set('logging.default', 'stack');

        report(new \RuntimeException('boom'));
        $this->app->terminate();

        $this->assertSame(['boom'], $this->shippedMessages());
    }

    public function test_reported_exception_is_shipped_when_processhub_is_not_in_default_stack(): void
    {
        Queue::fake();
        config()->set('logging.default', 'null');

        report(new \RuntimeException('boom'));
        $this->app->terminate();

        $this->assertSame(['boom'], $this->shippedMessages());
    }

    public function test_reported_exception_keeps_laravel_report_context_in_default_stack(): void
    {
        Queue::fake();
        config()->set('logging.channels.stack', ['driver' => 'stack', 'channels' => ['processhub']]);
        config()->set('logging.default', 'stack');

        report(new ContextualException('payment failed'));
        $this->app->terminate();

        $entries = $this->shippedEntries();
        $this->assertCount(1, $entries);
        $this->assertSame(42, $entries[0]['context']['order_id'] ?? null);
    }

    public function test_reported_exception_keeps_its_context_outside_default_stack(): void
    {
        Queue::fake();
        config()->set('logging.default', 'null');

        report(new ContextualException('payment failed'));
        $this->app->terminate();

        $entries = $this->shippedEntries();
        $this->assertCount(1, $entries);
        $this->assertSame(42, $entries[0]['context']['order_id'] ?? null);
    }

    public function test_processhub_reached_through_nested_stack_is_detected(): void
    {
        Queue::fake();
        config()->set('logging.channels.ph', config('logging.channels.processhub'));
        config()->set('logging.channels.inner', ['driver' => 'stack', 'channels' => 'ph']);
        config()->set('logging.channels.stack', ['driver' => 'stack', 'channels' => ['inner']]);
        config()->set('logging.default', 'stack');

        report(new \RuntimeException('boom'));
        $this->app->terminate();

        $this->assertSame(['boom'], $this->shippedMessages());
    }

    public function test_same_exception_logged_twice_is_shipped_twice(): void
    {
        Queue::fake();
        $e = new \RuntimeException('x');

        Log::channel('processhub')->warning('retrying', ['exception' => $e]);
        Log::channel('processhub')->error('gave up after retries', ['exception' => $e]);
        $this->app->terminate();

        $this->assertSame(['retrying', 'gave up after retries'], $this->shippedMessages());
    }

    public function test_distinct_exceptions_are_not_deduplicated(): void
    {
        Queue::fake();

        Log::channel('processhub')->error('first', ['exception' => new \RuntimeException('a')]);
        Log::channel('processhub')->error('second', ['exception' => new \RuntimeException('a')]);
        $this->app->terminate();

        $this->assertSame(['first', 'second'], $this->shippedMessages());
    }

    /**
     * @return array<int, string>
     */
    private function shippedMessages(): array
    {
        return array_column($this->shippedEntries(), 'message');
    }

    /**
     * @return array<int, array<string, mixed>>
     */
    private function shippedEntries(): array
    {
        return Queue::pushed(SendLogBatchJob::class)
            ->flatMap(fn (SendLogBatchJob $job) => $job->entries)
            ->values()
            ->all();
    }
}

class ContextualException extends \RuntimeException
{
    /**
     * @return array<string, mixed>
     */
    public function context(): array
    {
        return ['order_id' => 42];
    }
}
