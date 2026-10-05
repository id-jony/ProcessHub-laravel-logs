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
}
