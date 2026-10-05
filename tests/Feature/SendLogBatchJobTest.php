<?php

namespace ProcessHub\Logs\Tests\Feature;

use GuzzleHttp\Exception\ClientException;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Response as GuzzleResponse;
use GuzzleHttp\Psr7\Request as GuzzleRequest;
use Illuminate\Contracts\Queue\Job;
use Illuminate\Http\Client\ConnectionException;
use Illuminate\Support\Facades\Http;
use Mockery;
use ProcessHub\Logs\Exceptions\DeliveryFailedException;
use ProcessHub\Logs\Jobs\SendLogBatchJob;
use ProcessHub\Logs\Tests\TestCase;

/**
 * We can't easily intercept the Guzzle client inside `SendLogBatchJob::handle`
 * without DI, so these tests focus on the error-handling branches that don't
 * require a live network: fallback file on failed(), config guards, payload
 * shape. The happy-path HTTP round-trip is covered by the integration docs
 * (run `php artisan processhub:test` against a real tenant).
 */
class SendLogBatchJobTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        // Make sure fallback writes go somewhere isolated.
        config()->set(
            'processhub.fallback_path',
            sys_get_temp_dir() . '/processhub-fallback-test-' . uniqid() . '.log',
        );
    }

    protected function tearDown(): void
    {
        $path = config('processhub.fallback_path');
        if ($path && is_file($path)) @unlink($path);
        parent::tearDown();
    }

    public function test_silent_noop_when_url_or_token_is_missing(): void
    {
        config()->set('processhub.url', null);
        $job = new SendLogBatchJob([
            ['level' => 'ERROR', 'message' => 'x'],
        ]);
        // Should not throw; should not require network.
        $job->handle();
        $this->assertTrue(true);
    }

    public function test_silent_noop_on_empty_entries(): void
    {
        $job = new SendLogBatchJob([]);
        $job->handle(); // must not throw
        $this->assertTrue(true);
    }

    public function test_failed_appends_to_fallback_file(): void
    {
        $entries = [[
            'level' => 'ERROR',
            'message' => 'permanent-failure',
            'host' => 'unit-test',
        ]];

        $job = new SendLogBatchJob($entries);
        $job->failed(new \RuntimeException('simulated exhaustion'));

        $path = config('processhub.fallback_path');
        $this->assertFileExists($path);
        $content = file_get_contents($path);
        $this->assertStringContainsString('permanent-failure', $content);
        $this->assertStringContainsString('simulated exhaustion', $content);

        // Each line is valid JSON — flush command relies on this.
        $decoded = json_decode(trim($content), true);
        $this->assertIsArray($decoded);
        $this->assertSame($entries, $decoded['entries']);
        $this->assertArrayHasKey('failed_at', $decoded);
    }

    public function test_fallback_file_is_append_safe_across_calls(): void
    {
        $first = new SendLogBatchJob([['level' => 'ERROR', 'message' => 'one']]);
        $first->failed(new \RuntimeException('err-1'));

        $second = new SendLogBatchJob([['level' => 'WARN', 'message' => 'two']]);
        $second->failed(new \RuntimeException('err-2'));

        $lines = file(config('processhub.fallback_path'), FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
        $this->assertCount(2, $lines);
    }

    /**
     * Build a mock 429 ClientException carrying a Retry-After header so the
     * `release()` branch can be exercised in isolation. This test just verifies
     * the shape of the header — the `release()` call itself needs a real queue.
     */
    public function test_client_exception_factory_for_retry_after(): void
    {
        $request = new GuzzleRequest('POST', '/api/ingest/logs');
        $response = new GuzzleResponse(429, ['Retry-After' => '17']);
        $ex = new ClientException('rate limited', $request, $response);

        $this->assertSame(429, $ex->getResponse()->getStatusCode());
        $this->assertSame('17', $ex->getResponse()->getHeaderLine('Retry-After'));
    }

    public function test_guzzle_mock_handler_yields_configured_status(): void
    {
        // Sanity: tests infrastructure can build a MockHandler if future
        // maintainers want to swap SendLogBatchJob to inject a client.
        $mock = new MockHandler([
            new GuzzleResponse(200, [], '{"accepted":1}'),
        ]);
        $stack = HandlerStack::create($mock);
        $this->assertNotNull($stack);
    }

    public function test_successful_delivery_posts_whole_batch(): void
    {
        Http::fake(['ph.test/*' => Http::response(['accepted' => 2])]);
        $job = $this->jobWithQueueMock([['message' => 'a'], ['message' => 'b']], function ($queueJob) {
            $queueJob->shouldNotReceive('release');
            $queueJob->shouldNotReceive('fail');
        });

        $job->handle();

        Http::assertSent(fn ($request) => $request->url() === 'https://ph.test/api/ingest/logs'
            && count($request['logs']) === 2
            && $request->hasHeader('Authorization'));
    }

    public function test_rate_limit_releases_with_retry_after_regardless_of_attempts(): void
    {
        Http::fake(['ph.test/*' => Http::response('', 429, ['Retry-After' => '17'])]);
        $job = $this->jobWithQueueMock([['message' => 'a']], function ($queueJob) {
            $queueJob->allows('attempts')->andReturn(50);
            $queueJob->shouldReceive('release')->once()->with(17);
            $queueJob->shouldNotReceive('fail');
        });

        $job->handle();
    }

    public function test_retry_policy_is_time_based(): void
    {
        config()->set('processhub.retry_window_sec', 3600);
        $job = new SendLogBatchJob([]);

        $this->assertFalse(property_exists($job, 'tries'), 'attempt cap would let 429 exhaust the job');
        $this->assertEqualsWithDelta(now()->addHour()->getTimestamp(), $job->retryUntil()->getTimestamp(), 5);
        $this->assertSame([10, 30, 120, 300], $job->backoff());
        $this->assertTrue($job->failOnTimeout);
    }

    public function test_server_error_throws_for_backoff_retry(): void
    {
        Http::fake(['ph.test/*' => Http::response('', 503)]);
        $job = $this->jobWithQueueMock([['message' => 'a']], function ($queueJob) {
            $queueJob->shouldNotReceive('release');
        });

        $this->expectException(DeliveryFailedException::class);
        $job->handle();
    }

    public function test_network_error_throws_delivery_exception(): void
    {
        Http::fake(fn () => throw new ConnectionException('Connection refused'));
        $job = new SendLogBatchJob([['message' => 'a']]);

        $this->expectException(DeliveryFailedException::class);
        $job->handle();
    }

    public function test_client_error_fails_immediately(): void
    {
        Http::fake(['ph.test/*' => Http::response(['error' => 'bad token'], 401)]);
        $job = $this->jobWithQueueMock([['message' => 'a']], function ($queueJob) {
            $queueJob->shouldReceive('fail')->once()->with(Mockery::type(DeliveryFailedException::class));
            $queueJob->shouldNotReceive('release');
        });

        $job->handle();
    }

    /**
     * @param  array<int, array<string, mixed>>  $entries
     */
    private function jobWithQueueMock(array $entries, callable $expectations): SendLogBatchJob
    {
        $queueJob = Mockery::mock(Job::class);
        $expectations($queueJob);
        $job = new SendLogBatchJob($entries);
        $job->setJob($queueJob);

        return $job;
    }
}
