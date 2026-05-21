<?php

namespace ProcessHub\Logs\Tests\Feature\Payouts;

use GuzzleHttp\Client;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Response as GuzzleResponse;
use ProcessHub\Logs\Payouts\IngestClient;
use ProcessHub\Logs\Tests\TestCase;

/**
 * Test seam: subclass overrides the backoff sleep so we don't accumulate
 * real wall time across the retry-path tests.
 */
class TestableIngestClient extends IngestClient
{
    public int $sleeps = 0;
    protected function sleepMs(int $ms): void
    {
        $this->sleeps++;
    }
}

class IngestClientTest extends TestCase
{
    private function clientWith(array $queue): array
    {
        $mock = new MockHandler($queue);
        $stack = HandlerStack::create($mock);
        $guzzle = new Client(['handler' => $stack, 'http_errors' => false]);

        $client = new TestableIngestClient();
        $client->overrideClient($guzzle);
        return [$client, $mock];
    }

    private function sampleRow(): array
    {
        return [
            'gatewayPaymentId' => '362364',
            'paymentCreatedAt' => '2026-05-16T13:30:16Z',
            'rawStatus' => 'Проведён',
            'isCompleted' => true,
            'isFatalError' => false,
            'grossAmount' => '74000',
        ];
    }

    public function test_200_returns_accepted_and_watermark(): void
    {
        [$client] = $this->clientWith([
            new GuzzleResponse(200, [], json_encode([
                'ok' => true,
                'accepted' => 1,
                'skipped' => [],
                'watermark' => '362364',
            ])),
        ]);

        $result = $client->push([$this->sampleRow()], 'b-1');
        $this->assertTrue($result->ok);
        $this->assertSame(1, $result->accepted);
        $this->assertSame('362364', $result->watermark);
        $this->assertNull($result->error);
    }

    public function test_401_returns_unauthorized_without_retry(): void
    {
        [$client] = $this->clientWith([
            new GuzzleResponse(401, [], json_encode(['error' => 'invalid token'])),
        ]);
        $result = $client->push([$this->sampleRow()], 'b-1');
        $this->assertFalse($result->ok);
        $this->assertSame('UNAUTHORIZED', $result->error);
        $this->assertSame(0, $client->sleeps);
    }

    public function test_403_source_disabled_surfaces_code(): void
    {
        [$client] = $this->clientWith([
            new GuzzleResponse(403, [], json_encode([
                'error' => 'source disabled',
                'code' => 'SOURCE_DISABLED',
            ])),
        ]);
        $result = $client->push([$this->sampleRow()], 'b-1');
        $this->assertFalse($result->ok);
        $this->assertSame('SOURCE_DISABLED', $result->error);
    }

    public function test_404_source_not_configured(): void
    {
        [$client] = $this->clientWith([
            new GuzzleResponse(404, [], json_encode([
                'error' => 'no source',
                'code' => 'SOURCE_NOT_CONFIGURED',
            ])),
        ]);
        $result = $client->push([$this->sampleRow()], 'b-1');
        $this->assertFalse($result->ok);
        $this->assertSame('SOURCE_NOT_CONFIGURED', $result->error);
    }

    public function test_409_bad_config(): void
    {
        [$client] = $this->clientWith([
            new GuzzleResponse(409, [], json_encode([
                'error' => 'cycle in formula',
                'code' => 'BAD_CONFIG',
            ])),
        ]);
        $result = $client->push([$this->sampleRow()], 'b-1');
        $this->assertFalse($result->ok);
        $this->assertSame('BAD_CONFIG', $result->error);
    }

    public function test_413_payload_too_large(): void
    {
        [$client] = $this->clientWith([
            new GuzzleResponse(413, [], ''),
        ]);
        $result = $client->push([$this->sampleRow()], 'b-1');
        $this->assertFalse($result->ok);
        $this->assertSame('PAYLOAD_TOO_LARGE', $result->error);
    }

    public function test_429_retries_then_succeeds(): void
    {
        [$client] = $this->clientWith([
            new GuzzleResponse(429, ['Retry-After' => '1'], ''),
            new GuzzleResponse(429, ['Retry-After' => '1'], ''),
            new GuzzleResponse(200, [], json_encode([
                'ok' => true,
                'accepted' => 1,
                'skipped' => [],
                'watermark' => '362364',
            ])),
        ]);
        $result = $client->push([$this->sampleRow()], 'b-1');
        $this->assertTrue($result->ok);
        $this->assertSame(2, $client->sleeps);
    }

    public function test_5xx_retries_then_fails(): void
    {
        [$client] = $this->clientWith([
            new GuzzleResponse(500, [], ''),
            new GuzzleResponse(500, [], ''),
            new GuzzleResponse(500, [], ''),
        ]);
        $result = $client->push([$this->sampleRow()], 'b-1');
        $this->assertFalse($result->ok);
        $this->assertStringStartsWith('TRANSPORT_ERROR', $result->error ?? '');
        $this->assertSame(3, $client->sleeps);
    }

    public function test_empty_rows_is_silent_noop(): void
    {
        $client = new TestableIngestClient();
        $client->overrideClient(new Client(['handler' => HandlerStack::create(new MockHandler())]));
        $result = $client->push([], 'b-1');
        $this->assertTrue($result->ok);
        $this->assertSame(0, $result->accepted);
    }

    public function test_unconfigured_url_short_circuits(): void
    {
        config()->set('processhub.url', null);
        $client = new TestableIngestClient();
        $result = $client->push([$this->sampleRow()], 'b-1');
        $this->assertFalse($result->ok);
        $this->assertSame('NOT_CONFIGURED', $result->error);
    }

    public function test_payload_over_local_cap_short_circuits(): void
    {
        $row = $this->sampleRow();
        // Inflate one field beyond the cap (rawData accepted as array)
        $row['rawData'] = ['big' => str_repeat('x', 2_000_000)];
        [$client] = $this->clientWith([]); // no HTTP expected
        $result = $client->push([$row], 'b-1');
        $this->assertFalse($result->ok);
        $this->assertSame('PAYLOAD_TOO_LARGE', $result->error);
    }
}
