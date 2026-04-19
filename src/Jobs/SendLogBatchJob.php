<?php

namespace ProcessHub\Logs\Jobs;

use GuzzleHttp\Client;
use GuzzleHttp\Exception\ClientException;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;

/**
 * Async delivery of a log batch to ProcessHub.
 *
 * Retry policy: 3 attempts with exponential backoff (Laravel default).
 * After all attempts are exhausted, `failed()` appends the batch to the
 * fallback file — `processhub:flush-fallback` reads it and re-enqueues
 * later. That way we never lose logs to transient network outages.
 *
 * 429 (rate limit) is treated as a retryable error with a delay honouring
 * the Retry-After header — the default Laravel retry wouldn't know to wait
 * the specific number of seconds ProcessHub is asking for.
 *
 * 401 (invalid token) is a DEV-config problem, not a transient error — we
 * fail immediately and let the fallback file capture the batch so an admin
 * can re-ingest after fixing config.
 */
class SendLogBatchJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    /** Laravel retry attempts. */
    public int $tries = 3;

    /** Max time in seconds a single attempt can run. */
    public int $timeout = 30;

    public function __construct(
        /** @var array<int, array<string, mixed>> */
        public array $entries,
    ) {}

    public function handle(): void
    {
        $url = config('processhub.url');
        $token = config('processhub.token');
        if (! $url || ! $token || empty($this->entries)) {
            return;
        }

        $timeoutSec = max(1, (int) config('processhub.timeout_ms', 5000) / 1000);

        $client = new Client([
            'base_uri' => rtrim($url, '/'),
            'timeout' => $timeoutSec,
            'http_errors' => true,
        ]);

        try {
            $client->post('/api/ingest/logs', [
                'headers' => [
                    'Authorization' => "Bearer {$token}",
                    'Content-Type' => 'application/json',
                ],
                'json' => ['logs' => $this->entries],
            ]);
        } catch (ClientException $e) {
            $status = $e->getResponse()?->getStatusCode();
            // 429 — honour Retry-After; release back to the queue with a delay.
            if ($status === 429) {
                $retryAfter = (int) ($e->getResponse()?->getHeaderLine('Retry-After') ?: 30);
                $this->release($retryAfter);
                return;
            }
            // 4xx (except 429) means config is wrong — don't waste retries.
            if ($status >= 400 && $status < 500) {
                $this->fail($e);
                return;
            }
            // 5xx / network — bubble up so Laravel retries with backoff.
            throw $e;
        }
    }

    /**
     * Final fallback — append entries to a file so a scheduled flush can
     * retry later when ProcessHub / network recovers.
     */
    public function failed(\Throwable $exception): void
    {
        $path = config('processhub.fallback_path');
        if (! $path) {
            return;
        }
        @file_put_contents(
            $path,
            json_encode([
                'failed_at' => now()->toIso8601String(),
                'reason' => $exception->getMessage(),
                'entries' => $this->entries,
            ]) . "\n",
            FILE_APPEND | LOCK_EX,
        );
    }
}
