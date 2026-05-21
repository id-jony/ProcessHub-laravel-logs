<?php

namespace ProcessHub\Logs\Payouts;

use GuzzleHttp\Client;
use GuzzleHttp\ClientInterface;

/**
 * Thin HTTP client for `POST /api/ingest/payouts`.
 *
 * Responsibilities (and *only* these):
 *   - build the request (Bearer, JSON, batchId);
 *   - retry on 429 / 5xx with exponential backoff capped at `Limits::RETRY_MAX`;
 *   - convert response → {@see PushResult} the command can branch on;
 *   - never throw — Guzzle's `http_errors=false` plus a try/catch around the
 *     transport keeps the cron tick robust to network blips.
 *
 * The Guzzle client is injected through {@see overrideClient()} in tests
 * (MockHandler-based) and otherwise lazily built per-request from runtime
 * config (so remote-config edits to `url`/`token` are picked up without a
 * restart).
 *
 * Contract reference: docs/16-payouts-module.md → "Коды ответов".
 */
class IngestClient
{
    private ?ClientInterface $client = null;

    /** Test seam — bypass real network in PushPayoutsCommandTest / IngestClientTest. */
    public function overrideClient(?ClientInterface $client): void
    {
        $this->client = $client;
    }

    /**
     * @param list<array<string, mixed>> $rows
     */
    public function push(array $rows, string $batchId): PushResult
    {
        $url = config('processhub.url');
        $token = config('processhub.token');
        if (! $url || ! $token) {
            return new PushResult(false, 0, [], null, 'NOT_CONFIGURED');
        }
        if ($rows === []) {
            // No-op success: lets the command's "flush tail" branch behave
            // uniformly when nothing accumulated.
            return new PushResult(true, 0, [], null, null);
        }

        $body = json_encode(
            ['batchId' => $batchId, 'rows' => $rows],
            JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE,
        );
        if ($body === false) {
            return PushResult::transportFailure('JSON_ENCODE_FAILED');
        }
        if (strlen($body) > Limits::MAX_PAYLOAD_BYTES) {
            // Caller is responsible for splitting; surface a recognisable code
            // so the command can halve and retry without us guessing.
            return new PushResult(false, 0, [], null, 'PAYLOAD_TOO_LARGE');
        }

        $client = $this->client ?? $this->buildClient($url);

        for ($attempt = 0; $attempt < Limits::RETRY_MAX; $attempt++) {
            try {
                $response = $client->request('POST', '/api/ingest/payouts', [
                    'headers' => [
                        'Authorization' => "Bearer {$token}",
                        'Content-Type' => 'application/json',
                    ],
                    'body' => $body,
                ]);
            } catch (\Throwable $e) {
                // Curl / DNS / TLS failure — treat as 5xx-equivalent.
                logger()->warning('processhub:payouts transport error', [
                    'attempt' => $attempt + 1,
                    'error' => $e->getMessage(),
                ]);
                $this->sleepBackoff($attempt, null);
                continue;
            }

            $status = $response->getStatusCode();
            $rawBody = (string) $response->getBody();

            if ($status === 200) {
                return $this->parseSuccess($rawBody);
            }

            if ($status === 429) {
                $retryAfter = (int) $response->getHeaderLine('Retry-After');
                $this->sleepBackoff($attempt, $retryAfter > 0 ? $retryAfter : null);
                continue;
            }

            if ($status >= 500) {
                logger()->warning('processhub:payouts server error', [
                    'attempt' => $attempt + 1,
                    'status' => $status,
                    'body' => $this->truncate($rawBody),
                ]);
                $this->sleepBackoff($attempt, null);
                continue;
            }

            // 4xx (except 429) — client/config error, no point retrying.
            return $this->parseClientError($status, $rawBody);
        }

        // Retries exhausted — leave watermark intact so the next cron tick
        // re-tries from the same point.
        return PushResult::transportFailure('RETRIES_EXHAUSTED');
    }

    private function buildClient(string $url): Client
    {
        return new Client([
            'base_uri' => rtrim($url, '/'),
            'timeout' => Limits::HTTP_TIMEOUT_SEC,
            'http_errors' => false,
        ]);
    }

    private function parseSuccess(string $rawBody): PushResult
    {
        $decoded = json_decode($rawBody, true);
        if (! is_array($decoded)) {
            return PushResult::transportFailure('BAD_RESPONSE_JSON');
        }
        $skipped = [];
        if (isset($decoded['skipped']) && is_array($decoded['skipped'])) {
            foreach ($decoded['skipped'] as $row) {
                if (is_array($row) && isset($row['gatewayPaymentId'], $row['reason'])) {
                    $skipped[] = [
                        'gatewayPaymentId' => (string) $row['gatewayPaymentId'],
                        'reason' => (string) $row['reason'],
                    ];
                }
            }
        }
        $watermark = isset($decoded['watermark']) && is_scalar($decoded['watermark'])
            ? (string) $decoded['watermark']
            : null;
        $accepted = isset($decoded['accepted']) && is_int($decoded['accepted'])
            ? $decoded['accepted']
            : 0;

        return new PushResult(true, $accepted, $skipped, $watermark, null);
    }

    private function parseClientError(int $status, string $rawBody): PushResult
    {
        // ProcessHub returns `{ "error": "...", "code": "SOURCE_DISABLED" }`
        // for the structured 403/404/409 cases. The numeric status alone
        // isn't enough to disambiguate (e.g. 403 = SOURCE_DISABLED vs token
        // RBAC denial), so we surface both for the logger.
        $code = null;
        $message = null;
        $decoded = json_decode($rawBody, true);
        if (is_array($decoded)) {
            $code = isset($decoded['code']) && is_string($decoded['code']) ? $decoded['code'] : null;
            $message = isset($decoded['error']) && is_string($decoded['error']) ? $decoded['error'] : null;
        }

        $errorTag = match (true) {
            $status === 400 => 'BAD_REQUEST',
            $status === 401 => 'UNAUTHORIZED',
            $status === 403 => $code ?? 'FORBIDDEN',
            $status === 404 => $code ?? 'NOT_FOUND',
            $status === 409 => $code ?? 'CONFLICT',
            $status === 413 => 'PAYLOAD_TOO_LARGE',
            default => "HTTP_{$status}",
        };

        logger()->error('processhub:payouts client error', [
            'status' => $status,
            'code' => $errorTag,
            'message' => $message,
            'body' => $this->truncate($rawBody),
        ]);

        return new PushResult(false, 0, [], null, $errorTag);
    }

    /**
     * Sleep `Retry-After` if given, else exponential backoff with jitter.
     * Jitter avoids thundering-herd when N workers hit the same 429 wall.
     */
    private function sleepBackoff(int $attempt, ?int $retryAfterSeconds): void
    {
        if ($retryAfterSeconds !== null) {
            // Clamp to a sane ceiling so a misconfigured server can't park a
            // worker for hours; cron will re-tick anyway.
            $seconds = min($retryAfterSeconds, 60);
            $this->sleepMs($seconds * 1000);
            return;
        }
        $base = Limits::RETRY_BASE_MS * (2 ** $attempt);
        $jitter = random_int(0, (int) ($base / 2));
        $this->sleepMs($base + $jitter);
    }

    /**
     * Indirection so tests can monkey-patch via subclass without spending
     * real wall time. Default uses usleep.
     */
    protected function sleepMs(int $ms): void
    {
        if ($ms <= 0) {
            return;
        }
        usleep($ms * 1000);
    }

    private function truncate(string $s, int $max = 500): string
    {
        return strlen($s) > $max ? substr($s, 0, $max) . '…' : $s;
    }
}
