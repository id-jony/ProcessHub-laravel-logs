<?php

namespace ProcessHub\Logs\Support;

use Illuminate\Http\Client\Response;
use Illuminate\Support\Facades\Http;

/**
 * Thin wrapper over `POST /api/ingest/logs` shared by SendLogBatchJob and
 * `processhub:flush-fallback`. Never throws on HTTP status — callers decide
 * what 429 / 4xx / 5xx mean for them. Network errors surface as
 * `Illuminate\Http\Client\ConnectionException`.
 */
final class LogIngest
{
    /** Fallback delay when 429 comes without a usable Retry-After. */
    public const DEFAULT_RETRY_AFTER = 30;

    /**
     * @param  array<int, array<string, mixed>>  $entries
     */
    public static function post(string $url, string $token, array $entries): Response
    {
        $timeoutSec = (int) max(1, ceil((int) config('processhub.timeout_ms', 5000) / 1000));

        return Http::baseUrl(rtrim($url, '/'))
            ->withToken($token)
            ->acceptJson()
            ->timeout($timeoutSec)
            ->post('/api/ingest/logs', ['logs' => $entries]);
    }

    /**
     * Seconds to wait according to `Retry-After` (delta-seconds or HTTP-date),
     * clamped to [0, 3600].
     */
    public static function retryAfter(Response $response): int
    {
        $header = trim($response->header('Retry-After'));

        if ($header === '') {
            return self::DEFAULT_RETRY_AFTER;
        }
        if (ctype_digit($header)) {
            return min(3600, (int) $header);
        }

        $at = strtotime($header);

        return $at === false
            ? self::DEFAULT_RETRY_AFTER
            : max(0, min(3600, $at - time()));
    }
}
