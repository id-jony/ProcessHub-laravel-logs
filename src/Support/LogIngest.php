<?php

namespace ProcessHub\Logs\Support;

use GuzzleHttp\Exception\TransferException;
use Illuminate\Http\Client\ConnectionException;
use Illuminate\Http\Client\Response;
use Illuminate\Support\Facades\Http;

/**
 * Thin wrapper over `POST /api/ingest/logs` shared by SendLogBatchJob and
 * `processhub:flush-fallback`. Never throws on HTTP status — callers decide
 * what 429 / 4xx / 5xx mean for them. Every transport failure (connect,
 * reset, TLS, HTTP/2 …) surfaces as
 * `Illuminate\Http\Client\ConnectionException` on every Laravel version.
 */
final class LogIngest
{
    /** Fallback delay when 429 comes without a usable Retry-After. */
    public const DEFAULT_RETRY_AFTER = 30;

    /** Bounds for Retry-After: never spin, never park a batch for hours. */
    public const MIN_RETRY_AFTER = 1;

    public const MAX_RETRY_AFTER = 600;

    /**
     * @param  array<int, array<string, mixed>>  $entries
     *
     * @throws ConnectionException
     */
    public static function post(string $url, string $token, array $entries): Response
    {
        $timeoutSec = (int) max(1, ceil((int) config('processhub.timeout_ms', 5000) / 1000));

        try {
            return Http::baseUrl(rtrim($url, '/'))
                ->withToken($token)
                ->acceptJson()
                ->timeout($timeoutSec)
                ->withBody(self::encode($entries), 'application/json')
                ->post('/api/ingest/logs');
        } catch (TransferException $e) {
            // Laravel < 12 оборачивает в ConnectionException только ConnectException;
            // обрыв соединения, TLS, HTTP/2 приходят сырым RequestException.
            throw new ConnectionException($e->getMessage(), 0, $e);
        }
    }

    /**
     * Request body. One bad value (INF/NAN, broken UTF-8) is substituted
     * instead of failing the whole batch.
     *
     * @param  array<int, array<string, mixed>>  $entries
     *
     * @throws \JsonException
     */
    public static function encode(array $entries): string
    {
        return json_encode(['logs' => $entries], JSON_INVALID_UTF8_SUBSTITUTE | JSON_PARTIAL_OUTPUT_ON_ERROR)
            ?: throw new \JsonException(json_last_error_msg());
    }

    /**
     * Seconds to wait according to `Retry-After`, clamped to
     * [MIN_RETRY_AFTER, MAX_RETRY_AFTER]. Accepted: whole seconds (a negative
     * value means "now") or an IMF-fixdate (`Sun, 06 Nov 1994 08:49:37 GMT`,
     * RFC 7231 §7.1.1.1); anything else → DEFAULT_RETRY_AFTER.
     */
    public static function retryAfter(Response $response): int
    {
        $header = trim($response->header('Retry-After'));

        if (preg_match('/^-?\d+$/', $header)) {
            $seconds = (int) $header;
        } elseif (($at = self::httpDate($header)) !== null) {
            $seconds = $at->getTimestamp() - now()->getTimestamp();
        } else {
            $seconds = self::DEFAULT_RETRY_AFTER;
        }

        return max(self::MIN_RETRY_AFTER, min(self::MAX_RETRY_AFTER, $seconds));
    }

    private static function httpDate(string $value): ?\DateTimeImmutable
    {
        $date = \DateTimeImmutable::createFromFormat(DATE_RFC7231, $value, new \DateTimeZone('UTC'));

        // createFromFormat переносит переполнение (32 Oct → 1 Nov) и подгоняет
        // дату под день недели — строгий разбор: обратно та же строка.
        return $date !== false && $date->format(DATE_RFC7231) === $value ? $date : null;
    }
}
