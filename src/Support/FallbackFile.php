<?php

namespace ProcessHub\Logs\Support;

/**
 * Append-only JSON-lines file where undeliverable batches are parked until
 * `processhub:flush-fallback` re-ingests them.
 *
 * Line format: {"failed_at": ISO-8601, "reason": string, "entries": [...]}.
 */
final class FallbackFile
{
    /**
     * Never throws — called from failure paths (job `failed()`, queue push
     * errors, handler destructors) where the container may be half torn down.
     *
     * @param  array<int, array<string, mixed>>  $entries
     */
    public static function append(array $entries, string $reason): void
    {
        if ($entries === []) {
            return;
        }

        try {
            $path = config('processhub.fallback_path');
            if (! $path) {
                return;
            }
            @file_put_contents($path, self::line($entries, $reason), FILE_APPEND | LOCK_EX);
        } catch (\Throwable) {
            // Nowhere left to report to.
        }
    }

    /**
     * @param  array<int, array<string, mixed>>  $entries
     */
    public static function line(array $entries, string $reason): string
    {
        return json_encode([
            'failed_at' => date(DATE_ATOM),
            'reason' => $reason,
            'entries' => $entries,
        ], JSON_INVALID_UTF8_SUBSTITUTE | JSON_PARTIAL_OUTPUT_ON_ERROR) . "\n";
    }
}
