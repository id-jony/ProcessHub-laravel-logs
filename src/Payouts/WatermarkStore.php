<?php

namespace ProcessHub\Logs\Payouts;

use Illuminate\Support\Facades\File;

/**
 * High-watermark persistence for payout ingest.
 *
 * Stored as a single JSON file under `storage/app/processhub-payouts-watermark.json`
 * with shape `{"watermark":"362364","updatedAt":"2026-05-21T12:00:00Z"}`.
 *
 * Why a file (and not Redis/DB):
 *   - mirrors the pattern of {@see Config\RemoteConfigClient} (one less
 *     moving part for ops to think about);
 *   - survives `php artisan cache:clear` — losing the watermark to a routine
 *     cache flush would silently re-bootstrap millions of rows;
 *   - no schema migration, no Redis required for projects that don't run one.
 *
 * Write is atomic via tmp + rename. Read is fault-tolerant: a missing or
 * malformed file resets to `null`, and the next successful push restores
 * the value — the server-side upsert is idempotent so re-sending the same
 * `gatewayPaymentId` is harmless.
 */
final class WatermarkStore
{
    private ?string $pathOverride = null;

    public function path(): string
    {
        return $this->pathOverride ?? storage_path('app/processhub-payouts-watermark.json');
    }

    /** Test-only escape hatch; production code resolves the storage path. */
    public function overridePath(string $path): void
    {
        $this->pathOverride = $path;
    }

    public function get(): ?string
    {
        $path = $this->path();
        if (! File::exists($path)) {
            return null;
        }
        try {
            $raw = File::get($path);
            $decoded = json_decode($raw, true);
            if (! is_array($decoded) || ! isset($decoded['watermark'])) {
                return null;
            }
            $value = $decoded['watermark'];
            // Watermark is `gatewayPaymentId`, always string on the wire.
            // Coerce int → string so legacy files (pre-0.2.0 if ever migrated)
            // don't break.
            if (is_int($value)) {
                return (string) $value;
            }
            if (! is_string($value)) {
                return null;
            }
            return $value === '' ? null : $value;
        } catch (\Throwable $e) {
            logger()->warning('processhub:payouts watermark read failed', [
                'error' => $e->getMessage(),
                'path' => $path,
            ]);
            return null;
        }
    }

    /**
     * Persist `$watermark` atomically. Passing `null` removes the file —
     * exposed for tests / "reset and re-bootstrap" admin actions.
     */
    public function set(?string $watermark): void
    {
        $path = $this->path();
        $dir = dirname($path);
        if (! File::isDirectory($dir)) {
            File::makeDirectory($dir, 0755, true);
        }

        if ($watermark === null) {
            if (File::exists($path)) {
                File::delete($path);
            }
            return;
        }

        $payload = json_encode(
            [
                'watermark' => $watermark,
                'updatedAt' => gmdate('Y-m-d\TH:i:s\Z'),
            ],
            JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE,
        );
        if ($payload === false) {
            logger()->warning('processhub:payouts watermark encode failed');
            return;
        }

        // Atomic write: a concurrent reader either sees the old file or the
        // fully-written new one, never a half-flushed buffer.
        $tmp = $path . '.tmp';
        File::put($tmp, $payload);
        @rename($tmp, $path);
    }
}
