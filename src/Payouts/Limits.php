<?php

namespace ProcessHub\Logs\Payouts;

/**
 * Hard limits enforced both at compile-time (constants) and at runtime
 * (referenced from the client / command / job).
 *
 * The package mirrors the contract on the ProcessHub side
 * ({@see docs/16-payouts-module.md} → "Ограничения и rate-limit") with a
 * conservative safety margin: ProcessHub accepts 2 MiB, we cap at 1.8 MiB so
 * JSON-overhead spikes (long unicode names, escaped quotes) don't push us
 * past the server limit and bounce back as 413.
 *
 * All numbers are intentionally declared here — *not* in `config/processhub.php`
 * — because they are part of the protocol contract, not user preference.
 * Tweaking them requires a code change + version bump so the operator on the
 * ProcessHub side notices on the client-version dashboard.
 */
final class Limits
{
    /** Max rows per POST /api/ingest/payouts request. ProcessHub will 400 above this. */
    public const BATCH_SIZE = 1000;

    /** Soft-limit on request body. 10% headroom below ProcessHub's 2 MiB cap. */
    public const MAX_PAYLOAD_BYTES = 1_887_436; // 1.8 MiB

    /** Per-request HTTP timeout. Has to cover full-batch ingest + network jitter. */
    public const HTTP_TIMEOUT_SEC = 30;

    /** Retries inside a single push() for 429 / 5xx — beyond this, fail and wait for the next cron tick. */
    public const RETRY_MAX = 3;

    /** Exponential-backoff base. Actual wait is RETRY_BASE_MS * 2^attempt + small jitter. */
    public const RETRY_BASE_MS = 500;

    /**
     * `WithoutOverlapping` lock TTL for the single-row push job. Rapid status
     * changes within this window collapse to one POST (debounce). Tuned so a
     * burst of UPDATEs from a webhook doesn't queue dozens of identical jobs.
     */
    public const OBSERVER_DEDUP_TTL_SEC = 60;

    /**
     * Inter-page sleep during bootstrap. ProcessHub's per-token rate limit
     * (60 req/min) is enough back-pressure on its own; we don't insert
     * additional client-side delay.
     */
    public const BOOTSTRAP_PAGE_SLEEP_MS = 0;
}
