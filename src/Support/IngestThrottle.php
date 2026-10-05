<?php

namespace ProcessHub\Logs\Support;

use Illuminate\Contracts\Cache\Repository;
use Illuminate\Support\Facades\Cache;

/**
 * Shared (cross-process) limiter for POST /api/ingest/logs: ProcessHub allows
 * 60 requests per minute per token, and every worker / flush command of the
 * app spends the same budget.
 *
 * State lives in a Laravel cache store (atomic add + increment — redis,
 * database, memcached, array). The minute is split into 6 slots of 10 s with
 * `rate_limit_per_minute` spread over them, so a burst at the edge of two
 * minutes can't double the rate: any 60 s window gets at most the limit plus
 * one slot's share (50/min → ≤ 59). A 429 sets a global pause honoured by
 * all processes.
 *
 * If the cache store is unavailable the limiter steps aside (no limit) —
 * delivery must not depend on it.
 */
final class IngestThrottle
{
    private const SLOT_SECONDS = 10;

    private const SLOTS_PER_MINUTE = 6;

    private const KEY = 'processhub:ingest-throttle:';

    public function __construct(
        private readonly ?Repository $cache,
        private readonly int $perMinute,
    ) {}

    public static function make(): self
    {
        $perMinute = (int) config('processhub.rate_limit_per_minute', 50);

        try {
            $cache = $perMinute > 0 ? Cache::store(config('processhub.rate_limit_store') ?: null) : null;
        } catch (\Throwable) {
            $cache = null;
        }

        return new self($cache, $perMinute);
    }

    /**
     * Take a slot for one request.
     *
     * @return int 0 — slot taken, send now; otherwise seconds to wait (no slot taken)
     */
    public function acquire(): int
    {
        if ($this->cache === null || $this->perMinute <= 0) {
            return 0;
        }

        try {
            $now = now()->getTimestamp();
            $pausedUntil = (int) $this->cache->get(self::KEY . 'paused-until', 0);
            if ($pausedUntil > $now) {
                return $pausedUntil - $now;
            }

            $slot = intdiv($now, self::SLOT_SECONDS);
            $key = self::KEY . 'slot:' . $slot;
            $this->cache->add($key, 0, self::SLOT_SECONDS * 2);
            if ((int) $this->cache->increment($key) <= $this->capacity($slot)) {
                return 0;
            }

            return $this->nextOpenSlot($slot) * self::SLOT_SECONDS - $now;
        } catch (\Throwable) {
            return 0;
        }
    }

    /**
     * Stop all senders for the given time (429 / Retry-After). Never
     * shortens a pause already in place.
     */
    public function pauseFor(int $seconds): void
    {
        if ($this->cache === null || $seconds <= 0) {
            return;
        }

        try {
            $until = now()->getTimestamp() + $seconds;
            if ($until > (int) $this->cache->get(self::KEY . 'paused-until', 0)) {
                $this->cache->put(self::KEY . 'paused-until', $until, $seconds);
            }
        } catch (\Throwable) {
            // Без общего стора пауза держится только через release() задачи.
        }
    }

    /** Requests allowed in the slot: the per-minute limit spread evenly over 6 slots. */
    private function capacity(int $slot): int
    {
        $i = $slot % self::SLOTS_PER_MINUTE;

        return intdiv(($i + 1) * $this->perMinute, self::SLOTS_PER_MINUTE)
            - intdiv($i * $this->perMinute, self::SLOTS_PER_MINUTE);
    }

    private function nextOpenSlot(int $slot): int
    {
        do {
            $slot++;
        } while ($this->capacity($slot) === 0);

        return $slot;
    }
}
