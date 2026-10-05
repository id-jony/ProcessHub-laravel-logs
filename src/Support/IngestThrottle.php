<?php

namespace ProcessHub\Logs\Support;

use Illuminate\Contracts\Cache\LockProvider;
use Illuminate\Contracts\Cache\LockTimeoutException;
use Illuminate\Contracts\Cache\Repository;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Log;

/**
 * Shared (cross-process) limiter for POST /api/ingest/logs: ProcessHub allows
 * 60 requests per minute per token, and every worker / flush command of the
 * app spends the same budget.
 *
 * GCRA (token bucket): one request per `60 / rate_limit_per_minute` seconds
 * with a burst of ⌈limit / 10⌉, so any 60 s window gets at most limit + burst
 * requests (50/min → ≤ 55). A 429 sets a global pause honoured by all
 * processes.
 *
 * A refused caller gets its turn in a shared queue: callers refused one after
 * another are told to come back one interval apart instead of all at the same
 * moment, so a backlog of queued jobs is served in ~2 attempts per job rather
 * than retrying every few seconds. The turn is a hint, not a reservation —
 * nothing is consumed until the caller comes back and acquires; a newcomer
 * lines up behind those waiting instead of taking their turns (FIFO).
 *
 * The whole state is one cache key changed under `Cache::lock()`, so the
 * store must support atomic locks (redis, memcached, dynamodb, database,
 * file) and be shared by all senders: `array` is per-process, `file` only
 * covers one host. A store without locks or an unavailable store makes the
 * limiter step aside (no limit, a warning once per process) — delivery must
 * not depend on it.
 */
final class IngestThrottle
{
    private const KEY = 'processhub:ingest-throttle';

    /** Lock TTL: a crashed holder blocks the others for at most this long. */
    private const LOCK_SECONDS = 5;

    private const LOCK_WAIT_SECONDS = 3;

    /** A caller comes back up to a second after its turn (queue delays are whole seconds). */
    private const RETURN_SLACK_MS = 1000;

    private static bool $warned = false;

    public function __construct(
        private readonly ?Repository $cache,
        private readonly int $perMinute,
    ) {}

    public static function make(): self
    {
        $perMinute = (int) config('processhub.rate_limit_per_minute', 50);

        try {
            $cache = $perMinute > 0 ? Cache::store(config('processhub.rate_limit_store') ?: null) : null;
        } catch (\Throwable $e) {
            self::stepAside($e);
            $cache = null;
        }

        return new self($cache, $perMinute);
    }

    /**
     * Take the right to send one request.
     *
     * @param  int|null  $maxWait  longest wait the caller will accept; a turn
     *                             further away isn't queued (the caller gives up)
     * @param  bool  $newcomer  first attempt of the caller: while others wait
     *                          for their turns it lines up behind them instead
     *                          of taking the turn of the next one in line
     * @return int 0 — granted, send now; otherwise seconds until the caller's turn
     */
    public function acquire(?int $maxWait = null, bool $newcomer = false): int
    {
        if ($this->cache === null || $this->perMinute <= 0) {
            return 0;
        }

        try {
            return $this->locked(fn (): int => $this->take($maxWait, $newcomer));
        } catch (LockTimeoutException) {
            return $this->secondsUntil($this->interval());
        } catch (\Throwable $e) {
            self::stepAside($e);

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
            $this->locked(function () use ($seconds): void {
                $state = $this->state();
                $state['paused'] = max($state['paused'], $this->now() + $seconds * 1000);
                $this->save($state);
            });
        } catch (LockTimeoutException) {
            // Пауза этой задачи держится через её release().
        } catch (\Throwable $e) {
            // Без общего стора пауза держится только через release() задачи.
            self::stepAside($e);
        }
    }

    private function take(?int $maxWait, bool $newcomer): int
    {
        $now = $this->now();
        $state = $this->state();
        $tat = max($state['tat'], $now);
        $allowedAt = max($tat - $this->tolerance(), $state['paused']);

        if ($allowedAt <= $now && ! ($newcomer && $this->othersWaiting($state, $allowedAt, $now))) {
            $state['tat'] = $tat + $this->interval();
            $this->save($state);

            return 0;
        }

        $turn = max($allowedAt, $state['queue']);
        // Очередь задач работает с точностью до секунды: release($wait) делает
        // задачу доступной в начале секунды floor(now) + $wait — не раньше хода.
        $wait = max(1, intdiv($turn + 999, 1000) - intdiv($now, 1000));
        if ($maxWait === null || $wait <= $maxWait) {
            $state['queue'] = $turn + $this->interval();
            $this->save($state);
        }

        return $wait;
    }

    /**
     * Someone holds a turn that hasn't come (or came less than a second ago
     * — a released job shows up within the second after its turn), and the
     * free capacity is that turn's, not left over from an owner who never
     * came back (idle for over an interval — then anyone may take it).
     *
     * @param  array{tat: int, queue: int, paused: int}  $state
     */
    private function othersWaiting(array $state, int $allowedAt, int $now): bool
    {
        $slack = $this->interval() + self::RETURN_SLACK_MS;

        return $state['queue'] - $this->interval() + self::RETURN_SLACK_MS > $now
            && $allowedAt > $now - $slack;
    }

    private static function stepAside(\Throwable $e): void
    {
        if (self::$warned) {
            return;
        }
        self::$warned = true;

        try {
            Log::warning('ProcessHub ingest throttle is off: ' . $e->getMessage(), [
                'store' => config('processhub.rate_limit_store') ?: config('cache.default'),
            ]);
        } catch (\Throwable) {
            // Логировать некуда — лимитер просто не работает.
        }
    }

    /**
     * @template T
     *
     * @param  callable(): T  $callback
     * @return T
     */
    private function locked(callable $callback): mixed
    {
        $store = $this->cache?->getStore();
        if (! $store instanceof LockProvider) {
            throw new \LogicException('Cache store does not support atomic locks.');
        }

        return $store->lock(self::KEY . ':lock', self::LOCK_SECONDS)->block(self::LOCK_WAIT_SECONDS, $callback);
    }

    /**
     * Times in milliseconds: `tat` — theoretical arrival time of the next
     * request, `queue` — the next free turn for refused callers, `paused` —
     * end of the 429 pause.
     *
     * @return array{tat: int, queue: int, paused: int}
     */
    private function state(): array
    {
        $state = $this->cache?->get(self::KEY);

        return [
            'tat' => (int) ($state['tat'] ?? 0),
            'queue' => (int) ($state['queue'] ?? 0),
            'paused' => (int) ($state['paused'] ?? 0),
        ];
    }

    /**
     * @param  array{tat: int, queue: int, paused: int}  $state
     */
    private function save(array $state): void
    {
        // Ключ живёт, пока хоть одна отметка в будущем, и сам исчезает потом.
        $ttl = $this->secondsUntil(max($state) - $this->now()) + 60;
        $this->cache?->put(self::KEY, $state, $ttl);
    }

    /** Milliseconds between requests at the configured rate. */
    private function interval(): int
    {
        return intdiv(60_000 + $this->perMinute - 1, $this->perMinute);
    }

    /** How far ahead of its slot a request may go: a burst of ⌈limit / 10⌉. */
    private function tolerance(): int
    {
        return (intdiv($this->perMinute + 9, 10) - 1) * $this->interval();
    }

    private function now(): int
    {
        return (int) now()->format('Uv');
    }

    private function secondsUntil(int $milliseconds): int
    {
        return max(1, intdiv($milliseconds + 999, 1000));
    }
}
