<?php

namespace ProcessHub\Logs\Logging;

use Illuminate\Cache\RateLimiting\Unlimited;
use Illuminate\Contracts\Container\Container;
use Illuminate\Support\Carbon;
use ProcessHub\Logs\Jobs\SendLogBatchJob;
use ProcessHub\Logs\Support\BatchBuilder;
use ProcessHub\Logs\Support\FallbackFile;
use Symfony\Component\ErrorHandler\Error\FatalError;

/**
 * In-memory buffer between ProcessHubHandler and the queue: one
 * SendLogBatchJob per full batch instead of one job per log record.
 *
 * Bound as a container singleton. When a batch is shipped:
 *   - it fills up (`batch_size` / `batch_max_bytes`);
 *   - its oldest entry is older than `flush_interval_sec` — checked on every
 *     push and, in queue workers, after each job and on every worker loop
 *     tick (flushIfStale()), so a worker running thousands of one-log jobs
 *     still ships full batches;
 *   - the unit of work ends (end of HTTP request, console command, worker
 *     stop, PHP shutdown — including fatal errors): flush();
 *   - right away in long-running processes with no point to flush at
 *     (setBuffered(false)).
 *
 * Recursion guard: while a batch is being pushed or a SendLogBatchJob is
 * being processed the buffer is muted — records produced by the delivery
 * machinery itself (the queue backend erroring, the worker reporting the
 * job's exception) are dropped instead of feeding back into the channel. If
 * the push fails, the batch goes to the fallback file; the application never
 * sees the exception.
 *
 * Entries belong to the application that created the buffer: once it has
 * been flushed (end of a test) they are dropped instead of being pushed
 * into whatever container is current.
 */
class LogBuffer
{
    /** Memory allowed past an exhausted memory_limit to ship the buffer. */
    private const SHUTDOWN_EXTRA_MEMORY = 16 * 1024 * 1024;

    /**
     * Freed first thing when a fatal error is handled: room to ship the
     * buffer when memory_limit can't be raised (php_admin_value).
     */
    private const RESERVED_MEMORY = 256 * 1024;

    private static ?string $reservedMemory = null;

    private ?BatchBuilder $batch = null;

    /** Unix time of the oldest buffered entry. */
    private ?int $oldestAt = null;

    private bool $flushing = false;

    private int $muted = 0;

    private bool $inDeliveryJob = false;

    private bool $buffered = true;

    private bool $fatalErrorQueued = false;

    /** Process that owns the buffered entries (see claimForCurrentProcess()). */
    private int $pid;

    /** @var \WeakMap<\Throwable, true> exceptions thrown by SendLogBatchJob runs */
    private \WeakMap $deliveryExceptions;

    /** @var \WeakReference<Container>|null */
    private ?\WeakReference $app;

    public function __construct(?Container $app = null)
    {
        $this->pid = (int) getmypid();
        $this->deliveryExceptions = new \WeakMap();
        $this->app = $app !== null ? \WeakReference::create($app) : null;
        self::prepareForFatalError();
    }

    /**
     * @param  array<string, mixed>  $entry
     */
    public function push(array $entry): void
    {
        if ($this->isMuted()) {
            return;
        }

        $this->claimForCurrentProcess();
        $this->batch ??= BatchBuilder::fromConfig();

        $complete = $this->batch->add($entry);

        if ($this->batch->count() === 0) {
            $this->oldestAt = null;
        } elseif ($complete !== [] || $this->oldestAt === null) {
            $this->oldestAt = $this->now();
        }

        foreach ($complete as $entries) {
            $this->dispatch($entries);
        }

        if ($this->buffered) {
            $this->flushIfStale();
        } else {
            $this->flush();
        }
    }

    public function flush(): void
    {
        if ($this->flushing || $this->batch === null) {
            return;
        }

        $this->claimForCurrentProcess();
        if ($this->batch === null) {
            return;
        }

        $entries = $this->batch->drain();
        // Re-read limits on the next push — remote config may have changed.
        $this->batch = null;
        $this->oldestAt = null;

        if ($entries !== []) {
            $this->dispatch($entries);
        }
    }

    /**
     * Flush only if the oldest entry has waited `flush_interval_sec`.
     */
    public function flushIfStale(): void
    {
        if ($this->oldestAt !== null
            && $this->now() - $this->oldestAt >= max(0, (int) config('processhub.flush_interval_sec', 10))
        ) {
            $this->flush();
        }
    }

    /**
     * Last-chance flush while the process is going down (shutdown function,
     * FatalError being reported): after OOM / max_execution_time neither
     * terminating callbacks nor destructors run. Must never throw.
     */
    public function flushOnShutdown(): void
    {
        try {
            self::reclaimMemory();
            $this->flush();
        } catch (\Throwable) {
            // Nothing sensible left to do at shutdown.
        }
    }

    /**
     * A FatalError entry has just been buffered: ship it at once — rendering
     * the error after it is reported may die again and skip every later
     * shutdown function.
     */
    public function flushFatalError(): void
    {
        $this->fatalErrorQueued = true;
        $this->flushOnShutdown();
    }

    public function hasFatalError(): bool
    {
        return $this->fatalErrorQueued;
    }

    /**
     * Run a callback with buffering disabled.
     *
     * @template T
     *
     * @param  callable(): T  $callback
     * @return T
     */
    public function mute(callable $callback): mixed
    {
        $this->muted++;
        try {
            return $callback();
        } finally {
            $this->muted--;
        }
    }

    /**
     * Mute the buffer while the worker processes a SendLogBatchJob — until
     * the attempt is over (JobAttempted) or, on Laravel 10, until the worker
     * moves on (loop tick / stop). The exception the worker reports after
     * that is recognised by isDeliveryFailure().
     */
    public function setInDeliveryJob(bool $inDeliveryJob): void
    {
        $this->inDeliveryJob = $inDeliveryJob;
    }

    public function isMuted(): bool
    {
        return $this->flushing || $this->muted > 0 || $this->inDeliveryJob;
    }

    /**
     * False: every entry is queued right away. For long-running processes
     * that never reach a flush point (no worker loop, no end of command).
     */
    public function setBuffered(bool $buffered): void
    {
        $this->buffered = $buffered;
        if (! $buffered) {
            $this->flush();
        }
    }

    /**
     * Remember an exception thrown out of a SendLogBatchJob run (from
     * JobExceptionOccurred) — the worker reports it afterwards.
     */
    public function markDeliveryException(\Throwable $e): void
    {
        $this->deliveryExceptions[$e] = true;
    }

    /**
     * True for failures of log delivery itself — reporting them into the
     * ProcessHub channel would create a new batch per failed one.
     */
    public function isDeliveryFailure(mixed $e): bool
    {
        return SendLogBatchJob::isOwnFailure($e)
            || ($e instanceof \Throwable && isset($this->deliveryExceptions[$e]));
    }

    public function pending(): int
    {
        return $this->batch?->count() ?? 0;
    }

    /**
     * False once the application this buffer belongs to has been flushed.
     */
    public function isOwnerAlive(): bool
    {
        if ($this->app === null) {
            return true;
        }

        return $this->app->get()?->bound(self::class) ?? false;
    }

    /**
     * @param  array<int, array<string, mixed>>  $entries
     */
    private function dispatch(array $entries): void
    {
        if (! $this->isOwnerAlive()) {
            return;
        }

        $this->flushing = true;
        try {
            SendLogBatchJob::enqueue($entries);
        } catch (\Throwable $e) {
            // A sync connection runs the job inline and rethrows its failure
            // after failed() has already parked the batch.
            if (! $this->isDeliveryFailure($e)) {
                FallbackFile::append($entries, 'Queue push failed: ' . $e->getMessage());
            }
        } finally {
            $this->flushing = false;
        }
    }

    /**
     * Laravel builds a FatalError and runs its throttle check before our
     * reportable callback gets memory back; on OOM there is no memory left
     * to compile those classes, so they are loaded upfront.
     */
    private static function prepareForFatalError(): void
    {
        if (self::$reservedMemory !== null) {
            return;
        }

        self::$reservedMemory = str_repeat("\0", self::RESERVED_MEMORY);
        class_exists(FatalError::class);
        class_exists(Unlimited::class);
    }

    /**
     * Sentry's approach: drop our reserve and, on "Allowed memory size …
     * exhausted", raise memory_limit so the buffer can still be shipped.
     */
    private static function reclaimMemory(): void
    {
        self::$reservedMemory = null;

        $error = error_get_last();
        if ($error === null
            || preg_match('/^Allowed memory size of (\d+) bytes exhausted/', $error['message'], $m) !== 1
        ) {
            return;
        }

        $limit = (int) $m[1] + self::SHUTDOWN_EXTRA_MEMORY;
        // Лимит ниже текущего потребления ini_set не примет, а его warning
        // обработчик Laravel превратит в исключение; php_admin_value не
        // поднять вовсе — тогда остаётся только освобождённый резерв.
        if ($limit > memory_get_usage(true)) {
            @ini_set('memory_limit', (string) $limit);
        }
    }

    /**
     * A child created by pcntl_fork() inherits the parent's buffer; the parent
     * ships those entries itself, so the child drops them instead of sending
     * duplicates.
     */
    private function claimForCurrentProcess(): void
    {
        $pid = (int) getmypid();
        if ($pid === $this->pid) {
            return;
        }

        $this->pid = $pid;
        $this->batch = null;
        $this->oldestAt = null;
    }

    private function now(): int
    {
        return Carbon::now()->getTimestamp();
    }
}
