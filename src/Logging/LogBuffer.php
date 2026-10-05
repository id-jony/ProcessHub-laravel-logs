<?php

namespace ProcessHub\Logs\Logging;

use Illuminate\Support\Carbon;
use ProcessHub\Logs\Jobs\SendLogBatchJob;
use ProcessHub\Logs\Support\BatchBuilder;
use ProcessHub\Logs\Support\FallbackFile;

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
 *     stop, PHP shutdown — including fatal errors): flush().
 *
 * Recursion guard: while a batch is being pushed or a SendLogBatchJob is
 * being processed the buffer is muted — records produced by the delivery
 * machinery itself (the queue backend erroring, the worker reporting the
 * job's exception) are dropped instead of feeding back into the channel. If
 * the push fails, the batch goes to the fallback file; the application never
 * sees the exception.
 */
class LogBuffer
{
    /** Memory allowed past an exhausted memory_limit to ship the buffer. */
    private const SHUTDOWN_EXTRA_MEMORY = 16 * 1024 * 1024;

    private ?BatchBuilder $batch = null;

    /** Unix time of the oldest buffered entry. */
    private ?int $oldestAt = null;

    private bool $flushing = false;

    private int $muted = 0;

    private bool $inDeliveryJob = false;

    /** Process that owns the buffered entries (see claimForCurrentProcess()). */
    private int $pid;

    /** @var \WeakMap<\Throwable, true> */
    private \WeakMap $shippedExceptions;

    public function __construct()
    {
        $this->pid = (int) getmypid();
        $this->shippedExceptions = new \WeakMap();
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

        $this->flushIfStale();
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
            $error = error_get_last();
            if ($error !== null
                && preg_match('/^Allowed memory size of (\d+) bytes exhausted/', $error['message'], $m) === 1
            ) {
                // Сам flush (сериализация пачки, push в очередь) требует памяти,
                // а её не осталось — даём немного сверх лимита, как Sentry.
                ini_set('memory_limit', (string) ((int) $m[1] + self::SHUTDOWN_EXTRA_MEMORY));
            }

            $this->flush();
        } catch (\Throwable) {
            // Nothing sensible left to do at shutdown.
        }
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
     * Mute the buffer while the worker processes a SendLogBatchJob. Kept on
     * until the worker moves on (next job / loop tick / stop): the worker
     * reports a job's exception only after JobFailed and
     * JobReleasedAfterException have been dispatched.
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
     * True the first time a given exception object is offered — the same
     * exception reaches the channel twice when processhub is in the default
     * log stack (Laravel's own report log + HandleExceptionReported).
     */
    public function claimException(\Throwable $e): bool
    {
        if (isset($this->shippedExceptions[$e])) {
            return false;
        }
        $this->shippedExceptions[$e] = true;

        return true;
    }

    public function pending(): int
    {
        return $this->batch?->count() ?? 0;
    }

    /**
     * @param  array<int, array<string, mixed>>  $entries
     */
    private function dispatch(array $entries): void
    {
        $this->flushing = true;
        try {
            SendLogBatchJob::enqueue($entries);
        } catch (\Throwable $e) {
            FallbackFile::append($entries, 'Queue push failed: ' . $e->getMessage());
        } finally {
            $this->flushing = false;
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
