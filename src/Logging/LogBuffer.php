<?php

namespace ProcessHub\Logs\Logging;

use ProcessHub\Logs\Jobs\SendLogBatchJob;
use ProcessHub\Logs\Support\BatchBuilder;
use ProcessHub\Logs\Support\FallbackFile;

/**
 * In-memory buffer between ProcessHubHandler and the queue: one
 * SendLogBatchJob per full batch instead of one job per log record.
 *
 * Bound as a container singleton. Flushed when a batch fills up and at every
 * unit-of-work boundary wired in ProcessHubServiceProvider (end of HTTP
 * request, after each queue job / worker loop, after console commands) plus
 * handler close()/reset(), so long-lived workers don't sit on logs.
 *
 * Recursion guard: while a batch is being pushed (or a SendLogBatchJob is
 * running) the buffer is muted — records produced by the delivery machinery
 * itself (e.g. the queue backend erroring and that error being logged) are
 * dropped instead of feeding back into the channel. If the push fails, the
 * batch goes to the fallback file; the application never sees the exception.
 */
class LogBuffer
{
    private ?BatchBuilder $batch = null;

    private bool $flushing = false;

    private int $muted = 0;

    /**
     * @param  array<string, mixed>  $entry
     */
    public function push(array $entry): void
    {
        if ($this->isMuted()) {
            return;
        }

        $this->batch ??= BatchBuilder::fromConfig();

        foreach ($this->batch->add($entry) as $complete) {
            $this->dispatch($complete);
        }
    }

    public function flush(): void
    {
        if ($this->flushing || $this->batch === null) {
            return;
        }

        $entries = $this->batch->drain();
        // Re-read limits on the next push — remote config may have changed.
        $this->batch = null;

        if ($entries !== []) {
            $this->dispatch($entries);
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

    public function isMuted(): bool
    {
        return $this->flushing || $this->muted > 0;
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
}
