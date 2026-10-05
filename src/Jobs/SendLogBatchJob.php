<?php

namespace ProcessHub\Logs\Jobs;

use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\MaxAttemptsExceededException;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\Log;
use ProcessHub\Logs\Exceptions\DeliveryFailedException;
use ProcessHub\Logs\Logging\LogBuffer;
use ProcessHub\Logs\Support\FallbackFile;
use ProcessHub\Logs\Support\IngestThrottle;
use ProcessHub\Logs\Support\LogIngest;

/**
 * Async delivery of a log batch (up to 100 entries) to ProcessHub.
 *
 * Retry policy is time-based, not attempt-based:
 *   - the batch has `processhub.retry_window_sec` (1h by default, counted
 *     from the moment it becomes available) to get through — `retryDeadline`,
 *     checked by the job itself; attempt counters are irrelevant, so a long
 *     429 streak can't exhaust the job.
 *   - before each POST the shared IngestThrottle is asked for a go; refused →
 *     `release()` until the turn it hands out.
 *   - 429 → global pause for Retry-After + `release()`.
 *   - 5xx / any transport error → DeliveryFailedException, Laravel retries
 *     with `backoff()`.
 *   - 4xx (except 429) is a config problem (bad token, rejected payload) —
 *     retries won't help.
 *
 * Regular dead ends (4xx, deadline passed while the job sat in the queue,
 * next retry wouldn't fit before the deadline) park the batch in the
 * fallback file and delete the job — nothing piles up in failed_jobs;
 * `processhub:flush-fallback` re-ingests it later. The worker's own
 * `retryUntil()` is the deadline plus a day, so it never fails the job by
 * time first. `failed()` stays as a safety net for what the job can't
 * intercept (timeouts) and for a batch the fallback file can't take.
 */
class SendLogBatchJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    /** Max time in seconds a single attempt can run. */
    public int $timeout = 30;

    /** A timed-out attempt goes to `failed()` instead of being retried blindly. */
    public bool $failOnTimeout = true;

    /**
     * How long after `retryDeadline` the worker would give up on the job by
     * itself: the job parks the batch at the deadline, but under a backlog it
     * may be picked up hours later — then it still must be the job deciding.
     */
    private const WORKER_GRACE_SECONDS = 86_400;

    /**
     * Unix time until which delivery is retried. Null in jobs serialized by
     * older package versions — their deadline is the payload's retryUntil.
     */
    public ?int $retryDeadline = null;

    /**
     * @param  array<int, array<string, mixed>>  $entries
     * @param  int  $delaySeconds  how long the job waits in the queue before
     *                             its first attempt; shifts the deadline
     */
    public function __construct(
        public array $entries,
        int $delaySeconds = 0,
    ) {
        $this->entries = self::serializable($entries);
        $this->retryDeadline = now()->getTimestamp() + max(0, $delaySeconds) + self::retryWindow();
    }

    /**
     * Push a batch to the configured connection/queue, optionally delayed.
     *
     * @param  array<int, array<string, mixed>>  $entries
     */
    public static function enqueue(array $entries, int $delaySeconds = 0): void
    {
        self::push(new self($entries, $delaySeconds), $delaySeconds);
    }

    /**
     * True when the throwable describes a failure of log delivery itself
     * (the job's own exception or the worker giving up on it) — such reports
     * must not be logged back into the ProcessHub channel.
     */
    public static function isOwnFailure(mixed $e): bool
    {
        if ($e instanceof DeliveryFailedException) {
            return true;
        }

        // Covers TimeoutExceededException too (it extends this class).
        if (! $e instanceof MaxAttemptsExceededException) {
            return false;
        }

        // Early Laravel 10 releases set no `job` on the exception — the message
        // ("<class> has been attempted too many times…" / "<class> has timed
        // out.") still names the job.
        return isset($e->job)
            ? $e->job->resolveName() === static::class
            : str_starts_with($e->getMessage(), static::class . ' has ');
    }

    public function retryUntil(): \DateTimeInterface
    {
        $deadline = $this->retryDeadline ?? now()->getTimestamp() + self::retryWindow();

        return new \DateTimeImmutable('@' . ($deadline + self::WORKER_GRACE_SECONDS));
    }

    /**
     * @return array<int, int>
     */
    public function backoff(): array
    {
        return [10, 30, 120, 300];
    }

    public function handle(): void
    {
        $url = config('processhub.url');
        $token = config('processhub.token');
        if (! $url || ! $token || empty($this->entries)) {
            return;
        }

        // Anything logged while delivering (HTTP client, listeners of fail())
        // must not be buffered into yet another batch.
        app(LogBuffer::class)->mute(fn () => $this->deliver($url, $token));
    }

    /**
     * Last resort for failures the job couldn't intercept (timeout) — keep
     * entries in the fallback file.
     */
    public function failed(\Throwable $exception): void
    {
        FallbackFile::append($this->entries, $exception->getMessage());
    }

    private function deliver(string $url, string $token): void
    {
        if (! $this->fitsBeforeDeadline(0)) {
            $this->park('Retry window exhausted while waiting in the queue');

            return;
        }

        $throttle = IngestThrottle::make();
        $wait = $throttle->acquire($this->maxWait(), newcomer: $this->attempts() === 1);
        if ($wait > 0) {
            $this->releaseOrPark($wait, 'Local rate limit');

            return;
        }

        try {
            $response = LogIngest::post($url, $token, $this->entries);
        } catch (\Throwable $e) {
            $this->throwOrPark(DeliveryFailedException::network($e));

            return;
        }

        if ($response->successful()) {
            return;
        }

        $status = $response->status();

        if ($status === 429) {
            $wait = LogIngest::retryAfter($response);
            $throttle->pauseFor($wait);
            $this->releaseOrPark($wait, 'ProcessHub ingest responded HTTP 429');

            return;
        }

        if ($status >= 400 && $status < 500) {
            $this->park(DeliveryFailedException::status($status, $response->body())->getMessage());

            return;
        }

        $this->throwOrPark(DeliveryFailedException::status($status));
    }

    private function releaseOrPark(int $delay, string $reason): void
    {
        if (! $this->fitsBeforeDeadline($delay)) {
            $this->park($reason . '; retry window exhausted');
        } elseif ($this->isLegacy()) {
            $this->requeue($delay);
        } else {
            $this->release($delay);
        }
    }

    /**
     * Throw for a regular backoff retry, unless that retry would come after
     * the deadline — then park the batch now.
     */
    private function throwOrPark(DeliveryFailedException $e): void
    {
        $backoff = $this->backoff();
        $delay = $backoff[$this->attempts() - 1] ?? end($backoff);
        if (! $this->fitsBeforeDeadline($delay)) {
            $this->park($e->getMessage() . '; retry window exhausted');

            return;
        }

        if (! $this->isLegacy()) {
            throw $e;
        }

        $this->requeue($delay);
    }

    /** The next attempt starts after $delay and still has a full $timeout before the deadline. */
    private function fitsBeforeDeadline(int $delay): bool
    {
        $deadline = $this->deadline();

        return $deadline === null || now()->getTimestamp() + $delay + $this->timeout <= $deadline;
    }

    /** Longest wait for the throttle that still fits before the deadline. */
    private function maxWait(): ?int
    {
        $deadline = $this->deadline();

        return $deadline === null ? null : $deadline - now()->getTimestamp() - $this->timeout;
    }

    /**
     * Older jobs carry no `retryDeadline`: queued by 0.4 pre-releases, their
     * payload has the deadline as `retryUntil`; queued by 0.3 — none at all.
     */
    private function deadline(): ?int
    {
        $deadline = $this->retryDeadline ?? $this->job?->retryUntil();

        return $deadline === null ? null : (int) $deadline;
    }

    /**
     * A 0.3 payload has `maxTries` 3 and no `retryUntil`: every `release()`
     * brings it closer to MaxAttemptsExceeded — it's retried as a new job.
     */
    private function isLegacy(): bool
    {
        return $this->retryDeadline === null && $this->job !== null && $this->job->retryUntil() === null;
    }

    private function requeue(int $delay): void
    {
        self::push(new self($this->entries, $delay), $delay);
        $this->delete();
    }

    private static function push(self $job, int $delaySeconds): void
    {
        $queueName = config('processhub.queue');
        $connection = config('processhub.connection');

        if ($queueName) {
            $job->onQueue($queueName);
        }
        if ($connection) {
            $job->onConnection($connection);
        }

        $queue = app(QueueFactory::class)->connection($connection);
        if ($delaySeconds > 0) {
            $queue->laterOn($queueName, $delaySeconds, $job);
        } else {
            $queue->pushOn($queueName, $job);
        }
    }

    private function park(string $reason): void
    {
        if (! FallbackFile::append($this->entries, $reason)) {
            // Файл недоступен — пачка остаётся в failed_jobs, а не пропадает.
            $e = DeliveryFailedException::unparked($reason);
            if ($this->job === null) {
                throw $e;
            }
            Log::error('ProcessHub log batch could not be moved to the fallback file', [
                'reason' => $reason,
                'entries' => count($this->entries),
            ]);
            $this->fail($e);

            return;
        }

        $this->delete();
        Log::warning(config('processhub.fallback_path')
            ? 'ProcessHub log batch moved to the fallback file'
            : 'ProcessHub log batch dropped: fallback file is disabled', [
            'reason' => $reason,
            'entries' => count($this->entries),
        ]);
    }

    /**
     * Queue payloads are JSON: one string with broken UTF-8 would make the
     * whole batch unqueueable. Substituted the same way as when it's sent.
     *
     * @param  array<int, array<string, mixed>>  $entries
     * @return array<int, array<string, mixed>>
     */
    private static function serializable(array $entries): array
    {
        if (json_encode($entries) !== false) {
            return $entries;
        }

        /** @var array{logs?: array<int, array<string, mixed>>} $decoded */
        $decoded = json_decode(LogIngest::encode($entries), true);

        return $decoded['logs'] ?? [];
    }

    private static function retryWindow(): int
    {
        return max(60, (int) config('processhub.retry_window_sec', 3600));
    }
}
