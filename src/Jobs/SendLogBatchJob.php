<?php

namespace ProcessHub\Logs\Jobs;

use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\Jobs\SyncJob;
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
 * Retry policy is time-based, not attempt-based: the batch has
 * `processhub.retry_window_sec` (1h by default, counted from the moment it
 * becomes available) to get through — `retryDeadline`, checked by the job
 * itself, so a long 429 streak can't exhaust it.
 *
 * Every expected failure ends the attempt with `release()`; nothing is thrown
 * out of handle() — the worker would report it and, on messages like
 * "Connection reset by peer", restart itself:
 *   - the shared IngestThrottle refuses → until the turn it hands out;
 *   - 429 → global pause for Retry-After;
 *   - 408 / 5xx / any error of the HTTP call → `backoff()`.
 * Other 4xx is a config problem (bad token, rejected payload) — retries won't
 * help.
 *
 * Dead ends (4xx, deadline passed while the job sat in the queue, next retry
 * wouldn't fit before the deadline, a sync queue that can't delay) park the
 * batch in the fallback file and delete the job — nothing piles up in
 * failed_jobs; `processhub:flush-fallback` re-ingests it later. The worker's
 * own `retryUntil()` is the deadline plus a day, so it never fails the job by
 * time first. `failed()` stays as a safety net for what the job can't
 * intercept (timeouts).
 *
 * A job queued by 0.3 has no `retryDeadline`: it is sent only if the throttle
 * lets it through right away (it never takes a turn in the throttle's queue)
 * and is never retried — anything but success parks it.
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

    /** Unix time until which delivery is retried. Null in jobs queued by 0.3. */
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
        $job = new self($entries, $delaySeconds);
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
        // park() не смог записать пачку в файл — она остаётся только в failed_jobs.
        if (! $exception instanceof DeliveryFailedException) {
            FallbackFile::append($this->entries, $exception->getMessage());
        }
    }

    private function deliver(string $url, string $token): void
    {
        if (! $this->fitsBeforeDeadline(0)) {
            $this->park('Retry window exhausted while waiting in the queue');

            return;
        }

        $throttle = IngestThrottle::make();
        // Задача 0.3 берёт только свободный слот: в очередь ходов она не встаёт
        // и не обгоняет тех, кто ждёт в ней.
        $wait = $this->retryDeadline === null
            ? $throttle->acquire(0, newcomer: true)
            : $throttle->acquire($this->retryDeadline - now()->getTimestamp() - $this->timeout, newcomer: $this->attempts() === 1);
        if ($wait > 0) {
            $this->retryLater($wait, 'Local rate limit');

            return;
        }

        try {
            $response = LogIngest::post($url, $token, $this->entries);
        } catch (\Throwable $e) {
            $this->retryLater($this->backoffDelay(), 'ProcessHub ingest unreachable: ' . $e->getMessage());

            return;
        }

        if ($response->successful()) {
            return;
        }

        $status = $response->status();
        $reason = 'ProcessHub ingest responded HTTP ' . $status;

        if ($status === 429) {
            $wait = LogIngest::retryAfter($response);
            $throttle->pauseFor($wait);
            $this->retryLater($wait, $reason);
        } elseif ($status >= 400 && $status < 500 && $status !== 408) {
            $this->park(trim($reason . ' ' . mb_substr($response->body(), 0, 500)));
        } else {
            $this->retryLater($this->backoffDelay(), $reason);
        }
    }

    /**
     * Release the job for another attempt after $delay — or park the batch
     * now when there will be no such attempt.
     */
    private function retryLater(int $delay, string $reason): void
    {
        $obstacle = match (true) {
            $this->retryDeadline === null => 'job queued by 0.3 is not retried',
            $this->job === null || $this->job instanceof SyncJob => 'sync queue can\'t retry',
            ! $this->fitsBeforeDeadline($delay) => 'retry window exhausted',
            default => null,
        };

        if ($obstacle === null) {
            $this->release($delay);
        } else {
            $this->park($reason . '; ' . $obstacle);
        }
    }

    /** Delay before the next attempt after a failed POST. */
    private function backoffDelay(): int
    {
        $backoff = $this->backoff();

        return $backoff[$this->attempts() - 1] ?? end($backoff);
    }

    /** The next attempt starts after $delay and still has a full $timeout before the deadline. */
    private function fitsBeforeDeadline(int $delay): bool
    {
        return $this->retryDeadline === null
            || now()->getTimestamp() + $delay + $this->timeout <= $this->retryDeadline;
    }

    private function park(string $reason): void
    {
        if (! FallbackFile::append($this->entries, $reason)) {
            // Файл недоступен — пачка остаётся в failed_jobs, а не пропадает.
            $e = DeliveryFailedException::unparked($reason);
            if ($this->job === null) {
                throw $e;
            }
            $this->fail($e);

            return;
        }

        $this->delete();
        try {
            Log::warning(config('processhub.fallback_path')
                ? 'ProcessHub log batch moved to the fallback file'
                : 'ProcessHub log batch dropped: fallback file is disabled', [
                'reason' => $reason,
                'entries' => count($this->entries),
            ]);
        } catch (\Throwable) {
            // Пачка уже в файле — сбой канала логов задачу не валит.
        }
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
