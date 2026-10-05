<?php

namespace ProcessHub\Logs\Jobs;

use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Http\Client\ConnectionException;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\MaxAttemptsExceededException;
use Illuminate\Queue\SerializesModels;
use ProcessHub\Logs\Exceptions\DeliveryFailedException;
use ProcessHub\Logs\Logging\LogBuffer;
use ProcessHub\Logs\Support\FallbackFile;
use ProcessHub\Logs\Support\LogIngest;

/**
 * Async delivery of a log batch (up to 100 entries) to ProcessHub.
 *
 * Retry policy is time-based, not attempt-based:
 *   - `retryUntil()` gives the batch `processhub.retry_window_sec` (1h by
 *     default) to get through. Attempt counters are irrelevant, so a long
 *     429 streak can't exhaust the job the way `$tries = 3` used to.
 *   - 429 → `release(Retry-After)`; the job simply waits its turn.
 *   - 5xx / network → exception, Laravel retries with `backoff()`;
 *     `$maxExceptions` caps how many real failures we tolerate.
 *   - 4xx (except 429) is a config problem (bad token, rejected payload) —
 *     fail immediately, retries won't help.
 *
 * Whatever ends up failing (window expired, too many exceptions, timeout,
 * 4xx) lands in the fallback file via `failed()`;
 * `processhub:flush-fallback` re-ingests it later.
 */
class SendLogBatchJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    /** Max time in seconds a single attempt can run. */
    public int $timeout = 30;

    /** A timed-out attempt goes to `failed()` instead of being retried blindly. */
    public bool $failOnTimeout = true;

    /** 5xx / network failures tolerated within the retry window. */
    public int $maxExceptions = 10;

    public function __construct(
        /** @var array<int, array<string, mixed>> */
        public array $entries,
    ) {}

    /**
     * Push a batch to the configured connection/queue.
     *
     * @param  array<int, array<string, mixed>>  $entries
     */
    public static function enqueue(array $entries): void
    {
        $queueName = config('processhub.queue');
        $connection = config('processhub.connection');

        $job = new self($entries);
        if ($queueName) {
            $job->onQueue($queueName);
        }
        if ($connection) {
            $job->onConnection($connection);
        }

        app(QueueFactory::class)->connection($connection)->pushOn($queueName, $job);
    }

    /**
     * True when the throwable describes a failure of log delivery itself —
     * such reports must not be logged back into the ProcessHub channel.
     */
    public static function isOwnFailure(mixed $e): bool
    {
        if ($e instanceof DeliveryFailedException) {
            return true;
        }

        // Covers TimeoutExceededException too (it extends this class).
        return $e instanceof MaxAttemptsExceededException
            && isset($e->job)
            && $e->job->resolveName() === static::class;
    }

    public function retryUntil(): \DateTimeInterface
    {
        return now()->addSeconds(max(60, (int) config('processhub.retry_window_sec', 3600)));
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
     * Final fallback — append entries to a file so a scheduled flush can
     * retry later when ProcessHub / network recovers.
     */
    public function failed(\Throwable $exception): void
    {
        FallbackFile::append($this->entries, $exception->getMessage());
    }

    private function deliver(string $url, string $token): void
    {
        try {
            $response = LogIngest::post($url, $token, $this->entries);
        } catch (ConnectionException $e) {
            throw DeliveryFailedException::network($e);
        }

        if ($response->successful()) {
            return;
        }

        $status = $response->status();

        if ($status === 429) {
            $this->release(LogIngest::retryAfter($response));

            return;
        }

        if ($status >= 400 && $status < 500) {
            $this->fail(DeliveryFailedException::status($status, $response->body()));

            return;
        }

        throw DeliveryFailedException::status($status);
    }
}
