<?php

namespace ProcessHub\Logs\Commands;

use Illuminate\Console\Command;
use Illuminate\Http\Client\ConnectionException;
use Illuminate\Support\Sleep;
use ProcessHub\Logs\Support\BatchBuilder;
use ProcessHub\Logs\Support\FallbackFile;
use ProcessHub\Logs\Support\LogIngest;

/**
 * `php artisan processhub:flush-fallback [--limit=N] [--max-wait=120]`
 *
 * Re-ingests batches parked in the fallback file by SendLogBatchJob::failed.
 * The file can be hundreds of MB, so it is streamed line by line; entries
 * from many lines are packed into full batches (`processhub.batch_size`).
 *
 * Crash safety:
 *   1. The live file is atomically renamed to `<path>.flushing` (a snapshot);
 *      new failures keep appending to a fresh `<path>`.
 *   2. The snapshot is streamed and sent batch by batch. On 429 we wait
 *      `Retry-After` (up to --max-wait) and resend the same batch.
 *   3. If we stop early (limit, error, signal) the unsent remainder is
 *      written to `<path>.flushing.tmp` and renamed over the snapshot, so the
 *      next run resumes exactly there. A crash mid-run leaves the snapshot
 *      intact: delivery is at-least-once, nothing is lost.
 *   4. A fully sent snapshot is deleted and the next one is claimed.
 *
 * A non-blocking lock on `<path>.lock` prevents concurrent runs.
 */
class FlushFallbackCommand extends Command
{
    protected $signature = 'processhub:flush-fallback
        {--limit= : Max batches to send in this run}
        {--max-wait=120 : Max seconds to honour a single Retry-After before giving up}';

    protected $description = 'Re-ingest batches previously saved to the fallback file';

    private string $url;

    private string $token;

    private int $sent = 0;

    private int $sentEntries = 0;

    private int $malformed = 0;

    private bool $stopping = false;

    public function handle(): int
    {
        $path = config('processhub.fallback_path');
        if (! $path || ! $this->hasData($path)) {
            $this->line('Nothing to flush — fallback file is empty or missing.');
            return self::SUCCESS;
        }

        $url = config('processhub.url');
        $token = config('processhub.token');
        if (! $url || ! $token) {
            $this->error('PROCESSHUB_LOG_URL / _TOKEN not configured — cannot flush.');
            return self::FAILURE;
        }
        $this->url = $url;
        $this->token = $token;

        $lock = fopen($path . '.lock', 'c');
        if ($lock === false || ! flock($lock, LOCK_EX | LOCK_NB)) {
            $this->warn('Another processhub:flush-fallback run is in progress.');
            return self::SUCCESS;
        }

        try {
            $this->trapSignals();
            $complete = true;
            while (! $this->stopping && ($snapshot = $this->claimSnapshot($path)) !== null) {
                if (! $complete = $this->drainSnapshot($snapshot)) {
                    break;
                }
            }
        } finally {
            flock($lock, LOCK_UN);
            fclose($lock);
        }

        $this->info(sprintf(
            'Flushed %d batches (%d entries); %s.',
            $this->sent,
            $this->sentEntries,
            $complete && ! $this->stopping ? 'fallback is empty' : 'remainder kept for the next run',
        ));
        if ($this->malformed > 0) {
            $this->warn("Skipped {$this->malformed} malformed lines.");
        }

        return self::SUCCESS;
    }

    private function hasData(string $path): bool
    {
        return (is_file($path) && filesize($path) > 0) || is_file($path . '.flushing');
    }

    /**
     * Unfinished snapshot from a previous run wins; otherwise atomically take
     * over the live file.
     */
    private function claimSnapshot(string $path): ?string
    {
        $snapshot = $path . '.flushing';
        clearstatcache();

        if (is_file($snapshot)) {
            return $snapshot;
        }
        if (! is_file($path) || filesize($path) === 0) {
            return null;
        }

        return @rename($path, $snapshot) ? $snapshot : null;
    }

    /**
     * @return bool true when the snapshot was fully delivered and removed
     */
    private function drainSnapshot(string $snapshot): bool
    {
        $fh = fopen($snapshot, 'r');
        if ($fh === false) {
            $this->error("Failed to read {$snapshot}.");
            return false;
        }

        $batcher = BatchBuilder::fromConfig();

        try {
            while (true) {
                while (($line = fgets($fh)) !== false) {
                    $entries = $this->decodeLine($line);
                    foreach ($entries as $i => $entry) {
                        foreach ($batcher->add($entry) as $batch) {
                            $delivered = $this->deliver($batch);
                            if (! $delivered || $this->shouldStop()) {
                                $carry = array_merge(
                                    $delivered ? [] : $batch,
                                    array_slice($entries, $i + 1),
                                    $batcher->drain(),
                                );
                                return $this->keepRemainder($snapshot, $fh, $carry);
                            }
                        }
                    }
                }

                // A writer that opened the file before our rename may still be
                // appending; its LOCK_EX write must finish before we decide EOF.
                flock($fh, LOCK_EX);
                if (ftell($fh) >= fstat($fh)['size']) {
                    break;
                }
                flock($fh, LOCK_UN);
            }

            $last = $batcher->drain();
            if ($last !== [] && ! $this->deliver($last)) {
                return $this->keepRemainder($snapshot, $fh, $last);
            }

            unlink($snapshot);

            return true;
        } finally {
            fclose($fh);
        }
    }

    /**
     * @return array<int, array<string, mixed>>
     */
    private function decodeLine(string $line): array
    {
        $line = trim($line);
        if ($line === '') {
            return [];
        }

        $payload = json_decode($line, true);
        if (! is_array($payload) || ! isset($payload['entries']) || ! is_array($payload['entries'])) {
            $this->malformed++;
            return [];
        }

        return array_values(array_filter($payload['entries'], 'is_array'));
    }

    /**
     * Send one batch; on 429 wait Retry-After and resend.
     *
     * @param  array<int, array<string, mixed>>  $batch
     */
    private function deliver(array $batch): bool
    {
        while (true) {
            try {
                $response = LogIngest::post($this->url, $this->token, $batch);
            } catch (ConnectionException $e) {
                $this->error('ProcessHub unreachable: ' . $e->getMessage());
                return false;
            }

            if ($response->successful()) {
                $this->sent++;
                $this->sentEntries += count($batch);
                return true;
            }

            if ($response->status() !== 429) {
                $this->error(sprintf('ProcessHub responded HTTP %d: %s', $response->status(), mb_substr($response->body(), 0, 500)));
                return false;
            }

            $wait = LogIngest::retryAfter($response);
            if ($wait > (int) $this->option('max-wait') || $this->stopping) {
                $this->warn("Rate limited, Retry-After {$wait}s exceeds --max-wait — stopping.");
                return false;
            }
            $this->line("Rate limited — waiting {$wait}s (sent {$this->sent} batches so far)…");
            Sleep::for($wait)->seconds();
        }
    }

    private function shouldStop(): bool
    {
        $limit = $this->option('limit');

        return $this->stopping || ((int) $limit > 0 && $this->sent >= (int) $limit);
    }

    /**
     * Replace the snapshot with: carried-over entries + the unread tail.
     * Written to a temp file first so a crash never leaves a truncated
     * snapshot behind.
     *
     * @param  resource  $fh  positioned right after the last consumed line
     * @param  array<int, array<string, mixed>>  $carry
     */
    private function keepRemainder(string $snapshot, $fh, array $carry): bool
    {
        $tmp = $snapshot . '.tmp';
        $out = fopen($tmp, 'w');
        if ($out === false) {
            $this->error("Failed to write {$tmp}; snapshot left as is (batches may be resent).");
            return false;
        }

        if ($carry !== []) {
            fwrite($out, FallbackFile::line($carry, 'flush-fallback remainder'));
        }
        stream_copy_to_stream($fh, $out);
        fflush($out);
        fclose($out);

        rename($tmp, $snapshot);

        return false;
    }

    private function trapSignals(): void
    {
        if (! extension_loaded('pcntl')) {
            return;
        }

        $this->trap([SIGINT, SIGTERM], function (): void {
            $this->stopping = true;
        });
    }
}
