<?php

namespace ProcessHub\Logs\Commands;

use Illuminate\Console\Command;
use Illuminate\Http\Client\ConnectionException;
use Illuminate\Http\Client\Response;
use Illuminate\Support\Sleep;
use ProcessHub\Logs\Support\BatchBuilder;
use ProcessHub\Logs\Support\FallbackFile;
use ProcessHub\Logs\Support\LogIngest;

/**
 * `php artisan processhub:flush-fallback [--limit=N] [--max-wait=120] [--rate=50] [--max-runtime=300]`
 *
 * Re-ingests batches parked in the fallback file by SendLogBatchJob::failed.
 * The file can be hundreds of MB, so it is streamed line by line; entries
 * from many lines are packed into full batches (`processhub.batch_size`).
 *
 * Crash safety:
 *   1. The live file is renamed to `<path>.flushing` (a snapshot) under the
 *      lock shared with FallbackFile::append, so nobody writes into the
 *      snapshot afterwards; new failures go to a fresh `<path>`.
 *   2. The snapshot is never rewritten. After every delivered batch the
 *      position of the next entry is saved to `<path>.flushing.offset`
 *      (atomic tmp + rename); the next run resumes there. A crash between a
 *      POST and the checkpoint resends one batch: at-least-once, no loss.
 *   3. A fully processed snapshot is deleted and the next one is claimed.
 *
 * Delivery:
 *   - requests are paced to --rate per minute;
 *   - 429 waits Retry-After (at least 1 s, up to --max-wait);
 *   - 5xx / network errors back off exponentially; after
 *     MAX_CONSECUTIVE_FAILURES in a row the run stops with FAILURE;
 *   - other 4xx rejects the batch: it is split in halves until the offending
 *     entries are isolated, those go to `<path>.rejected` with the reason;
 *   - 401/403/404/405 mean misconfiguration — stop with FAILURE, nothing is
 *     skipped.
 *
 * A non-blocking lock on `<path>.lock` prevents concurrent runs.
 */
class FlushFallbackCommand extends Command
{
    protected $signature = 'processhub:flush-fallback
        {--limit= : Max batches to send in this run}
        {--max-wait=120 : Max seconds to honour a single Retry-After before giving up}
        {--rate= : Max requests per minute (default: processhub.rate_limit_per_minute, 50)}
        {--max-runtime=300 : Stop after this many seconds, keeping the remainder for the next run}';

    protected $description = 'Re-ingest batches previously saved to the fallback file';

    /** Минимальная пауза перед повтором — Retry-After: 0 не должен давать цикл без пауз. */
    private const MIN_PAUSE = 1;

    /** Пауза на 429 без пригодного Retry-After. */
    private const DEFAULT_RETRY_AFTER = 30;

    private const MAX_RETRY_AFTER = 3600;

    private const MAX_BACKOFF = 60;

    private const MAX_CONSECUTIVE_FAILURES = 5;

    /** Статусы, при которых повтор бесполезен, а пропуск пачки потерял бы данные. */
    private const CONFIG_ERRORS = [401, 403, 404, 405];

    private string $path;

    private string $url;

    private string $token;

    private int $intervalMs;

    private int $deadlineMs;

    private ?int $lastPostMs = null;

    private int $notBeforeMs = 0;

    private int $failures = 0;

    private int $sent = 0;

    private int $sentEntries = 0;

    private int $rejected = 0;

    private int $malformed = 0;

    private bool $stopping = false;

    private bool $failed = false;

    public function handle(): int
    {
        $this->resetState();

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
        $this->path = $path;
        $this->url = $url;
        $this->token = $token;

        $rate = (int) ($this->option('rate') ?? config('processhub.rate_limit_per_minute', 50));
        $this->intervalMs = (int) ceil(60_000 / max(1, $rate));
        $this->deadlineMs = $this->nowMs() + max(1, (int) $this->option('max-runtime')) * 1000;

        $lock = fopen($path . '.lock', 'c');
        if ($lock === false || ! flock($lock, LOCK_EX | LOCK_NB)) {
            $this->warn('Another processhub:flush-fallback run is in progress.');
            return self::SUCCESS;
        }

        try {
            $this->trapSignals();
            while (! $this->shouldStop() && ($snapshot = $this->claimSnapshot()) !== null) {
                if (! $this->drainSnapshot($snapshot)) {
                    break;
                }
            }
        } finally {
            flock($lock, LOCK_UN);
            fclose($lock);
        }

        $this->report();

        return $this->failed ? self::FAILURE : self::SUCCESS;
    }

    private function resetState(): void
    {
        // Artisan переиспользует экземпляр команды между вызовами в одном процессе.
        $this->lastPostMs = null;
        $this->notBeforeMs = 0;
        $this->failures = 0;
        $this->sent = 0;
        $this->sentEntries = 0;
        $this->rejected = 0;
        $this->malformed = 0;
        $this->stopping = false;
        $this->failed = false;
    }

    private function hasData(string $path): bool
    {
        clearstatcache();

        return (is_file($path) && filesize($path) > 0) || is_file($path . '.flushing');
    }

    /**
     * Unfinished snapshot from a previous run wins; otherwise take over the
     * live file under the writers' lock.
     */
    private function claimSnapshot(): ?string
    {
        $path = $this->path;
        $snapshot = $path . '.flushing';
        clearstatcache();

        if (is_file($snapshot)) {
            return $snapshot;
        }

        // Чекпойнт мог пережить снапшот (падение между двумя unlink) — к новому он не относится.
        @unlink($this->checkpointPath($snapshot));

        return FallbackFile::locked($path, static function () use ($path, $snapshot): ?string {
            clearstatcache();
            if (! is_file($path) || filesize($path) === 0) {
                return null;
            }

            return @rename($path, $snapshot) ? $snapshot : null;
        });
    }

    /**
     * @return bool true when the snapshot was fully processed and removed
     */
    private function drainSnapshot(string $snapshot): bool
    {
        $fh = @fopen($snapshot, 'r');
        if ($fh === false) {
            $this->error("Failed to read {$snapshot}.");
            $this->failed = true;
            return false;
        }

        try {
            return $this->streamSnapshot($snapshot, $fh);
        } finally {
            fclose($fh);
        }
    }

    /**
     * @param  resource  $fh
     */
    private function streamSnapshot(string $snapshot, $fh): bool
    {
        $inode = (int) (fstat($fh)['ino'] ?? 0);
        [$offset, $skip] = $this->readCheckpoint($snapshot, $fh, $inode);
        fseek($fh, $offset);

        $batcher = BatchBuilder::fromConfig();
        /** @var array<int, array{0: int, 1: int}> $positions [строка, индекс] каждой записи в $batcher */
        $positions = [];
        $torn = null;

        while (($lineStart = ftell($fh)) !== false && ($line = fgets($fh)) !== false) {
            $entries = $this->decodeLine($line);
            if ($entries === null) {
                if (str_ends_with($line, "\n")) {
                    $this->malformed++;
                } else {
                    // Хвост без "\n" — оборванная запись (писатель упал посреди fwrite):
                    // дописать её уже некому, поэтому сохраняем как есть в .rejected.
                    $torn = $line;
                }
                continue;
            }

            foreach ($entries as $i => $entry) {
                if ($lineStart === $offset && $i < $skip) {
                    continue;
                }
                $positions[] = [$lineStart, $i];

                foreach ($batcher->add($entry) as $batch) {
                    if (! $this->resolve($batch)) {
                        return false;
                    }
                    $positions = array_slice($positions, count($batch));
                    if (! $this->saveCheckpoint($snapshot, $inode, $positions[0] ?? [$lineStart, $i + 1])) {
                        return false;
                    }
                    if ($this->shouldStop()) {
                        return false;
                    }
                }
            }
        }

        $last = $batcher->drain();
        if ($last !== [] && ! $this->resolve($last)) {
            return false;
        }
        if ($torn !== null && ! $this->reject([], 'incomplete line (torn write)', $torn)) {
            return false;
        }

        unlink($snapshot);
        @unlink($this->checkpointPath($snapshot));

        return true;
    }

    /**
     * @return array<int, array<string, mixed>>|null null — строка не разбирается
     */
    private function decodeLine(string $line): ?array
    {
        $line = trim($line);
        if ($line === '') {
            return [];
        }

        $payload = json_decode($line, true);
        if (! is_array($payload) || ! isset($payload['entries']) || ! is_array($payload['entries'])) {
            return null;
        }

        return array_values(array_filter($payload['entries'], 'is_array'));
    }

    /**
     * Deliver the batch or move the entries ProcessHub refuses to `.rejected`.
     * A refused batch is split in halves so one bad entry doesn't sink the rest.
     *
     * @param  array<int, array<string, mixed>>  $batch
     * @return bool false — stop the run, the batch stays in the snapshot
     */
    private function resolve(array $batch): bool
    {
        $response = $this->send($batch);
        if ($response === null) {
            return false;
        }
        if ($response->successful()) {
            return true;
        }

        if (count($batch) > 1) {
            $half = intdiv(count($batch) + 1, 2);

            return $this->resolve(array_slice($batch, 0, $half))
                && $this->resolve(array_slice($batch, $half));
        }

        $reason = sprintf('HTTP %d: %s', $response->status(), mb_substr($response->body(), 0, 500));
        $this->warn("ProcessHub rejected an entry ({$reason}) — moved to {$this->rejectedPath()}.");

        return $this->reject($batch, $reason);
    }

    /**
     * POST with retries. Returns a successful response or a final 4xx
     * rejection; null when the run has to stop.
     *
     * @param  array<int, array<string, mixed>>  $batch
     */
    private function send(array $batch): ?Response
    {
        while ($this->waitForSlot()) {
            try {
                $response = LogIngest::post($this->url, $this->token, $batch);
            } catch (ConnectionException $e) {
                $this->transientFailure('ProcessHub unreachable: ' . $e->getMessage());
                continue;
            }

            $status = $response->status();
            if ($response->successful() || $this->isRejection($status)) {
                $this->failures = 0;
                if ($response->successful()) {
                    $this->sent++;
                    $this->sentEntries += count($batch);
                }
                return $response;
            }
            if ($status === 429) {
                if (! $this->rateLimited($response)) {
                    return null;
                }
                continue;
            }
            if (in_array($status, self::CONFIG_ERRORS, true)) {
                $this->error(sprintf(
                    'ProcessHub responded HTTP %d — check PROCESSHUB_LOG_URL / _TOKEN. Stopping, nothing skipped. %s',
                    $status,
                    mb_substr($response->body(), 0, 500),
                ));
                $this->failed = true;
                return null;
            }

            $this->transientFailure(sprintf('ProcessHub responded HTTP %d: %s', $status, mb_substr($response->body(), 0, 500)));
        }

        return null;
    }

    private function isRejection(int $status): bool
    {
        return $status >= 400 && $status < 500
            && ! in_array($status, [408, 429, ...self::CONFIG_ERRORS], true);
    }

    private function transientFailure(string $message): void
    {
        $this->failures++;
        $this->error($message);

        if ($this->failures >= self::MAX_CONSECUTIVE_FAILURES) {
            $this->error("Giving up after {$this->failures} consecutive failures; remainder kept for the next run.");
            $this->failed = true;
            return;
        }

        $this->pauseFor(min(self::MAX_BACKOFF, 2 ** $this->failures));
    }

    private function rateLimited(Response $response): bool
    {
        $wait = $this->retryAfter($response);
        if ($wait > (int) $this->option('max-wait')) {
            $this->warn("Rate limited, Retry-After {$wait}s exceeds --max-wait — stopping.");
            return false;
        }

        $this->line("Rate limited — waiting {$wait}s (sent {$this->sent} batches so far)…");
        $this->pauseFor($wait);

        return true;
    }

    /**
     * `Retry-After` в секундах или HTTP-date; не меньше MIN_PAUSE.
     */
    private function retryAfter(Response $response): int
    {
        $header = trim($response->header('Retry-After'));

        if (preg_match('/^-?\d+$/', $header)) {
            $seconds = (int) $header;
        } elseif ($header !== '' && ($at = strtotime($header)) !== false) {
            $seconds = $at - now()->getTimestamp();
        } else {
            $seconds = self::DEFAULT_RETRY_AFTER;
        }

        return max(self::MIN_PAUSE, min(self::MAX_RETRY_AFTER, $seconds));
    }

    private function pauseFor(int $seconds): void
    {
        $this->notBeforeMs = max($this->notBeforeMs, $this->nowMs() + $seconds * 1000);
    }

    /**
     * Единственная точка ожидания перед POST: темп --rate и паузы после
     * 429 / сбоев. false — ждать нельзя (сигнал, остановка по ошибке или
     * ожидание вышло бы за --max-runtime).
     */
    private function waitForSlot(): bool
    {
        if ($this->stopping || $this->failed) {
            return false;
        }

        $next = $this->lastPostMs === null ? 0 : $this->lastPostMs + $this->intervalMs;
        if (! $this->sleepUntil(max($this->notBeforeMs, $next))) {
            return false;
        }

        $this->lastPostMs = $this->nowMs();

        return true;
    }

    private function sleepUntil(int $targetMs): bool
    {
        $now = $this->nowMs();
        if (max($now, $targetMs) >= $this->deadlineMs) {
            $this->warn('--max-runtime reached; remainder kept for the next run.');
            $this->stopping = true;
            return false;
        }
        if ($targetMs > $now) {
            Sleep::for($targetMs - $now)->milliseconds();
        }

        // SIGTERM мог прийти во время сна.
        return ! $this->stopping;
    }

    private function shouldStop(): bool
    {
        $limit = (int) $this->option('limit');

        return $this->stopping
            || $this->failed
            || ($limit > 0 && $this->sent >= $limit)
            || $this->nowMs() >= $this->deadlineMs;
    }

    private function nowMs(): int
    {
        return (int) floor(now()->getPreciseTimestamp(3));
    }

    /**
     * @param  array<int, array<string, mixed>>  $entries
     */
    private function reject(array $entries, string $reason, ?string $raw = null): bool
    {
        $line = FallbackFile::line($entries, $reason, $raw === null ? [] : ['raw' => $raw]);
        if (@file_put_contents($this->rejectedPath(), $line, FILE_APPEND | LOCK_EX) === false) {
            $this->error("Failed to write {$this->rejectedPath()}; stopping without skipping anything.");
            $this->failed = true;
            return false;
        }
        $this->rejected += count($entries);

        return true;
    }

    private function rejectedPath(): string
    {
        return $this->path . '.rejected';
    }

    private function checkpointPath(string $snapshot): string
    {
        return $snapshot . '.offset';
    }

    /**
     * @param  resource  $fh
     * @return array{0: int, 1: int} [смещение строки, сколько записей в ней уже отправлено]
     */
    private function readCheckpoint(string $snapshot, $fh, int $inode): array
    {
        $data = json_decode((string) @file_get_contents($this->checkpointPath($snapshot)), true);
        if (! is_array($data) || ($data['inode'] ?? null) !== $inode) {
            return [0, 0];
        }

        $offset = (int) ($data['offset'] ?? 0);
        $size = (int) (fstat($fh)['size'] ?? 0);
        // Смещение обязано указывать на начало строки этого же файла.
        $valid = $offset === 0
            || ($offset > 0 && $offset <= $size && fseek($fh, $offset - 1) === 0 && fgetc($fh) === "\n");

        return $valid ? [$offset, max(0, (int) ($data['skip'] ?? 0))] : [0, 0];
    }

    /**
     * @param  array{0: int, 1: int}  $position
     */
    private function saveCheckpoint(string $snapshot, int $inode, array $position): bool
    {
        $file = $this->checkpointPath($snapshot);
        $json = json_encode(['inode' => $inode, 'offset' => $position[0], 'skip' => $position[1]]);

        if (@file_put_contents($file . '.tmp', $json) === false || ! @rename($file . '.tmp', $file)) {
            $this->error("Failed to write {$file}; stopping (the last batch may be resent).");
            $this->failed = true;
            return false;
        }

        return true;
    }

    private function report(): void
    {
        $this->info(sprintf(
            'Flushed %d batches (%d entries); %s.',
            $this->sent,
            $this->sentEntries,
            $this->hasData($this->path) ? 'remainder kept for the next run' : 'fallback is empty',
        ));
        if ($this->rejected > 0) {
            $this->warn("Rejected {$this->rejected} entries — see {$this->rejectedPath()}.");
        }
        if ($this->malformed > 0) {
            $this->warn("Skipped {$this->malformed} malformed lines.");
        }
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
