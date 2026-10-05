<?php

namespace ProcessHub\Logs\Commands;

use Illuminate\Console\Command;
use Illuminate\Queue\Failed\DatabaseFailedJobProvider;
use Illuminate\Queue\Failed\DatabaseUuidFailedJobProvider;
use Illuminate\Queue\Failed\FailedJobProviderInterface;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\DB;
use ProcessHub\Logs\Jobs\SendLogBatchJob;
use ProcessHub\Logs\Support\FallbackFile;

/**
 * `php artisan processhub:forget-failed [--dry-run] [--to-fallback]`
 *
 * Deletes failed-job records of SendLogBatchJob only — other jobs' records
 * are never touched. `failed()` parks the entries in the fallback file, so
 * the records are usually just noise; `--to-fallback` first appends each
 * record's entries to the fallback file (once per job uuid, even when both
 * stores hold it) and keeps records it can't read or write out.
 *
 *   - `queue.failed` storage: the database drivers are scanned in id chunks
 *     straight from the configured table (hundreds of thousands of rows
 *     without loading them at once); any other driver goes through
 *     FailedJobProviderInterface (ids/all + find + forget).
 *   - Horizon (when installed): its failed list, through its JobRepository.
 *     Horizon also expires these records itself after `horizon.trim.failed`.
 */
class ForgetFailedCommand extends Command
{
    protected $signature = 'processhub:forget-failed
        {--dry-run : Only count the records that would be deleted}
        {--to-fallback : Append the entries of every deleted record to the fallback file first}';

    protected $description = 'Delete failed SendLogBatchJob records (failed_jobs and Horizon), leaving other failed jobs alone';

    private const CHUNK = 1000;

    private const HORIZON_REPOSITORY = 'Laravel\Horizon\Contracts\JobRepository';

    /** Page size of Horizon's JobRepository::getFailed(). */
    private const HORIZON_PAGE = 50;

    private bool $dryRun = false;

    private bool $toFallback = false;

    /** @var array<string, true> uuids of jobs whose entries are already in the fallback file */
    private array $exported = [];

    private int $kept = 0;

    private bool $failed = false;

    public function handle(): int
    {
        $this->dryRun = (bool) $this->option('dry-run');
        $this->toFallback = (bool) $this->option('to-fallback') && ! $this->dryRun;
        $this->exported = [];
        $this->kept = 0;
        $this->failed = false;

        if ($this->laravel->bound(FailedJobProviderInterface::class)) {
            $this->report('failed jobs storage', $this->forgetInFailedJobs($this->laravel->make(FailedJobProviderInterface::class)));
        }

        if (! $this->failed && $this->laravel->bound(self::HORIZON_REPOSITORY)) {
            $this->report('Horizon', $this->forgetInHorizon($this->laravel->make(self::HORIZON_REPOSITORY)));
        }

        if ($this->kept > 0) {
            $this->warn("Kept {$this->kept} records whose entries couldn't be read.");
        }
        if ($this->failed) {
            $this->error('Failed to write ' . config('processhub.fallback_path') . '; stopped, the remaining records are kept.');
        }

        return $this->failed ? self::FAILURE : self::SUCCESS;
    }

    /**
     * @return array{0: int, 1: int, 2: int} [scanned, matched, deleted]
     */
    private function forgetInFailedJobs(FailedJobProviderInterface $failer): array
    {
        return $failer instanceof DatabaseFailedJobProvider || $failer instanceof DatabaseUuidFailedJobProvider
            ? $this->forgetInTable()
            : $this->forgetViaProvider($failer);
    }

    /**
     * @return array{0: int, 1: int, 2: int} [scanned, matched, deleted]
     */
    private function forgetInTable(): array
    {
        $connection = DB::connection(config('queue.failed.database'));
        $table = (string) config('queue.failed.table', 'failed_jobs');
        $scanned = 0;
        $matched = 0;
        $deleted = 0;

        // chunkById идёт по `id > последний`, поэтому удаление внутри обхода безопасно.
        $connection->table($table)->select(['id', 'payload'])->chunkById(self::CHUNK, function (Collection $rows) use ($connection, $table, &$scanned, &$matched, &$deleted): bool {
            $ids = [];
            foreach ($rows as $row) {
                $scanned++;
                if (self::isOwnPayload($row->payload ?? null)) {
                    $matched++;
                    if ($this->release($row->payload)) {
                        $ids[] = $row->id;
                    }
                }
            }
            if (! $this->dryRun && $ids !== []) {
                $deleted += $connection->table($table)->whereIn('id', $ids)->delete();
            }

            return ! $this->failed;
        });

        return [$scanned, $matched, $deleted];
    }

    /**
     * @return array{0: int, 1: int, 2: int} [scanned, matched, deleted]
     */
    private function forgetViaProvider(FailedJobProviderInterface $failer): array
    {
        // ids() объявлен в интерфейсе только через @method — сторонние провайдеры могут его не иметь.
        // @phpstan-ignore function.alreadyNarrowedType
        $ids = method_exists($failer, 'ids')
            ? $failer->ids()
            : array_map(fn ($job) => data_get($job, 'id'), $failer->all());
        $scanned = 0;
        $matched = 0;
        $deleted = 0;

        foreach ($ids as $id) {
            $scanned++;
            $payload = data_get($failer->find($id), 'payload');
            if (! self::isOwnPayload($payload)) {
                continue;
            }
            $matched++;
            if ($this->release($payload) && ! $this->dryRun && $failer->forget($id)) {
                $deleted++;
            }
            if ($this->failed) {
                break;
            }
        }

        return [$scanned, $matched, $deleted];
    }

    /**
     * Pages through Horizon's failed list (newest first). Deleting shifts the
     * ranks after the page, so the next page starts that much earlier; new
     * failures arriving meanwhile only make a page be scanned again.
     *
     * @return array{0: int, 1: int, 2: int} [scanned, matched, deleted]
     */
    private function forgetInHorizon(object $jobs): array
    {
        $scanned = 0;
        $matched = 0;
        $deleted = 0;
        $after = -1;

        while (! $this->failed && $after + 1 < (int) $jobs->totalFailed()) {
            $deletedOnPage = 0;
            foreach ($jobs->getFailed($after) as $job) {
                $scanned++;
                $payload = $job->payload ?? null;
                if (($job->name ?? null) !== SendLogBatchJob::class && ! self::isOwnPayload($payload)) {
                    continue;
                }
                $matched++;
                if ($this->release($payload) && ! $this->dryRun && (int) $jobs->deleteFailed($job->id) > 0) {
                    $deletedOnPage++;
                }
            }
            $after += self::HORIZON_PAGE - $deletedOnPage;
            $deleted += $deletedOnPage;
        }

        return [$scanned, $matched, $deleted];
    }

    /**
     * May the record go? With --to-fallback only once its entries are
     * safely in the fallback file.
     */
    private function release(mixed $payload): bool
    {
        if (! $this->toFallback) {
            return true;
        }

        $decoded = is_string($payload) ? json_decode($payload, true) : null;
        $uuid = is_array($decoded) ? (string) ($decoded['uuid'] ?? $decoded['id'] ?? '') : '';
        if ($uuid !== '' && isset($this->exported[$uuid])) {
            return true;
        }

        $command = is_array($decoded)
            ? @unserialize((string) ($decoded['data']['command'] ?? ''), ['allowed_classes' => [SendLogBatchJob::class]])
            : null;
        if (! $command instanceof SendLogBatchJob) {
            $this->kept++;
            return false;
        }

        if (! FallbackFile::store(array_values(array_filter($command->entries, 'is_array')), 'failed job ' . $uuid)) {
            $this->failed = true;
            return false;
        }
        if ($uuid !== '') {
            $this->exported[$uuid] = true;
        }

        return true;
    }

    private static function isOwnPayload(mixed $payload): bool
    {
        $decoded = is_string($payload) ? json_decode($payload, true) : null;
        if (! is_array($decoded)) {
            return false;
        }

        return ($decoded['displayName'] ?? null) === SendLogBatchJob::class
            || ($decoded['data']['commandName'] ?? null) === SendLogBatchJob::class;
    }

    /**
     * @param  array{0: int, 1: int, 2: int}  $counts  [scanned, matched, deleted]
     */
    private function report(string $store, array $counts): void
    {
        $this->info(sprintf(
            '%s: %d records scanned, %d of %s, %s.',
            $store,
            $counts[0],
            $counts[1],
            SendLogBatchJob::class,
            $this->dryRun ? 'none deleted (dry run)' : $counts[2] . ' deleted',
        ));
    }
}
