<?php

namespace ProcessHub\Logs\Payouts\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Str;
use InvalidArgumentException;
use ProcessHub\Logs\Payouts\IngestClient;
use ProcessHub\Logs\Payouts\Limits;
use ProcessHub\Logs\Payouts\PayoutsManager;
use ProcessHub\Logs\Payouts\PushResult;
use ProcessHub\Logs\Payouts\WatermarkStore;

/**
 * `php artisan processhub:payouts:push`
 *
 * Walks the registered model in `id`-ascending order from the high-watermark
 * forward, batching by {@see Limits::BATCH_SIZE}, posting to
 * `/api/ingest/payouts`, advancing the watermark on each successful 200.
 *
 * Bootstrap (first run, watermark = null) sweeps the full history page-by-
 * page; partial-failure is safe because ProcessHub upserts by
 * `gatewayPaymentId` — re-sent rows are idempotent.
 *
 * Exit codes:
 *   - SUCCESS — nothing to do, or all batches accepted.
 *   - FAILURE — a 4xx (other than 429) was returned mid-run. We stop and
 *     leave the watermark on the last successful batch so the operator can
 *     inspect the row that broke things; the next cron tick will resume
 *     from there once the operator clears the cause on the ProcessHub side
 *     (validation rule fix, token rotation, etc).
 *
 * Failure is intentionally surfaced as FAILURE (not silenced like heartbeat)
 * so the scheduler email / ProcessHub `ScheduledTaskFailed` listener picks
 * it up — payouts are billing-grade, not best-effort.
 */
class PushPayoutsCommand extends Command
{
    protected $signature = 'processhub:payouts:push {--verbose-output : Print per-batch progress}';
    protected $description = 'Incrementally push registered payout rows to ProcessHub';

    public function handle(
        PayoutsManager $manager,
        IngestClient $client,
        WatermarkStore $store,
    ): int {
        $url = config('processhub.url');
        $token = config('processhub.token');
        if (! $url || ! $token) {
            // Not configured; behave like heartbeat — silent success.
            return self::SUCCESS;
        }
        if (! config('processhub.payouts.enabled', true)) {
            return self::SUCCESS;
        }

        $source = $manager->source();
        if ($source === null) {
            // Operator installed the package but hasn't wired
            // `Payouts::register(...)` yet. INFO-log once per tick so the
            // ops dashboard sees the gap; silent SUCCESS otherwise so the
            // scheduler doesn't flap.
            logger()->info('processhub:payouts: no source registered, skipping');
            return self::SUCCESS;
        }

        $watermark = $store->get();
        if ($watermark === null) {
            // Use server-provided hint when local cache is empty — protects
            // against `cache:clear` resetting us to a full re-bootstrap.
            $hint = config('processhub.payouts.server_watermark');
            if (is_string($hint) && $hint !== '') {
                $watermark = $hint;
            }
        }

        $query = $source->model::query();
        if ($source->query !== null) {
            ($source->query)($query);
        }
        if ($watermark !== null) {
            $query->where('id', '>', $watermark);
        }
        $query->orderBy('id');

        $verbose = (bool) $this->option('verbose-output');
        $batch = [];
        $batchSeq = 0;
        $totalAccepted = 0;
        $totalSkipped = 0;

        foreach ($query->cursor() as $model) {
            try {
                $batch[] = $manager->mapOne($model);
            } catch (InvalidArgumentException $e) {
                // Local mapper produced an invalid row. Skip it (server would
                // 400 the whole batch otherwise) and continue — operator
                // sees the warning in logs.
                logger()->warning('processhub:payouts: bad row, skipped', [
                    'modelId' => $model->getKey(),
                    'error' => $e->getMessage(),
                ]);
                continue;
            }
            if (count($batch) >= Limits::BATCH_SIZE) {
                $result = $this->flushBatch($client, $batch, ++$batchSeq, $verbose);
                if (! $result->ok) {
                    return self::FAILURE;
                }
                if ($result->watermark !== null) {
                    $store->set($result->watermark);
                }
                $totalAccepted += $result->accepted;
                $totalSkipped += count($result->skipped);
                $batch = [];
            }
        }

        if (count($batch) > 0) {
            $result = $this->flushBatch($client, $batch, ++$batchSeq, $verbose);
            if (! $result->ok) {
                return self::FAILURE;
            }
            if ($result->watermark !== null) {
                $store->set($result->watermark);
            }
            $totalAccepted += $result->accepted;
            $totalSkipped += count($result->skipped);
        }

        if ($verbose) {
            $this->info("processhub:payouts: accepted={$totalAccepted} skipped={$totalSkipped} batches={$batchSeq}");
        }
        return self::SUCCESS;
    }

    /**
     * Push one batch with size-aware halving: if ProcessHub responds
     * PAYLOAD_TOO_LARGE (413), split and retry. Repeated up to 3 levels;
     * beyond that a single row is too large on its own — surface failure.
     */
    private function flushBatch(IngestClient $client, array $batch, int $seq, bool $verbose, int $depth = 0): PushResult
    {
        $batchId = (string) Str::uuid();
        if ($verbose) {
            $count = count($batch);
            $this->line("  → batch #{$seq} ({$count} rows) [{$batchId}]");
        }
        $result = $client->push($batch, $batchId);
        if ($result->error === 'PAYLOAD_TOO_LARGE' && count($batch) > 1 && $depth < 3) {
            $half = (int) ceil(count($batch) / 2);
            $first = array_slice($batch, 0, $half);
            $second = array_slice($batch, $half);
            $r1 = $this->flushBatch($client, $first, $seq, $verbose, $depth + 1);
            if (! $r1->ok) {
                return $r1;
            }
            $r2 = $this->flushBatch($client, $second, $seq, $verbose, $depth + 1);
            if (! $r2->ok) {
                return $r2;
            }
            // Combine: take later watermark, sum accepted, merge skipped.
            return new PushResult(
                true,
                $r1->accepted + $r2->accepted,
                array_merge($r1->skipped, $r2->skipped),
                $r2->watermark ?? $r1->watermark,
                null,
            );
        }
        return $result;
    }
}
