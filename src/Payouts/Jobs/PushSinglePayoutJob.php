<?php

namespace ProcessHub\Logs\Payouts\Jobs;

use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\Middleware\WithoutOverlapping;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Str;
use InvalidArgumentException;
use ProcessHub\Logs\Payouts\IngestClient;
use ProcessHub\Logs\Payouts\Limits;
use ProcessHub\Logs\Payouts\PayoutsManager;

/**
 * Queued single-row push, dispatched by {@see PayoutsModelObserver} on every
 * model `updated()`/`created()`.
 *
 * Why a job (not a sync HTTP call inside the observer):
 *   - keeps the original UPDATE transaction snappy — clients on legacy DBs
 *     can't afford a 30s network blip blocking the gateway request;
 *   - `WithoutOverlapping` middleware naturally dedupes rapid-fire writes on
 *     the same row;
 *   - retries / backoff are encapsulated in {@see IngestClient}, the job
 *     itself doesn't need `$tries > 1`.
 *
 * Watermark is **NOT** updated here on purpose — observer-driven pushes are
 * "speculative" delivery. The cron command remains the single source of
 * truth for "we have ingested through row N". Mixing the two would let a
 * lucky observer push skip rows the cron path would catch.
 */
class PushSinglePayoutJob implements ShouldQueue
{
    use Dispatchable;
    use InteractsWithQueue;
    use Queueable;
    use SerializesModels;

    /** No queue-level retries — IngestClient retries internally on 429/5xx. */
    public int $tries = 1;
    public int $timeout = 60;

    public function __construct(public Model $model) {}

    /** @return list<WithoutOverlapping> */
    public function middleware(): array
    {
        $key = "payouts:push:single:{$this->model->getKey()}";
        $mw = new WithoutOverlapping($key);
        $mw->dontRelease();
        $mw->expireAfter(Limits::OBSERVER_DEDUP_TTL_SEC);
        return [$mw];
    }

    public function handle(PayoutsManager $manager, IngestClient $client): void
    {
        try {
            $row = $manager->mapOne($this->model);
        } catch (InvalidArgumentException $e) {
            logger()->warning('processhub:payouts: bad row in observer push, skipped', [
                'modelId' => $this->model->getKey(),
                'error' => $e->getMessage(),
            ]);
            return;
        }
        $client->push([$row], (string) Str::uuid());
    }
}
