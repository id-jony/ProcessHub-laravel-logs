<?php

namespace ProcessHub\Logs\Payouts\Observers;

use Illuminate\Database\Eloquent\Model;
use ProcessHub\Logs\Payouts\Payouts;

/**
 * Eloquent observer auto-registered against the client's payout model when
 * `processhub.payouts.observe_model_changes` is on (default — also flipped
 * by remote config when `payoutSource.cadence.mode in ['on-status-change', 'both']`).
 *
 * Strategy: queue a single-row push on every `updated()` and `created()`.
 * We deliberately *don't* narrow on `wasChanged('status_text')`:
 *
 *   - the column name varies per client (registered via the mapper, not the
 *     observer);
 *   - ProcessHub upserts by `gatewayPaymentId`, so a redundant push when only
 *     `updated_at` changed is harmless (server returns `accepted=0` skipped);
 *   - `WithoutOverlapping` middleware on the job collapses bursts within
 *     {@see \ProcessHub\Logs\Payouts\Limits::OBSERVER_DEDUP_TTL_SEC} —
 *     rapid-fire writes don't pile up worker time.
 *
 * Clients who *do* want finer granularity can disable observer-mode via
 * remote config and call `Payouts::queue($model)` from their own code.
 */
class PayoutsModelObserver
{
    public function created(Model $model): void
    {
        Payouts::queue($model);
    }

    public function updated(Model $model): void
    {
        Payouts::queue($model);
    }
}
