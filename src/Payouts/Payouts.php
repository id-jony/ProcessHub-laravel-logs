<?php

namespace ProcessHub\Logs\Payouts;

use Illuminate\Support\Facades\Facade;

/**
 * Facade for the Payouts module.
 *
 *     use ProcessHub\Logs\Payouts\Payouts;
 *
 *     Payouts::register(Payment::class, fn ($p) => [...], fn ($q) => $q->where('type', 'PR'));
 *     Payouts::push($payment);   // sync POST
 *     Payouts::queue($payment);  // → Job
 *     Payouts::flush();          // run the incremental sync now
 *
 * Resolution goes through the `processhub.payouts` container alias so the
 * facade survives `app()->forgetInstance(...)` in tests (we re-bind the
 * singleton without losing the accessor).
 *
 * @method static void register(string $model, \Closure $map, ?\Closure $query = null)
 * @method static ?\ProcessHub\Logs\Payouts\PayoutSourceConfig source()
 * @method static void forget()
 * @method static \ProcessHub\Logs\Payouts\PushResult push(\Illuminate\Database\Eloquent\Model $model)
 * @method static void queue(\Illuminate\Database\Eloquent\Model $model)
 * @method static int flush()
 * @method static array<string, mixed> mapOne(\Illuminate\Database\Eloquent\Model $model)
 */
class Payouts extends Facade
{
    protected static function getFacadeAccessor(): string
    {
        return 'processhub.payouts';
    }
}
