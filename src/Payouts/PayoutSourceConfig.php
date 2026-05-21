<?php

namespace ProcessHub\Logs\Payouts;

use Closure;
use Illuminate\Database\Eloquent\Model;

/**
 * Immutable description of a client's registered payout model.
 *
 * Created by {@see PayoutsManager::register()} and consumed by every layer
 * downstream (command, observer, job). Holding it in a typed value object
 * keeps the manager free of associative-array juggling and lets PHPStan see
 * the contract.
 *
 * - `$model`  — FQCN of the Eloquent model the client wants observed/pushed.
 * - `$map`    — `fn(Model): array<string,mixed>` mapping a single row to the
 *               wire shape expected by ProcessHub (see docs/16, "rows[]").
 * - `$query`  — optional `fn(Builder): void` that scopes the model query for
 *               the cron pull (e.g. `->where('type', 'PR')`). Not applied
 *               for observer-driven pushes — by the time observer fires the
 *               operator has already accepted the row as in-scope.
 *
 * @phpstan-type Mapper Closure(Model): array<string, mixed>
 */
final class PayoutSourceConfig
{
    /**
     * @param class-string<Model> $model
     * @param Closure $map  Mapper closure — typed loosely so PHPStan doesn't
     *                      force callers to declare the exact Eloquent subclass.
     * @param Closure|null $query
     */
    public function __construct(
        public readonly string $model,
        public readonly Closure $map,
        public readonly ?Closure $query = null,
    ) {}
}
