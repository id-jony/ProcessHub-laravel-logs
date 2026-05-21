<?php

namespace ProcessHub\Logs\Payouts;

use Closure;
use Illuminate\Container\Container;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Str;
use InvalidArgumentException;
use ProcessHub\Logs\Payouts\Jobs\PushSinglePayoutJob;

/**
 * Central registry + façade implementation for the Payouts module.
 *
 * The client app calls `Payouts::register(Model::class, fn ($p) => [...])`
 * once at boot, then everything else (cron command, observer, job) reaches
 * back here through DI / the facade accessor `processhub.payouts`.
 *
 * Why a manager (not statics): we need DI-friendly state for tests
 * (`config()->set()` overrides, `Queue::fake()`, MockHandler swaps) and we
 * need it scoped to the application container so multi-tenant test
 * scenarios in Testbench don't bleed registration across cases.
 *
 * What this class does *not* do:
 *   - HTTP. That's {@see IngestClient}.
 *   - Cursor / batching loop. That's {@see Commands\PushPayoutsCommand}.
 *   - Watermark IO. That's {@see WatermarkStore}.
 *
 * It only resolves "from a registered model, what does ProcessHub want?"
 * and shells out push/queue/flush to the right collaborator.
 */
final class PayoutsManager
{
    private ?PayoutSourceConfig $source = null;

    /**
     * Register the client's payout model. Idempotent — calling twice
     * silently replaces the previous registration; useful in tests, harmless
     * in production where it would only happen via a hot-reload race.
     *
     * @param class-string<Model> $model
     */
    public function register(string $model, Closure $map, ?Closure $query = null): void
    {
        $this->source = new PayoutSourceConfig($model, $map, $query);
    }

    public function source(): ?PayoutSourceConfig
    {
        return $this->source;
    }

    /** Forget the registration. Test helper; not exposed via the facade by convention. */
    public function forget(): void
    {
        $this->source = null;
    }

    /**
     * Synchronously push a single model. Returns whatever IngestClient::push
     * returned — the caller can decide whether to surface the result. Errors
     * never throw (network is best-effort), so a boolean is enough.
     */
    public function push(Model $model): PushResult
    {
        try {
            $row = $this->mapOne($model);
        } catch (InvalidArgumentException $e) {
            logger()->warning('processhub:payouts: bad row in manual push, skipped', [
                'modelId' => $model->getKey(),
                'error' => $e->getMessage(),
            ]);
            return new PushResult(false, 0, [], null, 'BAD_ROW');
        }
        $client = Container::getInstance()->make(IngestClient::class);
        return $client->push([$row], (string) Str::uuid());
    }

    /**
     * Queue a single-row push. Returns void — the job picks up later. We
     * skip mapping here on purpose: the job re-fetches state at handle time
     * via the serialised model, which avoids stale snapshots if the row was
     * updated again between dispatch and handle.
     */
    public function queue(Model $model): void
    {
        if ($this->source === null) {
            // Observer race: registered model fired before AppServiceProvider
            // re-registered (e.g. config:cache + worker boot order). Silent —
            // cron will catch up.
            return;
        }
        PushSinglePayoutJob::dispatch($model)
            ->onConnection(config('processhub.payouts.connection'))
            ->onQueue(config('processhub.payouts.queue', 'default'));
    }

    /**
     * Run the full incremental sync. Used by the cron command; exposed on
     * the manager so test code (and future "force-flush now" admin actions)
     * can call it without spawning an artisan subprocess.
     */
    public function flush(): int
    {
        return Container::getInstance()
            ->make(\Illuminate\Contracts\Console\Kernel::class)
            ->call('processhub:payouts:push');
    }

    /**
     * Apply the registered mapper and validate the resulting row against
     * the wire contract. Throws InvalidArgumentException with a precise
     * field name so the caller can log and skip the single row without
     * killing the whole batch.
     *
     * Validation is intentionally lightweight — type/required-field only,
     * no value-range checks. ProcessHub validates business rules itself
     * (formula engine, status mapping); the package just enforces "shape so
     * it doesn't 400 immediately."
     *
     * @return array<string, mixed>
     */
    public function mapOne(Model $model): array
    {
        if ($this->source === null) {
            throw new InvalidArgumentException('No payout source registered. Call Payouts::register(...) in AppServiceProvider::boot.');
        }
        $row = ($this->source->map)($model);
        if (! is_array($row)) {
            throw new InvalidArgumentException('Payout mapper must return an array, got '.gettype($row));
        }

        // Required scalars — ProcessHub's zod schema rejects null on these.
        $requireString = ['gatewayPaymentId', 'paymentCreatedAt', 'rawStatus', 'grossAmount'];
        foreach ($requireString as $key) {
            if (! array_key_exists($key, $row) || ! is_string($row[$key]) || $row[$key] === '') {
                throw new InvalidArgumentException("Required field `{$key}` must be a non-empty string");
            }
        }
        $requireBool = ['isCompleted', 'isFatalError'];
        foreach ($requireBool as $key) {
            if (! array_key_exists($key, $row) || ! is_bool($row[$key])) {
                throw new InvalidArgumentException("Required field `{$key}` must be a boolean");
            }
        }

        // grossAmount must look like a decimal — server side uses string-based
        // big-decimal math, so "12,50" or "1.5e3" would silently corrupt totals.
        if (! preg_match('/^-?\d+(\.\d+)?$/', $row['grossAmount'])) {
            throw new InvalidArgumentException("Field `grossAmount` must match /^-?\\d+(\\.\\d+)?\$/, got `{$row['grossAmount']}`");
        }

        // paymentCreatedAt — ISO-8601-ish. We accept anything strtotime understands;
        // server-side zod does the strict parse. Reject obvious garbage early.
        if (strtotime($row['paymentCreatedAt']) === false) {
            throw new InvalidArgumentException("Field `paymentCreatedAt` must be ISO-8601, got `{$row['paymentCreatedAt']}`");
        }

        // Optional fields — normalise to expected types. rawData defaults to
        // an empty object so server doesn't fall through to null branches.
        if (isset($row['rawData']) && ! is_array($row['rawData'])) {
            throw new InvalidArgumentException('Field `rawData` must be an array when provided');
        }
        if (! array_key_exists('rawData', $row)) {
            $row['rawData'] = (object) [];
        }

        return $row;
    }
}
