<?php

namespace ProcessHub\Logs\Listeners;

use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Support\Facades\Log;
use ProcessHub\Logs\Jobs\SendLogBatchJob;
use ProcessHub\Logs\Logging\LogBuffer;
use ProcessHub\Logs\Logging\ProcessHubFactory;
use Symfony\Component\ErrorHandler\Error\FatalError;

/**
 * Auto-captures EVERY uncaught exception that flows through Laravel's
 * ExceptionHandler — the user doesn't need to wrap their code in
 * `Log::error(..., ['exception' => $e])` manually.
 *
 * Wires into `$handler->reportable(...)` (Laravel 8+). The provider registers
 * it while the handler is being resolved, so in apps configured through
 * `bootstrap/app.php` (`withExceptions`) it runs before the app's own
 * callbacks (Sentry's Integration::handles() among them):
 *   - failures of SendLogBatchJob itself stop reporting here (`false`) —
 *     neither Sentry nor the default log sees them;
 *   - a FatalError first gets memory back (LogBuffer::flushOnShutdown()),
 *     before anything else allocates;
 *   - everything else continues down the chain (`null`).
 *
 * When the default log channel already includes processhub, Laravel's own
 * report log ships the exception (with its `context()` and the handler's
 * context); otherwise it is logged here.
 */
class HandleExceptionReported
{
    public static function register(ExceptionHandler $handler): void
    {
        if (! method_exists($handler, 'reportable')) {
            return;
        }

        $buffer = app(LogBuffer::class);

        $handler->reportable(static function (\Throwable $e) use ($buffer): ?bool {
            if (SendLogBatchJob::isOwnFailure($e)) {
                return false;
            }

            if ($e instanceof FatalError) {
                $buffer->flushOnShutdown();
            }

            try {
                if (! self::defaultLogReachesProcessHub()) {
                    Log::channel('processhub')->error(
                        $e->getMessage() ?: get_class($e),
                        self::exceptionContext($e) + ['exception' => $e],
                    );
                }
            } catch (\Throwable) {
                // Observability MUST NOT break the app.
            }

            return null;
        });
    }

    /**
     * A fatal error Laravel didn't get to log (its report path failed or
     * skipped it) — ship it from error_get_last(). Called from the package's
     * last shutdown function, after LogBuffer::flushOnShutdown().
     */
    public static function reportMissedFatalError(LogBuffer $buffer): void
    {
        $error = error_get_last();
        if ($error === null
            || ! in_array($error['type'], [E_ERROR, E_PARSE, E_CORE_ERROR, E_COMPILE_ERROR], true)
            || $buffer->hasFatalError()
        ) {
            return;
        }

        try {
            Log::channel('processhub')->error($error['message'], [
                'exception' => new FatalError($error['message'], 0, $error, 0),
            ]);
        } catch (\Throwable) {
            // Nothing sensible left to do at shutdown.
        }
    }

    /**
     * Is processhub the default channel or part of the default stack?
     */
    private static function defaultLogReachesProcessHub(): bool
    {
        $pending = [config('logging.default')];
        $seen = [];

        while ($pending !== []) {
            $name = array_pop($pending);
            if (! is_string($name) || isset($seen[$name])) {
                continue;
            }
            $seen[$name] = true;

            $config = config('logging.channels.' . $name);
            if ($name === 'processhub'
                || (is_array($config) && is_string($config['via'] ?? null) && is_a($config['via'], ProcessHubFactory::class, true))
            ) {
                return true;
            }

            if (is_array($config) && ($config['driver'] ?? null) === 'stack') {
                $channels = $config['channels'] ?? [];
                array_push($pending, ...(is_string($channels) ? explode(',', $channels) : (array) $channels));
            }
        }

        return false;
    }

    /**
     * Same as Laravel's Handler::exceptionContext().
     *
     * @return array<string, mixed>
     */
    private static function exceptionContext(\Throwable $e): array
    {
        if (! method_exists($e, 'context')) {
            return [];
        }

        try {
            return (array) $e->context();
        } catch (\Throwable) {
            return [];
        }
    }
}
