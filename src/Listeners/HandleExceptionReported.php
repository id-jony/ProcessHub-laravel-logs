<?php

namespace ProcessHub\Logs\Listeners;

use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Support\Facades\Log;

/**
 * Auto-captures EVERY uncaught exception that flows through Laravel's
 * ExceptionHandler — the user doesn't need to wrap their code in
 * `Log::error(..., ['exception' => $e])` manually.
 *
 * Wires into `$handler->reportable(...)` which Laravel 8+ exposes on the
 * AbstractExceptionHandler. Returns `false` from the callback so the
 * default reporting chain (Sentry, Flare, etc.) still runs.
 *
 * Versions that don't support reportable() are a silent no-op.
 */
class HandleExceptionReported
{
    public static function register(ExceptionHandler $handler): void
    {
        if (! method_exists($handler, 'reportable')) {
            return;
        }

        $handler->reportable(function (\Throwable $e) {
            try {
                Log::channel('processhub')->error(
                    $e->getMessage() ?: get_class($e),
                    ['exception' => $e],
                );
            } catch (\Throwable) {
                // Observability MUST NOT break the app — if our own
                // channel throws, swallow and let default reporting run.
            }
            // Continue default reporting (don't suppress Sentry, etc.)
            return null;
        });
    }
}
