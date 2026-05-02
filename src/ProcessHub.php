<?php

namespace ProcessHub\Logs;

use Illuminate\Support\Facades\Facade;

/**
 * `ProcessHub::markDeploy(...)` — convenience facade so CI scripts and
 * deploy pipelines don't have to hand-roll curl:
 *
 *     ProcessHub::markDeploy('v1.4.2', env('GITHUB_SHA'));
 *
 *     # Or via the artisan command (which uses DI under the hood):
 *     php artisan processhub:deploy v1.4.2 --commit="$SHA"
 *
 * The real implementation lives in {@see ProcessHubManager} (separate file
 * so PSR-4 autoloading resolves it for typed DI in commands/jobs/listeners).
 *
 * @method static bool markDeploy(string $version, ?string $commitSha = null, bool $success = true, ?array $metadata = null)
 */
class ProcessHub extends Facade
{
    protected static function getFacadeAccessor(): string
    {
        return ProcessHubManager::class;
    }
}
