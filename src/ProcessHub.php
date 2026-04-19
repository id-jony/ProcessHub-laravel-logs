<?php

namespace ProcessHub\Logs;

use GuzzleHttp\Client;
use GuzzleHttp\Exception\ClientException;
use Illuminate\Support\Facades\Facade;

/**
 * `ProcessHub::markDeploy(...)` — convenience facade so CI scripts and
 * deploy pipelines don't have to hand-roll curl:
 *
 *     Artisan::call('deploy:marker', [
 *         'version' => 'v1.4.2',
 *         'commit'  => env('GITHUB_SHA'),
 *     ]);
 *
 *     // in a command:
 *     ProcessHub::markDeploy('v1.4.2', env('GITHUB_SHA'));
 *
 * Implemented as a regular class accessed through the facade to keep the
 * surface tiny — a full Manager with DI is overkill for one call.
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

/**
 * Real implementation behind the facade. Kept in the same file so a freshly
 * `composer require`'d app can use `ProcessHub::markDeploy()` without
 * needing to locate two classes.
 */
class ProcessHubManager
{
    /**
     * Post a deploy marker to ProcessHub. Returns true on 2xx, false otherwise.
     * Never throws — CI should not fail because observability is unreachable.
     *
     * @param array<string, mixed>|null $metadata arbitrary JSON-serialisable
     */
    public function markDeploy(
        string $version,
        ?string $commitSha = null,
        bool $success = true,
        ?array $metadata = null,
    ): bool {
        $url = config('processhub.url');
        $token = config('processhub.token');
        if (! $url || ! $token) return false;

        $client = new Client([
            'base_uri' => rtrim($url, '/'),
            'timeout' => 10,
            'http_errors' => false,
        ]);

        try {
            $res = $client->post('/api/ingest/deploy', [
                'headers' => [
                    'Authorization' => "Bearer {$token}",
                    'Content-Type' => 'application/json',
                ],
                'json' => array_filter([
                    'version' => $version,
                    'commitSha' => $commitSha,
                    'success' => $success,
                    'metadata' => $metadata,
                ], static fn ($v) => $v !== null),
            ]);
            return $res->getStatusCode() >= 200 && $res->getStatusCode() < 300;
        } catch (ClientException | \Throwable) {
            return false;
        }
    }
}
