<?php

namespace ProcessHub\Logs;

use GuzzleHttp\Client;
use GuzzleHttp\Exception\ClientException;

/**
 * Real implementation behind the `ProcessHub` facade.
 *
 * Lives in its OWN file so PSR-4 autoloading can find it via direct DI
 * (e.g. `public function handle(ProcessHubManager $manager)` in an artisan
 * command). When this class was co-located with the facade in
 * `src/ProcessHub.php`, container resolution by class name failed with
 * `BindingResolutionException: Target class does not exist` — Composer
 * only autoloads the class whose name matches the file name. The facade
 * still works either way (it lazily loads `ProcessHub.php` which used to
 * define both classes side-by-side), but direct typed injection didn't.
 *
 * Use either:
 *
 *     ProcessHub::markDeploy('v1.4.2', $sha);          // facade
 *     // or
 *     public function handle(ProcessHubManager $m) {   // DI
 *         $m->markDeploy('v1.4.2', $sha);
 *     }
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
