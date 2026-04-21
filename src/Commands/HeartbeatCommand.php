<?php

namespace ProcessHub\Logs\Commands;

use GuzzleHttp\Client;
use Illuminate\Console\Command;
use ProcessHub\Logs\Config\RemoteConfigClient;

/**
 * `php artisan processhub:heartbeat`
 *
 * Registered in the scheduler to run every minute. Posts to
 * /api/ingest/heartbeat with version + uptime so ProcessHub knows the app
 * is alive. Status flips to OFFLINE after 3 missed heartbeats.
 *
 * Failures are logged via Laravel's default logger (warning) and, when
 * `--verbose-output` is passed, printed to the console for diagnostics.
 * The command always returns SUCCESS so a transient network blip doesn't
 * bubble up as a scheduler failure.
 */
class HeartbeatCommand extends Command
{
    protected $signature = 'processhub:heartbeat {--verbose-output : Print response status and body}';
    protected $description = 'Ping ProcessHub heartbeat endpoint (called by scheduler)';

    /** @var float Process start time, captured once at boot for uptime. */
    protected static float $bootedAt;

    public static function markBootedNow(): void
    {
        self::$bootedAt = microtime(true);
    }

    public function handle(RemoteConfigClient $configClient): int
    {
        $url = config('processhub.url');
        $token = config('processhub.token');
        if (! $url || ! $token) {
            // Not configured — stay silent.
            return self::SUCCESS;
        }

        $client = new Client([
            'base_uri' => rtrim($url, '/'),
            'timeout' => 5,
            'http_errors' => false,
        ]);

        // Strip nulls — server's zod schema uses .optional() which accepts
        // missing keys but NOT explicit null. Sending {"version":null} → 400.
        $payload = array_filter([
            'version' => config('app.version'),
            'uptime' => isset(self::$bootedAt)
                ? (int) (microtime(true) - self::$bootedAt)
                : null,
        ], fn ($v) => $v !== null);

        $verbose = (bool) $this->option('verbose-output');

        try {
            $response = $client->post('/api/ingest/heartbeat', [
                'headers' => [
                    'Authorization' => "Bearer {$token}",
                    'Content-Type' => 'application/json',
                ],
                // Cast to object so an empty payload serialises as "{}", not "[]".
                'json' => (object) $payload,
            ]);

            $status = $response->getStatusCode();
            $body = (string) $response->getBody();

            if ($verbose) {
                $this->line("Status: {$status}");
                $this->line($body);
            }

            if ($status >= 400) {
                logger()->warning('processhub:heartbeat failed', [
                    'status' => $status,
                    'body' => $body,
                ]);
            } elseif ($status === 200) {
                // Parse config etag from response; pull a fresh config when
                // it differs from our cache. Don't let a malformed body
                // break heartbeat — just skip the refresh step on parse fail.
                try {
                    $decoded = json_decode($body, true);
                    $remoteEtag = $decoded['config']['etag'] ?? null;
                    if (is_string($remoteEtag)) {
                        $changed = $configClient->refreshIfChanged($remoteEtag);
                        if ($changed && $verbose) {
                            $this->info('Remote config refreshed (v'.$configClient->cachedVersion().').');
                        }
                    }
                } catch (\Throwable $parseErr) {
                    logger()->warning('processhub:heartbeat config parse failed', [
                        'error' => $parseErr->getMessage(),
                    ]);
                }
            }
        } catch (\Throwable $e) {
            if ($verbose) {
                $this->error($e->getMessage());
            }
            logger()->warning('processhub:heartbeat exception', [
                'error' => $e->getMessage(),
            ]);
        }

        return self::SUCCESS;
    }
}
