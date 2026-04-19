<?php

namespace ProcessHub\Logs\Commands;

use GuzzleHttp\Client;
use Illuminate\Console\Command;

/**
 * `php artisan processhub:heartbeat`
 *
 * Registered in the scheduler to run every minute. Posts to
 * /api/ingest/heartbeat with version + uptime so ProcessHub knows the app
 * is alive. Status flips to OFFLINE after 3 missed heartbeats.
 *
 * Failures are intentionally silent — a blip in the monitoring path
 * shouldn't spam the app's own error log.
 */
class HeartbeatCommand extends Command
{
    protected $signature = 'processhub:heartbeat';
    protected $description = 'Ping ProcessHub heartbeat endpoint (called by scheduler)';

    /** @var float Process start time, captured per-command for uptime. */
    protected static float $bootedAt;

    public static function markBootedNow(): void
    {
        self::$bootedAt = microtime(true);
    }

    public function handle(): int
    {
        $url = config('processhub.url');
        $token = config('processhub.token');
        if (! $url || ! $token) {
            // Not configured — stay silent.
            return self::SUCCESS;
        }

        $client = new Client([
            'base_uri' => rtrim($url, '/'),
            'timeout' => 3,
            'http_errors' => false,
        ]);

        try {
            $client->post('/api/ingest/heartbeat', [
                'headers' => [
                    'Authorization' => "Bearer {$token}",
                    'Content-Type' => 'application/json',
                ],
                'json' => [
                    'version' => config('app.version', null),
                    'uptime' => isset(self::$bootedAt)
                        ? (int) (microtime(true) - self::$bootedAt)
                        : null,
                ],
            ]);
        } catch (\Throwable) {
            // swallow — next tick will try again
        }

        return self::SUCCESS;
    }
}
