<?php

namespace ProcessHub\Logs\Commands;

use GuzzleHttp\Client;
use Illuminate\Console\Command;

/**
 * `php artisan processhub:test`
 *
 * Sends a synthetic ERROR to the ingest endpoint so the user can confirm
 * their token + URL are configured correctly and the network path works.
 * Does NOT go through the queue — a setup issue should surface immediately,
 * not six minutes later after a queue worker picks it up.
 */
class TestCommand extends Command
{
    protected $signature = 'processhub:test {--message= : Custom test message}';
    protected $description = 'Send a synthetic test event directly to ProcessHub';

    public function handle(): int
    {
        $url = config('processhub.url');
        $token = config('processhub.token');

        if (! $url) {
            $this->error('PROCESSHUB_LOG_URL is not set — check your .env');
            return self::FAILURE;
        }
        if (! $token) {
            $this->error('PROCESSHUB_LOG_TOKEN is not set — check your .env');
            return self::FAILURE;
        }

        $message = $this->option('message') ?: sprintf(
            'processhub:test from %s at %s',
            gethostname() ?: 'unknown-host',
            now()->toIso8601String(),
        );

        $client = new Client([
            'base_uri' => rtrim($url, '/'),
            'timeout' => 10,
            'http_errors' => false,
        ]);

        $this->line("POST {$url}/api/ingest/logs");

        try {
            $res = $client->post('/api/ingest/logs', [
                'headers' => [
                    'Authorization' => "Bearer {$token}",
                    'Content-Type' => 'application/json',
                ],
                'json' => [
                    'logs' => [[
                        'timestamp' => now()->toRfc3339String(),
                        'level' => 'ERROR',
                        'message' => $message,
                        'host' => gethostname() ?: null,
                        'contextType' => 'exception',
                        'context' => [
                            'class' => 'ProcessHub\\Logs\\TestEvent',
                            'message' => 'synthetic test via php artisan processhub:test',
                            'file' => __FILE__,
                            'line' => __LINE__,
                        ],
                    ]],
                ],
            ]);
        } catch (\Throwable $e) {
            $this->error('Network error: ' . $e->getMessage());
            return self::FAILURE;
        }

        $status = $res->getStatusCode();
        $body = (string) $res->getBody();
        $this->line("← HTTP {$status}");
        $this->line($body);

        if ($status >= 200 && $status < 300) {
            $this->info('Success! Check the application detail page in ProcessHub — the test event should appear on the Logs tab within a second.');
            return self::SUCCESS;
        }

        $this->error('Non-2xx response — the token/URL/rate-limit may be misconfigured.');
        return self::FAILURE;
    }
}
