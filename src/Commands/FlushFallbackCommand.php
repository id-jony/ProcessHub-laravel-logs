<?php

namespace ProcessHub\Logs\Commands;

use GuzzleHttp\Client;
use Illuminate\Console\Command;

/**
 * `php artisan processhub:flush-fallback`
 *
 * Reads the fallback file (where SendLogBatchJob::failed appends batches
 * that ProcessHub refused) and tries to re-ingest them. Each line is a
 * JSON payload with the original `entries` array.
 *
 * Safe-to-run-multiple-times — processed lines are truncated from the
 * file only after a successful POST. On any failure we put the line back.
 */
class FlushFallbackCommand extends Command
{
    protected $signature = 'processhub:flush-fallback';
    protected $description = 'Re-ingest batches previously saved to the fallback file';

    public function handle(): int
    {
        $path = config('processhub.fallback_path');
        if (! $path || ! is_file($path) || filesize($path) === 0) {
            $this->line('Nothing to flush — fallback file is empty or missing.');
            return self::SUCCESS;
        }

        $url = config('processhub.url');
        $token = config('processhub.token');
        if (! $url || ! $token) {
            $this->error('PROCESSHUB_LOG_URL / _TOKEN not configured — cannot flush.');
            return self::FAILURE;
        }

        $client = new Client([
            'base_uri' => rtrim($url, '/'),
            'timeout' => 10,
            'http_errors' => false,
        ]);

        $lines = file($path, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
        if ($lines === false) {
            $this->error('Failed to read fallback file.');
            return self::FAILURE;
        }

        $remaining = [];
        $ok = 0;
        $fail = 0;

        foreach ($lines as $line) {
            $payload = json_decode($line, true);
            $entries = is_array($payload) && isset($payload['entries'])
                ? $payload['entries']
                : null;
            if (! $entries) continue;

            $res = $client->post('/api/ingest/logs', [
                'headers' => [
                    'Authorization' => "Bearer {$token}",
                    'Content-Type' => 'application/json',
                ],
                'json' => ['logs' => $entries],
            ]);
            if ($res->getStatusCode() >= 200 && $res->getStatusCode() < 300) {
                $ok++;
            } else {
                $remaining[] = $line;
                $fail++;
            }
        }

        // Rewrite the file with only unprocessed lines.
        if (empty($remaining)) {
            @unlink($path);
        } else {
            file_put_contents($path, implode("\n", $remaining) . "\n", LOCK_EX);
        }

        $this->info("Flushed {$ok} batches; {$fail} still pending.");
        return self::SUCCESS;
    }
}
