<?php

namespace ProcessHub\Logs\Config;

use GuzzleHttp\Client;
use Illuminate\Contracts\Config\Repository as ConfigRepository;
use Illuminate\Support\Facades\File;

/**
 * Client for the ProcessHub remote-config feature.
 *
 * Flow:
 *   1. On boot, `bootstrap()` loads the last-cached config from
 *      `storage/app/processhub-config.json` (if present) and merges selected
 *      fields into Laravel's runtime config bag. Existing package code that
 *      reads `config('processhub.batch_size')` automatically picks it up —
 *      we do NOT need to patch every listener / handler.
 *   2. HeartbeatCommand reads the `config.etag` from the heartbeat response
 *      and calls `refreshIfChanged($etag)` — a no-op when the cached etag
 *      matches, otherwise pulls `/api/ingest/config` (304-aware) and
 *      re-bootstraps.
 *   3. CLI commands `processhub:config:refresh` and `processhub:config:show`
 *      exercise this same path for debugging.
 *
 * Precedence rule (spec-confirmed: "remote > env > default"): when remote
 * config has a value, the runtime config bag gets it; if the operator wants
 * to PIN a value locally they must `config([...])` AFTER the merge — in
 * practice, using `.env` for deliberate overrides is expected to be paired
 * with a comment and a PR to the server config, not a runtime hack.
 *
 * The cache file is a simple JSON blob:
 *   { "version": 7, "etag": "...", "config": { ... } }
 * Read/write is non-atomic — we accept occasional stale reads during a
 * refresh race, the next heartbeat heals it.
 */
class RemoteConfigClient
{
    protected ConfigRepository $config;

    /** @var array<string, mixed>|null In-memory cache of the effective config. Null until bootstrap(). */
    protected ?array $effective = null;

    protected ?string $cachedEtag = null;
    protected ?int $cachedVersion = null;

    public function __construct(ConfigRepository $config)
    {
        $this->config = $config;
    }

    /** Path to the persistent cache file. Overridable in tests. */
    public function cachePath(): string
    {
        return storage_path('app/processhub-config.json');
    }

    /**
     * Load cached config (if any) and merge into the runtime config bag so
     * subsequent `config('processhub.*')` calls see remote values. Safe to
     * call multiple times — idempotent, just re-reads the file.
     */
    public function bootstrap(): void
    {
        $path = $this->cachePath();
        if (! File::exists($path)) {
            $this->effective = null;
            return;
        }
        try {
            $raw = File::get($path);
            $payload = json_decode($raw, true);
            if (! is_array($payload) || ! isset($payload['config']) || ! is_array($payload['config'])) {
                return;
            }
            $this->effective = $payload['config'];
            $this->cachedEtag = $payload['etag'] ?? null;
            $this->cachedVersion = $payload['version'] ?? null;
            $this->applyToRuntimeConfig($payload['config']);
        } catch (\Throwable $e) {
            // Don't crash boot on a corrupted cache file — the next
            // heartbeat will attempt a refresh.
            logger()->warning('processhub: remote config bootstrap failed', [
                'error' => $e->getMessage(),
            ]);
        }
    }

    public function cachedEtag(): ?string
    {
        return $this->cachedEtag;
    }

    public function cachedVersion(): ?int
    {
        return $this->cachedVersion;
    }

    /** Returns the full effective config (merged remote > defaults). */
    public function effective(): array
    {
        return $this->effective ?? [];
    }

    /**
     * If the heartbeat response's etag differs from our cache, re-pull the
     * full config and re-bootstrap. Returns true when a refresh happened.
     */
    public function refreshIfChanged(?string $newEtag): bool
    {
        if ($newEtag === null) {
            return false;
        }
        if ($this->cachedEtag !== null && $this->cachedEtag === $newEtag) {
            return false;
        }
        return $this->refresh();
    }

    /**
     * Unconditional pull of the latest config from the server.
     *
     * Uses the `If-None-Match` header so the server can reply 304 when
     * nothing actually changed (protects against a botched local cache
     * causing perpetual refetches).
     */
    public function refresh(): bool
    {
        $url = $this->config->get('processhub.url');
        $token = $this->config->get('processhub.token');
        if (! $url || ! $token) {
            return false;
        }

        $client = new Client([
            'base_uri' => rtrim($url, '/'),
            'timeout' => 5,
            'http_errors' => false,
        ]);

        $headers = [
            'Authorization' => "Bearer {$token}",
            'Accept' => 'application/json',
        ];
        if ($this->cachedEtag !== null) {
            $headers['If-None-Match'] = $this->cachedEtag;
        }

        try {
            $response = $client->get('/api/ingest/config', ['headers' => $headers]);
            $status = $response->getStatusCode();

            if ($status === 304) {
                // Server confirms nothing changed — reset nothing, keep cache.
                return false;
            }
            if ($status !== 200) {
                logger()->warning('processhub: config refresh non-200', [
                    'status' => $status,
                    'body' => (string) $response->getBody(),
                ]);
                return false;
            }

            $body = json_decode((string) $response->getBody(), true);
            if (! is_array($body) || ! isset($body['etag'], $body['config']) || ! is_array($body['config'])) {
                logger()->warning('processhub: config refresh bad payload');
                return false;
            }

            // Persist + re-bootstrap.
            $this->writeCache($body);
            $this->effective = $body['config'];
            $this->cachedEtag = $body['etag'];
            $this->cachedVersion = $body['version'] ?? null;
            $this->applyToRuntimeConfig($body['config']);
            return true;
        } catch (\Throwable $e) {
            logger()->warning('processhub: config refresh exception', [
                'error' => $e->getMessage(),
            ]);
            return false;
        }
    }

    /**
     * Maps server-side camelCase keys onto Laravel's snake_case config
     * paths. Only fields that the package code reads via `config('processhub.*')`
     * need to be mapped; others (e.g. `minLevel`, `sampleRate`) are read
     * from `effective()` directly by the Handler.
     */
    protected function applyToRuntimeConfig(array $remote): void
    {
        $map = [
            'batchSize' => 'processhub.batch_size',
            'httpTimeoutSeconds' => 'processhub.timeout_seconds',
            'heartbeatIntervalSeconds' => 'processhub.heartbeat_interval_seconds',
            'captureQueries' => 'processhub.listeners.query',
            'captureJobs' => 'processhub.listeners.job_failures',
            'captureMails' => 'processhub.listeners.mail',
            'captureScheduled' => 'processhub.listeners.scheduled_tasks',
            'slowQueryThresholdMs' => 'processhub.listeners.query_slow_ms',
            'enabled' => 'processhub.enabled',
        ];
        foreach ($map as $remoteKey => $configPath) {
            if (array_key_exists($remoteKey, $remote)) {
                $this->config->set($configPath, $remote[$remoteKey]);
            }
        }
        // Extra runtime-only keys read by the Handler directly.
        if (isset($remote['minLevel'])) {
            $this->config->set('processhub.min_level', $remote['minLevel']);
        }
        if (isset($remote['sampleRate'])) {
            $this->config->set('processhub.sample_rate', $remote['sampleRate']);
        }
        if (isset($remote['contextTypeWhitelist']) && is_array($remote['contextTypeWhitelist'])) {
            $this->config->set('processhub.context_type_whitelist', $remote['contextTypeWhitelist']);
        }
        if (isset($remote['redactionPatterns']) && is_array($remote['redactionPatterns'])) {
            // Appended to the default regex list, NOT replacing it — the
            // built-in masks (JWT, Bearer, email, card) are always active.
            $this->config->set('processhub.redact.remote_patterns', $remote['redactionPatterns']);
        }

    }

    protected function writeCache(array $payload): void
    {
        $path = $this->cachePath();
        $dir = dirname($path);
        if (! File::isDirectory($dir)) {
            File::makeDirectory($dir, 0755, true);
        }
        File::put(
            $path,
            json_encode($payload, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT),
        );
    }

    /** For `processhub:config:show` diagnostics — emits the RAW cache. */
    public function dumpCache(): ?array
    {
        $path = $this->cachePath();
        if (! File::exists($path)) {
            return null;
        }
        $raw = File::get($path);
        $decoded = json_decode($raw, true);
        return is_array($decoded) ? $decoded : null;
    }
}
