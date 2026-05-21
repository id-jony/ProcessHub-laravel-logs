# Changelog

All notable changes to `processhub/laravel-logs` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] — 2026-05-21

### Added

- **Payouts module** — ship payment rows from the host app to the ProcessHub
  Payouts ingest endpoint (`POST /api/ingest/payouts`). The same
  `PROCESSHUB_LOG_TOKEN` covers it, no extra credentials.
  - `Payouts::register(Model::class, fn ($m) => [...], ?fn ($q) => ...)`
    facade for one-line wiring in `AppServiceProvider::boot`.
  - `processhub:payouts:push` artisan command — incremental cursor-based
    sync from a file-backed high-watermark, auto-scheduled by the service
    provider using `payouts.default_cron` (overridden at runtime by
    `payoutSource.cadence.cronExpr` from the heartbeat-config refresh).
  - Eloquent observer (`PayoutsModelObserver`) auto-registered against the
    registered model when `payoutSource.cadence.mode` ∈
    `['on-status-change', 'both']`. Dispatches `PushSinglePayoutJob` on
    `created()` / `updated()`; the job uses `WithoutOverlapping` middleware
    so a burst of writes to one row collapses to a single POST.
  - `IngestClient` — Guzzle-based, `http_errors=false`, never throws.
    Retries on 429 (`Retry-After`-aware) and 5xx with exponential backoff
    capped at 3 attempts. Surfaces structured 4xx codes from ProcessHub
    (`SOURCE_DISABLED`, `SOURCE_NOT_CONFIGURED`, `BAD_CONFIG`,
    `PAYLOAD_TOO_LARGE`) as `PushResult::$error` so the command can log
    something actionable.
  - `WatermarkStore` — atomic file-based persistence in
    `storage/app/processhub-payouts-watermark.json`. Survives
    `php artisan cache:clear`; falls back to the server-provided hint
    (`payoutSource.watermark`) when the local file is missing.
  - `RemoteConfigClient` extended to apply the `payoutSource` section:
    `enabled`, `cadence.{mode,cronExpr}`, `watermark`, `columnsHash`. A
    change in `columnsHash` is logged at INFO level (no re-shipping
    needed — ProcessHub recomputes formulas server-side).
  - Mapper validation (`PayoutsManager::mapOne`) — rejects rows with
    missing required fields, non-decimal `grossAmount`, or unparseable
    `paymentCreatedAt` before they ever leave the app. Bad rows are
    skipped with a WARNING log; the rest of the batch still ships.
  - Test suite — `PayoutsManagerTest`, `WatermarkStoreTest`, `LimitsTest`,
    `IngestClientTest` (all HTTP branches), `PushPayoutsCommandTest`
    (in-memory SQLite + mock Guzzle), `PayoutsModelObserverTest`
    (`Queue::fake()`).

### Env

New environment variables — all optional, sensible defaults:

- `PROCESSHUB_PAYOUTS_ENABLED` (default `true`)
- `PROCESSHUB_PAYOUTS_QUEUE` (default `default`)
- `PROCESSHUB_PAYOUTS_CONNECTION` (default unset)
- `PROCESSHUB_PAYOUTS_DEFAULT_CRON` (default `0 * * * *`)

### Limits

Contract with the ProcessHub `/api/ingest/payouts` endpoint:
- 1000 rows per batch
- 2 MiB max payload (package caps at 1.8 MiB for JSON-overhead headroom)
- 60 requests/min per token (sliding window, shared with logs/heartbeat)
- Idempotent upsert by `gatewayPaymentId` — re-sending the same row is safe

## [0.1.0] — 2026-04-20

### Added

- Monolog handler (`ProcessHubFactory` / `ProcessHubHandler`) — queues log
  batches for async delivery. Auto-extracts `Throwable` from context into
  structured exception payload with trimmed stack trace.
- Queue job (`SendLogBatchJob`) — 3× retry with exponential backoff, honours
  `Retry-After` on 429, fails fast on 4xx (config error), appends to fallback
  file on exhausted attempts.
- Middleware (`CorrelateRequestId`) — propagates `X-Request-Id` across the
  request lifecycle via `Log::shareContext`; UUID v4 generator without
  external dependency.
- Artisan commands:
  - `processhub:install` — publishes config + prints manual-step checklist.
  - `processhub:test` — sync POST to verify token/URL setup.
  - `processhub:heartbeat` — scheduled every minute (auto-registered).
  - `processhub:flush-fallback` — re-ingest batches saved during outages.
- Event listeners — `QueryExecuted` (opt-in, slow queries), `JobFailed`,
  `ScheduledTaskFailed`/`Skipped`, `MessageSent` (mail).
- Auto-capture of uncaught exceptions via `$handler->reportable()` hook.
- `ProcessHub::markDeploy($version, $commitSha, $success, $metadata)` facade
  for CI scripts.
- Defense-in-depth PII `Redactor` — matches server-side logic (keys:
  password/token/secret/authorization/api_key/cookie; value patterns:
  email/JWT/Bearer/card).
- Full test suite — `Redactor`, `CorrelateRequestId`, `SendLogBatchJob`
  failure branches.
- CI on GitHub Actions — PHP 8.1/8.2/8.3 × Laravel 10/11/12 matrix +
  PHPStan level 6 (larastan).

### Limits

Contract with the ProcessHub ingest endpoint (as of 2026-04):
- 100 entries per batch
- 2 MB max payload
- 60 requests per minute per token (sliding window)
- 16 KB max message length (truncated server-side if exceeded)
- 10 max JSON depth in `context` field
