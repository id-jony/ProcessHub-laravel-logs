# Changelog

All notable changes to `processhub/laravel-logs` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0] — 2026-10-05

Fixes a production incident: one queued job per log record + attempt-based
retries on 429 + `JobFailed` logging failures of the delivery job itself
formed a feedback loop (1.2M queued jobs, 600k failed, Redis OOM).

### Fixed

- **Real batching** — `ProcessHubHandler` buffers entries (new `LogBuffer`
  singleton) and queues one `SendLogBatchJob` per `batch_size` entries
  (capped at 100) / `batch_max_bytes`. In queue workers a partial batch is
  shipped by age (`flush_interval_sec`, 10 s; checked on push, `JobProcessed`,
  `JobFailed`, `Looping`), not after every job. Full flush on app
  `terminating`, `CommandFinished`, `WorkerStopping`, Octane
  `RequestTerminated`, handler `close()`/`reset()` and in a shutdown function
  — fatal errors (OOM, `max_execution_time`) deliver both the buffer and the
  `FatalError`. `SIGKILL` still loses the unsent buffer. A child after
  `pcntl_fork()` drops the inherited buffer instead of resending it.
- **No feedback loops** — the buffer is muted while a worker processes a
  `SendLogBatchJob` (until the next job / loop tick / worker stop), so the
  worker reporting its exception can't create a new batch; `HandleJobFailed`
  ignores failures of `SendLogBatchJob`; the handler drops reports of its own
  delivery failures. A throwing queue push no longer reaches the app — the
  batch goes to the fallback file.
- **One entry per exception** — the same exception object is shipped once,
  even with `processhub` in the default `stack`.
- **No Sentry/daily spam** — `DeliveryFailedException` is excluded from
  exception reporting (`dontReportWhen()` on Laravel 12, which also covers
  `MaxAttemptsExceeded`/`TimeoutExceeded` of `SendLogBatchJob`; `ignore()` on
  Laravel 11 and older).
- **429 no longer burns attempts** — `SendLogBatchJob` retries by time only:
  `retryUntil()` = `retry_window_sec` (1h) from the moment the batch is due,
  no `$tries`/`$maxExceptions`. `Retry-After` (seconds or HTTP-date, clamped
  to 1–600 s, 30 s when missing) on 429, `backoff()` 10/30/120/300 s on
  5xx / network, `$failOnTimeout = true`.
- **Every transport error is a delivery failure** — connect, reset, TLS,
  HTTP/2 (e.g. cURL 55/56/60/92 that Laravel < 12 passed through unwrapped)
  → `DeliveryFailedException` and a regular retry.
- **Bad values don't sink a batch** — `INF`/`NaN` and broken UTF-8 are
  substituted when encoding.
- **Dead ends go to the fallback file, not `failed_jobs`** — 4xx (except
  429) or a retry that wouldn't fit before the deadline appends the batch to
  the fallback file and deletes the job. `failed()` stays for timeouts and
  jobs picked up after their deadline.
- **Exceptions in context reach ProcessHub again** — `ProcessHubHandler`
  extracts the `Throwable` before `Redactor` runs (it used to turn the
  exception object into `[]`, so class/message/stack trace were lost).
  Message and trace are still masked by `Redactor`.
- **`processhub:flush-fallback`** streams the file instead of loading it and
  packs entries from many lines into full batches. The live file is taken
  over as a snapshot `<path>.flushing` under the lock `<path>.write.lock`
  shared with appends; the snapshot is never rewritten, progress goes to
  `<path>.flushing.offset` (after `kill -9` at most one batch is resent).
  Requests are paced by `--rate` and the shared limiter, 429 is waited out up
  to `--max-wait`, every pause is ≥ 1 s. A batch refused with 4xx (except
  401/403/404/405/408/429) is split in halves; single refused entries go to
  `<path>.rejected` with the reason (`cat <path>.rejected >> <path>` to
  retry). 401/403/404/405 or 5 temporary failures in a row (backoff
  2–16 s) stop the run with exit code 1, nothing skipped. A
  `<path>.flushing.tmp` left by an earlier build is no longer used — delete
  it by hand.

### Added

- `IngestThrottle` — rate limit for ingest POSTs shared by all workers and
  `processhub:flush-fallback` through a cache store; a 429 pauses every
  sender.
- `processhub:rebatch-queue --from=<queue> [--chunk] [--limit] [--rate]
  [--start-delay]` — repacks a Redis backlog of single-entry jobs into full
  batches (atomic chunk claim, crash recovery). Batches are spread at
  `--rate` per minute (default `rate_limit_per_minute`, `0` = all at once)
  after what already sits in the target queue, each with a full retry window
  from its due time; `queues:<from>:notify` is deleted once the backlog is
  drained. Prepare the backlog with `RENAMENX` of `queues:logs` and
  `queues:logs:notify`.
- `processhub:flush-fallback` options `--limit`, `--max-wait`, `--rate`
  (default `rate_limit_per_minute`), `--max-runtime` (300).
- `SendLogBatchJob::enqueue($entries, $delaySeconds = 0)`; the delay shifts
  the retry deadline.
- Config: `batch_max_bytes` (`PROCESSHUB_LOG_BATCH_MAX_BYTES`, 1800000),
  `flush_interval_sec` (`PROCESSHUB_LOG_FLUSH_INTERVAL_SEC`, 10),
  `retry_window_sec` (`PROCESSHUB_LOG_RETRY_WINDOW_SEC`, 3600),
  `rate_limit_per_minute` (`PROCESSHUB_RATE_LIMIT_PER_MINUTE`, 50, `0` = off),
  `rate_limit_store` (`PROCESSHUB_RATE_LIMIT_STORE`, default cache store;
  must be shared — redis / database).

### Changed

- `SendLogBatchJob` and `processhub:flush-fallback` use Laravel's HTTP client
  (`Http`) instead of a raw Guzzle client.
- `ProcessHubHandler` constructor takes `LogBuffer` instead of the queue
  factory (only relevant if you construct the handler manually).
- `Queue job failed` entries are now shipped with their exception:
  `contextType=exception` and `context.class` is the exception class, not the
  job class (the job class is no longer in the entry; `connection`, `queue`,
  `attempts` are kept).

## [0.3.0] — 2026-08-29

### Removed

- **BREAKING: Payouts module removed** — ProcessHub dropped the Payouts
  feature server-side (`POST /api/ingest/payouts` and the `payoutSource`
  section of `GET /api/ingest/config` no longer exist), so the whole client
  subsystem is gone: the `Payouts` facade and `src/Payouts/*`,
  `processhub:payouts:push` artisan command, `PayoutsModelObserver`,
  `PushSinglePayoutJob`, the payout scheduler hook, the `processhub.payouts`
  config section and `PROCESSHUB_PAYOUTS_*` env vars. Host apps that called
  `Payouts::register(...)` must delete that call. The config `etag` returned
  by `/api/ingest/config` and the heartbeat is now the plain app-config etag
  (byte-identical for installs that never had a payout source). Logs,
  heartbeat, deploy markers and remote config are unaffected.

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
