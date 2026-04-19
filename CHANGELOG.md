# Changelog

All notable changes to `processhub/laravel-logs` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
