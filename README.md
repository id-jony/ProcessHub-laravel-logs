# ProcessHub Logs — Laravel package

Ships logs, exceptions, and deploy markers from your Laravel application to [ProcessHub](https://processhub.io)'s centralised observability module. Get grouped exceptions, deploy-marker annotations on your event graph, and a single `requestId` trail across every log line of a user request.

> **Status**: MVP — production-ready for Laravel 10/11/12 on PHP 8.1+.

## Install

```bash
composer require processhub/laravel-logs
php artisan processhub:install
```

Then, per the install output:

1. Add credentials to `.env`:
    ```
    PROCESSHUB_LOG_URL=https://app.processhub.io
    PROCESSHUB_LOG_TOKEN=ph_live_<your-token>
    ```
    Get the token at `ProcessHub → Приложения → <your app> → Интеграция → Выпустить токен`. Copy immediately — it won't be shown again.

2. Register the logging channel in `config/logging.php`:
    ```php
    'channels' => [
        // … existing channels
        'processhub' => [
            'driver' => 'custom',
            'via'    => ProcessHub\Logs\Logging\ProcessHubFactory::class,
            'level'  => env('LOG_LEVEL', 'warning'),
        ],
    ],
    'stack' => [
        'driver'   => 'stack',
        'channels' => ['single', 'processhub'],
        'ignore_exceptions' => false,
    ],
    ```

3. Verify end-to-end:
    ```bash
    php artisan processhub:test
    ```
    You should see a synthetic ERROR appear in the ProcessHub application detail page within a second.

That's it. Every `Log::error`/`warning`/`info` (above the channel's `level`) now queues an async batch to ProcessHub.

## What it does

- **Log::error/warning/info → ApplicationLog rows** in ProcessHub, with structured context (exception class + stack trace when a `Throwable` is in context).
- **Exception grouping** — ProcessHub computes a stable fingerprint from `class + normalized message + top frame` so `User 1234 not found` and `User 9876 not found` group together; regressions (resolved → new occurrence) flip the group back to `open` and emit a pipeline trigger.
- **Heartbeat** — `processhub:heartbeat` runs every minute via the app's scheduler. Status flips to `OFFLINE` after 3 missed beats.
- **Request-id correlation** — `CorrelateRequestId` middleware propagates `X-Request-Id` through Monolog's shared context; ProcessHub UI pivots on it to show every log line of one HTTP request.
- **Deploy markers** — run `php artisan processhub:deploy "$RELEASE" --commit="$SHA"` as the last step of your Forge/Envoyer/CI deploy script. The «Релизы» tab shows the timeline; open exception groups auto-resolve when their next deploy lands. Pass `--failed` to record an unsuccessful deploy. Without `--commit` the command tries `git rev-parse HEAD`. Version is a positional argument, not `--version` (Symfony reserves the latter for printing the framework version). `ProcessHub::markDeploy(...)` facade also works for in-process callers.
- **Queue-based delivery** — all batches go through a dedicated queue (`logs` by default). Retry on 5xx / network, honour `Retry-After` on 429, fall back to a local file on exhausted attempts so nothing is lost.
- **PII redaction at the source** — `password` / `token` / `authorization` / `api_key` / `cookie` keys become `[REDACTED]`; emails / JWTs / `Bearer …` / credit-card numbers in message strings are masked before leaving the app.
- **Structured event listeners** — failed queue jobs, skipped/failed scheduled tasks become typed log entries (`contextType=job` / `scheduled`). Slow-query capture is available but off by default.

## Configuration

See `config/processhub.php` after publishing. Highlights:

| Env | Default | What |
|---|---|---|
| `PROCESSHUB_LOG_URL` | — | Base URL of your ProcessHub tenant |
| `PROCESSHUB_LOG_TOKEN` | — | `ph_live_<orgSlug>_<appSlug>_<rand>` |
| `PROCESSHUB_LOG_QUEUE` | `logs` | Queue name for `SendLogBatchJob` |
| `PROCESSHUB_LOG_CONNECTION` | default | Queue connection (`redis`, `database`, etc.) |
| `PROCESSHUB_LOG_BATCH_SIZE` | `100` | Max entries per batch (ProcessHub's hard limit) |
| `PROCESSHUB_LOG_TIMEOUT_MS` | `5000` | HTTP timeout |
| `PROCESSHUB_HEARTBEAT_ENABLED` | `true` | Disable if your env doesn't run the scheduler |
| `PROCESSHUB_LOG_SLOW_QUERIES` | `false` | Emit slow queries as logs |
| `PROCESSHUB_SLOW_QUERY_MS` | `1000` | Threshold when slow-query logging is on |

Custom redaction keys / patterns live in `config/processhub.php` under `redact.keys` and `redact.patterns`.

## Commands

| Command | What |
|---|---|
| `processhub:install` | Publish config + print manual-step checklist |
| `processhub:test` | Direct POST (no queue) to verify credentials / network |
| `processhub:heartbeat` | Single heartbeat ping; auto-scheduled every minute |
| `processhub:flush-fallback` | Re-ingest batches saved to `storage/logs/processhub-fallback.log` during outages |

You can wire `processhub:flush-fallback` into your own schedule if you want more aggressive retries — the package doesn't schedule it automatically.

## How it fails

- **Network down** — `SendLogBatchJob` retries 3× with exponential backoff. On exhaustion, `failed()` appends the batch as JSON to `storage/logs/processhub-fallback.log`. Run `php artisan processhub:flush-fallback` once the network is back.
- **4xx from ProcessHub** (bad token, revoked token) — jobs fail immediately (no retries); batch appended to fallback file for later re-ingest after you fix config.
- **429 rate limit** — batch is released back to the queue with the `Retry-After` delay (no lost time on exponential backoff that doesn't fit ProcessHub's rate-limit window).
- **`Log::error` before config is set** — handler silently drops (the install command warns you).

## Testing

```bash
composer install
vendor/bin/phpunit
```

Tests use [Orchestra Testbench](https://github.com/orchestral/testbench). There are unit tests for `Redactor` (pattern coverage), `CorrelateRequestId` middleware (validation + generation), and `SendLogBatchJob` (retry behaviour against a mocked Guzzle client).

## Contract with ProcessHub

This package targets the ingest contract documented in [ProcessHub docs — Applications module](https://processhub.io/docs/13-applications-module). Key limits (as of 2026-04):

- 100 entries per batch
- 2 MB max payload
- 60 requests/min per token (sliding window)
- 16 KB max message; longer is truncated server-side
- 10 max JSON depth in context

If ProcessHub changes the contract, bump the package's minor version so apps can upgrade in lock-step.

## License

MIT — see [LICENSE](LICENSE).
