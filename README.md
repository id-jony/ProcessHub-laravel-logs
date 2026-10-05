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

That's it. Every `Log::error`/`warning`/`info` (above the channel's `level`) is buffered in memory and shipped to ProcessHub in batches of up to 100 entries via the queue.

## What it does

- **Log::error/warning/info → ApplicationLog rows** in ProcessHub, with structured context (exception class + stack trace when a `Throwable` is in context).
- **Exception grouping** — ProcessHub computes a stable fingerprint from `class + normalized message + top frame` so `User 1234 not found` and `User 9876 not found` group together; regressions (resolved → new occurrence) flip the group back to `open` and emit a pipeline trigger.
- **Heartbeat** — `processhub:heartbeat` runs every minute via the app's scheduler. Status flips to `OFFLINE` after 3 missed beats.
- **Request-id correlation** — `CorrelateRequestId` middleware propagates `X-Request-Id` through Monolog's shared context; ProcessHub UI pivots on it to show every log line of one HTTP request.
- **Deploy markers** — run `php artisan processhub:deploy "$RELEASE" --commit="$SHA"` as the last step of your Forge/Envoyer/CI deploy script. The «Релизы» tab shows the timeline; open exception groups auto-resolve when their next deploy lands. Pass `--failed` to record an unsuccessful deploy. Without `--commit` the command tries `git rev-parse HEAD`. Version is a positional argument, not `--version` (Symfony reserves the latter for printing the framework version). `ProcessHub::markDeploy(...)` facade also works for in-process callers.
- **Batched, queue-based delivery** — records are buffered in memory and pushed as one `SendLogBatchJob` per `PROCESSHUB_LOG_BATCH_SIZE` entries (not one job per record) on a dedicated queue (`logs` by default). The buffer is flushed when full, at the end of every HTTP request, after every queue job / worker loop, after every console command and on Monolog `close()`/`reset()`. Retry on 5xx / network with backoff, honour `Retry-After` on 429, fall back to a local file when delivery finally fails so nothing is lost.
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
| `PROCESSHUB_LOG_BATCH_SIZE` | `100` | Max entries per batch (capped at ProcessHub's hard limit of 100) |
| `PROCESSHUB_LOG_BATCH_MAX_BYTES` | `1800000` | Max JSON size of a batch (server limit is 2 MB) |
| `PROCESSHUB_LOG_TIMEOUT_MS` | `5000` | HTTP timeout |
| `PROCESSHUB_LOG_RETRY_WINDOW_SEC` | `3600` | How long `SendLogBatchJob` keeps retrying (`retryUntil`) before the batch goes to the fallback file |
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
| `processhub:flush-fallback [--limit=N] [--max-wait=120]` | Re-ingest batches saved to `storage/logs/processhub-fallback.log` during outages |
| `processhub:rebatch-queue --from=<queue> [--chunk=1000] [--limit=N]` | Repack a Redis backlog of single-entry `SendLogBatchJob`s into full batches on `processhub.queue` |

You can wire `processhub:flush-fallback` into your own schedule if you want more aggressive retries — the package doesn't schedule it automatically. It streams the file (safe for 100+ MB), packs entries from many lines into full batches, waits out 429 (`Retry-After` up to `--max-wait`) and keeps the unsent remainder in `<fallback>.flushing` for the next run (`--limit` caps batches per run).

`processhub:rebatch-queue` is a recovery tool for backlogs left by versions < 0.4 (one job per log record). Move the backlog to a separate queue name first (e.g. stop the `logs` workers and `RENAME <prefix>queues:logs <prefix>queues:logs-backlog`, where `<prefix>` is your `database.redis.options.prefix`), then run `php artisan processhub:rebatch-queue --from=logs-backlog`. Jobs are claimed atomically in chunks; a crashed run is resumed from the claimed chunk; jobs of other classes are returned to the backlog. Only the ready list is processed (not delayed/reserved jobs).

## How it fails

- **Network down / 5xx** — `SendLogBatchJob` retries with backoff 10 s → 30 s → 2 min → 5 min (up to 10 failures within the retry window). Then `failed()` appends the batch as JSON to `storage/logs/processhub-fallback.log`. Run `php artisan processhub:flush-fallback` once the network is back.
- **4xx from ProcessHub** (bad token, revoked token) — jobs fail immediately (no retries); batch appended to fallback file for later re-ingest after you fix config.
- **429 rate limit** — batch is released back to the queue with the `Retry-After` delay. Retries are time-based (`retryUntil`, `PROCESSHUB_LOG_RETRY_WINDOW_SEC`), so a long rate-limit streak doesn't burn attempts.
- **Delivery failures never feed back into the channel** — failures of `SendLogBatchJob` itself are not reported as `Queue job failed` logs, reports of its own exceptions are dropped by the handler, and if pushing to the queue throws the batch goes straight to the fallback file without re-entering the logger.
- **`Log::error` before config is set** — handler silently drops (the install command warns you).

## Testing

```bash
composer install
vendor/bin/phpunit
```

Tests use [Orchestra Testbench](https://github.com/orchestral/testbench): `Redactor`, `CorrelateRequestId`, batching/flush hooks, `SendLogBatchJob` HTTP branches (`Http::fake`), `HandleJobFailed`, `processhub:flush-fallback`. Worker retry and `processhub:rebatch-queue` tests need Redis and run only when `PROCESSHUB_TEST_REDIS_HOST` is set (they FLUSH DB `PROCESSHUB_TEST_REDIS_DB`, 15 by default):

```bash
docker run -d --rm -p 6390:6379 redis:7-alpine
PROCESSHUB_TEST_REDIS_HOST=127.0.0.1 PROCESSHUB_TEST_REDIS_PORT=6390 vendor/bin/phpunit
```

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
