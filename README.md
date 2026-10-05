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
- **Batched, queue-based delivery** — records are buffered in memory and pushed as one `SendLogBatchJob` per `PROCESSHUB_LOG_BATCH_SIZE` entries / `PROCESSHUB_LOG_BATCH_MAX_BYTES` (not one job per record) on a dedicated queue (`logs` by default).
    - A partial batch is shipped once its oldest entry is `PROCESSHUB_LOG_FLUSH_INTERVAL_SEC` (10 s) old — checked on every new record and, in queue workers, after each job and on every worker loop tick. A worker running thousands of one-log jobs therefore still ships full batches.
    - Full flush when the unit of work ends: end of HTTP request, `CommandFinished`, `WorkerStopping`, Octane `RequestTerminated`, Monolog `close()`/`reset()` and PHP shutdown — including fatal errors (OOM, `max_execution_time`): both the buffer and the `FatalError` itself get through.
    - `SIGKILL` loses whatever is still in the buffer. A child created by `pcntl_fork()` drops the inherited buffer (the parent ships it), so nothing is sent twice.
    - Delivery honours a shared rate limit and `Retry-After`, retries 5xx / network errors with backoff and parks batches in a local fallback file when delivery is impossible, so nothing is lost (see [How it fails](#how-it-fails)).
- **PII redaction at the source** — `password` / `token` / `authorization` / `api_key` / `cookie` keys become `[REDACTED]`; emails / JWTs / `Bearer …` / credit-card numbers in message strings are masked before leaving the app.
- **Structured event listeners** — failed queue jobs and skipped/failed scheduled tasks become log entries. `Queue job failed` carries the job's exception, so it is shipped with `contextType=exception` and `context.class` is the **exception** class (plus `connection`, `queue`, `attempts`); the job class is not kept. `Scheduled task skipped` is `contextType=scheduled` with `command`; a failed task with an exception becomes `contextType=exception` and keeps `command`. Slow-query capture is available but off by default.

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
| `PROCESSHUB_LOG_FLUSH_INTERVAL_SEC` | `10` | Max age of a partial batch in a long-lived process (queue worker) before it is queued |
| `PROCESSHUB_LOG_TIMEOUT_MS` | `5000` | HTTP timeout |
| `PROCESSHUB_LOG_RETRY_WINDOW_SEC` | `3600` | How long `SendLogBatchJob` keeps retrying (`retryUntil`, min 60) before the batch goes to the fallback file |
| `PROCESSHUB_RATE_LIMIT_PER_MINUTE` | `50` | Shared limit of ingest POSTs per minute across all workers and `flush-fallback` (ProcessHub allows 60/min per token). `0` disables the limiter |
| `PROCESSHUB_RATE_LIMIT_STORE` | `cache.default` | Cache store holding the limiter state. Must be shared by all processes (`redis`, `database`, `memcached`) — `file`/`array` on several hosts won't limit anything. If the store is unavailable the limiter steps aside |
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
| `processhub:flush-fallback [--limit=N] [--max-wait=120] [--rate=N] [--max-runtime=300]` | Re-ingest batches saved to `storage/logs/processhub-fallback.log` during outages |
| `processhub:rebatch-queue --from=<queue> [--chunk=1000] [--limit=N] [--rate=N] [--start-delay=S]` | Repack a Redis backlog of single-entry `SendLogBatchJob`s into full batches on `processhub.queue` |

### `processhub:flush-fallback`

The package doesn't schedule it. Recommended:

```php
Schedule::command('processhub:flush-fallback --limit=100 --rate=20 --max-runtime=240')
    ->everyFiveMinutes()->withoutOverlapping(10)->runInBackground();
```

- **Options** — `--limit` caps delivered batches per run; `--max-wait` is the longest `Retry-After` it will wait out (longer → stop, remainder kept); `--rate` is requests per minute (default `PROCESSHUB_RATE_LIMIT_PER_MINUTE`; `0` turns local pacing off); `--max-runtime` stops the run after that many seconds, keeping the remainder. On top of `--rate` it takes slots from the same shared limiter as the queue workers.
- **Streaming** — the file is read line by line (safe for 100+ MB); entries from many lines are packed into full batches.
- **Snapshot + checkpoint** — the live file is renamed to `<fallback>.flushing` under the lock `<fallback>.write.lock` that every append also takes, so new failures go to a fresh file. The snapshot is never rewritten: after each delivered batch the position is saved to `<fallback>.flushing.offset`. After `kill -9` at most one batch is sent again. `SIGINT`/`SIGTERM` stop the run cleanly. Concurrent runs are blocked by `<fallback>.lock`.
- **Rejected entries** — on a 4xx other than 401/403/404/405/408/429 the batch is split in halves until the refused entries are isolated; each one goes to `<fallback>.rejected` with the reason (a torn last line of the snapshot ends up there too). Once fixed, return them with `cat <fallback>.rejected >> <fallback>`.
- **Stops with exit code 1, nothing skipped** — on 401/403/404/405 (check URL/token) and after 5 temporary failures in a row (5xx, 408, network; backoff 2/4/8/16 s). Every pause, including `Retry-After`, is at least 1 s.
- **Upgrading from an earlier 0.4 build** — a leftover `<fallback>.flushing.tmp` is no longer used; delete it by hand.

### `processhub:rebatch-queue`

A recovery tool for backlogs left by versions < 0.4 (one job per log record). Stop the `logs` workers and move the backlog to a separate queue name (`<prefix>` is your `database.redis.options.prefix`):

```
RENAMENX <prefix>queues:logs <prefix>queues:logs-backlog
RENAMENX <prefix>queues:logs:notify <prefix>queues:logs-backlog:notify
```

then start the workers again and run `php artisan processhub:rebatch-queue --from=logs-backlog`.

- **Pacing** — new batches are delayed so that `--rate` of them per minute become due, the i-th one at `start + i×60/rate` s, where `start` is after everything already in the target queue (ready + delayed + reserved, at the same rate) or `--start-delay` seconds. Each batch gets its full retry window from its own due time. `--rate` defaults to `PROCESSHUB_RATE_LIMIT_PER_MINUTE`; `--rate=0` pushes everything at once. With heavy live log traffic sharing the limit, lower it to 30–40.
- **Safety** — jobs are claimed atomically in chunks (`--chunk`, max 5000; `--limit` caps chunks per run); a crashed run re-processes the claimed chunk (at most one chunk sent twice); jobs of other classes are returned to the backlog. Only the ready list is processed (not delayed/reserved jobs). Once the backlog is empty the command deletes `queues:<from>:notify`.

## How it fails

Retries are limited only by time: a batch has `PROCESSHUB_LOG_RETRY_WINDOW_SEC` (1 h) from the moment it becomes due (`SendLogBatchJob::enqueue($entries, $delaySeconds)` shifts the deadline by the delay). There is no attempt or exception cap.

- **Network down / 5xx** — every transport error (connect, reset, TLS, HTTP/2 — e.g. cURL 55/56/60/92, which Laravel < 12 doesn't wrap) and every 5xx counts as a delivery failure: `DeliveryFailedException`, retried with backoff 10 s → 30 s → 2 min → 5 min (then every 5 min).
- **429 / shared rate limit** — before each POST the job takes a slot from the shared limiter (`IngestThrottle`); no slot → `release()` until one is free. A 429 pauses all senders (via the limiter's store; with the limiter off only this job waits) for `Retry-After` (seconds or HTTP-date, clamped to 1–600 s, 30 s without the header) and releases the job. Waiting doesn't burn attempts.
- **4xx from ProcessHub** (bad token, rejected payload) — no retries.
- **Dead ends go to the fallback file, not `failed_jobs`** — on a 4xx (other than 429), or when the next retry wouldn't fit before the deadline, the batch is appended to `storage/logs/processhub-fallback.log` and the job is deleted. Only what the job can't intercept (an attempt timing out, a job picked up after its deadline) reaches `failed()`, which also appends the batch to the file. Run `processhub:flush-fallback` once the problem is fixed.
- **Bad values don't sink a batch** — `INF`/`NaN` and broken UTF-8 are substituted when encoding.
- **Delivery never feeds back into the channel** — while a worker processes a `SendLogBatchJob` the buffer is muted until the worker moves on (next job, loop tick, stop), so nothing logged or reported about it, including the worker reporting its exception, becomes a new batch. `Queue job failed` isn't logged for `SendLogBatchJob`, and if pushing to the queue throws, the batch goes straight to the fallback file.
- **No duplicate exceptions** — an exception object is shipped once, even when `processhub` is also in the default `stack` (Laravel's own report log + the auto-capture hook).
- **No Sentry/daily spam** — `DeliveryFailedException` is excluded from the app's exception reporting: through `dontReportWhen()` where it exists (Laravel 12, also covering `MaxAttemptsExceeded`/`TimeoutExceeded` of `SendLogBatchJob`), otherwise through `ignore()` (Laravel 11 and older).
- **`Log::error` before config is set** — handler silently drops (the install command warns you).

## Testing

```bash
composer install
vendor/bin/phpunit
```

Tests use [Orchestra Testbench](https://github.com/orchestral/testbench): `Redactor`, `CorrelateRequestId`, `FallbackFile`, batching/flush hooks (including fatal errors and forked children), `SendLogBatchJob` HTTP branches (`Http::fake`), `IngestThrottle`, `HandleJobFailed`, `processhub:flush-fallback`. Worker delivery-loop and `processhub:rebatch-queue` tests need Redis and run only when `PROCESSHUB_TEST_REDIS_HOST` is set (port `PROCESSHUB_TEST_REDIS_PORT`, 6379 by default; they FLUSH DB `PROCESSHUB_TEST_REDIS_DB`, 15 by default):

```bash
docker run -d --rm -p 6390:6379 redis:7-alpine
PROCESSHUB_TEST_REDIS_HOST=127.0.0.1 PROCESSHUB_TEST_REDIS_PORT=6390 vendor/bin/phpunit
```

Static analysis needs more than the default memory:

```bash
vendor/bin/phpstan analyse --memory-limit=1G
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
