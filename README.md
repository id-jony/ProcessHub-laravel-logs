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
- **Uncaught exceptions** — captured through the exception handler's `reportable()` hook. With `processhub` in the default log channel (or the default stack), Laravel's own report log is the shipped entry, with the exception's `context()` and the handler's context (`userId`, …); otherwise the hook logs the exception itself (adding its `context()`). The same exception logged twice is shipped twice.
- **Heartbeat** — `processhub:heartbeat` runs every minute via the app's scheduler. Status flips to `OFFLINE` after 3 missed beats.
- **Request-id correlation** — `CorrelateRequestId` middleware propagates `X-Request-Id` through Monolog's shared context; ProcessHub UI pivots on it to show every log line of one HTTP request.
- **Deploy markers** — run `php artisan processhub:deploy "$RELEASE" --commit="$SHA"` as the last step of your Forge/Envoyer/CI deploy script. The «Релизы» tab shows the timeline; open exception groups auto-resolve when their next deploy lands. Pass `--failed` to record an unsuccessful deploy. Without `--commit` the command tries `git rev-parse HEAD`. Version is a positional argument, not `--version` (Symfony reserves the latter for printing the framework version). `ProcessHub::markDeploy(...)` facade also works for in-process callers.
- **Batched, queue-based delivery** — records are buffered in memory and pushed as one `SendLogBatchJob` per `PROCESSHUB_LOG_BATCH_SIZE` entries / `PROCESSHUB_LOG_BATCH_MAX_BYTES` (not one job per record) on a dedicated queue (`logs` by default).
    - A partial batch is shipped once its oldest entry is `PROCESSHUB_LOG_FLUSH_INTERVAL_SEC` (10 s) old — checked on every new record and, in queue workers, after each job and on every worker loop tick. A worker running thousands of one-log jobs therefore still ships full batches.
    - Full flush when the unit of work ends: end of HTTP request, `CommandFinished`, `WorkerStopping`, Octane `RequestTerminated`, Monolog `close()`/`reset()` and PHP shutdown.
    - Long-running commands without a worker loop or an end (`horizon`, `horizon:supervisor`, `schedule:work`, `queue:listen`, `reverb:start`, `pulse:check`, `pulse:work`, `octane:start`, plus your own daemons listed in `processhub.unbuffered_commands`) queue every entry right away — there is no point at which a buffered one would be shipped.
    - Entries of a flushed application (end of a test) are dropped instead of being pushed into the next application's queue or the fallback file.
    - `SIGKILL` loses whatever is still in the buffer. A child created by `pcntl_fork()` drops the inherited buffer (the parent ships it), so nothing is sent twice.
    - Delivery honours a shared rate limit and `Retry-After`, retries 5xx / 408 / network errors with backoff and parks batches in a local fallback file when delivery is impossible, so nothing is lost (see [How it fails](#how-it-fails)).
- **Fatal errors (OOM, `max_execution_time`)** — both the buffer and the `FatalError` itself get through. The package preloads the classes Laravel needs to build the `FatalError`. Its reportable callback runs before Sentry, the default log and error rendering (when Sentry is wired in `bootstrap/app.php`, see [How it fails](#how-it-fails)): it raises `memory_limit` by 16 MB on "Allowed memory size … exhausted" (only above current usage) and ships the buffer and the error at once. A fatal error Laravel didn't log (`dontReport`, throttling, a failing logger) is shipped from `error_get_last()` by the package's last shutdown function — once.
- **PII redaction at the source** — `password` / `token` / `authorization` / `api_key` / `cookie` keys become `[REDACTED]`; emails / JWTs / `Bearer …` / credit-card numbers in message strings are masked before leaving the app.
- **Structured event listeners** — failed queue jobs and skipped/failed scheduled tasks become log entries. `Queue job failed` carries the job's exception, so it is shipped with `contextType=exception` and `context.class` is the **exception** class (plus `connection`, `queue`, `attempts`); the job class is not kept. When a job fails for good there are two entries: `Queue job failed` and the worker's report of the exception. `Scheduled task skipped` is `contextType=scheduled` with `command`; a failed task with an exception becomes `contextType=exception` and keeps `command`. Sent mail becomes an INFO `Mail sent` entry (`listeners.mail`, on by default; shipped only if the channel `level` lets INFO through). Slow-query capture is available but off by default.

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
| `PROCESSHUB_LOG_TIMEOUT_MS` | `5000` | HTTP timeout (rounded up to whole seconds) |
| `PROCESSHUB_LOG_RETRY_WINDOW_SEC` | `3600` | How long `SendLogBatchJob` keeps retrying (min 60) before the batch goes to the fallback file. The job checks the deadline itself — also when a backlog delays it past the deadline; the worker's `retryUntil` is the deadline + 24 h, so nothing lands in `failed_jobs` by time |
| `PROCESSHUB_RATE_LIMIT_PER_MINUTE` | `50` | Shared limit of ingest POSTs per minute across all workers and `flush-fallback` (ProcessHub allows 60/min per token): one request per 60/limit s with a burst of ⌈limit/10⌉ — any 60 s window gets at most limit + burst (50 → 55). `0` disables the limiter |
| `PROCESSHUB_RATE_LIMIT_STORE` | `cache.default` | Cache store holding the limiter state (one key, changed under `Cache::lock`). Must support atomic locks and be shared by all processes: `redis`, `memcached`, `dynamodb`, `database` (with the `cache_locks` table); `file` only on a single host; `array` is per-process. A store without locks or an unavailable one makes the limiter step aside (a warning once per process) |
| `PROCESSHUB_HEARTBEAT_ENABLED` | `true` | Disable if your env doesn't run the scheduler |
| `PROCESSHUB_LOG_SLOW_QUERIES` | `false` | Emit slow queries as logs |
| `PROCESSHUB_SLOW_QUERY_MS` | `1000` | Threshold when slow-query logging is on |

Without env variables:

- `fallback_path` — `storage/logs/processhub-fallback.log`; `null` turns the fallback off (batches that can't be delivered are dropped).
- `unbuffered_commands` — your own long-running artisan commands (bot long-polling, custom daemons) where entries are queued right away; the built-in list is above.
- Custom redaction keys / patterns live under `redact.keys` and `redact.patterns`.

Remote config from ProcessHub (pulled by the heartbeat into `storage/app/processhub-config.json`, read at boot — long-running workers need `horizon:terminate` / `queue:restart` to see a change) overrides some of these:

- `batchSize` → `batch_size`, at least 10 (smaller batches don't make delivery faster — `flushIntervalSeconds` does — they only multiply jobs and POSTs); `0`, negative, `null` or non-numeric keep the local value.
- `flushIntervalSeconds` → `flush_interval_sec`, clamped to 1–60; `httpTimeoutSeconds` → `timeout_ms`, clamped to 1–20 s.
- `minLevel` — extra level filter on top of the channel `level`; PSR names or ProcessHub's `INFO` / `WARN` / `ERROR`, any case.
- `enabled` — `false`, `"false"`, `0`, `"0"`, `"off"` stop shipping; `null` or an unknown value keeps it on.

## Commands

| Command | What |
|---|---|
| `processhub:install` | Publish config + print manual-step checklist |
| `processhub:test` | Direct POST (no queue) to verify credentials / network |
| `processhub:heartbeat` | Single heartbeat ping; auto-scheduled every minute |
| `processhub:flush-fallback [--limit=N] [--max-wait=120] [--rate=N] [--max-runtime=300]` | Re-ingest batches saved to `storage/logs/processhub-fallback.log` during outages |
| `processhub:rebatch-queue --from=<queue> [--chunk=1000] [--limit=N] [--rate=N] [--start-delay=S] [--max-ahead=M]` | Repack a Redis backlog of single-entry `SendLogBatchJob`s into full batches on `processhub.queue` |
| `processhub:forget-failed [--dry-run] [--to-fallback]` | Delete failed-job records of `SendLogBatchJob` only (`failed_jobs` and Horizon) |

### `processhub:flush-fallback`

The package doesn't schedule it. Recommended:

```php
Schedule::command('processhub:flush-fallback --limit=100 --rate=20 --max-runtime=240')
    ->everyFiveMinutes()->withoutOverlapping(10)->runInBackground();
```

- **Options** — `--limit` caps delivered batches per run; `--max-wait` is the longest `Retry-After` it will wait out (longer → stop, remainder kept); `--rate` is requests per minute (default `PROCESSHUB_RATE_LIMIT_PER_MINUTE`; `0` turns local pacing off); `--max-runtime` stops the run after that many seconds, keeping the remainder. On top of `--rate` every request takes its turn from the same shared limiter as the queue workers, lining up behind jobs already waiting for theirs.
- **Streaming** — the file is read line by line (safe for 100+ MB); entries from many lines are packed into full batches.
- **Snapshot + checkpoint** — the live file is renamed to `<fallback>.flushing` under the lock `<fallback>.write.lock` that every append also takes, so new failures go to a fresh file. The snapshot is never rewritten: after each delivered batch the position is saved to `<fallback>.flushing.offset`. After `kill -9` at most one batch is sent again. The checkpoint is written once before the first request: if it can't be (permissions — run the command as the app's user, not root — or a full disk), the run stops with exit code 1 without sending anything. `SIGINT`/`SIGTERM` stop the run cleanly. Concurrent runs are blocked by `<fallback>.lock`.
- **Rejected entries** — on a 4xx other than 401/403/404/405/408/429 the entries named in a validation response (`errors` keys `logs.<i>…`) go to `<fallback>.rejected` with their messages and the rest is resent (a batch refused entirely costs one request); without such keys (e.g. 413) the batch is split in halves until the refused entries are isolated. Unparseable lines and a torn last line go to `<fallback>.rejected` as `raw`. Once fixed, return them with `cat <fallback>.rejected >> <fallback>`.
- **Stops with exit code 1, nothing skipped** — on 401/403/404/405 (check URL/token) and after 5 temporary failures in a row (5xx, 408, network; backoff 2/4/8/16 s). Every pause, including `Retry-After`, is at least 1 s.

### `processhub:rebatch-queue`

A recovery tool for backlogs left by versions < 0.4 (one job per log record); see [Recovering from a pre-0.4 backlog](#recovering-from-a-pre-04-backlog). Works on a Redis queue connection only; `--from` must differ from `processhub.queue`.

- **Pacing** — new batches are delayed so that `--rate` of them per minute become due, the i-th one at `start + i×60/rate` s, where `start` is after everything already in the target queue (after its last delayed job, and after ready + delayed + reserved drained at the same rate — whichever is later) or `--start-delay` seconds. Each batch gets its full retry window from its own due time. `--rate` defaults to 90 % of `PROCESSHUB_RATE_LIMIT_PER_MINUTE` (45 of 50), the rest is left to live traffic; `--rate=0` pushes everything at once. Keep the sum of `--rate` and a concurrently running `flush-fallback --rate` under the shared limit.
- **Rolling window** — `--max-ahead=M` (needs `--rate` > 0) stops claiming new chunks once the next batch would be due later than M minutes; the rest stays in the backlog and the next run continues the schedule. Recommended for big backlogs: `--rate=45 --max-ahead=60` every 30 minutes until the backlog is empty (bounds Redis memory, Horizon's payload copies and what an outage can expire).
- **Safety** — jobs are claimed atomically in chunks (`--chunk`, max 5000; `--limit` caps chunks per run), taking as many items from `queues:<from>:notify`. After each pushed batch the fully queued jobs are trimmed from `queues:<from>:rebatching`, so a crash (even `kill -9`) re-sends at most one batch; jobs of other classes and unreadable payloads are returned to the backlog, once per run. A lock `queues:<from>:rebatch-lock` (60 s, renewed every chunk) keeps runs apart — after `kill -9` wait up to a minute. `SIGINT`/`SIGTERM` stop after the current chunk. Only the ready list is processed (not delayed/reserved jobs). Once the backlog is empty the command deletes `queues:<from>:notify`.

### `processhub:forget-failed`

Deletes failed-job records of `SendLogBatchJob` only — never other jobs' (matched by the payload's job class, not by a mention in its data): from the `queue.failed` storage (database drivers scanned in id chunks of 1000, other drivers via `FailedJobProviderInterface`) and, when Horizon is installed, from its failed list. `--dry-run` only counts. `--to-fallback` first appends each record's entries to the fallback file (once per job uuid — `failed_jobs` and Horizon hold the same failure); records it can't read are kept, and a write failure (including `fallback_path = null`) stops the run, keeping every record not written out. Don't use `queue:flush` / `queue:prune-failed` / `horizon:clear` for this — they hit other jobs' records too.

## Recovering from a pre-0.4 backlog

Versions < 0.4 queued one job per log record; a stuck ingest could leave millions of them in the `logs` queue plus a pile of failed `SendLogBatchJob`s. `<prefix>` below is your `database.redis.options.prefix`.

1. **Deploy 0.4 and restart the workers** (`horizon:terminate` / `queue:restart`). Leave `flush-fallback` out of the schedule for now — it shares the rate limit with the backlog.
2. **Move the backlog aside** — atomically, workers keep running:
    ```
    RENAMENX <prefix>queues:logs <prefix>queues:logs-backlog
    RENAMENX <prefix>queues:logs:notify <prefix>queues:logs-backlog:notify
    ```
    `RENAMENX` never overwrites a backlog left by a previous attempt — if it returns 0, pick another name (e.g. `logs-backlog-2`). Delayed/reserved jobs stay in `logs`; workers send each one if the limiter has a free slot and park it in the fallback file otherwise (step 4 delivers it).
3. **Repack it** with a rolling window, repeating until the backlog is empty (`LLEN <prefix>queues:logs-backlog` and `…:rebatching` are 0):
    ```bash
    php artisan processhub:rebatch-queue --from=logs-backlog --rate=45 --max-ahead=60   # every 30 minutes
    ```
    Watch that `queues:logs` doesn't grow and no "moved to the fallback file" warnings appear; lower `--rate` otherwise.
4. **Flush the fallback file** once the repacked batches are delivered (`queues:logs:delayed` is about empty):
    ```bash
    php artisan processhub:flush-fallback --rate=40 --max-runtime=5400
    ```
    `Ctrl-C` stops cleanly; the next run resumes from the checkpoint. Then add the [scheduled flush](#processhubflush-fallback).
5. **Clean up failed-job records** (0.3 jobs that had already used up their 3 attempts are failed by the worker before they run — their batch goes to the fallback file and a record stays in `failed_jobs`/Horizon): `php artisan processhub:forget-failed --dry-run` to count, then `php artisan processhub:forget-failed` — or with `--to-fallback` if some of those batches never made it into the fallback file (already delivered entries are then sent again), followed by another `flush-fallback`. Horizon also expires them by itself after `horizon.trim.failed`.

## How it fails

Retries are limited only by time: a batch has `PROCESSHUB_LOG_RETRY_WINDOW_SEC` (1 h) from the moment it becomes due (`SendLogBatchJob::enqueue($entries, $delaySeconds)` shifts the deadline by the delay). There is no attempt or exception cap.

- **Network down / 5xx / 408** — any error of the HTTP call (connect, reset, TLS, HTTP/2 — e.g. cURL 55/56/60/92, which Laravel < 12 doesn't wrap — or anything else the client throws), every 5xx and 408 release the job with backoff 10 s → 30 s → 2 min → 5 min (then every 5 min). Nothing is thrown out of the job for an expected failure, so the worker neither reports it nor takes "Connection reset by peer" for a lost queue connection and restarts.
- **429 / shared rate limit** — before each POST the job asks the shared limiter (`IngestThrottle`); refused → it gets a turn in a shared line and is released until then (jobs come back one interval apart, first come first served — a backlog costs ~2 attempts per job; a turn beyond the deadline parks the batch at once). A 429 pauses all senders (via the limiter's store; with the limiter off only this job waits) for `Retry-After` (whole seconds or an IMF-fixdate HTTP-date, clamped to 1–600 s; anything else or no header — 30 s) and releases the job. Waiting doesn't burn attempts.
- **Jobs queued by 0.3** (`$tries = 3`, no `retryUntil`, no deadline) — sent only if the limiter lets them through right away (they never take a turn in its line, so they can't push fresh batches' turns past their deadline); anything else — a busy limiter, 429, 5xx, 408, a network error, 4xx — parks the batch in the fallback file and deletes the job. `flush-fallback` delivers them later in full batches.
- **4xx from ProcessHub** (bad token, rejected payload) — no retries.
- **Dead ends go to the fallback file, not `failed_jobs`** — on a 4xx (other than 408/429), when the job is picked up after its deadline, when the next retry wouldn't fit before it, or on the `sync` connection (it can't delay a retry), the batch is appended to `storage/logs/processhub-fallback.log` and the job is deleted. An attempt timing out reaches `failed()`, which also appends the batch to the file. If the fallback file can't be written, the job is failed instead (before anything is logged — a broken log channel can't keep it out of `failed_jobs`) and the batch stays only in `failed_jobs` (`queue:retry`). Run `processhub:flush-fallback` once the problem is fixed.
- **Bad values don't sink a batch** — `INF`/`NaN` and broken UTF-8 are substituted when the job is created and when encoding, so such a batch can still be queued and sent.
- **Delivery never feeds back into the channel** — while a worker processes a `SendLogBatchJob` the buffer is muted until the worker moves on (next loop tick, next job, worker stop or end of the command), so the worker's report of an exception that escaped the job (a bug, not a delivery failure) is dropped too. `Queue job failed` isn't logged for `SendLogBatchJob`. If pushing to the queue throws, the batch goes straight to the fallback file.
- **No Sentry/daily spam** — expected delivery failures throw nothing. What may still be reported about the job itself (`MaxAttemptsExceeded`/`TimeoutExceeded` of `SendLogBatchJob`, `DeliveryFailedException`) is stopped by the package's reportable callback returning `false`, which ends Laravel's report chain on Laravel 10–12. It is registered while the exception handler is being resolved, i.e. before `withExceptions()` callbacks in `bootstrap/app.php` (where Sentry's `Integration::handles()` lives). That order isn't guaranteed when something resolves the handler earlier — e.g. `nunomaduro/collision` (installed with dev dependencies) does it in console commands, queue workers included — and with a classic `app/Exceptions/Handler.php` the app's own `reportable()` callbacks always come first. In those cases put this line before Sentry's — first in `withExceptions()`:
    ```php
    ->withExceptions(function (Exceptions $exceptions) {
        $exceptions->reportable(fn (\Throwable $e) => \ProcessHub\Logs\Jobs\SendLogBatchJob::isOwnFailure($e) ? false : null);
        Integration::handles($exceptions);
    })
    ```
    or in `Handler::register()`: `$this->reportable(fn (\Throwable $e) => \ProcessHub\Logs\Jobs\SendLogBatchJob::isOwnFailure($e) ? false : null);`.
- **Fatal errors under `php_admin_value[memory_limit]`** — the limit can't be raised, so the buffer may not be shipped on OOM. A system OOM (not the PHP limit) and `SIGKILL` lose the buffer.
- **Out of memory in many small allocations** — Laravel keeps only 32 KB for its own fatal-error handler, which runs before any package code; on some heaps `report()` needs more and dies, and then the buffer and the error are lost. Whether it happens depends on heap fragmentation, not on the package.
- **`Log::error` before config is set** — handler silently drops (the install command warns you).

## Testing

```bash
composer install
vendor/bin/phpunit
```

Tests use [Orchestra Testbench](https://github.com/orchestral/testbench): `Redactor`, `CorrelateRequestId`, `FallbackFile`, batching/flush hooks (including fatal errors and forked children), exception capture, `SendLogBatchJob` HTTP branches (`Http::fake`), `IngestThrottle`, `HandleJobFailed`, `processhub:flush-fallback`, `processhub:forget-failed`. Tests with `pcntl_fork()` need the `pcntl`/`posix` extensions. Worker delivery-loop, Redis delivery and `processhub:rebatch-queue` tests (and the Redis variant of the limiter test) need Redis and run only when `PROCESSHUB_TEST_REDIS_HOST` is set (port `PROCESSHUB_TEST_REDIS_PORT`, 6379 by default; they FLUSH DB `PROCESSHUB_TEST_REDIS_DB`, 15 by default):

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
