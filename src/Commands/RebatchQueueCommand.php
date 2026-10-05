<?php

namespace ProcessHub\Logs\Commands;

use Illuminate\Console\Command;
use Illuminate\Redis\Connections\Connection as RedisConnection;
use Illuminate\Support\Facades\Queue;
use Illuminate\Support\Facades\Redis;
use Illuminate\Support\Str;
use ProcessHub\Logs\Jobs\SendLogBatchJob;
use ProcessHub\Logs\Support\BatchBuilder;

/**
 * `php artisan processhub:rebatch-queue --from=logs-backlog [--chunk=1000] [--limit=N] [--rate=N] [--start-delay=S] [--max-ahead=M]`
 *
 * Recovery tool for backlogs of one-entry SendLogBatchJob payloads (left by
 * package versions that queued a job per log record). Drains the ready list
 * of the given Redis queue, packs the entries into full batches and pushes
 * them as new SendLogBatchJob onto `processhub.queue`.
 *
 * Preparing the backlog (keys carry the `database.redis.options.prefix`):
 *   RENAMENX <prefix>queues:logs <prefix>queues:logs-backlog
 *   RENAMENX <prefix>queues:logs:notify <prefix>queues:logs-backlog:notify
 * RENAMENX never overwrites a backlog left by a previous run.
 *
 * Pacing: new batches are delayed so that `--rate` of them (default
 * `processhub.rate_limit_per_minute`) become available per minute, evenly
 * spaced — each one gets its full retry window from the moment it's due.
 * The schedule continues after whatever already sits in the target queue:
 * after its last delayed job and after its ready + reserved + delayed jobs
 * drained at `--rate` (whichever is later), or after `--start-delay`
 * seconds when given. `--max-ahead` stops the run once the next batch would
 * be due later than that many minutes — the rest stays in the backlog for
 * the next run (bounds Redis memory, including Horizon's payload copies).
 * `--rate=0` disables pacing.
 *
 * Safety:
 *   - a chunk is moved atomically (Lua) from `queues:<from>` to
 *     `queues:<from>:rebatching`, dropping as many items from
 *     `queues:<from>:notify` as Laravel's own pop would;
 *   - after every pushed batch the jobs whose entries are all queued are
 *     trimmed from the claimed list (jobs of other classes / unreadable
 *     payloads in that range are returned to the source tail in the same
 *     Lua call), so a crash re-sends at most one batch and loses nothing:
 *     the next run starts with what is left in the claimed list;
 *   - a lock `queues:<from>:rebatch-lock` (60 s, renewed every chunk) keeps
 *     concurrent runs apart;
 *   - the run stops after seeing as many jobs as the backlog held at start,
 *     so returned jobs aren't looped over.
 *
 * Only the ready list is processed — delayed/reserved jobs are left alone.
 */
class RebatchQueueCommand extends Command
{
    protected $signature = 'processhub:rebatch-queue
        {--from= : Name of the backlog queue (e.g. logs-backlog)}
        {--chunk=1000 : Jobs claimed from the backlog per step}
        {--limit= : Max chunks to process in this run}
        {--rate= : Batches per minute to schedule (default processhub.rate_limit_per_minute, 0 = all at once)}
        {--start-delay= : Seconds before the first batch is due (default: after what the target queue already holds)}
        {--max-ahead= : Stop once the next batch would be due later than this many minutes (needs --rate > 0)}';

    protected $description = 'Repack single-entry SendLogBatchJob backlog into full batches';

    /** KEYS: source, claimed, notify; ARGV: count. */
    private const CLAIM_SCRIPT = <<<'LUA'
local items = redis.call('lrange', KEYS[1], 0, tonumber(ARGV[1]) - 1)
if #items > 0 then
    redis.call('ltrim', KEYS[1], #items, -1)
    redis.call('ltrim', KEYS[3], #items, -1)
    redis.call('rpush', KEYS[2], unpack(items))
end
return #items
LUA;

    /** KEYS: claimed, source, notify; ARGV: consumed count, payloads to return... */
    private const ACK_SCRIPT = <<<'LUA'
for i = 2, #ARGV do
    redis.call('rpush', KEYS[2], ARGV[i])
    redis.call('rpush', KEYS[3], 1)
end
redis.call('ltrim', KEYS[1], tonumber(ARGV[1]), -1)
return 1
LUA;

    /** KEYS: lock; ARGV: owner, ttl. Takes a free lock or renews our own. */
    private const LOCK_SCRIPT = <<<'LUA'
local owner = redis.call('get', KEYS[1])
if owner and owner ~= ARGV[1] then
    return 0
end
redis.call('set', KEYS[1], ARGV[1], 'EX', tonumber(ARGV[2]))
return 1
LUA;

    private const UNLOCK_SCRIPT = <<<'LUA'
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
end
return 0
LUA;

    /** KEYS: delayed zset. Score (unix time) of the latest delayed job. */
    private const LAST_DUE_SCRIPT = <<<'LUA'
local last = redis.call('zrange', KEYS[1], -1, -1, 'WITHSCORES')
return last[2]
LUA;

    private const MAX_CHUNK = 5000;

    private const LOCK_TTL = 60;

    private RedisConnection $redis;

    private string $source;

    private string $claimed;

    /** Batches per minute; 0 = no pacing. */
    private int $rate = 0;

    /** Delay of the first scheduled batch. */
    private int $startDelay = 0;

    /** Batches scheduled so far in this run. */
    private int $scheduled = 0;

    private bool $stopping = false;

    public function handle(): int
    {
        $this->scheduled = 0;
        $this->stopping = false;

        $from = (string) $this->option('from');
        if ($from === '') {
            $this->error('--from=<queue> is required.');
            return self::FAILURE;
        }

        $connectionName = config('processhub.connection') ?: config('queue.default');
        $queueConfig = config("queue.connections.{$connectionName}", []);
        if (($queueConfig['driver'] ?? null) !== 'redis') {
            $this->error("Queue connection [{$connectionName}] is not a redis connection.");
            return self::FAILURE;
        }

        // Same resolution SendLogBatchJob::enqueue ends up with.
        $target = config('processhub.queue') ?: ($queueConfig['queue'] ?? 'default');
        if ($target === $from) {
            $this->error('--from must differ from the working queue processhub.queue.');
            return self::FAILURE;
        }

        $this->redis = Redis::connection($queueConfig['connection'] ?? 'default');
        $this->source = 'queues:' . $from;
        $this->claimed = $this->source . ':rebatching';
        $lock = $this->source . ':rebatch-lock';
        $owner = Str::random(20);

        if (! $this->lock($lock, $owner)) {
            $this->error(sprintf(
                'Another processhub:rebatch-queue run holds %s (a killed run releases it within %d s).',
                $lock,
                self::LOCK_TTL,
            ));
            return self::FAILURE;
        }

        try {
            $this->trapSignals();
            $this->planSchedule($connectionName, $target);

            return $this->rebatch($from, $target, $lock, $owner);
        } finally {
            $this->script(self::UNLOCK_SCRIPT, [$lock], [$owner]);
        }
    }

    private function rebatch(string $from, string $target, string $lock, string $owner): int
    {
        $chunk = max(1, min(self::MAX_CHUNK, (int) $this->option('chunk')));
        $limit = $this->option('limit') !== null ? max(1, (int) $this->option('limit')) : null;
        $maxAhead = $this->option('max-ahead') !== null ? max(0, (int) $this->option('max-ahead')) * 60 : null;

        // Jobs returned to the tail must not be seen twice; bound the run by
        // what was there at start (plus a previously claimed chunk).
        $budget = (int) $this->redis->llen($this->source);
        $totals = ['jobs' => 0, 'entries' => 0, 'batches' => 0, 'returned' => 0];
        $this->info(sprintf(
            'Backlog %s: %d jobs ready, %d claimed earlier. First batch due in %d s.',
            $from,
            $budget,
            $this->redis->llen($this->claimed),
            $this->delayFor(0),
        ));

        for ($step = 1; $limit === null || $step <= $limit; $step++) {
            if ($this->stopping) {
                $this->warn('Stopped by signal; the rest stays in the backlog.');
                break;
            }
            if (! $this->lock($lock, $owner)) {
                $this->error("Lost {$lock} to another run; stopping.");
                return self::FAILURE;
            }

            $recovered = (int) $this->redis->llen($this->claimed) > 0;
            if (! $recovered) {
                if ($budget <= 0) {
                    break;
                }
                if ($this->rate > 0 && $maxAhead !== null && $this->delayFor($this->scheduled) > $maxAhead) {
                    $this->line(sprintf('Next batch would be due in %d s > --max-ahead; the rest stays in the backlog.', $this->delayFor($this->scheduled)));
                    break;
                }
                $taken = (int) $this->script(self::CLAIM_SCRIPT, [$this->source, $this->claimed, $this->source . ':notify'], [min($chunk, $budget)]);
                if ($taken === 0) {
                    break;
                }
                $budget -= $taken;
            }

            $stats = $this->processClaimed();
            foreach ($stats as $key => $value) {
                $totals[$key] += $value;
            }

            $this->line(sprintf(
                '#%d%s: %d jobs → %d entries → %d batches, %d returned; backlog left %d',
                $step,
                $recovered ? ' (recovered)' : '',
                $stats['jobs'],
                $stats['entries'],
                $stats['batches'],
                $stats['returned'],
                $this->redis->llen($this->source),
            ));
        }

        $this->info(sprintf(
            'Done: %d jobs, %d entries repacked into %d batches on [%s]; %d jobs returned to [%s].',
            $totals['jobs'],
            $totals['entries'],
            $totals['batches'],
            $target,
            $totals['returned'],
            $from,
        ));
        if ($this->rate > 0 && $this->scheduled > 0) {
            $this->info(sprintf(
                'Paced at %d batches/min: the last batch is due in %d min.',
                $this->rate,
                (int) ceil($this->delayFor($this->scheduled - 1) / 60),
            ));
        }

        $this->dropNotifyListIfDrained();

        return self::SUCCESS;
    }

    /**
     * By default the new batches queue up behind what the target already
     * holds, so consecutive runs (and runs with a different --rate) don't
     * overlap: after its last delayed job, and after all its jobs drained
     * at --rate when workers lag behind the schedule.
     */
    private function planSchedule(string $connectionName, string $target): void
    {
        $this->startDelay = 0;
        $rate = $this->option('rate');
        $this->rate = max(0, $rate !== null ? (int) $rate : (int) config('processhub.rate_limit_per_minute', 50));
        if ($this->rate === 0) {
            return;
        }

        $startDelay = $this->option('start-delay');
        if ($startDelay !== null) {
            $this->startDelay = max(0, (int) $startDelay);
            return;
        }

        $drain = intdiv(Queue::connection($connectionName)->size($target) * 60, $this->rate);
        $lastDue = $this->script(self::LAST_DUE_SCRIPT, ['queues:' . $target . ':delayed'], []);
        $afterLast = is_numeric($lastDue)
            ? (int) ceil((float) $lastDue) - now()->getTimestamp() + intdiv(60, $this->rate)
            : 0;

        $this->startDelay = max(0, $drain, $afterLast);
    }

    /** Seconds until the n-th (0-based) batch of this run is due. */
    private function delayFor(int $n): int
    {
        return $this->rate > 0 ? $this->startDelay + intdiv($n * 60, $this->rate) : 0;
    }

    /**
     * Each pushed job left an item in `queues:<from>:notify`; once the backlog
     * is gone the leftovers are dead weight.
     */
    private function dropNotifyListIfDrained(): void
    {
        $left = (int) $this->redis->llen($this->source) + (int) $this->redis->llen($this->claimed)
            + (int) $this->redis->zcard($this->source . ':delayed') + (int) $this->redis->zcard($this->source . ':reserved');

        if ($left === 0 && (int) $this->redis->del($this->source . ':notify') > 0) {
            $this->line("Removed {$this->source}:notify.");
        }
    }

    /**
     * Repack the claimed list. `$pending` mirrors its head: for every job
     * not yet acknowledged — how many entries must be queued before it is
     * done, and its payload if it has to go back to the source.
     *
     * @return array{jobs: int, entries: int, batches: int, returned: int}
     */
    private function processClaimed(): array
    {
        $payloads = $this->redis->lrange($this->claimed, 0, -1);
        $batcher = BatchBuilder::fromConfig();
        $stats = ['jobs' => count($payloads), 'entries' => 0, 'batches' => 0, 'returned' => 0];
        /** @var array<int, array{0: int, 1: string|null}> $pending */
        $pending = [];
        $queued = 0;

        foreach ($payloads as $payload) {
            $entries = $this->extractEntries($payload);
            if ($entries === null) {
                $pending[] = [$stats['entries'], $payload];
                $stats['returned']++;
                continue;
            }

            $stats['entries'] += count($entries);
            $pending[] = [$stats['entries'], null];
            foreach ($entries as $entry) {
                foreach ($batcher->add($entry) as $batch) {
                    $queued += $this->schedule($batch);
                    $stats['batches']++;
                    $this->acknowledge($pending, $queued);
                }
            }
        }

        $rest = $batcher->drain();
        if ($rest !== []) {
            $queued += $this->schedule($rest);
            $stats['batches']++;
        }
        $this->acknowledge($pending, $queued);

        return $stats;
    }

    /**
     * @param  array<int, array<string, mixed>>  $batch
     * @return int entries queued
     */
    private function schedule(array $batch): int
    {
        SendLogBatchJob::enqueue($batch, $this->delayFor($this->scheduled++));

        return count($batch);
    }

    /**
     * Trim the jobs whose entries are all queued from the claimed list and
     * return foreign ones among them to the source — atomically.
     *
     * @param  array<int, array{0: int, 1: string|null}>  $pending
     */
    private function acknowledge(array &$pending, int $queued): void
    {
        $done = 0;
        $foreign = [];
        while ($done < count($pending) && $pending[$done][0] <= $queued) {
            if ($pending[$done][1] !== null) {
                $foreign[] = $pending[$done][1];
            }
            $done++;
        }
        if ($done === 0) {
            return;
        }

        $this->script(self::ACK_SCRIPT, [$this->claimed, $this->source, $this->source . ':notify'], [$done, ...$foreign]);
        $pending = array_slice($pending, $done);
    }

    /**
     * @return array<int, array<string, mixed>>|null null when the payload is
     *                                                not a readable SendLogBatchJob
     */
    private function extractEntries(string $payload): ?array
    {
        $decoded = json_decode($payload, true);
        $data = is_array($decoded) ? ($decoded['data'] ?? null) : null;

        if (! is_array($data) || ($data['commandName'] ?? null) !== SendLogBatchJob::class) {
            $this->getOutput()->getErrorStyle()->writeln(sprintf(
                'Skipping foreign job %s (returned to the backlog).',
                is_array($decoded) ? ($decoded['displayName'] ?? 'unknown') : 'unparseable payload',
            ));
            return null;
        }

        $command = @unserialize((string) ($data['command'] ?? ''), ['allowed_classes' => [SendLogBatchJob::class]]);
        if (! $command instanceof SendLogBatchJob) {
            $this->getOutput()->getErrorStyle()->writeln('Unreadable SendLogBatchJob payload (returned to the backlog).');
            return null;
        }

        return array_values(array_filter($command->entries, 'is_array'));
    }

    private function lock(string $key, string $owner): bool
    {
        return (int) $this->script(self::LOCK_SCRIPT, [$key], [$owner, self::LOCK_TTL]) === 1;
    }

    /**
     * Laravel's eval() (script, numKeys, ...args) — keys get the connection
     * prefix with both phpredis and predis.
     *
     * @param  array<int, string>  $keys
     * @param  array<int, string|int>  $args
     */
    private function script(string $lua, array $keys, array $args): mixed
    {
        // @phpstan-ignore arguments.count, argument.type, argument.type
        return $this->redis->eval($lua, count($keys), ...$keys, ...$args);
    }

    private function trapSignals(): void
    {
        if (! extension_loaded('pcntl')) {
            return;
        }

        $this->trap([SIGINT, SIGTERM], function (): void {
            $this->stopping = true;
        });
    }
}
