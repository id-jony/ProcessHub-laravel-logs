<?php

namespace ProcessHub\Logs\Commands;

use Illuminate\Console\Command;
use Illuminate\Redis\Connections\Connection as RedisConnection;
use Illuminate\Support\Facades\Redis;
use ProcessHub\Logs\Jobs\SendLogBatchJob;
use ProcessHub\Logs\Support\BatchBuilder;

/**
 * `php artisan processhub:rebatch-queue --from=logs-backlog [--chunk=1000] [--limit=N]`
 *
 * Recovery tool for backlogs of one-entry SendLogBatchJob payloads (left by
 * package versions that queued a job per log record). Drains the ready list
 * of the given Redis queue, packs the entries into full batches and pushes
 * them as new SendLogBatchJob onto `processhub.queue`.
 *
 * Safety: each chunk is moved atomically (Lua: LRANGE + LTRIM + RPUSH) from
 * `queues:<from>` to `queues:<from>:rebatching`, processed, and only then
 * deleted. A crash leaves the claimed chunk in the side list, and the next
 * run starts by re-processing it — at most one chunk can be re-sent twice,
 * nothing is lost. Jobs of other classes (or unreadable payloads) are pushed
 * back to the tail of the source queue untouched; the run stops after
 * seeing as many jobs as the backlog held at start, so they aren't looped.
 *
 * Only the ready list is processed — delayed/reserved jobs are left alone.
 */
class RebatchQueueCommand extends Command
{
    protected $signature = 'processhub:rebatch-queue
        {--from= : Name of the backlog queue (e.g. logs-backlog)}
        {--chunk=1000 : Jobs claimed from the backlog per step}
        {--limit= : Max chunks to process in this run}';

    protected $description = 'Repack single-entry SendLogBatchJob backlog into full batches';

    private const CLAIM_SCRIPT = <<<'LUA'
local items = redis.call('lrange', KEYS[1], 0, tonumber(ARGV[1]) - 1)
if #items > 0 then
    redis.call('ltrim', KEYS[1], #items, -1)
    redis.call('rpush', KEYS[2], unpack(items))
end
return #items
LUA;

    private const MAX_CHUNK = 5000;

    public function handle(): int
    {
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

        /** @var RedisConnection $redis */
        $redis = Redis::connection($queueConfig['connection'] ?? 'default');
        $source = 'queues:' . $from;
        $claimed = $source . ':rebatching';
        $chunk = max(1, min(self::MAX_CHUNK, (int) $this->option('chunk')));
        $limit = $this->option('limit') !== null ? max(1, (int) $this->option('limit')) : null;

        // Jobs returned to the tail must not be seen twice; bound the run by
        // what was there at start (plus a previously claimed chunk).
        $budget = (int) $redis->llen($source);
        $totals = ['jobs' => 0, 'entries' => 0, 'batches' => 0, 'returned' => 0];
        $this->info(sprintf('Backlog %s: %d jobs ready, %d claimed earlier.', $from, $budget, $redis->llen($claimed)));

        for ($step = 1; $limit === null || $step <= $limit; $step++) {
            $recovered = (int) $redis->llen($claimed) > 0;
            if (! $recovered) {
                if ($budget <= 0) {
                    break;
                }
                // Laravel's eval() signature (script, numKeys, ...args), not \Redis::eval's.
                // @phpstan-ignore arguments.count, argument.type, argument.type
                $taken = (int) $redis->eval(self::CLAIM_SCRIPT, 2, $source, $claimed, min($chunk, $budget));
                if ($taken === 0) {
                    break;
                }
                $budget -= $taken;
            }

            $stats = $this->processClaimed($redis, $claimed, $source);
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
                $redis->llen($source),
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

        return self::SUCCESS;
    }

    /**
     * @return array{jobs: int, entries: int, batches: int, returned: int}
     */
    private function processClaimed(RedisConnection $redis, string $claimed, string $source): array
    {
        $payloads = $redis->lrange($claimed, 0, -1);
        $batcher = BatchBuilder::fromConfig();
        $stats = ['jobs' => count($payloads), 'entries' => 0, 'batches' => 0, 'returned' => 0];
        $foreign = [];

        foreach ($payloads as $payload) {
            $entries = $this->extractEntries($payload);
            if ($entries === null) {
                $foreign[] = $payload;
                continue;
            }
            foreach ($entries as $entry) {
                $stats['entries']++;
                foreach ($batcher->add($entry) as $batch) {
                    SendLogBatchJob::enqueue($batch);
                    $stats['batches']++;
                }
            }
        }

        $rest = $batcher->drain();
        if ($rest !== []) {
            SendLogBatchJob::enqueue($rest);
            $stats['batches']++;
        }

        if ($foreign !== []) {
            $redis->rpush($source, ...$foreign);
            $stats['returned'] = count($foreign);
        }

        $redis->del($claimed);

        return $stats;
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
}
