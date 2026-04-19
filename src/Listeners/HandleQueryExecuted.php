<?php

namespace ProcessHub\Logs\Listeners;

use Illuminate\Database\Events\QueryExecuted;
use Illuminate\Support\Facades\Log;

/**
 * Optional — only active when config('processhub.listeners.query') is on.
 * Emits an INFO log for queries slower than the configured threshold;
 * ProcessHub renders them with SQL syntax highlighting.
 *
 * Off by default because every `->get()` would flood the channel.
 */
class HandleQueryExecuted
{
    public function handle(QueryExecuted $event): void
    {
        $threshold = (int) config('processhub.listeners.query_slow_ms', 1000);
        if ($event->time < $threshold) return;

        Log::channel('processhub')->info('Slow query', [
            'type' => 'query',
            'sql' => $event->sql,
            'bindings' => $event->bindings,
            'durationMs' => $event->time,
            'connection' => $event->connectionName,
        ]);
    }
}
