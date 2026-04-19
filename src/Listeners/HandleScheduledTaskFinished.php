<?php

namespace ProcessHub\Logs\Listeners;

use Illuminate\Support\Facades\Log;

/**
 * Generic handler for ScheduledTaskFailed / ScheduledTaskSkipped events.
 * Both events expose `$task->command` (the string artisan invocation) and
 * carry an exception or reason — we forward that as a WARN / ERROR.
 *
 * The event class is resolved dynamically in ServiceProvider::registerEventListeners
 * because availability differs across Laravel versions.
 */
class HandleScheduledTaskFinished
{
    public function handle(object $event): void
    {
        $command = property_exists($event, 'task') && isset($event->task->command)
            ? $event->task->command
            : 'unknown';
        $failed = method_exists($event, 'getName')
            ? str_contains(strtolower($event::class), 'failed')
            : str_contains(strtolower($event::class), 'failed');

        $level = $failed ? 'error' : 'warning';

        Log::channel('processhub')->{$level}(
            $failed ? 'Scheduled task failed' : 'Scheduled task skipped',
            [
                'type' => 'scheduled',
                'command' => $command,
                'exception' => property_exists($event, 'exception') ? $event->exception : null,
                'reason' => property_exists($event, 'reason') ? $event->reason : null,
            ],
        );
    }
}
