<?php

namespace ProcessHub\Logs\Listeners;

use Illuminate\Queue\Events\JobFailed;
use Illuminate\Support\Facades\Log;

/**
 * Forwards every failed queue job as an ERROR log with structured context
 * — ProcessHub groups failed jobs by job class + exception fingerprint,
 * which is usually what the user wants to see ("Jobs::SendReceipt failed
 * 47 times last hour").
 */
class HandleJobFailed
{
    public function handle(JobFailed $event): void
    {
        $payload = $event->job->payload() ?? [];
        Log::channel('processhub')->error('Queue job failed', [
            'type' => 'job',
            'class' => $payload['displayName'] ?? 'unknown',
            'connection' => $event->connectionName,
            'queue' => $event->job->getQueue(),
            'attempts' => $event->job->attempts(),
            'exception' => $event->exception,
        ]);
    }
}
