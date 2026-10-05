<?php

namespace ProcessHub\Logs\Listeners;

use Illuminate\Queue\Events\JobFailed;
use Illuminate\Support\Facades\Log;
use ProcessHub\Logs\Jobs\SendLogBatchJob;

/**
 * Forwards every failed queue job as an ERROR log with structured context
 * — ProcessHub groups failed jobs by job class + exception fingerprint,
 * which is usually what the user wants to see ("Jobs::SendReceipt failed
 * 47 times last hour").
 *
 * Failures of SendLogBatchJob itself are skipped: their batches already go
 * to the fallback file via `failed()`, and logging them into the same
 * channel would enqueue a new SendLogBatchJob per failure — a feedback loop.
 */
class HandleJobFailed
{
    public function handle(JobFailed $event): void
    {
        if ($event->job->resolveName() === SendLogBatchJob::class) {
            return;
        }

        $payload = $event->job->payload();
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
