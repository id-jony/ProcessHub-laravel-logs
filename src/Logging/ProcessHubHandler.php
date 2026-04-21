<?php

namespace ProcessHub\Logs\Logging;

use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Monolog\Handler\AbstractProcessingHandler;
use Monolog\Level;
use Monolog\LogRecord;
use ProcessHub\Logs\Jobs\SendLogBatchJob;
use ProcessHub\Logs\Redaction\Redactor;

/**
 * Monolog handler which queues log records for async delivery to ProcessHub.
 *
 * Почему queue, а не sync HTTP:
 *   - Ingest endpoint может быть временно недоступен — ретрай через Laravel
 *     queue infrastructure + fallback на файл при исчерпании attempts.
 *   - Синхронный POST на каждое Log::error добавил бы 50-200 мс латенси
 *     клиенскому запросу. Для ERROR в hot-path это неприемлемо.
 *
 * Redaction применяется здесь (на клиенте) до постановки в очередь —
 * чувствительные данные не попадают даже в Redis/БД-драйвер очереди.
 */
class ProcessHubHandler extends AbstractProcessingHandler
{
    /**
     * Whitelist of `contextType` discriminators accepted by the server's
     * zod enum. Anything else is dropped to null so the user's own
     * `context.type` (e.g. "video", "audio") doesn't clash with our
     * reserved wire-format field and trigger a 400.
     */
    private const ALLOWED_CONTEXT_TYPES = [
        'exception', 'query', 'job', 'mail', 'http', 'scheduled',
    ];

    public function __construct(
        private readonly QueueFactory $queue,
        Level|int|string $level = Level::Warning,
        bool $bubble = true,
    ) {
        parent::__construct($level, $bubble);
    }

    protected function write(LogRecord $record): void
    {
        $url = config('processhub.url');
        $token = config('processhub.token');
        if (! $url || ! $token) {
            // Package not configured — silently drop. Install command will
            // have already warned the user.
            return;
        }

        $entry = $this->buildEntry($record);

        // Push to a dedicated queue so a log firehose doesn't starve the
        // app's business jobs.
        $queueName = config('processhub.queue');
        $connection = config('processhub.connection');

        $job = new SendLogBatchJob([$entry]);
        if ($queueName) {
            $job->onQueue($queueName);
        }
        if ($connection) {
            $job->onConnection($connection);
        }

        $this->queue->connection($connection)->pushOn($queueName, $job);
    }

    /**
     * Convert a Monolog record to the ProcessHub wire-format `ApplicationLog`.
     */
    protected function buildEntry(LogRecord $record): array
    {
        $context = Redactor::redact($record->context ?? []);

        // contextType is a closed enum on the server (exception/query/job/mail/
        // http/scheduled). User code often puts domain-specific values in
        // `context.type` ("video", "audio", …) — those must NOT leak into the
        // wire-format contextType, else the batch is rejected with 400.
        $rawType = $context['type'] ?? null;
        $contextType = is_string($rawType) && in_array($rawType, self::ALLOWED_CONTEXT_TYPES, true)
            ? $rawType
            : null;

        // If a Throwable was passed in context (Laravel Exception handler does
        // this) — extract a structured exception payload so ProcessHub can
        // render the stack trace and group by fingerprint.
        if (($context['exception'] ?? null) instanceof \Throwable) {
            /** @var \Throwable $e */
            $e = $context['exception'];
            $contextType = 'exception';
            $context = [
                'class' => get_class($e),
                'message' => $e->getMessage(),
                'file' => $e->getFile(),
                'line' => $e->getLine(),
                'trace' => array_slice(
                    explode("\n", $e->getTraceAsString()),
                    0,
                    30,
                ),
            ]
                // Preserve surviving keys from the original context so
                // request-id and host aren't lost.
                + array_diff_key($context, array_flip(['exception']));
        }

        $requestId = null;
        if (isset($context['request_id']) && is_string($context['request_id'])) {
            $requestId = $context['request_id'];
            unset($context['request_id']);
        }

        return array_filter([
            'timestamp' => $record->datetime->format(\DateTimeInterface::RFC3339_EXTENDED),
            'level' => $this->normaliseLevel($record->level),
            'message' => $record->message,
            'host' => gethostname() ?: null,
            'requestId' => $requestId,
            'contextType' => $contextType,
            'context' => ! empty($context) ? $context : null,
        ], static fn ($v) => $v !== null);
    }

    /**
     * Laravel/PSR levels → ProcessHub's 3-level scheme (INFO/WARN/ERROR).
     */
    protected function normaliseLevel(Level $level): string
    {
        return match (true) {
            $level->value >= Level::Error->value => 'ERROR',
            $level->value >= Level::Warning->value => 'WARN',
            default => 'INFO',
        };
    }
}
