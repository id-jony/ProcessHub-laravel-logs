<?php

namespace ProcessHub\Logs\Logging;

use Monolog\Handler\AbstractProcessingHandler;
use Monolog\Level;
use Monolog\LogRecord;
use ProcessHub\Logs\Jobs\SendLogBatchJob;
use ProcessHub\Logs\Redaction\Redactor;
use Symfony\Component\ErrorHandler\Error\FatalError;

/**
 * Monolog handler which queues log records for async delivery to ProcessHub.
 *
 * Records are collected in LogBuffer and shipped as one SendLogBatchJob per
 * `processhub.batch_size` entries (not one job per record); see LogBuffer
 * for when partial batches are shipped.
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

    /**
     * PSR-3 / RFC 5424 numeric severities. Used to compare a record's level
     * against `processhub.min_level` (string from remote config).
     *
     * NOTE: this is a SECOND filter on top of the Monolog channel `level`
     * configured in `config/logging.php`. We can only TIGHTEN further — if
     * the channel is at WARNING, setting min_level=info won't make INFO
     * events appear (Monolog drops them before write() is even called).
     * Document this clearly in the UI and README.
     */
    private const PSR3_LEVELS = [
        'debug'     => 100,
        'info'      => 200,
        'notice'    => 250,
        'warning'   => 300,
        'warn'      => 300, // ProcessHub's own name (INFO/WARN/ERROR)
        'error'     => 400,
        'critical'  => 500,
        'alert'     => 550,
        'emergency' => 600,
    ];

    public function __construct(
        private readonly LogBuffer $buffer,
        Level|int|string $level = Level::Warning,
        bool $bubble = true,
    ) {
        parent::__construct($level, $bubble);
    }

    protected function write(LogRecord $record): void
    {
        // Records produced while delivering logs (queue push, running
        // SendLogBatchJob) or reporting its failures would feed back into
        // this very channel — drop them.
        if ($this->buffer->isMuted()
            || SendLogBatchJob::isOwnFailure($record->context['exception'] ?? null)
        ) {
            return;
        }

        $url = config('processhub.url');
        $token = config('processhub.token');
        if (! $url || ! $token) {
            // Package not configured — silently drop. Install command will
            // have already warned the user.
            return;
        }

        // Master kill-switch from remote config — when ops flips
        // `enabled = false` in the ProcessHub UI, drop the record before
        // queueing ("false", 0, "off" too; unknown values keep it on).
        // Heartbeat refreshes the config file; long-running workers pick it
        // up after a restart. Server-side ingest also enforces this.
        if (filter_var(config('processhub.enabled') ?? true, FILTER_VALIDATE_BOOL, FILTER_NULL_ON_FAILURE) === false) {
            return;
        }

        // Remote min_level filter — the Monolog channel level is the lower
        // bound (we cannot loosen it past what the channel passes), but we
        // CAN tighten further at runtime via remote config without redeploy.
        // Example: channel level=info, remote min_level=error → only ERROR+
        // events get queued. This is the bug fix for "поставил error, всё
        // равно приходит INFO/WARN" — handler used to ignore the setting.
        $minLevelName = config('processhub.min_level');
        if (is_string($minLevelName)) {
            $minLevelName = strtolower($minLevelName);
            if (isset(self::PSR3_LEVELS[$minLevelName])
                && $record->level->value < self::PSR3_LEVELS[$minLevelName]
            ) {
                return;
            }
        }

        $this->buffer->push($this->buildEntry($record));

        if (($record->context['exception'] ?? null) instanceof FatalError) {
            $this->buffer->flushFatalError();
        }
    }

    public function close(): void
    {
        $this->buffer->flush();
        parent::close();
    }

    /**
     * Called by Laravel/Octane between units of work on long-lived processes.
     */
    public function reset(): void
    {
        $this->buffer->flush();
        parent::reset();
    }

    /**
     * Convert a Monolog record to the ProcessHub wire-format `ApplicationLog`.
     *
     * @return array<string, mixed>
     */
    protected function buildEntry(LogRecord $record): array
    {
        // The Throwable is taken out BEFORE redaction: Redactor walks objects
        // via get_object_vars(), which turns an exception into [] and loses
        // the class/message/trace.
        $rawContext = $record->context;
        $exception = $rawContext['exception'] ?? null;
        if ($exception instanceof \Throwable) {
            unset($rawContext['exception']);
        }
        $context = Redactor::redact($rawContext);

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
        if ($exception instanceof \Throwable) {
            $e = $exception;
            $contextType = 'exception';
            // Message and trace may carry PII (emails, tokens) — mask them too.
            $context = Redactor::redact([
                'class' => get_class($e),
                'message' => $e->getMessage(),
                'file' => $e->getFile(),
                'line' => $e->getLine(),
                'trace' => array_slice(
                    explode("\n", $e->getTraceAsString()),
                    0,
                    30,
                ),
            ])
                // Preserve surviving keys from the original context so
                // request-id and host aren't lost.
                + $context;
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
