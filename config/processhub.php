<?php

/**
 * ProcessHub Logs configuration.
 *
 * Публикуется командой `php artisan vendor:publish --tag=processhub-config`
 * и читается из переменных окружения (.env).
 */

return [
    /*
    |--------------------------------------------------------------------------
    | Ingest endpoint
    |--------------------------------------------------------------------------
    */
    'url' => env('PROCESSHUB_LOG_URL'),
    'token' => env('PROCESSHUB_LOG_TOKEN'),

    /*
    |--------------------------------------------------------------------------
    | Delivery
    |--------------------------------------------------------------------------
    |
    | queue    — имя очереди для SendLogBatchJob. Используйте отдельную
    |            очередь ('logs') чтобы всплески не блокировали business jobs.
    | connection — connection имя, обычно 'redis' или 'database'.
    | batch_size — сколько событий отправлять одним запросом (до 100 по
    |              контракту ProcessHub).
    | timeout_ms — сколько ждать HTTP-ответа.
    */
    'queue' => env('PROCESSHUB_LOG_QUEUE', 'logs'),
    'connection' => env('PROCESSHUB_LOG_CONNECTION', null),
    'batch_size' => (int) env('PROCESSHUB_LOG_BATCH_SIZE', 100),
    'timeout_ms' => (int) env('PROCESSHUB_LOG_TIMEOUT_MS', 5000),

    /*
    |--------------------------------------------------------------------------
    | Fallback
    |--------------------------------------------------------------------------
    |
    | Если ingest недоступен или все retry кончились — события сохраняются
    | в локальный файл (storage/logs/processhub-fallback.log), чтобы не
    | потеряться. Scheduled command `processhub:flush-fallback` пытается
    | доотправить при следующем апе сети.
    */
    'fallback_path' => storage_path('logs/processhub-fallback.log'),

    /*
    |--------------------------------------------------------------------------
    | Heartbeat
    |--------------------------------------------------------------------------
    |
    | Schedule зарегистрирован в ServiceProvider — каждую минуту (60 сек) шлёт
    | POST /api/ingest/heartbeat. ProcessHub флипает статус на OFFLINE через
    | 3× этой паузы (180 сек).
    */
    'heartbeat_enabled' => (bool) env('PROCESSHUB_HEARTBEAT_ENABLED', true),

    /*
    |--------------------------------------------------------------------------
    | PII Redaction
    |--------------------------------------------------------------------------
    |
    | keys    — ключи в context-массивах, чьи значения целиком заменяются
    |           на [REDACTED]. Matching регистронезависимый по substring.
    | patterns — regex → replacement для строковых значений. По умолчанию
    |            закрываем email / JWT / Bearer / credit cards.
    */
    'redact' => [
        'keys' => [
            'password', 'passwd', 'secret', 'token', 'authorization', 'api_key',
            'apikey', 'private_key', 'cookie', 'csrf', 'otp',
        ],
        'patterns' => [
            '/\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b/' => '[JWT]',
            '/Bearer\s+[A-Za-z0-9._~+\/=-]{10,}/i' => 'Bearer [REDACTED]',
            '/\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b/' => '[EMAIL]',
            '/\b\d(?:[ -]?\d){12,18}\b/' => '[CARD]',
        ],
    ],

    /*
    |--------------------------------------------------------------------------
    | Event listeners — structured context per Laravel event
    |--------------------------------------------------------------------------
    |
    | Оборачивают QueryExecuted / JobFailed / и т.п. в log entries с
    | contextType — ProcessHub UI рендерит их специализированными виджетами
    | (SQL highlighting для query, stack trace для exception).
    */
    'listeners' => [
        'query' => (bool) env('PROCESSHUB_LOG_SLOW_QUERIES', false),
        'query_slow_ms' => (int) env('PROCESSHUB_SLOW_QUERY_MS', 1000),
        'job_failures' => true,
        'scheduled_tasks' => true,
    ],
];
