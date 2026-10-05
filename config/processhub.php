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
    |              контракту ProcessHub). Handler копит записи в памяти и
    |              ставит одну задачу на пачку.
    | batch_max_bytes — потолок JSON-размера пачки (сервер принимает до 2 МБ).
    | flush_interval_sec — сколько секунд неполная пачка ждёт в памяти
    |              долгоживущего процесса (queue-воркер), прежде чем
    |              уйти в очередь. Конец HTTP-запроса / команды / воркера
    |              отправляет пачку сразу.
    | timeout_ms — сколько ждать HTTP-ответа (округляется вверх до секунд).
    | retry_window_sec — сколько секунд (не меньше 60) SendLogBatchJob пытается
    |              доставить пачку с момента, когда она должна уйти (429 ждёт
    |              Retry-After и попыток не тратит), потом — fallback-файл.
    |              Срок проверяет сама задача, даже если её взяли из очереди
    |              позже; retryUntil воркера — срок + сутки.
    | rate_limit_per_minute — общий для всех процессов лимит POST в
    |              /api/ingest/logs (ProcessHub пускает 60/мин на токен;
    |              50 оставляет запас под heartbeat и прочее). Запрос раз в
    |              60/лимит с, всплеск до ⌈лимит/10⌉: в любые 60 с — не больше
    |              лимит + всплеск (50 → 55). 0 — без лимита.
    | rate_limit_store — cache store для состояния лимита (null — cache.default).
    |              Нужен общий для всех воркеров и с атомарными блокировками:
    |              redis / memcached / database (таблица cache_locks); file —
    |              только на одном хосте, array — внутри одного процесса.
    |              Store без блокировок или недоступный — лимит отключается
    |              (предупреждение в лог раз на процесс).
    | unbuffered_commands — долгоживущие artisan-команды без точки сброса
    |              (свои демоны, long-polling), где записи уходят в очередь
    |              сразу. Уже учтены: horizon, horizon:supervisor,
    |              schedule:work, queue:listen, reverb:start, pulse:check,
    |              pulse:work, octane:start. queue:work/horizon:work не
    |              нужны — их цикл отправляет пачки по возрасту.
    */
    'queue' => env('PROCESSHUB_LOG_QUEUE', 'logs'),
    'connection' => env('PROCESSHUB_LOG_CONNECTION', null),
    'batch_size' => (int) env('PROCESSHUB_LOG_BATCH_SIZE', 100),
    'batch_max_bytes' => (int) env('PROCESSHUB_LOG_BATCH_MAX_BYTES', 1800000),
    'flush_interval_sec' => (int) env('PROCESSHUB_LOG_FLUSH_INTERVAL_SEC', 10),
    'timeout_ms' => (int) env('PROCESSHUB_LOG_TIMEOUT_MS', 5000),
    'retry_window_sec' => (int) env('PROCESSHUB_LOG_RETRY_WINDOW_SEC', 3600),
    'rate_limit_per_minute' => (int) env('PROCESSHUB_RATE_LIMIT_PER_MINUTE', 50),
    'rate_limit_store' => env('PROCESSHUB_RATE_LIMIT_STORE'),
    'unbuffered_commands' => [],

    /*
    |--------------------------------------------------------------------------
    | Fallback
    |--------------------------------------------------------------------------
    |
    | Если пачку не доставить (4xx, истёк retry_window_sec, таймаут попытки) —
    | события сохраняются в локальный файл (storage/logs/processhub-fallback.log),
    | чтобы не потеряться. Доотправляет их `processhub:flush-fallback` — пакет
    | её не планирует, добавьте в расписание сами (см. README).
    | null — fallback выключен: недоставленные пачки теряются.
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
    | mail — INFO-запись «Mail sent» на каждое отправленное письмо.
    */
    'listeners' => [
        'query' => (bool) env('PROCESSHUB_LOG_SLOW_QUERIES', false),
        'query_slow_ms' => (int) env('PROCESSHUB_SLOW_QUERY_MS', 1000),
        'job_failures' => true,
        'scheduled_tasks' => true,
        'mail' => true,
    ],
];
