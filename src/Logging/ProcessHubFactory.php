<?php

namespace ProcessHub\Logs\Logging;

use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Monolog\Level;
use Monolog\Logger;

/**
 * Factory used by `config/logging.php` to create the "processhub" channel.
 *
 * Usage:
 *   'processhub' => [
 *       'driver' => 'custom',
 *       'via'    => ProcessHub\Logs\Logging\ProcessHubFactory::class,
 *       'level'  => env('LOG_LEVEL', 'warning'),
 *   ],
 */
class ProcessHubFactory
{
    public function __invoke(array $config): Logger
    {
        $level = $this->resolveLevel($config['level'] ?? 'warning');
        $handler = new ProcessHubHandler(
            app(QueueFactory::class),
            $level,
        );

        return new Logger('processhub', [$handler]);
    }

    protected function resolveLevel(string|int|Level $raw): Level
    {
        if ($raw instanceof Level) {
            return $raw;
        }
        if (is_int($raw)) {
            return Level::from($raw);
        }
        return match (strtolower($raw)) {
            'debug' => Level::Debug,
            'info' => Level::Info,
            'notice' => Level::Notice,
            'warning', 'warn' => Level::Warning,
            'error' => Level::Error,
            'critical' => Level::Critical,
            'alert' => Level::Alert,
            'emergency' => Level::Emergency,
            default => Level::Warning,
        };
    }
}
