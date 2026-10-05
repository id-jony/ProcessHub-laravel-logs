<?php

/**
 * Run by FatalErrorFlushTest in a separate PHP process: boots the package,
 * writes a log record and dies ($argv[1]: memory-large | memory-small |
 * timeout | uncaught). Jobs land in the sqlite database $argv[2].
 */

use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Schema;
use Orchestra\Testbench\Foundation\Application;
use ProcessHub\Logs\Logging\ProcessHubFactory;
use ProcessHub\Logs\ProcessHubServiceProvider;

require __DIR__ . '/../../vendor/autoload.php';

[, $mode, $database] = $argv;

Application::create(options: ['extra' => [
    'providers' => [ProcessHubServiceProvider::class],
    'dont-discover' => ['*'],
]]);

config([
    'database.default' => 'jobs',
    'database.connections.jobs' => ['driver' => 'sqlite', 'database' => $database, 'prefix' => ''],
    'queue.default' => 'database',
    'queue.connections.database' => [
        'driver' => 'database', 'connection' => 'jobs', 'table' => 'jobs', 'queue' => 'default', 'retry_after' => 90,
    ],
    'logging.default' => 'stack',
    'logging.channels.stack' => ['driver' => 'stack', 'channels' => ['processhub']],
    'logging.channels.processhub' => ['driver' => 'custom', 'via' => ProcessHubFactory::class, 'level' => 'debug'],
    'processhub.url' => 'https://ph.test',
    'processhub.token' => 'ph_live_test_test_00000000000000000000000000000000',
    'processhub.connection' => 'database',
    'processhub.queue' => 'logs',
    'processhub.fallback_path' => $database . '.fallback.log',
]);

Schema::create('jobs', function (Blueprint $table) {
    $table->id();
    $table->string('queue')->index();
    $table->longText('payload');
    $table->unsignedTinyInteger('attempts');
    $table->unsignedInteger('reserved_at')->nullable();
    $table->unsignedInteger('available_at');
    $table->unsignedInteger('created_at');
});

// Registered after the package: still has to be shipped.
register_shutdown_function(fn () => Log::warning('late shutdown'));

Log::warning('before fatal');

if (str_starts_with($mode, 'memory')) {
    ini_set('memory_limit', (string) (memory_get_usage() + 16 * 1024 * 1024));
    $chunk = $mode === 'memory-large' ? 1024 * 1024 : 100;
    $hog = [];
    while (true) {
        $hog[] = str_repeat('x', $chunk);
    }
}

if ($mode === 'timeout') {
    set_time_limit(1);
    while (true) {
        // Busy loop until max_execution_time.
    }
}

throw new RuntimeException('uncaught in script');
