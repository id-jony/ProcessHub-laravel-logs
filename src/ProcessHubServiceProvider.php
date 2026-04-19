<?php

namespace ProcessHub\Logs;

use Illuminate\Console\Scheduling\Schedule;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Http\Kernel;
use Illuminate\Support\ServiceProvider;
use ProcessHub\Logs\Commands\FlushFallbackCommand;
use ProcessHub\Logs\Commands\HeartbeatCommand;
use ProcessHub\Logs\Commands\InstallCommand;
use ProcessHub\Logs\Commands\TestCommand;
use ProcessHub\Logs\Listeners\HandleExceptionReported;
use ProcessHub\Logs\Middleware\CorrelateRequestId;

class ProcessHubServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        // Merge defaults — user's `config/processhub.php` wins after publish.
        $this->mergeConfigFrom(__DIR__ . '/../config/processhub.php', 'processhub');

        // Manager backing the `ProcessHub` facade (ProcessHub::markDeploy).
        $this->app->singleton(ProcessHubManager::class);
    }

    public function boot(): void
    {
        // 1. Make config publishable so apps can tweak it.
        $this->publishes([
            __DIR__ . '/../config/processhub.php' => config_path('processhub.php'),
        ], 'processhub-config');

        // 2. Register artisan commands.
        if ($this->app->runningInConsole()) {
            $this->commands([
                InstallCommand::class,
                TestCommand::class,
                HeartbeatCommand::class,
                FlushFallbackCommand::class,
            ]);
        }

        // 3. Register request-id middleware globally so every controller
        //    call gets an X-Request-Id propagated into Monolog context.
        /** @var Kernel $kernel */
        $kernel = $this->app->make(Kernel::class);
        if (method_exists($kernel, 'prependMiddleware')) {
            $kernel->prependMiddleware(CorrelateRequestId::class);
        }

        // 4. Hook into scheduler — heartbeat every minute.
        $this->app->booted(function () {
            if (! config('processhub.heartbeat_enabled')) {
                return;
            }
            /** @var Schedule $schedule */
            $schedule = $this->app->make(Schedule::class);
            $schedule->command('processhub:heartbeat')
                ->everyMinute()
                ->withoutOverlapping()
                ->runInBackground();
        });

        // 5. Register Laravel event listeners (QueryExecuted, JobFailed, …).
        $this->registerEventListeners();

        // 6. Auto-capture uncaught exceptions through the Handler's
        //    reportable() hook — the user doesn't need to manually wrap
        //    exceptions into Log::error() calls.
        $this->app->booted(function () {
            try {
                $handler = $this->app->make(ExceptionHandler::class);
                HandleExceptionReported::register($handler);
            } catch (\Throwable) {
                // Non-standard Handler (Laravel 11+ bootstrap-style) — the
                // user must wire Log::error manually. Don't crash boot.
            }
        });
    }

    protected function registerEventListeners(): void
    {
        $listeners = config('processhub.listeners', []);
        $events = $this->app['events'];

        if ($listeners['query'] ?? false) {
            $events->listen(\Illuminate\Database\Events\QueryExecuted::class, Listeners\HandleQueryExecuted::class);
        }
        if ($listeners['job_failures'] ?? true) {
            $events->listen(\Illuminate\Queue\Events\JobFailed::class, Listeners\HandleJobFailed::class);
        }
        if ($listeners['scheduled_tasks'] ?? true) {
            // Laravel's ScheduledTaskFailed / ScheduledTaskSkipped — availability varies
            // across versions, so we wire them defensively.
            foreach ([
                'Illuminate\Console\Events\ScheduledTaskFailed',
                'Illuminate\Console\Events\ScheduledTaskSkipped',
            ] as $eventClass) {
                if (class_exists($eventClass)) {
                    $events->listen($eventClass, Listeners\HandleScheduledTaskFinished::class);
                }
            }
        }
        if ($listeners['mail'] ?? true) {
            $events->listen(
                \Illuminate\Mail\Events\MessageSent::class,
                Listeners\HandleMailSent::class,
            );
        }
    }
}
