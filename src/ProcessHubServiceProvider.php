<?php

namespace ProcessHub\Logs;

use Illuminate\Console\Scheduling\Schedule;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Http\Kernel;
use Illuminate\Support\ServiceProvider;
use ProcessHub\Logs\Commands\ConfigRefreshCommand;
use ProcessHub\Logs\Commands\ConfigShowCommand;
use ProcessHub\Logs\Commands\DeployCommand;
use ProcessHub\Logs\Commands\FlushFallbackCommand;
use ProcessHub\Logs\Commands\HeartbeatCommand;
use ProcessHub\Logs\Commands\InstallCommand;
use ProcessHub\Logs\Commands\TestCommand;
use ProcessHub\Logs\Config\RemoteConfigClient;
use ProcessHub\Logs\Listeners\HandleExceptionReported;
use ProcessHub\Logs\Middleware\CorrelateRequestId;
use ProcessHub\Logs\Payouts\Commands\PushPayoutsCommand;
use ProcessHub\Logs\Payouts\IngestClient as PayoutsIngestClient;
use ProcessHub\Logs\Payouts\Observers\PayoutsModelObserver;
use ProcessHub\Logs\Payouts\PayoutsManager;
use ProcessHub\Logs\Payouts\WatermarkStore as PayoutsWatermarkStore;

class ProcessHubServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        // Capture boot time once so heartbeats can report real uptime.
        HeartbeatCommand::markBootedNow();

        // Merge defaults — user's `config/processhub.php` wins after publish.
        $this->mergeConfigFrom(__DIR__ . '/../config/processhub.php', 'processhub');

        // Manager backing the `ProcessHub` facade (ProcessHub::markDeploy).
        $this->app->singleton(ProcessHubManager::class);

        // Remote-config client — singleton because it caches state across
        // the request lifecycle and heartbeat ticks.
        $this->app->singleton(RemoteConfigClient::class);

        // Payouts module — singletons so registration done in
        // `AppServiceProvider::boot` survives across HTTP requests and
        // queue worker jobs.
        $this->app->singleton(PayoutsManager::class);
        $this->app->singleton(PayoutsIngestClient::class);
        $this->app->singleton(PayoutsWatermarkStore::class);
        $this->app->alias(PayoutsManager::class, 'processhub.payouts');
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
                ConfigRefreshCommand::class,
                ConfigShowCommand::class,
                DeployCommand::class,
                PushPayoutsCommand::class,
            ]);
        }

        // 2a. Load any cached remote config into the runtime config bag
        //     BEFORE listeners/scheduler read their flags. This is why all
        //     downstream code can keep using `config('processhub.*')` —
        //     remote values override env/defaults transparently.
        $this->app->make(RemoteConfigClient::class)->bootstrap();

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

        // 4a. Payouts scheduler — uses the remote-config-driven cron expr so
        //     ProcessHub-side cadence changes propagate without a deploy.
        $this->app->booted(function () {
            if (! config('processhub.payouts.enabled', true)) {
                return;
            }
            $cron = config('processhub.payouts.default_cron');
            if (! is_string($cron) || $cron === '') {
                return;
            }
            /** @var Schedule $schedule */
            $schedule = $this->app->make(Schedule::class);
            $schedule->command('processhub:payouts:push')
                ->cron($cron)
                ->withoutOverlapping()
                ->runInBackground();
        });

        // 4b. Payouts observer auto-registration — wired here (not in
        //     register()) because we need the host app's AppServiceProvider::boot
        //     to have run `Payouts::register(Model::class, ...)` first.
        $this->app->booted(function () {
            if (! config('processhub.payouts.enabled', true)) {
                return;
            }
            if (! config('processhub.payouts.observe_model_changes', true)) {
                return;
            }
            $manager = $this->app->make(PayoutsManager::class);
            $source = $manager->source();
            if ($source === null) {
                // No model registered yet — observer would have nothing to bind to.
                return;
            }
            // observe() is idempotent at the Eloquent layer, so re-running on
            // every boot is fine.
            $source->model::observe(PayoutsModelObserver::class);
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
