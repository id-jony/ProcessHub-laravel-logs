<?php

namespace ProcessHub\Logs;

use Illuminate\Console\Events\CommandFinished;
use Illuminate\Console\Events\CommandStarting;
use Illuminate\Console\Scheduling\Schedule;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Contracts\Http\Kernel;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Queue\Events\JobProcessed;
use Illuminate\Queue\Events\JobProcessing;
use Illuminate\Queue\Events\Looping;
use Illuminate\Queue\Events\WorkerStopping;
use Illuminate\Queue\Jobs\SyncJob;
use Illuminate\Support\ServiceProvider;
use ProcessHub\Logs\Commands\ConfigRefreshCommand;
use ProcessHub\Logs\Commands\ConfigShowCommand;
use ProcessHub\Logs\Commands\DeployCommand;
use ProcessHub\Logs\Commands\FlushFallbackCommand;
use ProcessHub\Logs\Commands\ForgetFailedCommand;
use ProcessHub\Logs\Commands\HeartbeatCommand;
use ProcessHub\Logs\Commands\InstallCommand;
use ProcessHub\Logs\Commands\RebatchQueueCommand;
use ProcessHub\Logs\Commands\TestCommand;
use ProcessHub\Logs\Config\RemoteConfigClient;
use ProcessHub\Logs\Jobs\SendLogBatchJob;
use ProcessHub\Logs\Listeners\HandleExceptionReported;
use ProcessHub\Logs\Logging\LogBuffer;
use ProcessHub\Logs\Middleware\CorrelateRequestId;

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

        // Per-process buffer of log entries awaiting a SendLogBatchJob.
        $this->app->singleton(LogBuffer::class);

        // Auto-capture uncaught exceptions through the Handler's reportable()
        // hook — the user doesn't need to wrap them into Log::error() calls.
        $this->registerExceptionHooks();
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
                RebatchQueueCommand::class,
                ForgetFailedCommand::class,
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

        // 5. Register Laravel event listeners (QueryExecuted, JobFailed, …).
        $this->registerEventListeners();

        // 5a. Flush buffered log entries at every unit-of-work boundary.
        $this->registerBufferFlushHooks();
    }

    /**
     * Registered after registerEventListeners() so the checks on JobFailed
     * run after HandleJobFailed has logged the failure.
     *
     * Queue workers ship by size or age only (`flush_interval_sec`), not
     * after every job — otherwise a worker running thousands of jobs that
     * each log one line would send a one-entry batch per job. A full flush
     * happens when the unit of work really ends.
     */
    protected function registerBufferFlushHooks(): void
    {
        // Resolved eagerly (it's a plain object) so every hook — and Octane's
        // per-request sandboxes — work with the instance the handler writes to.
        $buffer = $this->app->make(LogBuffer::class);
        $events = $this->app['events'];

        $flush = static fn () => $buffer->flush();
        $this->app->terminating($flush);
        $events->listen([
            WorkerStopping::class,
            CommandFinished::class,
            'Laravel\Octane\Events\RequestTerminated',
        ], $flush);

        $events->listen([
            JobProcessed::class,
            JobFailed::class,
            Looping::class,
        ], static fn () => $buffer->flushIfStale());

        // Daemons without a worker loop (Horizon master, schedule:work, …)
        // have nothing that would ship an entry before the next one or exit.
        $events->listen(CommandStarting::class, static function (CommandStarting $event) use ($buffer): void {
            if (in_array($event->command, self::unbufferedCommands(), true)) {
                $buffer->setBuffered(false);
            }
        });

        $this->registerDeliveryJobMute($buffer, $events);
        $this->registerShutdownFlush($buffer);
    }

    /**
     * Everything logged while a worker processes a SendLogBatchJob must not
     * produce another batch — including the worker's report of an exception
     * that escaped the job, which comes after the attempt is over. So the
     * mute lasts until the worker moves on: next loop tick, next job, worker
     * stop. A sync job runs inline (no worker loop to unmute after it) and
     * mutes itself in handle().
     */
    protected function registerDeliveryJobMute(LogBuffer $buffer, Dispatcher $events): void
    {
        $events->listen(JobProcessing::class, static function (JobProcessing $event) use ($buffer): void {
            $buffer->setInDeliveryJob(
                ! $event->job instanceof SyncJob
                && $event->job->resolveName() === SendLogBatchJob::class,
            );
        });
        $events->listen([
            Looping::class,
            WorkerStopping::class,
            CommandFinished::class,
        ], static fn () => $buffer->setInDeliveryJob(false));
    }

    /**
     * Fatal errors (OOM, max_execution_time) skip terminating callbacks and
     * destructors; a shutdown function is the only place left to flush.
     * Laravel's HandleExceptions logs the FatalError from its own shutdown
     * function (HandleExceptionReported reclaims memory at the start of
     * that report), so ours re-registers itself from inside the shutdown
     * phase — that puts it after every function registered during the
     * request. If Laravel didn't log the FatalError, it is shipped here.
     */
    protected function registerShutdownFlush(LogBuffer $buffer): void
    {
        // Weak: in test suites each booted app would otherwise stay in memory.
        $ref = \WeakReference::create($buffer);

        register_shutdown_function(static function () use ($ref): void {
            register_shutdown_function(static function () use ($ref): void {
                $buffer = $ref->get();
                if ($buffer === null || ! $buffer->isOwnerAlive()) {
                    return;
                }
                $buffer->flushOnShutdown();
                HandleExceptionReported::reportMissedFatalError($buffer);
            });
        });
    }

    /**
     * Hooks into the exception handler while it is being resolved: the
     * container runs `resolving` callbacks before `afterResolving` ones, and
     * `withExceptions()` in bootstrap/app.php (where Sentry is wired) is an
     * afterResolving callback — so our reportable callback comes first.
     */
    protected function registerExceptionHooks(): void
    {
        $register = function (ExceptionHandler $handler): void {
            try {
                HandleExceptionReported::register($handler);
            } catch (\Throwable) {
                // Non-standard Handler — the user must wire Log::error
                // manually. Don't crash boot.
            }
        };

        if ($this->app->resolved(ExceptionHandler::class)) {
            $register($this->app->make(ExceptionHandler::class));
        }
        $this->app->resolving(ExceptionHandler::class, $register);

        // Resolved (and its class loaded) now, not on a fatal error: on OOM
        // the shutdown handler has no memory to compile it.
        $this->app->booted(function (): void {
            try {
                $this->app->make(ExceptionHandler::class);
            } catch (\Throwable) {
                // No handler bound — nothing to hook into.
            }
        });
    }

    /**
     * Long-running commands that never reach a flush point: entries are
     * queued right away there. Queue workers are not listed — their loop
     * flushes by age on every tick.
     *
     * @return array<int, string>
     */
    protected static function unbufferedCommands(): array
    {
        return array_merge([
            'horizon',
            'horizon:supervisor',
            'schedule:work',
            'queue:listen',
            'reverb:start',
            'pulse:check',
            'pulse:work',
            'octane:start',
        ], (array) config('processhub.unbuffered_commands', []));
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
