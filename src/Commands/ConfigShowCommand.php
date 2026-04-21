<?php

namespace ProcessHub\Logs\Commands;

use Illuminate\Console\Command;
use ProcessHub\Logs\Config\RemoteConfigClient;

/**
 * `php artisan processhub:config:show`
 *
 * Prints the effective config on this host — combining cached remote values
 * with Laravel's runtime config bag. Primary diagnostic for "why is my host
 * still sending INFO when I set minLevel=warning in the UI?" questions.
 */
class ConfigShowCommand extends Command
{
    protected $signature = 'processhub:config:show';
    protected $description = 'Print the effective ProcessHub config on this host';

    public function handle(RemoteConfigClient $client): int
    {
        $cache = $client->dumpCache();
        $this->info('== Cache file ==');
        $this->line($client->cachePath());
        if ($cache === null) {
            $this->line('(not present — no remote config pulled yet)');
        } else {
            $this->line('version: '.($cache['version'] ?? '—'));
            $this->line('etag: '.($cache['etag'] ?? '—'));
        }

        $this->newLine();
        $this->info('== Effective runtime values (config()) ==');
        $rows = [
            ['url', config('processhub.url') ?: '(unset)'],
            ['token', config('processhub.token') ? '(set)' : '(unset)'],
            ['enabled', var_export(config('processhub.enabled', true), true)],
            ['min_level', config('processhub.min_level', 'info')],
            ['sample_rate', config('processhub.sample_rate', 1)],
            ['batch_size', config('processhub.batch_size')],
            ['timeout_ms', config('processhub.timeout_ms')],
            ['heartbeat_interval', config('processhub.heartbeat_interval_seconds', 60)],
            ['listeners.query', var_export(config('processhub.listeners.query', false), true)],
            ['listeners.job_failures', var_export(config('processhub.listeners.job_failures', true), true)],
            ['listeners.mail', var_export(config('processhub.listeners.mail', false), true)],
            ['listeners.scheduled_tasks', var_export(config('processhub.listeners.scheduled_tasks', true), true)],
            ['listeners.query_slow_ms', config('processhub.listeners.query_slow_ms', 0)],
            ['context_type_whitelist', implode(',', (array) config('processhub.context_type_whitelist', []))],
            ['redact.remote_patterns', count((array) config('processhub.redact.remote_patterns', [])).' pattern(s)'],
        ];
        $this->table(['key', 'value'], $rows);

        $this->newLine();
        $this->line('Precedence: remote > env > package default.');
        $this->line('To force refresh: php artisan processhub:config:refresh');

        return self::SUCCESS;
    }
}
