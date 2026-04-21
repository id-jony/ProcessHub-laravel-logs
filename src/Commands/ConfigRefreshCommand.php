<?php

namespace ProcessHub\Logs\Commands;

use Illuminate\Console\Command;
use ProcessHub\Logs\Config\RemoteConfigClient;

/**
 * `php artisan processhub:config:refresh`
 *
 * Forces a pull of the latest remote config from ProcessHub, bypassing the
 * heartbeat-driven lazy refresh. Useful when you've just saved a change in
 * the ProcessHub UI and want to verify it on this host without waiting for
 * the next heartbeat tick.
 */
class ConfigRefreshCommand extends Command
{
    protected $signature = 'processhub:config:refresh';
    protected $description = 'Force-pull the remote config from ProcessHub';

    public function handle(RemoteConfigClient $client): int
    {
        $changed = $client->refresh();
        if ($changed) {
            $this->info('Remote config updated (version '.$client->cachedVersion().', etag '.$client->cachedEtag().').');
            return self::SUCCESS;
        }
        $this->line('No change (server returned 304 or fetch failed). See logs for details.');
        return self::SUCCESS;
    }
}
