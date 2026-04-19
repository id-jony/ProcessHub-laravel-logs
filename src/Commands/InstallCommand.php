<?php

namespace ProcessHub\Logs\Commands;

use Illuminate\Console\Command;

/**
 * `php artisan processhub:install`
 *
 * One-shot onboarding helper — publishes config, prints a checklist that
 * the user still needs to do by hand (env vars, logging channel). We
 * deliberately DON'T modify `config/logging.php` automatically — that file
 * has too much variation across Laravel projects, and a broken merge would
 * be worse than a 5-line copy-paste.
 */
class InstallCommand extends Command
{
    protected $signature = 'processhub:install';
    protected $description = 'Publish ProcessHub config and print setup checklist';

    public function handle(): int
    {
        $this->info('ProcessHub Logs — installation helper');
        $this->newLine();

        // Publish config file.
        $this->call('vendor:publish', [
            '--tag' => 'processhub-config',
            '--force' => false,
        ]);

        $this->newLine();
        $this->info('Next steps — do these by hand:');
        $this->newLine();

        $this->line('  <fg=yellow>1.</> Add to your <fg=cyan>.env</>:');
        $this->line('     PROCESSHUB_LOG_URL=https://app.processhub.io');
        $this->line('     PROCESSHUB_LOG_TOKEN=ph_live_<your-token>');
        $this->newLine();

        $this->line('  <fg=yellow>2.</> In <fg=cyan>config/logging.php</>, add the "processhub" channel:');
        $this->line("     'processhub' => [");
        $this->line("         'driver' => 'custom',");
        $this->line("         'via' => ProcessHub\\Logs\\Logging\\ProcessHubFactory::class,");
        $this->line("         'level' => env('LOG_LEVEL', 'warning'),");
        $this->line("     ],");
        $this->newLine();

        $this->line('  <fg=yellow>3.</> Add "processhub" to the <fg=cyan>stack</> channel\'s channels array:');
        $this->line("     'stack' => [");
        $this->line("         'channels' => ['single', 'processhub'],");
        $this->line("     ],");
        $this->newLine();

        $this->line('  <fg=yellow>4.</> Run <fg=cyan>php artisan processhub:test</> to verify.');
        $this->newLine();

        $this->info('Done. ProcessHub will start receiving your logs on the next request.');

        return self::SUCCESS;
    }
}
