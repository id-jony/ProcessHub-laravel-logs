<?php

namespace ProcessHub\Logs\Commands;

use Illuminate\Console\Command;
use ProcessHub\Logs\ProcessHubManager;

/**
 * `php artisan processhub:deploy`
 *
 * Posts a deploy marker to ProcessHub so the «Релизы» tab on
 * /applications/[slug] shows release timestamps and exception
 * groups can auto-resolve when a new version goes live.
 *
 * Designed to run as the FINAL step in a CI/Forge/Envoyer deploy script:
 *
 *     # In Forge/Envoyer recipe, after `php artisan migrate --force`:
 *     php artisan processhub:deploy "$RELEASE" --commit="$COMMIT_SHA"
 *
 *     # If the deploy itself failed and you want a marker recorded
 *     # (so the failure shows on the timeline):
 *     php artisan processhub:deploy "$RELEASE" --failed
 *
 * NOTE: version is a POSITIONAL argument, not `--version`. Symfony Console
 * reserves `--version` at the application level (it prints the framework
 * version and exits before any command runs). Hence the bare argument.
 *
 * Defaults:
 *  - Positional `version` falls back to `config('app.version')` if omitted.
 *    If neither argument nor config provides one, the command fails — a
 *    deploy marker without a version is useless on the timeline.
 *  - `--commit` falls back to the current git HEAD if the deploy is run
 *    from inside the repo working tree. Set `--commit=""` to suppress.
 *
 * Always returns SUCCESS unless inputs are missing — a transient ProcessHub
 * outage must NOT fail the deploy step.
 */
class DeployCommand extends Command
{
    protected $signature = 'processhub:deploy
        {version? : Release tag (default: config("app.version"))}
        {--commit= : Commit SHA (default: git HEAD if available)}
        {--failed : Record this deploy as failed (success=false)}
        {--metadata=* : key=value pairs added to metadata JSON}
        {--verbose-output : Print response status and body}';

    protected $description = 'Send a deploy marker to ProcessHub';

    public function handle(ProcessHubManager $manager): int
    {
        $url = config('processhub.url');
        $token = config('processhub.token');
        if (! $url || ! $token) {
            $this->error('PROCESSHUB_LOG_URL / PROCESSHUB_LOG_TOKEN not set — check your .env');
            return self::FAILURE;
        }

        $version = $this->argument('version') ?: config('app.version');
        if (! is_string($version) || $version === '') {
            $this->error('No version: pass it as an argument (php artisan processhub:deploy v1.4.2) or set config("app.version").');
            return self::FAILURE;
        }

        $commitOpt = $this->option('commit');
        $commit = $commitOpt === null
            ? $this->detectGitHead()        // not provided — try to autodetect
            : ($commitOpt === '' ? null : $commitOpt); // empty string = explicit suppress

        $success = ! (bool) $this->option('failed');

        $metadata = $this->parseMetadata((array) $this->option('metadata'));

        $verbose = (bool) $this->option('verbose-output');
        if ($verbose) {
            $this->line("POST {$url}/api/ingest/deploy");
            $this->line(sprintf(
                'version=%s commit=%s success=%s metadata=%s',
                $version,
                $commit ?? '(none)',
                $success ? 'true' : 'false',
                $metadata === null ? '(none)' : json_encode($metadata),
            ));
        }

        $ok = $manager->markDeploy($version, $commit, $success, $metadata);

        if ($ok) {
            $this->info("Deploy marker recorded: {$version}".($commit ? " ({$commit})" : ''));
            return self::SUCCESS;
        }

        // Don't fail the deploy step — observability is best-effort.
        $this->warn('ProcessHub did not accept the deploy marker. Check connectivity and the application token. Deploy is NOT failed because of this.');
        return self::SUCCESS;
    }

    /**
     * Best-effort `git rev-parse HEAD`. Returns null when git is unavailable
     * or the cwd isn't a working tree (typical of release-zip deploys where
     * the .git directory is stripped — that's fine, just pass --commit).
     */
    protected function detectGitHead(): ?string
    {
        $output = [];
        $exitCode = 0;
        // 2>/dev/null silences "fatal: not a git repository" — we just want
        // the SHA or nothing.
        @exec('git rev-parse HEAD 2>/dev/null', $output, $exitCode);
        if ($exitCode !== 0 || empty($output)) {
            return null;
        }
        $sha = trim($output[0] ?? '');
        return $sha !== '' ? $sha : null;
    }

    /**
     * Convert `--metadata=key=value` repetitions into an associative array.
     * Returns null when nothing was passed so the JSON payload omits the
     * field instead of sending an empty object.
     *
     * @param  array<int, string>  $pairs
     * @return array<string, string>|null
     */
    protected function parseMetadata(array $pairs): ?array
    {
        if (empty($pairs)) return null;
        $out = [];
        foreach ($pairs as $pair) {
            if (! is_string($pair)) continue;
            $eq = strpos($pair, '=');
            if ($eq === false || $eq === 0) {
                $this->warn("Ignored metadata entry without '=': {$pair}");
                continue;
            }
            $key = substr($pair, 0, $eq);
            $value = substr($pair, $eq + 1);
            $out[$key] = $value;
        }
        return $out === [] ? null : $out;
    }
}
