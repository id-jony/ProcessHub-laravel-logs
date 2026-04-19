<?php

namespace ProcessHub\Logs\Tests;

use Orchestra\Testbench\TestCase as Orchestra;
use ProcessHub\Logs\ProcessHubServiceProvider;

/**
 * Base Testbench case — loads the ServiceProvider under a bare-bones Laravel
 * app so package components (config, middleware, commands, listeners) behave
 * as they would in a real host app.
 */
abstract class TestCase extends Orchestra
{
    protected function getPackageProviders($app): array
    {
        return [ProcessHubServiceProvider::class];
    }

    protected function defineEnvironment($app): void
    {
        // Sensible defaults for every test. Individual tests override via
        // config()->set() when they need different behaviour (e.g. to test
        // that missing token is a silent no-op).
        $app['config']->set('processhub.url', 'https://ph.test');
        $app['config']->set('processhub.token', 'ph_live_test_test_00000000000000000000000000000000');
        // Sync queue by default so tests don't need a worker.
        $app['config']->set('queue.default', 'sync');
    }
}
