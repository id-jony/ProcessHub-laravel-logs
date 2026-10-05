<?php

namespace ProcessHub\Logs\Tests;

use Orchestra\Testbench\TestCase as Orchestra;
use ProcessHub\Logs\Logging\ProcessHubFactory;
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
        $app['config']->set('queue.failed.driver', 'null');
        $app['config']->set('logging.channels.processhub', [
            'driver' => 'custom',
            'via' => ProcessHubFactory::class,
            'level' => 'debug',
        ]);
        // Fallback writes go somewhere isolated.
        $app['config']->set(
            'processhub.fallback_path',
            sys_get_temp_dir() . '/processhub-fallback-test-' . uniqid() . '.log',
        );

        // Redis-backed tests (see Concerns\UsesRedis) — predis, so no ext needed.
        $app['config']->set('database.redis.client', 'predis');
        $app['config']->set('database.redis.options.prefix', 'processhub_test:');
        $app['config']->set('database.redis.default', [
            'host' => env('PROCESSHUB_TEST_REDIS_HOST', '127.0.0.1'),
            'port' => (int) env('PROCESSHUB_TEST_REDIS_PORT', 6379),
            'database' => (int) env('PROCESSHUB_TEST_REDIS_DB', 15),
        ]);
    }

    protected function tearDown(): void
    {
        $path = config('processhub.fallback_path');
        foreach (['', '.flushing', '.flushing.tmp', '.lock', '.write.lock', '.rejected', '.flushing.offset', '.flushing.offset.tmp'] as $suffix) {
            if ($path && is_file($path . $suffix)) {
                @unlink($path . $suffix);
            }
        }
        parent::tearDown();
    }
}
