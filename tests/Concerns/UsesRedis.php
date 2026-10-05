<?php

namespace ProcessHub\Logs\Tests\Concerns;

use Illuminate\Support\Facades\Redis;

/**
 * Tests that need a real Redis (queue worker, rebatch). Opt-in: skipped unless
 * PROCESSHUB_TEST_REDIS_HOST is set (port PROCESSHUB_TEST_REDIS_PORT, 6379).
 * Uses DB PROCESSHUB_TEST_REDIS_DB (15 by default) and FLUSHES it.
 */
trait UsesRedis
{
    protected function setUpRedis(): void
    {
        if (! env('PROCESSHUB_TEST_REDIS_HOST')) {
            $this->markTestSkipped('Set PROCESSHUB_TEST_REDIS_HOST to run Redis-backed tests.');
        }

        try {
            Redis::connection()->flushdb();
        } catch (\Throwable $e) {
            $this->markTestSkipped('Redis is not available: ' . $e->getMessage());
        }

        config()->set('processhub.connection', 'redis');
        config()->set('processhub.queue', 'logs');
    }
}
