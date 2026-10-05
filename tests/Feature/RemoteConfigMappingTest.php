<?php

namespace ProcessHub\Logs\Tests\Feature;

use Illuminate\Support\Facades\File;
use ProcessHub\Logs\Config\RemoteConfigClient;
use ProcessHub\Logs\Tests\TestCase;

class RemoteConfigMappingTest extends TestCase
{
    private string $path;

    protected function setUp(): void
    {
        parent::setUp();
        $this->path = sys_get_temp_dir() . '/processhub-remote-config-' . getmypid() . '.json';
    }

    protected function tearDown(): void
    {
        @unlink($this->path);
        parent::tearDown();
    }

    /**
     * @param  array<string, mixed>  $remote
     */
    private function bootstrapWith(array $remote): void
    {
        File::put($this->path, json_encode(['version' => 2, 'etag' => 'x', 'config' => $remote]));
        $path = $this->path;
        $client = new class(config(), $path) extends RemoteConfigClient {
            public function __construct(\Illuminate\Contracts\Config\Repository $config, private string $file)
            {
                parent::__construct($config);
            }

            public function cachePath(): string
            {
                return $this->file;
            }
        };
        $client->bootstrap();
    }

    public function test_remote_http_timeout_and_flush_interval_reach_package_keys(): void
    {
        $this->bootstrapWith(['httpTimeoutSeconds' => 7, 'flushIntervalSeconds' => 5]);

        $this->assertSame(7000, config('processhub.timeout_ms'));
        $this->assertSame(5, config('processhub.flush_interval_sec'));
    }

    public function test_remote_values_are_clamped(): void
    {
        $this->bootstrapWith(['httpTimeoutSeconds' => 300, 'flushIntervalSeconds' => 3600]);

        $this->assertSame(20000, config('processhub.timeout_ms'));
        $this->assertSame(60, config('processhub.flush_interval_sec'));

        $this->bootstrapWith(['httpTimeoutSeconds' => 0, 'flushIntervalSeconds' => -5]);

        $this->assertSame(1000, config('processhub.timeout_ms'));
        $this->assertSame(0, config('processhub.flush_interval_sec'));
    }
}
