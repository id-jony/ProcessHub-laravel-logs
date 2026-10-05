<?php

namespace ProcessHub\Logs\Tests\Feature;

use Illuminate\Support\Facades\File;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Queue;
use PHPUnit\Framework\Attributes\DataProvider;
use ProcessHub\Logs\Jobs\SendLogBatchJob;
use ProcessHub\Logs\Logging\LogBuffer;
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
        // 0 сбрасывал бы пачку на каждой записи — задача на строку.
        $this->assertSame(1, config('processhub.flush_interval_sec'));
    }

    /**
     * @return array<string, array{0: mixed, 1: int}>
     */
    public static function batchSizes(): array
    {
        return [
            'regular' => [50, 50],
            'numeric string' => ['40', 40],
            'tiny is raised' => [1, 10],
            'zero is ignored' => [0, 100],
            'negative is ignored' => [-5, 100],
            'null is ignored' => [null, 100],
            'garbage is ignored' => ['many', 100],
        ];
    }

    /**
     * A typo on the server must not bring back one job per log line.
     */
    #[DataProvider('batchSizes')]
    public function test_remote_batch_size_never_degrades_batching(mixed $remote, int $expected): void
    {
        config()->set('processhub.batch_size', 100);

        $this->bootstrapWith(['batchSize' => $remote]);

        $this->assertSame($expected, config('processhub.batch_size'));
    }

    /**
     * @return array<string, array{0: array<string, mixed>, 1: array<int, string>}>
     */
    public static function remoteFilters(): array
    {
        return [
            'minLevel WARN (ProcessHub name)' => [['minLevel' => 'WARN'], ['warning', 'error']],
            'minLevel warn' => [['minLevel' => 'warn'], ['warning', 'error']],
            'minLevel ERROR' => [['minLevel' => 'ERROR'], ['error']],
            'minLevel INFO' => [['minLevel' => 'INFO'], ['info', 'warning', 'error']],
            'enabled false' => [['enabled' => false], []],
            'enabled "false"' => [['enabled' => 'false'], []],
            'enabled 0' => [['enabled' => 0], []],
            'enabled "0"' => [['enabled' => '0'], []],
            'enabled true' => [['enabled' => true], ['info', 'warning', 'error']],
            'enabled "true"' => [['enabled' => 'true'], ['info', 'warning', 'error']],
            'enabled null' => [['enabled' => null], ['info', 'warning', 'error']],
        ];
    }

    /**
     * @param  array<string, mixed>  $remote
     * @param  array<int, string>  $expected
     */
    #[DataProvider('remoteFilters')]
    public function test_remote_filters_understand_server_values(array $remote, array $expected): void
    {
        Queue::fake();
        $this->bootstrapWith($remote);

        foreach (['info', 'warning', 'error'] as $level) {
            Log::channel('processhub')->{$level}($level);
        }
        app(LogBuffer::class)->flush();

        $sent = Queue::pushed(SendLogBatchJob::class)->flatMap(fn (SendLogBatchJob $job) => array_column($job->entries, 'message'))->all();
        $this->assertSame($expected, $sent);
    }
}
