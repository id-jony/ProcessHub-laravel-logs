<?php

namespace ProcessHub\Logs\Tests\Unit\Payouts;

use ProcessHub\Logs\Payouts\WatermarkStore;
use ProcessHub\Logs\Tests\TestCase;

class WatermarkStoreTest extends TestCase
{
    private string $tmpPath;

    protected function setUp(): void
    {
        parent::setUp();
        $this->tmpPath = sys_get_temp_dir() . '/processhub-payouts-watermark-test-' . uniqid() . '.json';
    }

    protected function tearDown(): void
    {
        if (is_file($this->tmpPath)) {
            @unlink($this->tmpPath);
        }
        parent::tearDown();
    }

    private function store(): WatermarkStore
    {
        $store = new WatermarkStore();
        $store->overridePath($this->tmpPath);
        return $store;
    }

    public function test_get_returns_null_when_file_missing(): void
    {
        $this->assertNull($this->store()->get());
    }

    public function test_set_then_get_roundtrip(): void
    {
        $store = $this->store();
        $store->set('362364');
        $this->assertSame('362364', $store->get());
        $this->assertFileExists($this->tmpPath);
    }

    public function test_get_returns_null_on_malformed_json(): void
    {
        file_put_contents($this->tmpPath, '{not json');
        $this->assertNull($this->store()->get());
    }

    public function test_get_returns_null_on_missing_key(): void
    {
        file_put_contents($this->tmpPath, json_encode(['updatedAt' => 'x']));
        $this->assertNull($this->store()->get());
    }

    public function test_set_null_removes_the_file(): void
    {
        $store = $this->store();
        $store->set('1');
        $this->assertFileExists($this->tmpPath);
        $store->set(null);
        $this->assertFileDoesNotExist($this->tmpPath);
    }

    public function test_set_overwrite_keeps_latest_value(): void
    {
        $store = $this->store();
        $store->set('1');
        $store->set('2');
        $this->assertSame('2', $store->get());
    }

    public function test_coerces_integer_watermark_in_legacy_files(): void
    {
        file_put_contents($this->tmpPath, json_encode(['watermark' => 1234]));
        $this->assertSame('1234', $this->store()->get());
    }
}
