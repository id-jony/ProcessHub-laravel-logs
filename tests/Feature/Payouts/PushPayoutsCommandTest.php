<?php

namespace ProcessHub\Logs\Tests\Feature\Payouts;

use GuzzleHttp\Client;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Response as GuzzleResponse;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\Schema;
use ProcessHub\Logs\Payouts\IngestClient;
use ProcessHub\Logs\Payouts\PayoutsManager;
use ProcessHub\Logs\Payouts\WatermarkStore;
use ProcessHub\Logs\Tests\TestCase;

/**
 * Tiny in-memory Eloquent model the command can cursor over without
 * pulling in MySQL/PostgreSQL.
 */
class FakePaymentRow extends Model
{
    protected $table = 'fake_payments';
    public $timestamps = false;
    protected $guarded = [];
    protected $casts = [
        'completed' => 'bool',
        'fatal_error' => 'bool',
    ];
}

class PushPayoutsCommandTest extends TestCase
{
    private string $tmpWatermark;

    protected function defineEnvironment($app): void
    {
        parent::defineEnvironment($app);
        $app['config']->set('database.default', 'testbench');
        $app['config']->set('database.connections.testbench', [
            'driver' => 'sqlite',
            'database' => ':memory:',
            'prefix' => '',
        ]);
    }

    protected function setUp(): void
    {
        parent::setUp();
        Schema::create('fake_payments', function ($table) {
            $table->increments('id');
            $table->string('status_text');
            $table->boolean('completed');
            $table->boolean('fatal_error');
            $table->string('amount');
            $table->timestamps();
        });

        $this->tmpWatermark = sys_get_temp_dir() . '/processhub-payouts-cmd-' . uniqid() . '.json';
        $store = $this->app->make(WatermarkStore::class);
        $store->overridePath($this->tmpWatermark);
    }

    protected function tearDown(): void
    {
        if (is_file($this->tmpWatermark)) {
            @unlink($this->tmpWatermark);
        }
        parent::tearDown();
    }

    private function registerModel(): void
    {
        /** @var PayoutsManager $manager */
        $manager = $this->app->make(PayoutsManager::class);
        $manager->register(FakePaymentRow::class, fn (FakePaymentRow $p) => [
            'gatewayPaymentId' => (string) $p->id,
            'paymentCreatedAt' => $p->created_at?->toIso8601String() ?? '2026-05-16T13:30:16Z',
            'rawStatus' => $p->status_text,
            'isCompleted' => (bool) $p->completed,
            'isFatalError' => (bool) $p->fatal_error,
            'grossAmount' => (string) $p->amount,
        ]);
    }

    private function mockIngest(array $responses): MockHandler
    {
        $mock = new MockHandler($responses);
        $stack = HandlerStack::create($mock);
        $guzzle = new Client(['handler' => $stack, 'http_errors' => false]);

        /** @var IngestClient $client */
        $client = $this->app->make(IngestClient::class);
        $client->overrideClient($guzzle);
        return $mock;
    }

    public function test_no_source_registered_succeeds_silently(): void
    {
        $exitCode = $this->artisan('processhub:payouts:push')->run();
        $this->assertSame(0, $exitCode);
    }

    public function test_disabled_via_config_succeeds_silently(): void
    {
        config()->set('processhub.payouts.enabled', false);
        $this->registerModel();
        FakePaymentRow::create([
            'status_text' => 'Проведён', 'completed' => true, 'fatal_error' => false, 'amount' => '100',
        ]);
        // No mock — if command tried to POST, it would fail.
        $exitCode = $this->artisan('processhub:payouts:push')->run();
        $this->assertSame(0, $exitCode);
    }

    public function test_bootstrap_sends_all_rows_and_persists_watermark(): void
    {
        $this->registerModel();
        FakePaymentRow::create(['status_text' => 'Проведён', 'completed' => true, 'fatal_error' => false, 'amount' => '100']);
        FakePaymentRow::create(['status_text' => 'Проведён', 'completed' => true, 'fatal_error' => false, 'amount' => '200']);

        $this->mockIngest([
            new GuzzleResponse(200, [], json_encode([
                'ok' => true, 'accepted' => 2, 'skipped' => [], 'watermark' => '2',
            ])),
        ]);

        $exitCode = $this->artisan('processhub:payouts:push')->run();
        $this->assertSame(0, $exitCode);

        $store = $this->app->make(WatermarkStore::class);
        $this->assertSame('2', $store->get());
    }

    public function test_incremental_skips_rows_below_watermark(): void
    {
        $this->registerModel();
        FakePaymentRow::create(['status_text' => 'X', 'completed' => true, 'fatal_error' => false, 'amount' => '100']);
        FakePaymentRow::create(['status_text' => 'Y', 'completed' => true, 'fatal_error' => false, 'amount' => '200']);

        // Pre-seed watermark = 1 so only id=2 ships.
        $this->app->make(WatermarkStore::class)->set('1');

        $this->mockIngest([
            new GuzzleResponse(200, [], json_encode([
                'ok' => true, 'accepted' => 1, 'skipped' => [], 'watermark' => '2',
            ])),
        ]);
        $exitCode = $this->artisan('processhub:payouts:push')->run();
        $this->assertSame(0, $exitCode);
        $this->assertSame('2', $this->app->make(WatermarkStore::class)->get());
    }

    public function test_bad_row_is_skipped_rest_still_ship(): void
    {
        $manager = $this->app->make(PayoutsManager::class);
        $manager->register(FakePaymentRow::class, function (FakePaymentRow $p) {
            // id=2 produces an invalid row (missing grossAmount).
            $row = [
                'gatewayPaymentId' => (string) $p->id,
                'paymentCreatedAt' => '2026-05-16T13:30:16Z',
                'rawStatus' => $p->status_text,
                'isCompleted' => (bool) $p->completed,
                'isFatalError' => (bool) $p->fatal_error,
                'grossAmount' => (string) $p->amount,
            ];
            if ($p->id === 2) {
                $row['grossAmount'] = 'not-a-number';
            }
            return $row;
        });
        FakePaymentRow::create(['status_text' => 'X', 'completed' => true, 'fatal_error' => false, 'amount' => '100']);
        FakePaymentRow::create(['status_text' => 'Y', 'completed' => true, 'fatal_error' => false, 'amount' => '200']);
        FakePaymentRow::create(['status_text' => 'Z', 'completed' => true, 'fatal_error' => false, 'amount' => '300']);

        // Expect ONE batch with 2 rows (1 and 3), the bad row is dropped.
        $this->mockIngest([
            new GuzzleResponse(200, [], json_encode([
                'ok' => true, 'accepted' => 2, 'skipped' => [], 'watermark' => '3',
            ])),
        ]);

        $exitCode = $this->artisan('processhub:payouts:push')->run();
        $this->assertSame(0, $exitCode);
        $this->assertSame('3', $this->app->make(WatermarkStore::class)->get());
    }

    public function test_4xx_returns_failure_and_does_not_advance_watermark_past_failure(): void
    {
        $this->registerModel();
        FakePaymentRow::create(['status_text' => 'X', 'completed' => true, 'fatal_error' => false, 'amount' => '100']);

        $this->mockIngest([
            new GuzzleResponse(403, [], json_encode(['error' => 'disabled', 'code' => 'SOURCE_DISABLED'])),
        ]);
        $exitCode = $this->artisan('processhub:payouts:push')->run();
        $this->assertSame(1, $exitCode);
        $this->assertNull($this->app->make(WatermarkStore::class)->get());
    }
}
