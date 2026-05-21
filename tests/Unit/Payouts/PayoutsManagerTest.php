<?php

namespace ProcessHub\Logs\Tests\Unit\Payouts;

use Illuminate\Database\Eloquent\Model;
use InvalidArgumentException;
use ProcessHub\Logs\Payouts\PayoutsManager;
use ProcessHub\Logs\Tests\TestCase;

/**
 * Anonymous Eloquent stand-in — we only need `getKey()` and attribute
 * access for the mapper.
 */
class FakePayment extends Model
{
    public $timestamps = false;
    protected $guarded = [];

    public function getKey()
    {
        return $this->attributes['id'] ?? null;
    }
}

class PayoutsManagerTest extends TestCase
{
    public function test_mapOne_returns_normalised_row_for_valid_input(): void
    {
        $manager = new PayoutsManager();
        $manager->register(FakePayment::class, fn (FakePayment $p) => [
            'gatewayPaymentId' => (string) $p->id,
            'paymentCreatedAt' => '2026-05-16T13:30:16Z',
            'rawStatus' => 'Проведён',
            'isCompleted' => true,
            'isFatalError' => false,
            'grossAmount' => '74000',
        ]);

        $row = $manager->mapOne(new FakePayment(['id' => 362364]));

        $this->assertSame('362364', $row['gatewayPaymentId']);
        $this->assertSame('Проведён', $row['rawStatus']);
        $this->assertTrue($row['isCompleted']);
        // rawData is auto-filled to an empty object so server doesn't see null.
        $this->assertArrayHasKey('rawData', $row);
    }

    public function test_mapOne_throws_without_registration(): void
    {
        $this->expectException(InvalidArgumentException::class);
        (new PayoutsManager())->mapOne(new FakePayment(['id' => 1]));
    }

    public function test_mapOne_rejects_missing_required_field(): void
    {
        $manager = new PayoutsManager();
        $manager->register(FakePayment::class, fn () => [
            'gatewayPaymentId' => '1',
            'paymentCreatedAt' => '2026-05-16T13:30:16Z',
            'rawStatus' => 'X',
            // missing isCompleted
            'isFatalError' => false,
            'grossAmount' => '100',
        ]);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('isCompleted');
        $manager->mapOne(new FakePayment(['id' => 1]));
    }

    public function test_mapOne_rejects_non_decimal_amount(): void
    {
        $manager = new PayoutsManager();
        $manager->register(FakePayment::class, fn () => [
            'gatewayPaymentId' => '1',
            'paymentCreatedAt' => '2026-05-16T13:30:16Z',
            'rawStatus' => 'X',
            'isCompleted' => true,
            'isFatalError' => false,
            'grossAmount' => '12,50', // comma is locale corruption
        ]);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('grossAmount');
        $manager->mapOne(new FakePayment(['id' => 1]));
    }

    public function test_mapOne_rejects_non_iso_date(): void
    {
        $manager = new PayoutsManager();
        $manager->register(FakePayment::class, fn () => [
            'gatewayPaymentId' => '1',
            'paymentCreatedAt' => 'обы́чно вторник',
            'rawStatus' => 'X',
            'isCompleted' => true,
            'isFatalError' => false,
            'grossAmount' => '100',
        ]);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('paymentCreatedAt');
        $manager->mapOne(new FakePayment(['id' => 1]));
    }

    public function test_register_replaces_previous_source(): void
    {
        $manager = new PayoutsManager();
        $manager->register(FakePayment::class, fn () => ['a' => 1]);
        $manager->register(FakePayment::class, fn () => ['b' => 2]);
        $source = $manager->source();
        $this->assertNotNull($source);
        $this->assertSame(['b' => 2], ($source->map)(new FakePayment()));
    }

    public function test_forget_clears_source(): void
    {
        $manager = new PayoutsManager();
        $manager->register(FakePayment::class, fn () => []);
        $manager->forget();
        $this->assertNull($manager->source());
    }
}
