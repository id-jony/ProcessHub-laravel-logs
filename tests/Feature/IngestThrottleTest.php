<?php

namespace ProcessHub\Logs\Tests\Feature;

use Illuminate\Contracts\Cache\Repository;
use Illuminate\Support\Carbon;
use Mockery;
use ProcessHub\Logs\Support\IngestThrottle;
use ProcessHub\Logs\Tests\TestCase;

class IngestThrottleTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        config()->set('processhub.rate_limit_store', 'array');
        // Start of a minute = start of slot 0.
        $this->travelTo(Carbon::createFromTimestamp(intdiv(time(), 60) * 60 + 60));
    }

    public function test_limit_is_spread_over_the_minute(): void
    {
        config()->set('processhub.rate_limit_per_minute', 50);
        $throttle = IngestThrottle::make();

        $perSlot = [];
        for ($slot = 0; $slot < 6; $slot++) {
            $granted = 0;
            while ($throttle->acquire() === 0) {
                $granted++;
            }
            $perSlot[] = $granted;
            $this->travel(10)->seconds();
        }

        $this->assertSame([8, 8, 9, 8, 8, 9], $perSlot);
        $this->assertSame(50, array_sum($perSlot));
    }

    public function test_wait_points_to_next_slot_with_capacity(): void
    {
        config()->set('processhub.rate_limit_per_minute', 2);
        $throttle = IngestThrottle::make();

        // 2/min → slots 0..5 get 0,0,1,0,0,1.
        $this->assertSame(20, $throttle->acquire());
        $this->travel(25)->seconds();
        $this->assertSame(0, $throttle->acquire());
        $this->assertSame(25, $throttle->acquire());
    }

    public function test_budget_is_shared_between_instances(): void
    {
        config()->set('processhub.rate_limit_per_minute', 6);

        $this->assertSame(0, IngestThrottle::make()->acquire());
        $this->assertSame(10, IngestThrottle::make()->acquire());
    }

    public function test_pause_stops_all_senders_and_is_never_shortened(): void
    {
        IngestThrottle::make()->pauseFor(30);
        IngestThrottle::make()->pauseFor(5);

        $this->assertSame(30, IngestThrottle::make()->acquire());
        $this->travel(31)->seconds();
        $this->assertSame(0, IngestThrottle::make()->acquire());
    }

    public function test_zero_limit_disables_throttling(): void
    {
        config()->set('processhub.rate_limit_per_minute', 0);
        $throttle = IngestThrottle::make();

        for ($i = 0; $i < 100; $i++) {
            $this->assertSame(0, $throttle->acquire());
        }
    }

    public function test_unknown_store_does_not_break_delivery(): void
    {
        config()->set('processhub.rate_limit_store', 'no-such-store');
        $throttle = IngestThrottle::make();

        $throttle->pauseFor(30);
        $this->assertSame(0, $throttle->acquire());
    }

    public function test_failing_store_does_not_break_delivery(): void
    {
        $cache = Mockery::mock(Repository::class);
        $cache->allows('get')->andThrow(new \RuntimeException('Connection refused'));
        $throttle = new IngestThrottle($cache, 50);

        $throttle->pauseFor(30);
        $this->assertSame(0, $throttle->acquire());
    }
}
