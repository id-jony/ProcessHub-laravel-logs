<?php

namespace ProcessHub\Logs\Tests\Feature\Payouts;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\Queue;
use Illuminate\Support\Facades\Schema;
use ProcessHub\Logs\Payouts\Jobs\PushSinglePayoutJob;
use ProcessHub\Logs\Payouts\Observers\PayoutsModelObserver;
use ProcessHub\Logs\Payouts\PayoutsManager;
use ProcessHub\Logs\Tests\TestCase;

class ObservedPayment extends Model
{
    protected $table = 'observed_payments';
    public $timestamps = false;
    protected $guarded = [];
}

class PayoutsModelObserverTest extends TestCase
{
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
        Schema::create('observed_payments', function ($t) {
            $t->increments('id');
            $t->string('status_text');
        });

        /** @var PayoutsManager $manager */
        $manager = $this->app->make(PayoutsManager::class);
        $manager->register(ObservedPayment::class, fn ($p) => [
            'gatewayPaymentId' => (string) $p->id,
            'paymentCreatedAt' => '2026-05-16T13:30:16Z',
            'rawStatus' => $p->status_text,
            'isCompleted' => true,
            'isFatalError' => false,
            'grossAmount' => '100',
        ]);

        ObservedPayment::observe(PayoutsModelObserver::class);
    }

    public function test_update_dispatches_single_push_job(): void
    {
        Queue::fake();
        $p = ObservedPayment::create(['status_text' => 'A']);
        Queue::assertPushed(PushSinglePayoutJob::class, 1); // from created()

        $p->status_text = 'B';
        $p->save();
        Queue::assertPushed(PushSinglePayoutJob::class, 2); // + updated()
    }

    public function test_no_dispatch_when_source_unregistered(): void
    {
        $this->app->make(PayoutsManager::class)->forget();

        Queue::fake();
        ObservedPayment::create(['status_text' => 'A']);
        Queue::assertNothingPushed();
    }
}
