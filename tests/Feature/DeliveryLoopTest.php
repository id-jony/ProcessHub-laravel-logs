<?php

namespace ProcessHub\Logs\Tests\Feature;

use Illuminate\Queue\Events\JobExceptionOccurred;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Queue;
use Illuminate\Support\Facades\Redis;
use ProcessHub\Logs\Jobs\SendLogBatchJob;
use ProcessHub\Logs\Logging\LogBuffer;
use ProcessHub\Logs\Tests\Concerns\UsesRedis;
use ProcessHub\Logs\Tests\TestCase;

/**
 * A SendLogBatchJob failing with an arbitrary exception is reported by the
 * worker into the default log — which includes processhub. That report must
 * not turn into another SendLogBatchJob.
 */
class DeliveryLoopTest extends TestCase
{
    use UsesRedis;

    private int $attempts = 0;

    protected function setUp(): void
    {
        parent::setUp();
        $this->setUpRedis();

        config()->set('logging.channels.stack', ['driver' => 'stack', 'channels' => ['processhub']]);
        config()->set('logging.default', 'stack');
    }

    public function test_failing_delivery_job_does_not_enqueue_reports_about_itself(): void
    {
        $this->failDelivery();
        Carbon::setTestNow('2026-10-05 12:00:00');

        SendLogBatchJob::enqueue([['level' => 'ERROR', 'message' => 'original']]);

        for ($pass = 0; $pass < 3; $pass++) {
            $this->artisan('queue:work', [
                'connection' => 'redis', '--queue' => 'logs', '--once' => true,
            ])->assertSuccessful();
            // Past the job's backoff so the next pass picks it up again.
            Carbon::setTestNow(Carbon::now()->addMinutes(10));
        }

        $this->assertSame(3, $this->attempts);
        app(LogBuffer::class)->flush();
        $this->assertSame([['original']], $this->queuedMessages());
    }

    public function test_daemon_worker_does_not_enqueue_reports_about_delivery_job(): void
    {
        $this->failDelivery();

        SendLogBatchJob::enqueue([['level' => 'ERROR', 'message' => 'original']]);

        $this->artisan('queue:work', [
            'connection' => 'redis', '--queue' => 'logs', '--max-jobs' => 1, '--sleep' => 0,
        ])->assertSuccessful();

        $this->assertSame(1, $this->attempts);
        $this->assertSame([['original']], $this->queuedMessages());
    }

    /**
     * An exception that escapes handle() (a bug, not a delivery failure) is
     * reported by the worker after the attempt — still not into processhub.
     */
    public function test_exception_escaping_the_job_is_not_logged_back(): void
    {
        Http::fake();
        $reported = 0;
        Event::listen(JobExceptionOccurred::class, function () use (&$reported) {
            $reported++;
        });

        SendLogBatchJob::enqueue([['level' => 'ERROR', 'message' => 'original']]);
        // Не строка — deliver(string $url) бросит TypeError мимо обработки сбоев доставки.
        config()->set('processhub.url', ['https://ph.test']);

        $this->artisan('queue:work', [
            'connection' => 'redis', '--queue' => 'logs', '--max-jobs' => 1, '--sleep' => 0,
        ])->assertSuccessful();
        app(LogBuffer::class)->flush();

        $this->assertSame(1, $reported);
        $this->assertSame([['original']], $this->queuedMessages());
    }

    private function failDelivery(): void
    {
        Http::fake(function () {
            $this->attempts++;
            throw new \RuntimeException('unexpected client failure');
        });
    }

    /**
     * Messages of every SendLogBatchJob left in the queue (ready or delayed).
     *
     * @return array<int, array<int, string>>
     */
    private function queuedMessages(): array
    {
        $redis = Redis::connection();
        $payloads = array_merge(
            $redis->lrange('queues:logs', 0, -1),
            $redis->zrange('queues:logs:delayed', 0, -1),
            $redis->zrange('queues:logs:reserved', 0, -1),
        );

        return array_map(
            fn (string $payload) => array_column(unserialize(json_decode($payload, true)['data']['command'])->entries, 'message'),
            $payloads,
        );
    }
}
