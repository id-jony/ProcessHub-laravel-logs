<?php

namespace ProcessHub\Logs\Tests\Feature;

use Illuminate\Contracts\Queue\Job;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Support\Facades\Queue;
use Mockery;
use ProcessHub\Logs\Jobs\SendLogBatchJob;
use ProcessHub\Logs\Tests\TestCase;

class HandleJobFailedTest extends TestCase
{
    public function test_failure_of_delivery_job_is_not_logged_back(): void
    {
        Queue::fake();

        event(new JobFailed('redis', $this->job(SendLogBatchJob::class), new \RuntimeException('attempted too many times')));

        Queue::assertNothingPushed();
    }

    public function test_failure_of_business_job_is_shipped(): void
    {
        Queue::fake();

        event(new JobFailed('redis', $this->job('App\\Jobs\\SendReceipt'), new \RuntimeException('boom')));

        Queue::assertPushed(SendLogBatchJob::class, function (SendLogBatchJob $job) {
            return count($job->entries) === 1
                && $job->entries[0]['message'] === 'Queue job failed'
                && $job->entries[0]['context']['connection'] === 'redis'
                && $job->entries[0]['context']['message'] === 'boom';
        });
    }

    private function job(string $class): Job
    {
        $job = Mockery::mock(Job::class);
        $job->allows('resolveName')->andReturn($class);
        $job->allows('payload')->andReturn(['displayName' => $class]);
        $job->allows('getQueue')->andReturn('default');
        $job->allows('attempts')->andReturn(3);

        return $job;
    }
}
