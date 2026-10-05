<?php

namespace ProcessHub\Logs\Tests\Feature;

use GuzzleHttp\Exception\ClientException;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Response as GuzzleResponse;
use GuzzleHttp\Psr7\Request as GuzzleRequest;
use Illuminate\Contracts\Queue\Job;
use Illuminate\Http\Client\ConnectionException;
use Illuminate\Queue\MaxAttemptsExceededException;
use Illuminate\Queue\TimeoutExceededException;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Queue;
use Mockery;
use PHPUnit\Framework\Attributes\DataProvider;
use ProcessHub\Logs\Exceptions\DeliveryFailedException;
use ProcessHub\Logs\Jobs\SendLogBatchJob;
use ProcessHub\Logs\Support\LogIngest;
use ProcessHub\Logs\Tests\TestCase;

/**
 * We can't easily intercept the Guzzle client inside `SendLogBatchJob::handle`
 * without DI, so these tests focus on the error-handling branches that don't
 * require a live network: fallback file on failed(), config guards, payload
 * shape. The happy-path HTTP round-trip is covered by the integration docs
 * (run `php artisan processhub:test` against a real tenant).
 */
class SendLogBatchJobTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        // Make sure fallback writes go somewhere isolated.
        config()->set(
            'processhub.fallback_path',
            sys_get_temp_dir() . '/processhub-fallback-test-' . uniqid() . '.log',
        );
    }

    protected function tearDown(): void
    {
        $path = config('processhub.fallback_path');
        if ($path && is_file($path)) @unlink($path);
        parent::tearDown();
    }

    public function test_silent_noop_when_url_or_token_is_missing(): void
    {
        config()->set('processhub.url', null);
        $job = new SendLogBatchJob([
            ['level' => 'ERROR', 'message' => 'x'],
        ]);
        // Should not throw; should not require network.
        $job->handle();
        $this->assertTrue(true);
    }

    public function test_silent_noop_on_empty_entries(): void
    {
        $job = new SendLogBatchJob([]);
        $job->handle(); // must not throw
        $this->assertTrue(true);
    }

    public function test_failed_appends_to_fallback_file(): void
    {
        $entries = [[
            'level' => 'ERROR',
            'message' => 'permanent-failure',
            'host' => 'unit-test',
        ]];

        $job = new SendLogBatchJob($entries);
        $job->failed(new \RuntimeException('simulated exhaustion'));

        $path = config('processhub.fallback_path');
        $this->assertFileExists($path);
        $content = file_get_contents($path);
        $this->assertStringContainsString('permanent-failure', $content);
        $this->assertStringContainsString('simulated exhaustion', $content);

        // Each line is valid JSON — flush command relies on this.
        $decoded = json_decode(trim($content), true);
        $this->assertIsArray($decoded);
        $this->assertSame($entries, $decoded['entries']);
        $this->assertArrayHasKey('failed_at', $decoded);
    }

    public function test_fallback_file_is_append_safe_across_calls(): void
    {
        $first = new SendLogBatchJob([['level' => 'ERROR', 'message' => 'one']]);
        $first->failed(new \RuntimeException('err-1'));

        $second = new SendLogBatchJob([['level' => 'WARN', 'message' => 'two']]);
        $second->failed(new \RuntimeException('err-2'));

        $lines = file(config('processhub.fallback_path'), FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
        $this->assertCount(2, $lines);
    }

    /**
     * Build a mock 429 ClientException carrying a Retry-After header so the
     * `release()` branch can be exercised in isolation. This test just verifies
     * the shape of the header — the `release()` call itself needs a real queue.
     */
    public function test_client_exception_factory_for_retry_after(): void
    {
        $request = new GuzzleRequest('POST', '/api/ingest/logs');
        $response = new GuzzleResponse(429, ['Retry-After' => '17']);
        $ex = new ClientException('rate limited', $request, $response);

        $this->assertSame(429, $ex->getResponse()->getStatusCode());
        $this->assertSame('17', $ex->getResponse()->getHeaderLine('Retry-After'));
    }

    public function test_guzzle_mock_handler_yields_configured_status(): void
    {
        // Sanity: tests infrastructure can build a MockHandler if future
        // maintainers want to swap SendLogBatchJob to inject a client.
        $mock = new MockHandler([
            new GuzzleResponse(200, [], '{"accepted":1}'),
        ]);
        $stack = HandlerStack::create($mock);
        $this->assertNotNull($stack);
    }

    public function test_successful_delivery_posts_whole_batch(): void
    {
        Http::fake(['ph.test/*' => Http::response(['accepted' => 2])]);
        $job = $this->jobWithQueueMock([['message' => 'a'], ['message' => 'b']], function ($queueJob) {
            $queueJob->shouldNotReceive('release');
            $queueJob->shouldNotReceive('fail');
        });

        $job->handle();

        Http::assertSent(fn ($request) => $request->url() === 'https://ph.test/api/ingest/logs'
            && count($request['logs']) === 2
            && $request->hasHeader('Authorization'));
    }

    public function test_rate_limit_releases_with_retry_after_regardless_of_attempts(): void
    {
        Http::fake(['ph.test/*' => Http::response('', 429, ['Retry-After' => '17'])]);
        $job = $this->jobWithQueueMock([['message' => 'a']], function ($queueJob) {
            $queueJob->allows('attempts')->andReturn(50);
            $queueJob->shouldReceive('release')->once()->with(17);
            $queueJob->shouldNotReceive('fail');
        });

        $job->handle();
    }

    public function test_retry_policy_is_time_based(): void
    {
        config()->set('processhub.retry_window_sec', 3600);
        $job = new SendLogBatchJob([]);

        $this->assertFalse(property_exists($job, 'tries'), 'attempt cap would let 429 exhaust the job');
        $this->assertEqualsWithDelta(now()->addHour()->getTimestamp(), $job->retryDeadline, 5);
        // Срок проверяет сама задача; воркер не должен успеть пометить её упавшей.
        $this->assertSame($job->retryDeadline + 86_400, $job->retryUntil()->getTimestamp());
        $this->assertSame([10, 30, 120, 300], $job->backoff());
        $this->assertTrue($job->failOnTimeout);
    }

    /**
     * @return array<string, array{0: int, 1: int, 2: int}>
     */
    public static function temporaryStatuses(): array
    {
        return [
            '503, 1st attempt' => [503, 1, 10],
            '500, 3rd attempt' => [500, 3, 120],
            '408 is temporary too' => [408, 2, 30],
            'past the backoff list' => [502, 9, 300],
        ];
    }

    /**
     * Expected failures end the attempt with release() — an exception would
     * be reported by the worker and, for some messages, restart it.
     */
    #[DataProvider('temporaryStatuses')]
    public function test_temporary_error_releases_with_backoff(int $status, int $attempts, int $delay): void
    {
        Http::fake(['ph.test/*' => Http::response('', $status)]);
        $job = $this->jobWithQueueMock([['message' => 'a']], function ($queueJob) use ($attempts, $delay) {
            $queueJob->allows('attempts')->andReturn($attempts);
            $queueJob->shouldReceive('release')->once()->with($delay);
            $queueJob->shouldNotReceive('delete');
            $queueJob->shouldNotReceive('fail');
        });

        $job->handle();

        $this->assertFileDoesNotExist(config('processhub.fallback_path'));
    }

    /**
     * @return array<string, array{0: \Closure(): never}>
     */
    public static function transportErrors(): array
    {
        return [
            'connection exception' => [fn () => throw new ConnectionException('Connection refused')],
            // Laravel 11 не оборачивает обрыв соединения, TLS, HTTP/2 в ConnectionException.
            'raw guzzle exception' => [fn ($request) => throw new RequestException(
                'cURL error 56: Recv failure: Connection reset by peer',
                new GuzzleRequest('POST', $request->url()),
            )],
            'runtime exception' => [fn () => throw new \RuntimeException('Connection reset by peer')],
            'type error' => [fn () => throw new \TypeError('client bug')],
        ];
    }

    #[DataProvider('transportErrors')]
    public function test_any_error_of_the_http_call_releases_with_backoff(\Closure $failure): void
    {
        Http::fake($failure);
        $job = $this->jobWithQueueMock([['message' => 'a']], function ($queueJob) {
            $queueJob->shouldReceive('release')->once()->with(10);
            $queueJob->shouldNotReceive('fail');
        });

        $job->handle();
    }

    public function test_job_without_a_queue_to_retry_on_parks_the_batch(): void
    {
        Http::fake(fn () => throw new ConnectionException('Connection refused'));

        (new SendLogBatchJob([['message' => 'inline']]))->handle();

        $line = json_decode(trim((string) file_get_contents(config('processhub.fallback_path'))), true);
        $this->assertSame('inline', $line['entries'][0]['message']);
        $this->assertStringContainsString("sync queue can't retry", $line['reason']);
    }

    public function test_client_error_parks_batch_in_fallback_without_failing_the_job(): void
    {
        Http::fake(['ph.test/*' => Http::response(['error' => 'bad token'], 401)]);
        $job = $this->jobWithQueueMock([['message' => 'rejected']], function ($queueJob) {
            $queueJob->shouldReceive('delete')->once();
            $queueJob->shouldNotReceive('fail');
            $queueJob->shouldNotReceive('release');
        });

        $job->handle();

        $line = json_decode(trim((string) file_get_contents(config('processhub.fallback_path'))), true);
        $this->assertSame([['message' => 'rejected']], $line['entries']);
        $this->assertStringContainsString('HTTP 401', $line['reason']);
    }

    public function test_unencodable_values_do_not_break_the_batch(): void
    {
        Http::fake(['ph.test/*' => Http::response(['accepted' => 2])]);

        (new SendLogBatchJob([
            ['message' => 'inf', 'context' => ['ratio' => INF, 'avg' => NAN]],
            ['message' => "bad utf8 \xB1"],
        ]))->handle();

        Http::assertSent(fn ($request) => count($request['logs']) === 2
            && $request['logs'][0]['context'] === ['ratio' => 0, 'avg' => 0]
            && $request['logs'][1]['message'] === "bad utf8 \u{FFFD}");
    }

    public function test_rate_limit_pauses_all_senders(): void
    {
        config()->set('processhub.rate_limit_store', 'array');
        // Ход после паузы — в целых секундах очереди, считая от начала текущей.
        $this->freezeSecond();
        Http::fake(['ph.test/*' => Http::response('', 429, ['Retry-After' => '17'])]);

        $first = $this->jobWithQueueMock([['message' => 'a']], fn ($queueJob) => $queueJob->shouldReceive('release')->once()->with(17));
        $first->handle();

        $second = $this->jobWithQueueMock([['message' => 'b']], fn ($queueJob) => $queueJob->shouldReceive('release')->once()->with(17));
        $second->handle();

        Http::assertSentCount(1);
    }

    public function test_local_rate_limit_releases_without_posting(): void
    {
        config()->set('processhub.rate_limit_store', 'array');
        config()->set('processhub.rate_limit_per_minute', 6);
        $this->travelTo(Carbon::createFromTimestamp(intdiv(time(), 60) * 60 + 60));
        Http::fake(['ph.test/*' => Http::response(['accepted' => 1])]);

        $this->jobWithQueueMock([['message' => 'a']], fn ($queueJob) => $queueJob->shouldNotReceive('release'))->handle();
        $this->jobWithQueueMock([['message' => 'b']], function ($queueJob) {
            $queueJob->shouldReceive('release')->once()->with(10);
            $queueJob->shouldNotReceive('fail');
        })->handle();

        Http::assertSentCount(1);
    }

    /**
     * @return array<string, array{0: string, 1: int}>
     */
    public static function retryAfterHeaders(): array
    {
        return [
            'seconds' => ['17', 17],
            'zero is at least a second' => ['0', 1],
            'capped' => ['86400', 600],
            'http-date' => ['@120', 120],
            'past http-date' => ['@-30', 1],
            'garbage' => ['soon', LogIngest::DEFAULT_RETRY_AFTER],
            'missing' => ['', LogIngest::DEFAULT_RETRY_AFTER],
            'negative' => ['-5', 1],
            'optional whitespace' => [' 17 ', 17],
            'signed' => ['+5', LogIngest::DEFAULT_RETRY_AFTER],
            'fraction' => ['1.5', LogIngest::DEFAULT_RETRY_AFTER],
            'with unit' => ['5s', LogIngest::DEFAULT_RETRY_AFTER],
        ];
    }

    /**
     * Only IMF-fixdate is a date; whatever strtotime() would make of the
     * rest (relative phrases, ISO 8601, other zones) is not a Retry-After.
     *
     * @return array<string, array{0: string, 1: int}>
     */
    public static function retryAfterDates(): array
    {
        $default = LogIngest::DEFAULT_RETRY_AFTER;

        return [
            'imf-fixdate' => ['Mon, 05 Oct 2026 12:02:00 GMT', 120],
            'wrong weekday' => ['Fri, 05 Oct 2026 12:02:00 GMT', $default],
            'day out of range' => ['Mon, 32 Oct 2026 12:02:00 GMT', $default],
            'day not zero-padded' => ['Mon, 5 Oct 2026 12:02:00 GMT', $default],
            'numeric zone' => ['Mon, 05 Oct 2026 12:02:00 +0000', $default],
            'rfc 850' => ['Monday, 05-Oct-26 12:02:00 GMT', $default],
            'asctime' => ['Mon Oct  5 12:02:00 2026', $default],
            'iso 8601' => ['2026-10-05T12:02:00Z', $default],
            'relative' => ['+2 minutes', $default],
            'word' => ['tomorrow', $default],
        ];
    }

    #[DataProvider('retryAfterDates')]
    public function test_retry_after_accepts_only_imf_fixdate(string $header, int $expected): void
    {
        Carbon::setTestNow(Carbon::parse('2026-10-05 12:00:00', 'UTC'));
        Http::fake(['ph.test/*' => Http::response('', 429, ['Retry-After' => $header])]);

        $this->assertSame($expected, LogIngest::retryAfter(Http::get('https://ph.test/x')));
    }

    #[DataProvider('retryAfterHeaders')]
    public function test_retry_after_parsing(string $header, int $expected): void
    {
        $this->freezeTime();
        if (str_starts_with($header, '@')) {
            $header = gmdate('D, d M Y H:i:s \G\M\T', now()->getTimestamp() + (int) substr($header, 1));
        }
        Http::fake(['ph.test/*' => Http::response('', 429, $header === '' ? [] : ['Retry-After' => $header])]);

        $this->assertSame($expected, LogIngest::retryAfter(Http::get('https://ph.test/x')));
    }

    public function test_server_error_near_deadline_parks_batch_instead_of_retrying(): void
    {
        Http::fake(['ph.test/*' => Http::response('', 503)]);
        $job = $this->jobWithQueueMock([['message' => 'late']], function ($queueJob) {
            $queueJob->allows('attempts')->andReturn(3);
            $queueJob->shouldReceive('delete')->once();
            $queueJob->shouldNotReceive('fail');
        });
        // Next backoff (120 s) + attempt timeout no longer fit before the deadline.
        $job->retryDeadline = now()->getTimestamp() + 140;

        $job->handle();

        $line = json_decode(trim((string) file_get_contents(config('processhub.fallback_path'))), true);
        $this->assertSame('late', $line['entries'][0]['message']);
        $this->assertStringContainsString('retry window exhausted', $line['reason']);
    }

    public function test_rate_limit_past_deadline_parks_batch(): void
    {
        Http::fake(['ph.test/*' => Http::response('', 429, ['Retry-After' => '120'])]);
        $job = $this->jobWithQueueMock([['message' => 'late']], function ($queueJob) {
            $queueJob->shouldReceive('delete')->once();
            $queueJob->shouldNotReceive('release');
        });
        $job->retryDeadline = now()->getTimestamp() + 100;

        $job->handle();

        $this->assertFileExists(config('processhub.fallback_path'));
    }

    public function test_delay_shifts_retry_deadline(): void
    {
        config()->set('processhub.retry_window_sec', 3600);
        $this->freezeTime();

        $job = new SendLogBatchJob([['message' => 'a']], 600);

        $this->assertSame(now()->getTimestamp() + 4200, $job->retryDeadline);
        $this->assertSame(now()->getTimestamp() + 4200 + 86_400, $job->retryUntil()->getTimestamp());
    }

    public function test_job_picked_up_after_its_deadline_parks_batch_without_posting(): void
    {
        Http::fake();
        $job = $this->jobWithQueueMock([['message' => 'stale']], function ($queueJob) {
            $queueJob->shouldReceive('delete')->once();
            $queueJob->shouldNotReceive('release');
            $queueJob->shouldNotReceive('fail');
        });
        $job->retryDeadline = now()->getTimestamp() - 1;

        $job->handle();

        Http::assertNothingSent();
        $line = json_decode(trim((string) file_get_contents(config('processhub.fallback_path'))), true);
        $this->assertSame('stale', $line['entries'][0]['message']);
        $this->assertStringContainsString('waiting in the queue', $line['reason']);
    }

    public function test_unwritable_fallback_fails_job_instead_of_losing_batch(): void
    {
        config()->set('processhub.fallback_path', '/nonexistent-dir/processhub-fallback.log');
        Http::fake(['ph.test/*' => Http::response(['error' => 'bad token'], 401)]);
        $job = $this->jobWithQueueMock([['message' => 'precious']], function ($queueJob) {
            // failed_jobs хранит пачку — её можно вернуть queue:retry.
            $queueJob->shouldReceive('fail')->once()->with(Mockery::type(DeliveryFailedException::class));
            $queueJob->shouldNotReceive('delete');
        });

        $job->handle();
    }

    /**
     * fail() comes first: a broken log channel must not keep the batch out of
     * failed_jobs (it would be retried by backoff for a day instead).
     */
    public function test_unwritable_fallback_fails_job_even_when_logging_is_broken(): void
    {
        config()->set('processhub.fallback_path', '/nonexistent-dir/processhub-fallback.log');
        $this->breakLogging();
        Http::fake(['ph.test/*' => Http::response(['error' => 'bad token'], 401)]);
        $job = $this->jobWithQueueMock([['message' => 'precious']], function ($queueJob) {
            $queueJob->shouldReceive('fail')->once()->with(Mockery::type(DeliveryFailedException::class));
        });

        $job->handle();
    }

    public function test_parked_batch_survives_broken_logging(): void
    {
        $this->breakLogging();
        Http::fake(['ph.test/*' => Http::response(['error' => 'bad token'], 401)]);
        $job = $this->jobWithQueueMock([['message' => 'parked']], function ($queueJob) {
            $queueJob->shouldReceive('delete')->once();
        });

        $job->handle();

        $this->assertStringContainsString('parked', (string) file_get_contents(config('processhub.fallback_path')));
    }

    /**
     * failed() after park() couldn't write the file must not write the batch
     * there after all — it would end up both in the file and in failed_jobs
     * (and come back twice through queue:retry).
     */
    public function test_failed_after_unwritable_fallback_does_not_append_again(): void
    {
        $job = new SendLogBatchJob([['message' => 'once']]);

        $job->failed(DeliveryFailedException::unparked('HTTP 401'));

        $this->assertFileDoesNotExist(config('processhub.fallback_path'));
    }

    public function test_unwritable_fallback_without_queue_throws(): void
    {
        config()->set('processhub.fallback_path', '/nonexistent-dir/processhub-fallback.log');
        Http::fake(['ph.test/*' => Http::response(['error' => 'bad token'], 401)]);

        $this->expectException(DeliveryFailedException::class);
        (new SendLogBatchJob([['message' => 'precious']]))->handle();
    }

    public function test_broken_utf8_does_not_make_batch_unqueueable(): void
    {
        $job = new SendLogBatchJob([['message' => "bad \xB1 byte", 'context' => ["k\xB1" => 'v']], ['message' => 'ok']]);

        // Payload очереди — JSON с сериализованной задачей внутри.
        $this->assertNotFalse(json_encode(['command' => serialize($job)]));
        $this->assertSame("bad \u{FFFD} byte", $job->entries[0]['message']);
        $this->assertSame(['ok'], array_column(array_slice($job->entries, 1), 'message'));
    }

    /**
     * @return array<string, array{0: int}>
     */
    public static function legacyFailures(): array
    {
        return ['503' => [503], '408' => [408], '429' => [429], '401' => [401]];
    }

    /**
     * A job queued by 0.3 has no deadline: it isn't retried and isn't
     * re-queued — the batch goes to the fallback file (flush-fallback sends
     * it later, packed into full batches).
     */
    #[DataProvider('legacyFailures')]
    public function test_job_queued_by_v03_is_parked_instead_of_retried(int $status): void
    {
        Queue::fake();
        Http::fake(['ph.test/*' => Http::response('', $status)]);
        $job = $this->legacyJob();
        $queueJob = Mockery::mock(Job::class);
        $queueJob->allows('attempts')->andReturn(1);
        $queueJob->shouldReceive('delete')->once();
        $queueJob->shouldNotReceive('release');
        $job->setJob($queueJob);

        $job->handle();

        Queue::assertNothingPushed();
        $line = json_decode(trim((string) file_get_contents(config('processhub.fallback_path'))), true);
        $this->assertSame([['message' => 'old']], $line['entries']);
    }

    /**
     * The limiter's queue of turns belongs to jobs with a deadline. 0.3 jobs
     * (a backlog of thousands after the upgrade) used to take a turn each and
     * push it hours ahead — every fresh batch then missed its window and
     * went to the fallback file.
     */
    public function test_jobs_queued_by_v03_do_not_take_turns_in_the_limiter(): void
    {
        config()->set('processhub.rate_limit_store', 'array');
        $this->freezeSecond();
        Queue::fake();
        Http::fake(['ph.test/*' => Http::response(['accepted' => 1])]);

        for ($i = 0; $i < 200; $i++) {
            $queueJob = Mockery::mock(Job::class);
            $queueJob->allows('attempts')->andReturn(2);
            $queueJob->allows('delete');
            $queueJob->shouldNotReceive('release');
            $this->legacyJob()->setJob($queueJob)->handle();
        }

        // Burst of 5 went out; the rest was parked, nothing re-queued.
        Http::assertSentCount(5);
        Queue::assertNothingPushed();
        $this->assertCount(195, file(config('processhub.fallback_path')));

        // A fresh batch is told to come back after one interval, not after 195 of them.
        $this->jobWithQueueMock([['message' => 'fresh']], fn ($queueJob) => $queueJob->shouldReceive('release')->once()->with(2))->handle();
    }

    /** A 0.3 job doesn't jump ahead of jobs waiting for their turn either. */
    public function test_job_queued_by_v03_does_not_take_a_waiting_jobs_turn(): void
    {
        config()->set('processhub.rate_limit_store', 'array');
        config()->set('processhub.rate_limit_per_minute', 6);
        $this->freezeSecond();
        Http::fake(['ph.test/*' => Http::response(['accepted' => 1])]);

        $this->jobWithQueueMock([['message' => 'first']], fn ($queueJob) => $queueJob->shouldNotReceive('release'))->handle();
        $this->jobWithQueueMock([['message' => 'waits']], fn ($queueJob) => $queueJob->shouldReceive('release')->once()->with(10))->handle();
        $this->travel(10)->seconds();

        $queueJob = Mockery::mock(Job::class);
        $queueJob->allows('attempts')->andReturn(1);
        $queueJob->shouldReceive('delete')->once();
        $this->legacyJob()->setJob($queueJob)->handle();

        Http::assertSentCount(1);
        $this->assertStringContainsString('job queued by 0.3 is not retried', (string) file_get_contents(config('processhub.fallback_path')));
    }

    /**
     * @return array<string, array{0: \Throwable, 1: bool}>
     */
    public static function failures(): array
    {
        $own = SendLogBatchJob::class;

        return [
            'unparked batch' => [DeliveryFailedException::unparked('HTTP 401'), true],
            'attempts, L10 message' => [new MaxAttemptsExceededException($own . ' has been attempted too many times or run too long. The job may have previously timed out.'), true],
            'other job, L10 message' => [new MaxAttemptsExceededException('App\Jobs\Foo has been attempted too many times.'), false],
            'unrelated' => [new \RuntimeException($own . ' has failed'), false],
        ];
    }

    #[DataProvider('failures')]
    public function test_is_own_failure(\Throwable $e, bool $expected): void
    {
        $this->assertSame($expected, SendLogBatchJob::isOwnFailure($e));
    }

    public function test_worker_give_up_exceptions_are_own_failures(): void
    {
        $own = Mockery::mock(Job::class);
        $own->allows('resolveName')->andReturn(SendLogBatchJob::class);
        $other = Mockery::mock(Job::class);
        $other->allows('resolveName')->andReturn('App\Jobs\SendReceipt');

        $this->assertTrue(SendLogBatchJob::isOwnFailure(MaxAttemptsExceededException::forJob($own)));
        $this->assertTrue(SendLogBatchJob::isOwnFailure(TimeoutExceededException::forJob($own)));
        $this->assertFalse(SendLogBatchJob::isOwnFailure(MaxAttemptsExceededException::forJob($other)));
        $this->assertFalse(SendLogBatchJob::isOwnFailure(TimeoutExceededException::forJob($other)));
    }

    /** Shape of a job queued by 0.3: `$tries = 3`, no `retryDeadline`. */
    private function legacyJob(): SendLogBatchJob
    {
        $class = SendLogBatchJob::class;
        $job = @unserialize(sprintf(
            'O:%d:"%s":2:{s:7:"entries";a:1:{i:0;a:1:{s:7:"message";s:3:"old";}}s:5:"tries";i:3;}',
            strlen($class),
            $class,
        ));
        $this->assertInstanceOf(SendLogBatchJob::class, $job);

        return $job;
    }

    private function breakLogging(): void
    {
        config()->set('logging.channels.broken', ['driver' => 'monolog', 'handler' => \Monolog\Handler\StreamHandler::class, 'with' => ['stream' => '/nonexistent-dir/laravel.log']]);
        config()->set('logging.default', 'broken');
    }

    /**
     * @param  array<int, array<string, mixed>>  $entries
     */
    private function jobWithQueueMock(array $entries, callable $expectations): SendLogBatchJob
    {
        $queueJob = Mockery::mock(Job::class);
        $expectations($queueJob);
        // Первая попытка, если тест не задал иначе (первое объявление выигрывает).
        $queueJob->allows('attempts')->andReturn(1);
        $job = new SendLogBatchJob($entries);
        $job->setJob($queueJob);

        return $job;
    }
}
