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

    public function test_server_error_throws_for_backoff_retry(): void
    {
        Http::fake(['ph.test/*' => Http::response('', 503)]);
        $job = $this->jobWithQueueMock([['message' => 'a']], function ($queueJob) {
            $queueJob->allows('attempts')->andReturn(1);
            $queueJob->shouldNotReceive('release');
            $queueJob->shouldNotReceive('delete');
        });

        $this->expectException(DeliveryFailedException::class);
        $job->handle();
    }

    public function test_network_error_throws_delivery_exception(): void
    {
        Http::fake(fn () => throw new ConnectionException('Connection refused'));
        $job = new SendLogBatchJob([['message' => 'a']]);

        $this->expectException(DeliveryFailedException::class);
        $job->handle();
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

    /**
     * Laravel 11 doesn't wrap a reset connection / TLS / HTTP/2 error into
     * ConnectionException — the raw Guzzle exception must still become ours.
     */
    public function test_raw_guzzle_transport_error_becomes_delivery_exception(): void
    {
        Http::fake(fn ($request) => throw new RequestException(
            'cURL error 56: Recv failure: Connection reset by peer',
            new GuzzleRequest('POST', $request->url()),
        ));
        $job = new SendLogBatchJob([['message' => 'a']]);

        try {
            $job->handle();
            $this->fail('DeliveryFailedException expected');
        } catch (DeliveryFailedException $e) {
            $this->assertTrue(SendLogBatchJob::isOwnFailure($e));
            $this->assertStringContainsString('cURL error 56', $e->getMessage());
        }
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

    public function test_job_serialized_by_older_version_uses_payload_deadline(): void
    {
        Http::fake(['ph.test/*' => Http::response('', 503)]);
        $class = SendLogBatchJob::class;
        // Shape of a job queued before `retryDeadline` existed.
        $job = unserialize(sprintf(
            'O:%d:"%s":2:{s:7:"entries";a:1:{i:0;a:1:{s:7:"message";s:3:"old";}}s:13:"maxExceptions";i:10;}',
            strlen($class),
            $class,
        ));
        $this->assertInstanceOf(SendLogBatchJob::class, $job);
        $this->assertNull($job->retryDeadline);

        $queueJob = Mockery::mock(Job::class);
        $queueJob->allows('attempts')->andReturn(1);
        // 503 → backoff 10 s + 30 s timeout no longer fit before the payload deadline.
        $queueJob->allows('retryUntil')->andReturn(now()->getTimestamp() + 35);
        $queueJob->shouldReceive('delete')->once();
        $job->setJob($queueJob);

        $job->handle();

        $this->assertFileExists(config('processhub.fallback_path'));
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

    public function test_job_queued_by_v03_is_retried_as_a_new_job(): void
    {
        Queue::fake();
        Http::fake(['ph.test/*' => Http::response('', 503)]);
        $job = $this->legacyJob();
        $queueJob = Mockery::mock(Job::class);
        // Payload 0.3: maxTries 3, retryUntil нет — release() на 3-й попытке дал бы MaxAttemptsExceeded.
        $queueJob->allows('attempts')->andReturn(3);
        $queueJob->allows('retryUntil')->andReturn(null);
        $queueJob->shouldReceive('delete')->once();
        $queueJob->shouldNotReceive('release');
        $job->setJob($queueJob);

        $job->handle();

        Queue::assertPushed(SendLogBatchJob::class, fn (SendLogBatchJob $retry) => $retry->entries === [['message' => 'old']]
            && $retry->retryDeadline === now()->getTimestamp() + 120 + 3600);
    }

    /**
     * @return array<string, array{0: \Throwable, 1: bool}>
     */
    public static function failures(): array
    {
        $own = SendLogBatchJob::class;

        return [
            'delivery' => [DeliveryFailedException::status(503), true],
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
