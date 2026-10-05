<?php

namespace ProcessHub\Logs\Tests\Feature;

use Carbon\CarbonInterval;
use Illuminate\Http\Client\ConnectionException;
use Illuminate\Http\Client\Request;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Sleep;
use ProcessHub\Logs\Support\FallbackFile;
use ProcessHub\Logs\Tests\TestCase;

class FlushFallbackCommandTest extends TestCase
{
    /** @var array<int, float> секунды каждого Sleep */
    private array $slept = [];

    protected function setUp(): void
    {
        parent::setUp();
        Carbon::setTestNow('2026-10-05 12:00:00');
        Sleep::fake(syncWithCarbon: true);
        Sleep::whenFakingSleep(function (CarbonInterval $duration): void {
            $this->slept[] = round($duration->totalMilliseconds / 1000, 3);
        });
    }

    protected function tearDown(): void
    {
        $path = config('processhub.fallback_path');
        foreach (['.rejected', '.flushing.offset', '.flushing.offset.tmp', '.write.lock'] as $suffix) {
            @unlink($path . $suffix);
        }
        parent::tearDown();
    }

    public function test_entries_from_many_lines_are_packed_into_full_batches(): void
    {
        Http::fake(['ph.test/*' => Http::response(['accepted' => true])]);
        $this->writeFallback(lines: 5, perLine: 50);

        $this->artisan('processhub:flush-fallback')->assertSuccessful();

        $this->assertSame([100, 100, 50], $this->sentSizes());
        $this->assertSame($this->expectedMessages(250), $this->sentMessages());
        $this->assertFileDoesNotExist($this->path());
        $this->assertFileDoesNotExist($this->path() . '.flushing');
    }

    public function test_requests_are_paced_to_default_rate(): void
    {
        Http::fake(['ph.test/*' => Http::response(['ok' => true])]);
        $this->writeFallback(lines: 4, perLine: 100);

        $this->artisan('processhub:flush-fallback')->assertSuccessful();

        // 50 запросов в минуту → не чаще раза в 1,2 с.
        $this->assertSame([1.2, 1.2, 1.2], $this->slept);
        $this->assertCount(4, Http::recorded());
    }

    public function test_rate_comes_from_option_or_config(): void
    {
        Http::fake(['ph.test/*' => Http::response(['ok' => true])]);
        $this->writeFallback(lines: 3, perLine: 100);

        $this->artisan('processhub:flush-fallback', ['--rate' => 30])->assertSuccessful();
        $this->assertSame([2.0, 2.0], $this->slept);

        $this->slept = [];
        config()->set('processhub.rate_limit_per_minute', 20);
        $this->writeFallback(lines: 2, perLine: 100);

        $this->artisan('processhub:flush-fallback')->assertSuccessful();
        $this->assertSame([3.0], $this->slept);
    }

    public function test_rate_limit_waits_for_retry_after_and_continues(): void
    {
        Http::fakeSequence('ph.test/*')
            ->push(['ok' => true])
            ->push('', 429, ['Retry-After' => '7'])
            ->whenEmpty(Http::response(['ok' => true]));
        $this->writeFallback(lines: 3, perLine: 100);

        $this->artisan('processhub:flush-fallback', ['--rate' => 60])->assertSuccessful();

        $this->assertSame([1.0, 7.0, 1.0], $this->slept);
        $this->assertSame($this->expectedMessages(300), array_values(array_unique($this->sentMessages())));
        $this->assertFileDoesNotExist($this->path() . '.flushing');
    }

    public function test_retry_after_zero_negative_or_missing_never_spins(): void
    {
        Http::fakeSequence('ph.test/*')
            ->push('', 429, ['Retry-After' => '0'])
            ->push('', 429, ['Retry-After' => '-5'])
            ->push('', 429)
            ->push('', 429, ['Retry-After' => 'soon'])
            ->whenEmpty(Http::response(['ok' => true]));
        $this->writeFallback(lines: 1, perLine: 10);

        $this->artisan('processhub:flush-fallback', ['--rate' => 600])->assertSuccessful();

        // Минимум 1 с; без пригодного заголовка — 30 с.
        $this->assertSame([1.0, 1.0, 30.0, 30.0], $this->slept);
        $this->assertCount(5, Http::recorded());
    }

    public function test_retry_after_as_http_date(): void
    {
        $at = Carbon::now()->addSeconds(20)->toRfc7231String();
        Http::fakeSequence('ph.test/*')
            ->push('', 429, ['Retry-After' => $at])
            ->whenEmpty(Http::response(['ok' => true]));
        $this->writeFallback(lines: 1, perLine: 10);

        $this->artisan('processhub:flush-fallback')->assertSuccessful();

        $this->assertSame([20.0], $this->slept);
    }

    public function test_retry_after_above_max_wait_stops_run(): void
    {
        Http::fake(['ph.test/*' => Http::response('', 429, ['Retry-After' => '600'])]);
        $this->writeFallback(lines: 1, perLine: 10);

        $this->artisan('processhub:flush-fallback', ['--max-wait' => 60])->assertSuccessful();

        Sleep::assertNeverSlept();
        $remainder = file($this->path() . '.flushing', FILE_IGNORE_NEW_LINES);
        $this->assertCount(10, json_decode($remainder[0], true)['entries']);
    }

    public function test_server_errors_back_off_exponentially_and_continue(): void
    {
        Http::fakeSequence('ph.test/*')
            ->push(['ok' => true])
            ->push('', 503)
            ->push('', 502)
            ->whenEmpty(Http::response(['ok' => true]));
        $this->writeFallback(lines: 4, perLine: 70);

        $this->artisan('processhub:flush-fallback', ['--rate' => 60])->assertSuccessful();

        $this->assertSame([1.0, 2.0, 4.0, 1.0], $this->slept);
        $this->assertSame($this->expectedMessages(280), array_values(array_unique($this->sentMessages())));
        $this->assertFileDoesNotExist($this->path() . '.flushing');
    }

    public function test_consecutive_failures_stop_with_failure_and_keep_everything(): void
    {
        $attempts = 0;
        $down = true;
        Http::fake(function () use (&$attempts, &$down) {
            $attempts++;
            if ($down) {
                throw new ConnectionException('Connection refused');
            }

            return Http::response(['ok' => true]);
        });
        $this->writeFallback(lines: 3, perLine: 100);

        $this->artisan('processhub:flush-fallback', ['--rate' => 600])
            ->expectsOutputToContain('Giving up after 5 consecutive failures')
            ->assertFailed();

        $this->assertSame([2.0, 4.0, 8.0, 16.0], $this->slept);
        $this->assertSame(5, $attempts);

        $down = false;
        $this->artisan('processhub:flush-fallback')->assertSuccessful();

        $this->assertSame($this->expectedMessages(300), $this->sentMessages());
    }

    public function test_snapshot_is_not_rewritten_when_nothing_was_sent(): void
    {
        Http::fake(['ph.test/*' => Http::response('', 503)]);
        $this->writeFallback(lines: 3, perLine: 100);
        $original = file_get_contents($this->path());

        $this->artisan('processhub:flush-fallback')->assertFailed();
        $inode = fileinode($this->path() . '.flushing');

        $this->artisan('processhub:flush-fallback')->assertFailed();

        clearstatcache();
        $this->assertSame($original, file_get_contents($this->path() . '.flushing'));
        $this->assertSame($inode, fileinode($this->path() . '.flushing'));
    }

    public function test_rejected_entry_is_isolated_and_moved_aside(): void
    {
        Http::fake(function (Request $request) {
            $bad = in_array('m-37', array_column($request->data()['logs'], 'message'), true);

            return $bad
                ? Http::response(['message' => 'logs.37.level is invalid'], 422)
                : Http::response(['ok' => true]);
        });
        $this->writeFallback(lines: 3, perLine: 50);

        $this->artisan('processhub:flush-fallback', ['--rate' => 6000])->assertSuccessful();

        $delivered = $this->deliveredMessages();
        sort($delivered, SORT_NATURAL);
        $this->assertSame(array_values(array_diff($this->expectedMessages(150), ['m-37'])), $delivered);

        $rejected = file($this->path() . '.rejected', FILE_IGNORE_NEW_LINES);
        $this->assertCount(1, $rejected);
        $line = json_decode($rejected[0], true);
        $this->assertSame('m-37', $line['entries'][0]['message']);
        $this->assertStringContainsString('HTTP 422', $line['reason']);
        $this->assertFileDoesNotExist($this->path() . '.flushing');
    }

    public function test_too_large_batch_is_split(): void
    {
        Http::fake(fn (Request $request) => count($request->data()['logs']) > 30
            ? Http::response('', 413)
            : Http::response(['ok' => true]));
        $this->writeFallback(lines: 1, perLine: 100);

        $this->artisan('processhub:flush-fallback', ['--rate' => 6000])->assertSuccessful();

        $delivered = $this->deliveredMessages();
        sort($delivered, SORT_NATURAL);
        $this->assertSame($this->expectedMessages(100), $delivered);
        $this->assertFileDoesNotExist($this->path() . '.rejected');
    }

    public function test_auth_error_stops_with_failure_and_skips_nothing(): void
    {
        Http::fake(['ph.test/*' => Http::response(['message' => 'Unauthenticated.'], 401)]);
        $this->writeFallback(lines: 2, perLine: 100);
        $original = file_get_contents($this->path());

        $this->artisan('processhub:flush-fallback')
            ->expectsOutputToContain('HTTP 401')
            ->assertFailed();

        $this->assertCount(1, Http::recorded());
        $this->assertSame($original, file_get_contents($this->path() . '.flushing'));
        $this->assertFileDoesNotExist($this->path() . '.rejected');
    }

    public function test_limit_keeps_exact_remainder_for_next_run(): void
    {
        Http::fake(['ph.test/*' => Http::response(['ok' => true])]);
        $this->writeFallback(lines: 5, perLine: 30);

        $this->artisan('processhub:flush-fallback', ['--limit' => 1])->assertSuccessful();

        $this->assertSame([100], $this->sentSizes());
        $this->assertFileExists($this->path() . '.flushing');

        // New failures arriving meanwhile land in the live file.
        FallbackFile::append([['level' => 'ERROR', 'message' => 'late']], 'x');

        $this->artisan('processhub:flush-fallback')->assertSuccessful();

        $this->assertSame([...$this->expectedMessages(150), 'late'], $this->sentMessages());
        $this->assertFileDoesNotExist($this->path());
        $this->assertFileDoesNotExist($this->path() . '.flushing');
        $this->assertFileDoesNotExist($this->path() . '.flushing.offset');
    }

    public function test_max_runtime_stops_and_keeps_remainder(): void
    {
        Http::fake(['ph.test/*' => Http::response(['ok' => true])]);
        $this->writeFallback(lines: 5, perLine: 100);

        $this->artisan('processhub:flush-fallback', ['--rate' => 60, '--max-runtime' => 3])
            ->expectsOutputToContain('--max-runtime reached')
            ->assertSuccessful();

        $this->assertSame([100, 100, 100], $this->sentSizes());

        $this->artisan('processhub:flush-fallback')->assertSuccessful();

        $this->assertSame($this->expectedMessages(500), $this->sentMessages());
    }

    public function test_checkpoint_of_another_file_is_ignored(): void
    {
        Http::fake(['ph.test/*' => Http::response(['ok' => true])]);
        $this->writeFallback(lines: 3, perLine: 100);
        $this->artisan('processhub:flush-fallback', ['--limit' => 1])->assertSuccessful();

        // Снапшот подменили (новый inode) — старое смещение к нему не относится.
        $replacement = $this->path() . '.replacement';
        file_put_contents($replacement, FallbackFile::line([['level' => 'ERROR', 'message' => 'other']], 'x'));
        rename($replacement, $this->path() . '.flushing');

        $this->artisan('processhub:flush-fallback')->assertSuccessful();

        $this->assertSame('other', $this->sentMessages()[100]);
    }

    public function test_torn_last_line_is_moved_aside_not_lost(): void
    {
        Http::fake(['ph.test/*' => Http::response(['ok' => true])]);
        $this->writeFallback(lines: 1, perLine: 3);
        $torn = substr(FallbackFile::line([['level' => 'ERROR', 'message' => 'torn']], 'x'), 0, 40);
        file_put_contents($this->path(), $torn, FILE_APPEND);

        $this->artisan('processhub:flush-fallback')
            ->doesntExpectOutputToContain('malformed')
            ->assertSuccessful();

        $this->assertSame($this->expectedMessages(3), $this->sentMessages());
        $rejected = json_decode(file_get_contents($this->path() . '.rejected'), true);
        $this->assertSame($torn, $rejected['raw']);
    }

    public function test_malformed_lines_are_skipped(): void
    {
        Http::fake(['ph.test/*' => Http::response(['ok' => true])]);
        $this->writeFallback(lines: 1, perLine: 3);
        file_put_contents($this->path(), "not json\n", FILE_APPEND);

        $this->artisan('processhub:flush-fallback')
            ->expectsOutputToContain('Skipped 1 malformed lines.')
            ->assertSuccessful();

        $this->assertSame($this->expectedMessages(3), $this->sentMessages());
    }

    public function test_nothing_to_flush(): void
    {
        Http::fake();

        $this->artisan('processhub:flush-fallback')
            ->expectsOutputToContain('Nothing to flush')
            ->assertSuccessful();

        Http::assertNothingSent();
    }

    private function path(): string
    {
        return config('processhub.fallback_path');
    }

    private function writeFallback(int $lines, int $perLine): void
    {
        $n = 0;
        for ($l = 0; $l < $lines; $l++) {
            $entries = [];
            for ($i = 0; $i < $perLine; $i++) {
                $entries[] = ['level' => 'ERROR', 'message' => 'm-' . $n++];
            }
            FallbackFile::append($entries, 'test');
        }
    }

    /**
     * @return array<int, string>
     */
    private function expectedMessages(int $count): array
    {
        return array_map(fn ($i) => 'm-' . $i, range(0, $count - 1));
    }

    /**
     * @return array<int, int>
     */
    private function sentSizes(): array
    {
        return Http::recorded()->map(fn ($pair) => count($pair[0]['logs']))->values()->all();
    }

    /**
     * @return array<int, string>
     */
    private function sentMessages(): array
    {
        return Http::recorded()
            ->flatMap(fn ($pair) => array_column($pair[0]['logs'], 'message'))
            ->values()
            ->all();
    }

    /**
     * @return array<int, string>
     */
    private function deliveredMessages(): array
    {
        return Http::recorded()
            ->filter(fn ($pair) => $pair[1]->successful())
            ->flatMap(fn ($pair) => array_column($pair[0]['logs'], 'message'))
            ->values()
            ->all();
    }
}
