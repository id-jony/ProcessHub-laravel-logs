<?php

namespace ProcessHub\Logs\Tests\Feature;

use Illuminate\Support\Facades\Http;
use Illuminate\Support\Sleep;
use ProcessHub\Logs\Support\FallbackFile;
use ProcessHub\Logs\Tests\TestCase;

class FlushFallbackCommandTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        Sleep::fake();
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

    public function test_rate_limit_waits_for_retry_after_and_continues(): void
    {
        Http::fakeSequence('ph.test/*')
            ->push(['ok' => true])
            ->push('', 429, ['Retry-After' => '7'])
            ->whenEmpty(Http::response(['ok' => true]));
        $this->writeFallback(lines: 3, perLine: 100);

        $this->artisan('processhub:flush-fallback')->assertSuccessful();

        Sleep::assertSleptTimes(1);
        Sleep::assertSequence([Sleep::for(7)->seconds()]);
        $this->assertSame($this->expectedMessages(300), array_values(array_unique($this->sentMessages())));
        $this->assertFileDoesNotExist($this->path() . '.flushing');
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
    }

    public function test_server_error_stops_without_losing_entries(): void
    {
        Http::fakeSequence('ph.test/*')
            ->push(['ok' => true])
            ->push('', 503)
            ->whenEmpty(Http::response(['ok' => true]));
        $this->writeFallback(lines: 4, perLine: 70);

        $this->artisan('processhub:flush-fallback')->assertSuccessful();
        $this->assertFileExists($this->path() . '.flushing');

        $this->artisan('processhub:flush-fallback')->assertSuccessful();

        $delivered = array_slice($this->sentMessages(), 0, 100);
        $retried = array_slice($this->sentMessages(), 200); // 2nd request (503) is not delivered
        $this->assertSame($this->expectedMessages(280), [...$delivered, ...$retried]);
        $this->assertFileDoesNotExist($this->path() . '.flushing');
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
}
