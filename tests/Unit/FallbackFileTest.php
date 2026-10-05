<?php

namespace ProcessHub\Logs\Tests\Unit;

use ProcessHub\Logs\Support\FallbackFile;
use ProcessHub\Logs\Tests\TestCase;

class FallbackFileTest extends TestCase
{
    protected function tearDown(): void
    {
        @unlink(config('processhub.fallback_path') . '.write.lock');
        parent::tearDown();
    }

    public function test_append_after_torn_tail_starts_a_new_line(): void
    {
        $path = config('processhub.fallback_path');
        file_put_contents($path, '{"failed_at":"2026-10-05T12:00:00+00:00","reas');

        FallbackFile::append([['level' => 'ERROR', 'message' => 'next']], 'x');

        $lines = file($path, FILE_IGNORE_NEW_LINES);
        $this->assertCount(2, $lines);
        $this->assertSame('next', json_decode($lines[1], true)['entries'][0]['message']);
    }

    public function test_locked_excludes_other_holders_of_the_write_lock(): void
    {
        $path = config('processhub.fallback_path');

        $heldElsewhere = FallbackFile::locked($path, function () use ($path): bool {
            $other = fopen($path . '.write.lock', 'c');
            $acquired = flock($other, LOCK_EX | LOCK_NB);
            fclose($other);

            return ! $acquired;
        });

        $this->assertTrue($heldElsewhere);

        $other = fopen($path . '.write.lock', 'c');
        $this->assertTrue(flock($other, LOCK_EX | LOCK_NB), 'lock must be released after the callback');
        fclose($other);
    }

    public function test_append_waits_for_the_write_lock(): void
    {
        if (! function_exists('pcntl_fork') || ! function_exists('posix_kill')) {
            $this->markTestSkipped('pcntl and posix are required.');
        }

        $path = config('processhub.fallback_path');
        $lock = fopen($path . '.write.lock', 'c');
        flock($lock, LOCK_EX);

        $pid = pcntl_fork();
        if ($pid === 0) {
            FallbackFile::append([['level' => 'ERROR', 'message' => 'child']], 'x');
            // Завершаемся без shutdown-обработчиков PHPUnit.
            posix_kill(posix_getpid(), SIGKILL);
        }

        usleep(200_000);
        clearstatcache();
        $writtenWhileLocked = is_file($path) && filesize($path) > 0;

        flock($lock, LOCK_UN);
        fclose($lock);
        pcntl_waitpid($pid, $status);

        $this->assertFalse($writtenWhileLocked, 'append must not write while the lock is held');
        $this->assertStringContainsString('"child"', (string) file_get_contents($path));
    }
}
