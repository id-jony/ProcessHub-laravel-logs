<?php

namespace ProcessHub\Logs\Tests\Feature;

use PHPUnit\Framework\Attributes\DataProvider;
use ProcessHub\Logs\Jobs\SendLogBatchJob;
use ProcessHub\Logs\Tests\TestCase;
use Symfony\Component\Process\PhpExecutableFinder;
use Symfony\Component\Process\Process;

/**
 * A fatal error skips terminating callbacks and destructors — the buffer must
 * still reach the queue, together with the FatalError Laravel logs on shutdown
 * (or the package itself, when Laravel doesn't) — exactly once.
 *
 * Not covered, because it can't be: Laravel's shutdown handler runs before
 * any function the package registers, and a fatal error inside it skips
 * every later one. If Laravel skips reporting the FatalError (dontReport /
 * throttling) and rendering it then runs out of memory, the buffer is lost.
 */
class FatalErrorFlushTest extends TestCase
{
    /**
     * @return array<string, array{string, string, string, bool}>
     */
    public static function fatalErrors(): array
    {
        $fatal = 'Symfony\Component\ErrorHandler\Error\FatalError';

        return [
            'out of memory, large allocation' => ['memory-large', $fatal, 'Allowed memory size', false],
            'out of memory, small allocations' => ['memory-small', $fatal, 'Allowed memory size', false],
            'max_execution_time' => ['timeout', $fatal, 'Maximum execution time', false],
            'max_execution_time, not reported by Laravel' => ['timeout', $fatal, 'Maximum execution time', true],
            'uncaught exception' => ['uncaught', \RuntimeException::class, 'uncaught in script', false],
        ];
    }

    #[DataProvider('fatalErrors')]
    public function test_buffer_and_fatal_error_are_queued(string $mode, string $class, string $fatalMessage, bool $unreported): void
    {
        if ($mode === 'memory-small' && version_compare($this->app->version(), '11.0.0', '<')) {
            // Laravel 10 keeps ~32 KB for its shutdown handler, which allocates
            // more before any package code runs; whether it fits depends on heap
            // fragmentation, so the buffer may be lost there (see README).
            $this->markTestSkipped('Laravel 10: out of memory in small allocations is not guaranteed.');
        }

        $database = tempnam(sys_get_temp_dir(), 'processhub-fatal-');

        try {
            $process = new Process(
                [
                    (new PhpExecutableFinder())->find(), __DIR__ . '/../Fixtures/fatal-error.php',
                    $mode, $database, $unreported ? 'unreported' : 'reported',
                ],
                timeout: 60,
            );
            $process->run();

            $this->assertStringContainsString($fatalMessage, $process->getOutput() . $process->getErrorOutput());

            $entries = $this->queuedEntries($database);
            $this->assertSame('before fatal', $entries[0]['message'] ?? null, $process->getErrorOutput());
            $this->assertCount(3, $entries);
            // Laravel logs the error before later shutdown functions run; the
            // package ships an unreported one from its own last function.
            [$fatal, $late] = $unreported ? [$entries[2], $entries[1]] : [$entries[1], $entries[2]];
            $this->assertSame('exception', $fatal['contextType'] ?? null);
            $this->assertSame($class, $fatal['context']['class']);
            $this->assertStringContainsString($fatalMessage, $fatal['message']);
            $this->assertSame('late shutdown', $late['message']);
            $this->assertFileDoesNotExist($database . '.fallback.log');
        } finally {
            @unlink($database);
            @unlink($database . '.fallback.log');
            @unlink($database . '.laravel.log');
        }
    }

    /**
     * @return array<int, array<string, mixed>>
     */
    private function queuedEntries(string $database): array
    {
        $pdo = new \PDO('sqlite:' . $database);
        $rows = $pdo->query("select payload from jobs where queue = 'logs' order by id")->fetchAll(\PDO::FETCH_COLUMN);

        $entries = [];
        foreach ($rows as $payload) {
            $job = unserialize(json_decode($payload, true)['data']['command']);
            $this->assertInstanceOf(SendLogBatchJob::class, $job);
            array_push($entries, ...$job->entries);
        }

        return $entries;
    }
}
