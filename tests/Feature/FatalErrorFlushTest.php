<?php

namespace ProcessHub\Logs\Tests\Feature;

use PHPUnit\Framework\Attributes\DataProvider;
use ProcessHub\Logs\Jobs\SendLogBatchJob;
use ProcessHub\Logs\Tests\TestCase;
use Symfony\Component\Process\PhpExecutableFinder;
use Symfony\Component\Process\Process;

/**
 * A fatal error skips terminating callbacks and destructors — the buffer must
 * still reach the queue, together with the FatalError Laravel logs on shutdown.
 */
class FatalErrorFlushTest extends TestCase
{
    /**
     * @return array<string, array{string, string, string}>
     */
    public static function fatalErrors(): array
    {
        $fatal = 'Symfony\Component\ErrorHandler\Error\FatalError';

        return [
            'out of memory, large allocation' => ['memory-large', $fatal, 'Allowed memory size'],
            'out of memory, small allocations' => ['memory-small', $fatal, 'Allowed memory size'],
            'max_execution_time' => ['timeout', $fatal, 'Maximum execution time'],
            'uncaught exception' => ['uncaught', \RuntimeException::class, 'uncaught in script'],
        ];
    }

    #[DataProvider('fatalErrors')]
    public function test_buffer_and_fatal_error_are_queued(string $mode, string $class, string $fatalMessage): void
    {
        $database = tempnam(sys_get_temp_dir(), 'processhub-fatal-');

        try {
            $process = new Process(
                [(new PhpExecutableFinder())->find(), __DIR__ . '/../Fixtures/fatal-error.php', $mode, $database],
                timeout: 60,
            );
            $process->run();

            $this->assertStringContainsString($fatalMessage, $process->getOutput() . $process->getErrorOutput());

            $entries = $this->queuedEntries($database);
            $this->assertSame('before fatal', $entries[0]['message'] ?? null, $process->getErrorOutput());
            $this->assertCount(3, $entries);
            $this->assertSame('exception', $entries[1]['contextType']);
            $this->assertSame($class, $entries[1]['context']['class']);
            $this->assertStringContainsString($fatalMessage, $entries[1]['message']);
            $this->assertSame('late shutdown', $entries[2]['message']);
            $this->assertFileDoesNotExist($database . '.fallback.log');
        } finally {
            @unlink($database);
            @unlink($database . '.fallback.log');
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
