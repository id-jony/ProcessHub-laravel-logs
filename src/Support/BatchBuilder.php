<?php

namespace ProcessHub\Logs\Support;

/**
 * Accumulates log entries into batches that respect the ingest contract:
 * at most `processhub.batch_size` entries (hard cap 100) and roughly
 * `processhub.batch_max_bytes` of JSON per request (server cap is 2 MB).
 *
 * Shared by the Monolog buffer, `processhub:flush-fallback` and
 * `processhub:rebatch-queue` so every path ships identically-shaped batches.
 */
final class BatchBuilder
{
    /** ProcessHub rejects batches with more entries than this. */
    public const MAX_ENTRIES = 100;

    /** Default byte budget — leaves headroom under the 2 MB server cap. */
    public const DEFAULT_MAX_BYTES = 1_800_000;

    /** @var array<int, array<string, mixed>> */
    private array $entries = [];

    private int $bytes = 0;

    public function __construct(
        private readonly int $maxEntries,
        private readonly int $maxBytes,
    ) {}

    public static function fromConfig(): self
    {
        return new self(
            max(1, min(self::MAX_ENTRIES, (int) config('processhub.batch_size', self::MAX_ENTRIES))),
            max(1024, (int) config('processhub.batch_max_bytes', self::DEFAULT_MAX_BYTES)),
        );
    }

    /**
     * Add an entry; returns batches that became complete as a result
     * (usually none, at most two when a byte overflow and the count limit
     * coincide).
     *
     * @param  array<string, mixed>  $entry
     * @return array<int, array<int, array<string, mixed>>>
     */
    public function add(array $entry): array
    {
        $complete = [];
        $size = self::sizeOf($entry);

        if ($this->entries !== [] && $this->bytes + $size > $this->maxBytes) {
            $complete[] = $this->drain();
        }

        $this->entries[] = $entry;
        $this->bytes += $size;

        if (count($this->entries) >= $this->maxEntries) {
            $complete[] = $this->drain();
        }

        return $complete;
    }

    /**
     * Take whatever is accumulated and reset.
     *
     * @return array<int, array<string, mixed>>
     */
    public function drain(): array
    {
        $entries = $this->entries;
        $this->entries = [];
        $this->bytes = 0;

        return $entries;
    }

    public function count(): int
    {
        return count($this->entries);
    }

    /**
     * @param  array<string, mixed>  $entry
     */
    private static function sizeOf(array $entry): int
    {
        $json = json_encode($entry, JSON_INVALID_UTF8_SUBSTITUTE | JSON_PARTIAL_OUTPUT_ON_ERROR);

        // +1 for the comma separating entries in the `logs` array.
        return ($json === false ? 0 : strlen($json)) + 1;
    }
}
