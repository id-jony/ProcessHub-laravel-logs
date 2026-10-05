<?php

namespace ProcessHub\Logs\Support;

/**
 * Append-only JSON-lines file where undeliverable batches are parked until
 * `processhub:flush-fallback` re-ingests them.
 *
 * Line format: {"failed_at": ISO-8601, "reason": string, "entries": [...]}.
 *
 * Дозапись и переименование живого файла в снапшот командой flush-fallback
 * идут под общей блокировкой `<path>.write.lock` (см. `locked()`): писатель
 * открывает, пишет и закрывает файл, держа её, — после rename в снапшот
 * больше никто не пишет.
 */
final class FallbackFile
{
    /**
     * Never throws — called from failure paths (job `failed()`, queue push
     * errors, handler destructors) where the container may be half torn down.
     *
     * @param  array<int, array<string, mixed>>  $entries
     */
    public static function append(array $entries, string $reason): void
    {
        if ($entries === []) {
            return;
        }

        try {
            $path = config('processhub.fallback_path');
            if (! $path) {
                return;
            }
            $line = self::line($entries, $reason);
            self::locked($path, static fn () => self::write($path, $line));
        } catch (\Throwable) {
            // Nowhere left to report to.
        }
    }

    /**
     * @param  array<int, array<string, mixed>>  $entries
     * @param  array<string, mixed>  $extra  дополнительные поля строки (например, `raw`)
     */
    public static function line(array $entries, string $reason, array $extra = []): string
    {
        return json_encode([
            'failed_at' => date(DATE_ATOM),
            'reason' => $reason,
            'entries' => $entries,
            ...$extra,
        ], JSON_INVALID_UTF8_SUBSTITUTE | JSON_PARTIAL_OUTPUT_ON_ERROR) . "\n";
    }

    /**
     * Выполнить `$callback` под блокировкой, общей для `append()` и захвата
     * живого файла командой flush-fallback. Если lock-файл не открылся,
     * callback всё равно выполняется: потерять запись хуже редкой гонки.
     *
     * @template T
     *
     * @param  callable(): T  $callback
     * @return T
     */
    public static function locked(string $path, callable $callback): mixed
    {
        $lock = @fopen($path . '.write.lock', 'c');
        if ($lock === false) {
            return $callback();
        }

        try {
            flock($lock, LOCK_EX);

            return $callback();
        } finally {
            flock($lock, LOCK_UN);
            fclose($lock);
        }
    }

    private static function write(string $path, string $line): void
    {
        $fh = @fopen($path, 'a+');
        if ($fh === false) {
            return;
        }

        try {
            flock($fh, LOCK_EX);
            // Хвост без "\n" остаётся от оборванной записи: без перевода строки
            // он склеился бы с новой строкой и испортил бы и её.
            $size = fstat($fh)['size'] ?? 0;
            if ($size > 0 && fseek($fh, -1, SEEK_END) === 0 && fread($fh, 1) !== "\n") {
                $line = "\n" . $line;
            }
            fwrite($fh, $line);
            fflush($fh);
        } finally {
            flock($fh, LOCK_UN);
            fclose($fh);
        }
    }
}
