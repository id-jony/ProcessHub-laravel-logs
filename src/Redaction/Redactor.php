<?php

namespace ProcessHub\Logs\Redaction;

/**
 * Defense-in-depth PII redaction before sending a log batch to ProcessHub.
 *
 * Two layers (matching the server-side logic, so both defences agree):
 *   1. Sensitive-key match (password / token / secret / …) → whole value
 *      replaced with [REDACTED].
 *   2. Regex patterns on string values (email, JWT, Bearer, card).
 *
 * Recursion is bounded at 10 levels to survive pathological inputs;
 * circular structures are detected with a spl_object_hash map.
 */
class Redactor
{
    protected const MAX_DEPTH = 10;

    /**
     * @return mixed   same shape, with sensitive fragments masked.
     */
    public static function redact(mixed $value): mixed
    {
        return self::walk($value, 0, []);
    }

    protected static function walk(mixed $value, int $depth, array $seen): mixed
    {
        if ($depth > self::MAX_DEPTH) return '[MAX_DEPTH]';

        if (is_string($value)) {
            return self::maskString($value);
        }
        if (is_array($value)) {
            $out = [];
            foreach ($value as $key => $v) {
                if (is_string($key) && self::isSensitiveKey($key)) {
                    $out[$key] = '[REDACTED]';
                    continue;
                }
                $out[$key] = self::walk($v, $depth + 1, $seen);
            }
            return $out;
        }
        if (is_object($value)) {
            $hash = spl_object_hash($value);
            if (isset($seen[$hash])) return '[CIRCULAR]';
            $seen[$hash] = true;
            // Convert public-properties only — we never want to leak reflection-private state.
            $arr = get_object_vars($value);
            return self::walk($arr, $depth + 1, $seen);
        }
        return $value;
    }

    protected static function isSensitiveKey(string $key): bool
    {
        $keys = config('processhub.redact.keys', []);
        $keyLower = strtolower($key);
        foreach ($keys as $s) {
            if (str_contains($keyLower, strtolower((string) $s))) return true;
        }
        return false;
    }

    protected static function maskString(string $s): string
    {
        $patterns = config('processhub.redact.patterns', []);
        foreach ($patterns as $rx => $replacement) {
            $s = (string) preg_replace($rx, $replacement, $s);
        }
        return $s;
    }
}
