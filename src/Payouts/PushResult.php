<?php

namespace ProcessHub\Logs\Payouts;

/**
 * Return value of {@see IngestClient::push()}.
 *
 * `$ok = true` and a non-null `$watermark` mean ProcessHub accepted (at least
 * part of) the batch and the caller should advance its high-watermark. Any
 * 4xx that is not 429 sets `$ok = false` with a stringly-typed `$error`
 * code (`SOURCE_DISABLED`, `SOURCE_NOT_CONFIGURED`, `BAD_CONFIG`,
 * `PAYLOAD_TOO_LARGE`, `BAD_REQUEST`, `UNAUTHORIZED`, `TRANSPORT_ERROR`).
 *
 * It's a plain DTO on purpose — no behaviour, no factory methods. The
 * command branches on `$ok` and inspects `$error` for the human-readable
 * code; nothing else needs polymorphism here.
 *
 * @phpstan-type SkippedRow array{gatewayPaymentId: string, reason: string}
 */
final class PushResult
{
    /**
     * @param list<array{gatewayPaymentId: string, reason: string}> $skipped
     */
    public function __construct(
        public readonly bool $ok,
        public readonly int $accepted,
        public readonly array $skipped,
        public readonly ?string $watermark,
        public readonly ?string $error,
    ) {}

    public static function transportFailure(string $reason): self
    {
        return new self(false, 0, [], null, "TRANSPORT_ERROR:{$reason}");
    }
}
