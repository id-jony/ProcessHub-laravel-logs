<?php

namespace ProcessHub\Logs\Exceptions;

use RuntimeException;

/**
 * Raised by SendLogBatchJob when ProcessHub can't accept a batch.
 *
 * A dedicated type lets ProcessHubHandler recognise (and drop) reports about
 * its own delivery failures — otherwise every failed delivery would be
 * logged back into the same channel and feed itself.
 */
final class DeliveryFailedException extends RuntimeException
{
    public static function status(int $status, string $body = ''): self
    {
        return new self(trim(sprintf(
            'ProcessHub ingest responded HTTP %d %s',
            $status,
            mb_substr($body, 0, 500),
        )));
    }

    public static function network(\Throwable $previous): self
    {
        return new self('ProcessHub ingest unreachable: ' . $previous->getMessage(), 0, $previous);
    }
}
