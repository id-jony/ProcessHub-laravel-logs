<?php

namespace ProcessHub\Logs\Exceptions;

use RuntimeException;

/**
 * A batch SendLogBatchJob had to park, but the fallback file couldn't take
 * it — the job fails with this exception and the batch stays in failed_jobs.
 *
 * A dedicated type lets the package recognise (and drop) reports about its
 * own delivery failures — otherwise they would be logged back into the same
 * channel and feed themselves.
 */
final class DeliveryFailedException extends RuntimeException
{
    public static function unparked(string $reason): self
    {
        return new self('ProcessHub log batch could not be written to the fallback file: ' . $reason);
    }
}
