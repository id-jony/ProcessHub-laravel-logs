<?php

namespace ProcessHub\Logs\Tests\Unit\Payouts;

use ProcessHub\Logs\Payouts\Limits;
use ProcessHub\Logs\Tests\TestCase;

/**
 * Sanity-check: package constants must stay within the contract documented
 * in docs/16-payouts-module.md. Tightening these constants requires both
 * a code change and a docs update — the test is a guardrail against an
 * accidental edit drifting the package outside the protocol.
 */
class LimitsTest extends TestCase
{
    public function test_batch_size_within_server_limit(): void
    {
        $this->assertLessThanOrEqual(1000, Limits::BATCH_SIZE);
        $this->assertGreaterThan(0, Limits::BATCH_SIZE);
    }

    public function test_payload_cap_below_server_2mib(): void
    {
        $this->assertLessThan(2 * 1024 * 1024, Limits::MAX_PAYLOAD_BYTES);
        $this->assertGreaterThan(1_000_000, Limits::MAX_PAYLOAD_BYTES);
    }

    public function test_retry_budget_is_finite(): void
    {
        $this->assertGreaterThan(0, Limits::RETRY_MAX);
        $this->assertLessThanOrEqual(5, Limits::RETRY_MAX);
    }
}
