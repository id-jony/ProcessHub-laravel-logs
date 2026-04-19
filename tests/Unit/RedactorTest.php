<?php

namespace ProcessHub\Logs\Tests\Unit;

use ProcessHub\Logs\Redaction\Redactor;
use ProcessHub\Logs\Tests\TestCase;

class RedactorTest extends TestCase
{
    public function test_replaces_sensitive_keys_at_any_depth(): void
    {
        $out = Redactor::redact([
            'name' => 'alice',
            'password' => 'hunter2',
            'nested' => ['api_key' => 'k-123', 'ok' => 'value'],
        ]);

        $this->assertSame('alice', $out['name']);
        $this->assertSame('[REDACTED]', $out['password']);
        $this->assertSame('[REDACTED]', $out['nested']['api_key']);
        $this->assertSame('value', $out['nested']['ok']);
    }

    public function test_case_insensitive_substring_match(): void
    {
        $out = Redactor::redact([
            'Authorization' => 'Bearer xyz',
            'userToken' => 'abc',
            'REFRESH_TOKEN_X' => 'd',
        ]);
        $this->assertSame('[REDACTED]', $out['Authorization']);
        $this->assertSame('[REDACTED]', $out['userToken']);
        $this->assertSame('[REDACTED]', $out['REFRESH_TOKEN_X']);
    }

    public function test_rewrites_email_in_string_values(): void
    {
        $this->assertSame(
            'ping [EMAIL] urgently',
            Redactor::redact('ping alice@example.com urgently'),
        );
    }

    public function test_rewrites_jwt_in_string_values(): void
    {
        $jwt = 'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTYifQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c';
        $this->assertSame('token=[JWT]', Redactor::redact("token={$jwt}"));
    }

    public function test_rewrites_bearer_tokens_in_strings(): void
    {
        $this->assertSame(
            'Authorization: Bearer [REDACTED]',
            Redactor::redact('Authorization: Bearer abc.def.ghijklmnop12345'),
        );
    }

    public function test_rewrites_credit_cards(): void
    {
        $this->assertSame(
            '[CARD] expires 12/28',
            Redactor::redact('4111 1111 1111 1111 expires 12/28'),
        );
    }

    public function test_circular_reference_is_safe(): void
    {
        $a = new \stdClass();
        $a->name = 'cycle';
        $a->self = $a;

        $out = Redactor::redact($a);
        $this->assertSame('cycle', $out['name']);
        $this->assertSame('[CIRCULAR]', $out['self']);
    }

    public function test_bounds_recursion_depth(): void
    {
        // 15 levels deep — past MAX_DEPTH (10).
        $leaf = ['value' => 'x'];
        for ($i = 0; $i < 15; $i++) {
            $leaf = ['nested' => $leaf];
        }
        $out = Redactor::redact($leaf);

        $cursor = $out;
        $hit = false;
        for ($i = 0; $i < 20; $i++) {
            if ($cursor === '[MAX_DEPTH]') {
                $hit = true;
                break;
            }
            if (! is_array($cursor) || ! isset($cursor['nested'])) break;
            $cursor = $cursor['nested'];
        }
        $this->assertTrue($hit);
    }

    public function test_does_not_mutate_input(): void
    {
        $input = ['password' => 'x', 'name' => 'a'];
        Redactor::redact($input);
        $this->assertSame(['password' => 'x', 'name' => 'a'], $input);
    }
}
