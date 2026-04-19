<?php

namespace ProcessHub\Logs\Tests\Unit;

use Illuminate\Http\Request;
use Illuminate\Http\Response;
use ProcessHub\Logs\Middleware\CorrelateRequestId;
use ProcessHub\Logs\Tests\TestCase;

class CorrelateRequestIdTest extends TestCase
{
    public function test_generates_uuid_when_header_absent(): void
    {
        $request = Request::create('/test');
        $middleware = new CorrelateRequestId();
        $response = $middleware->handle($request, fn () => new Response('ok'));

        $rid = $response->headers->get('X-Request-Id');
        $this->assertNotEmpty($rid);
        $this->assertMatchesRegularExpression(
            '/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/',
            $rid,
        );
    }

    public function test_reuses_valid_incoming_header(): void
    {
        $request = Request::create('/test');
        $request->headers->set('X-Request-Id', 'req-abc-123');

        $middleware = new CorrelateRequestId();
        $response = $middleware->handle($request, fn () => new Response('ok'));

        $this->assertSame('req-abc-123', $response->headers->get('X-Request-Id'));
    }

    public function test_rejects_oversized_header_and_regenerates(): void
    {
        $request = Request::create('/test');
        $request->headers->set('X-Request-Id', str_repeat('a', 200));

        $middleware = new CorrelateRequestId();
        $response = $middleware->handle($request, fn () => new Response('ok'));

        $rid = $response->headers->get('X-Request-Id');
        $this->assertLessThanOrEqual(128, strlen($rid));
        $this->assertNotSame(str_repeat('a', 200), $rid);
    }

    public function test_rejects_crlf_injection_and_regenerates(): void
    {
        $request = Request::create('/test');
        // Symfony HeaderBag strips raw CRLF itself; test the next-worst case:
        // spaces + slashes, which the regex still rejects.
        $request->headers->set('X-Request-Id', 'abc def/injected');

        $middleware = new CorrelateRequestId();
        $response = $middleware->handle($request, fn () => new Response('ok'));

        $rid = $response->headers->get('X-Request-Id');
        $this->assertStringNotContainsString(' ', $rid);
        $this->assertStringNotContainsString('/', $rid);
    }

    public function test_request_attribute_is_set(): void
    {
        $request = Request::create('/test');
        $capturedRid = null;
        $middleware = new CorrelateRequestId();
        $middleware->handle($request, function (Request $r) use (&$capturedRid) {
            $capturedRid = $r->attributes->get('processhub.request_id');
            return new Response('ok');
        });
        $this->assertNotEmpty($capturedRid);
    }
}
