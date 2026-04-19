<?php

namespace ProcessHub\Logs\Middleware;

use Closure;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Log;
use Symfony\Component\HttpFoundation\Response;

/**
 * Ensures every incoming request has an X-Request-Id and that the id is
 * shared by every Log::* call made within the request.
 *
 * Behaviour:
 *   - If a client sent X-Request-Id → reuse it (trace propagation).
 *   - Otherwise generate a UUID.
 *   - Monolog context is populated so ProcessHubHandler picks it up and
 *     writes to ApplicationLog.requestId — enables the cross-log pivot.
 *   - Response echoes the same header back, useful for clients to trace.
 */
class CorrelateRequestId
{
    public const HEADER = 'X-Request-Id';

    public function handle(Request $request, Closure $next): Response
    {
        $rid = $request->headers->get(self::HEADER);
        if (! $this->isValid($rid)) {
            $rid = $this->generate();
        }

        // Attach to Monolog's shared context — inherited by every Log::*.
        Log::shareContext(['request_id' => $rid]);

        $request->headers->set(self::HEADER, $rid);
        $request->attributes->set('processhub.request_id', $rid);

        /** @var Response $response */
        $response = $next($request);
        $response->headers->set(self::HEADER, $rid);
        return $response;
    }

    protected function isValid(?string $raw): bool
    {
        if ($raw === null || $raw === '') return false;
        if (strlen($raw) > 128) return false;
        // Mirror ProcessHub's sanitiser — rejects CRLF (log injection),
        // control chars, and unicode spoofing. See /src/lib/request-id.ts
        // in the ProcessHub repo.
        return (bool) preg_match('/^[A-Za-z0-9._:-]{1,128}$/', $raw);
    }

    protected function generate(): string
    {
        // UUID v4 without requiring the ramsey/uuid dep.
        $data = random_bytes(16);
        $data[6] = chr(ord($data[6]) & 0x0f | 0x40);
        $data[8] = chr(ord($data[8]) & 0x3f | 0x80);
        return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($data), 4));
    }
}
