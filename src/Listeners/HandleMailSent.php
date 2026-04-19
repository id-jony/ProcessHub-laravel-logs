<?php

namespace ProcessHub\Logs\Listeners;

use Illuminate\Mail\Events\MessageSent;
use Illuminate\Support\Facades\Log;

/**
 * Forwards every sent email as an INFO log with structured context so
 * ProcessHub renders "mail delivered" entries with to/from/subject chips
 * and can correlate with any related task/ticket by requestId.
 *
 * Recipient emails are automatically masked by the Redactor — ProcessHub
 * sees `[EMAIL]` markers unless a per-org setting turns redaction off.
 */
class HandleMailSent
{
    public function handle(MessageSent $event): void
    {
        $message = $event->message;
        $to = array_values(array_map(
            fn ($addr) => $addr->getAddress(),
            $message->getTo(),
        ));
        $from = array_values(array_map(
            fn ($addr) => $addr->getAddress(),
            $message->getFrom(),
        ));

        Log::channel('processhub')->info('Mail sent', [
            'type' => 'mail',
            'subject' => $message->getSubject(),
            'to' => $to,
            'from' => $from[0] ?? null,
            'messageId' => method_exists($message, 'getMessageId')
                ? $message->getMessageId()
                : null,
            'data' => $event->data ?? [],
        ]);
    }
}
