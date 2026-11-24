package com.vnpt.einvoice.gamification.event;

import java.time.Instant;

public record InvoiceIssuedEvent(
        String eventId,
        String eventType,
        int schemaVersion,
        String invoiceId,
        String companyId,
        Instant occurredAt
) {
    public static final String TYPE = "INVOICE_ISSUED";
    public static final int SCHEMA_VERSION = 1;
}
