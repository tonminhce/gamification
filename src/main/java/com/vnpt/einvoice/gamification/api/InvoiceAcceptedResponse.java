package com.vnpt.einvoice.gamification.api;

public record InvoiceAcceptedResponse(
        String invoiceId,
        String companyId,
        String eventId,
        String status
) {
}
