package com.vnpt.einvoice.gamification.api;

public record InvoiceBatchAcceptedResponse(
        int acceptedCount,
        String status
) {
}
