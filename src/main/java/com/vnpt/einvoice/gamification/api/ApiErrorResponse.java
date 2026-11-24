package com.vnpt.einvoice.gamification.api;

import java.time.Instant;

public record ApiErrorResponse(
        Instant timestamp,
        int status,
        String error,
        String message
) {
}
