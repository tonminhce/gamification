package com.vnpt.einvoice.gamification.api;

public record CompanySummaryResponse(
        String companyId,
        long points,
        Long rank,
        int discountPercent
) {
}
