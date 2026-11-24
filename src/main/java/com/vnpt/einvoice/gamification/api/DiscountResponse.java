package com.vnpt.einvoice.gamification.api;

public record DiscountResponse(
        String companyId,
        long points,
        int discountPercent
) {
}
