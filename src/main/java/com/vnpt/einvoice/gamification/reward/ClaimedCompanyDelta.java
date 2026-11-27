package com.vnpt.einvoice.gamification.reward;

public record ClaimedCompanyDelta(
        String companyId,
        long deltaPoints
) {
}
