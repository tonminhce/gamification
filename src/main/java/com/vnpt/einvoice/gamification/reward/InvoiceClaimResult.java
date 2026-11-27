package com.vnpt.einvoice.gamification.reward;

import java.util.List;

public record InvoiceClaimResult(
        String batchUid,
        boolean recovered,
        List<ClaimedCompanyDelta> companies
) {
}
