package com.vnpt.einvoice.gamification.event;

import java.time.Instant;

public record CompanyRewardDelta(
        String deltaId,
        String companyId,
        long deltaPoints,
        Instant windowStart,
        Instant windowEnd,
        int schemaVersion
) {
    public static final String TYPE = "COMPANY_REWARD_DELTA";
    public static final int SCHEMA_VERSION = 1;
}
