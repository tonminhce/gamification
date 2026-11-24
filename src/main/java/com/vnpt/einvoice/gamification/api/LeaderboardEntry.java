package com.vnpt.einvoice.gamification.api;

public record LeaderboardEntry(
        long rank,
        String companyId,
        long points
) {
}
