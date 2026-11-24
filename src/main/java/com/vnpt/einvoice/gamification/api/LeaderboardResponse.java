package com.vnpt.einvoice.gamification.api;

import java.util.List;

public record LeaderboardResponse(
        int limit,
        List<LeaderboardEntry> entries
) {
}
