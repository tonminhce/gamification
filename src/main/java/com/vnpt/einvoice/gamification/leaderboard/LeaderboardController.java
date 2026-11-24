package com.vnpt.einvoice.gamification.leaderboard;

import com.vnpt.einvoice.gamification.api.LeaderboardResponse;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Validated
@RestController
public class LeaderboardController {

    private final LeaderboardService leaderboardService;

    public LeaderboardController(LeaderboardService leaderboardService) {
        this.leaderboardService = leaderboardService;
    }

    @GetMapping("/api/leaderboard/top")
    LeaderboardResponse top(
            @RequestParam(defaultValue = "10")
            @Min(1) @Max(100) int limit
    ) {
        return leaderboardService.top(limit);
    }
}
