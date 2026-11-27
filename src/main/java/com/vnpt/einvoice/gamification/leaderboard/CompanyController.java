package com.vnpt.einvoice.gamification.leaderboard;

import com.vnpt.einvoice.gamification.api.CompanySummaryResponse;
import com.vnpt.einvoice.gamification.api.DiscountResponse;
import com.vnpt.einvoice.gamification.api.PointsResponse;
import com.vnpt.einvoice.gamification.api.RankResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class CompanyController {

    private final LeaderboardService leaderboardService;

    public CompanyController(LeaderboardService leaderboardService) {
        this.leaderboardService = leaderboardService;
    }

    @GetMapping("/api/companies/{companyId}/points")
    PointsResponse points(@PathVariable String companyId) {
        return leaderboardService.points(companyId);
    }

    @GetMapping("/api/companies/{companyId}/rank")
    RankResponse rank(@PathVariable String companyId) {
        return leaderboardService.rank(companyId);
    }

    @GetMapping("/api/companies/{companyId}/discount")
    DiscountResponse discount(@PathVariable String companyId) {
        return leaderboardService.discount(companyId);
    }

    @GetMapping("/api/companies/{companyId}/summary")
    CompanySummaryResponse summary(@PathVariable String companyId) {
        return leaderboardService.summary(companyId);
    }
}
