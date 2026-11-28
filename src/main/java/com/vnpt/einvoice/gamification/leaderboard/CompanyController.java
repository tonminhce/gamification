package com.vnpt.einvoice.gamification.leaderboard;

import com.vnpt.einvoice.gamification.api.CompanySummaryResponse;
import com.vnpt.einvoice.gamification.api.DiscountResponse;
import com.vnpt.einvoice.gamification.api.PointsResponse;
import com.vnpt.einvoice.gamification.api.RankResponse;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@Validated
@RestController
public class CompanyController {

    private final LeaderboardService leaderboardService;

    public CompanyController(LeaderboardService leaderboardService) {
        this.leaderboardService = leaderboardService;
    }

    @GetMapping("/api/companies/{companyId}/points")
    PointsResponse points(@PathVariable @NotBlank @Size(max = 64) String companyId) {
        return leaderboardService.points(companyId);
    }

    @GetMapping("/api/companies/{companyId}/rank")
    RankResponse rank(@PathVariable @NotBlank @Size(max = 64) String companyId) {
        return leaderboardService.rank(companyId);
    }

    @GetMapping("/api/companies/{companyId}/discount")
    DiscountResponse discount(@PathVariable @NotBlank @Size(max = 64) String companyId) {
        return leaderboardService.discount(companyId);
    }

    @GetMapping("/api/companies/{companyId}/summary")
    CompanySummaryResponse summary(@PathVariable @NotBlank @Size(max = 64) String companyId) {
        return leaderboardService.summary(companyId);
    }
}
