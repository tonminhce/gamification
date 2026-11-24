package com.vnpt.einvoice.gamification.leaderboard;

import com.vnpt.einvoice.gamification.api.DiscountResponse;
import com.vnpt.einvoice.gamification.api.LeaderboardEntry;
import com.vnpt.einvoice.gamification.api.LeaderboardResponse;
import com.vnpt.einvoice.gamification.api.PointsResponse;
import com.vnpt.einvoice.gamification.api.RankResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Service
public class LeaderboardService {

    private final StringRedisTemplate redisTemplate;
    private final DiscountService discountService;
    private final String leaderboardKey;

    public LeaderboardService(
            StringRedisTemplate redisTemplate,
            DiscountService discountService,
            @Value("${app.leaderboard.key}") String leaderboardKey
    ) {
        this.redisTemplate = redisTemplate;
        this.discountService = discountService;
        this.leaderboardKey = leaderboardKey;
    }

    public LeaderboardResponse top(int limit) {
        Set<ZSetOperations.TypedTuple<String>> tuples = redisTemplate.opsForZSet()
                .reverseRangeWithScores(leaderboardKey, 0, limit - 1);

        if (tuples == null || tuples.isEmpty()) {
            return new LeaderboardResponse(limit, List.of());
        }

        List<LeaderboardEntry> entries = new ArrayList<>(tuples.size());
        long position = 0;
        long rank = 0;
        Double previousScore = null;
        for (ZSetOperations.TypedTuple<String> tuple : tuples) {
            position++;
            double score = tuple.getScore();
            if (previousScore == null || score != previousScore) {
                rank = position;
            }
            entries.add(new LeaderboardEntry(rank, tuple.getValue(), (long) score));
            previousScore = score;
        }
        return new LeaderboardResponse(limit, entries);
    }

    public PointsResponse points(String companyId) {
        Double score = redisTemplate.opsForZSet().score(leaderboardKey, companyId);
        return new PointsResponse(companyId, score == null ? 0L : score.longValue());
    }

    public RankResponse rank(String companyId) {
        Double score = redisTemplate.opsForZSet().score(leaderboardKey, companyId);
        if (score == null) {
            return new RankResponse(companyId, null);
        }
        Long higherScores = redisTemplate.opsForZSet().count(
                leaderboardKey,
                Math.nextUp(score),
                Double.POSITIVE_INFINITY
        );
        if (higherScores == null) {
            throw new IllegalStateException("Redis returned no rank count");
        }
        return new RankResponse(companyId, higherScores + 1);
    }

    public DiscountResponse discount(String companyId) {
        long points = points(companyId).points();
        return new DiscountResponse(companyId, points, discountService.discountPercent(points));
    }
}
