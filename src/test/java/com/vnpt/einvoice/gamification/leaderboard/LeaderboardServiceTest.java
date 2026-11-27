package com.vnpt.einvoice.gamification.leaderboard;

import com.vnpt.einvoice.gamification.api.CompanySummaryResponse;
import com.vnpt.einvoice.gamification.api.LeaderboardResponse;
import com.vnpt.einvoice.gamification.api.PointsResponse;
import com.vnpt.einvoice.gamification.api.RankResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.DefaultTypedTuple;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LeaderboardServiceTest {

    private static final String KEY = "einv:leaderboard";

    @Mock
    StringRedisTemplate redisTemplate;

    @Mock
    ZSetOperations<String, String> zSetOperations;

    LeaderboardService service;

    @BeforeEach
    void setUp() {
        lenient().when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        service = new LeaderboardService(redisTemplate, new DiscountService(), KEY);
    }

    @Test
    void unknownCompanyHasZeroPointsAndNoRank() {
        when(zSetOperations.score(KEY, "UNKNOWN")).thenReturn(null);
        when(redisTemplate.execute(any(RedisScript.class), eq(List.of(KEY)), eq("UNKNOWN"))).thenReturn(null);

        assertEquals(new PointsResponse("UNKNOWN", 0), service.points("UNKNOWN"));
        assertEquals(new RankResponse("UNKNOWN", null), service.rank("UNKNOWN"));
        assertEquals(new CompanySummaryResponse("UNKNOWN", 0L, null, 0), service.summary("UNKNOWN"));
    }

    @Test
    void companyRankUsesTheSameCompetitionRankForTies() {
        when(redisTemplate.execute(any(RedisScript.class), eq(List.of(KEY)), eq("COMPANY-A")))
                .thenReturn(List.of("100", "0"));
        when(redisTemplate.execute(any(RedisScript.class), eq(List.of(KEY)), eq("COMPANY-B")))
                .thenReturn(List.of("100", "0"));

        assertEquals(new RankResponse("COMPANY-A", 1L), service.rank("COMPANY-A"));
        assertEquals(new RankResponse("COMPANY-B", 1L), service.rank("COMPANY-B"));
        assertEquals(new CompanySummaryResponse("COMPANY-A", 100L, 1L, 5), service.summary("COMPANY-A"));
    }

    @Test
    void topCalculatesCompetitionRankForTiedScores() {
        Set<ZSetOperations.TypedTuple<String>> tuples = new LinkedHashSet<>();
        tuples.add(new DefaultTypedTuple<>("COMPANY-B", 100.0));
        tuples.add(new DefaultTypedTuple<>("COMPANY-A", 100.0));

        when(zSetOperations.reverseRangeWithScores(KEY, 0, 1)).thenReturn(tuples);

        LeaderboardResponse response = service.top(2);

        assertEquals(2, response.limit());
        assertEquals(1L, response.entries().get(0).rank());
        assertEquals(1L, response.entries().get(1).rank());
        assertEquals(100L, response.entries().get(0).points());
    }

    @Test
    void topMicroCacheCoalescesRedisReadsWithinTtlWindow() {
        Set<ZSetOperations.TypedTuple<String>> tuples = new LinkedHashSet<>();
        tuples.add(new DefaultTypedTuple<>("COMPANY-A", 100.0));
        when(zSetOperations.reverseRangeWithScores(KEY, 0, 9)).thenReturn(tuples);

        LeaderboardService cachedService = new LeaderboardService(redisTemplate, new DiscountService(), KEY, 500L);
        cachedService.top(10);
        cachedService.top(10);
        cachedService.top(10);

        verify(zSetOperations, times(1)).reverseRangeWithScores(KEY, 0, 9);
    }
}
