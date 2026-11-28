package com.vnpt.einvoice.gamification.leaderboard;

import com.vnpt.einvoice.gamification.api.CompanySummaryResponse;
import com.vnpt.einvoice.gamification.api.DiscountResponse;
import com.vnpt.einvoice.gamification.api.LeaderboardResponse;
import com.vnpt.einvoice.gamification.api.PointsResponse;
import com.vnpt.einvoice.gamification.api.RankResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.DefaultTypedTuple;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
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
        when(redisTemplate.execute(ArgumentMatchers.<RedisScript<List<?>>>any(), eq(List.of(KEY)), eq("UNKNOWN")))
                .thenReturn(null);

        assertEquals(new PointsResponse("UNKNOWN", 0), service.points("UNKNOWN"));
        assertEquals(new RankResponse("UNKNOWN", null), service.rank("UNKNOWN"));
        assertEquals(new DiscountResponse("UNKNOWN", 0L, 0), service.discount("UNKNOWN"));
        assertEquals(new CompanySummaryResponse("UNKNOWN", 0L, null, 0), service.summary("UNKNOWN"));
    }

    @Test
    void companyRankUsesTheSameCompetitionRankForTies() {
        doReturn(List.of("100", "0")).when(redisTemplate)
                .execute(ArgumentMatchers.<RedisScript<List<?>>>any(), eq(List.of(KEY)), eq("COMPANY-A"));
        doReturn(List.of("100", "0")).when(redisTemplate)
                .execute(ArgumentMatchers.<RedisScript<List<?>>>any(), eq(List.of(KEY)), eq("COMPANY-B"));

        assertEquals(new RankResponse("COMPANY-A", 1L), service.rank("COMPANY-A"));
        assertEquals(new RankResponse("COMPANY-B", 1L), service.rank("COMPANY-B"));
        assertEquals(new CompanySummaryResponse("COMPANY-A", 100L, 1L, 5), service.summary("COMPANY-A"));
    }

    @Test
    void discountReturnsTieredPercentFromCompanyPoints() {
        when(zSetOperations.score(KEY, "COMPANY-A")).thenReturn(100.0);
        when(zSetOperations.score(KEY, "COMPANY-C")).thenReturn(500.0);
        when(zSetOperations.score(KEY, "COMPANY-D")).thenReturn(1_000.0);

        assertEquals(new DiscountResponse("COMPANY-A", 100L, 5), service.discount("COMPANY-A"));
        assertEquals(new DiscountResponse("COMPANY-C", 500L, 10), service.discount("COMPANY-C"));
        assertEquals(new DiscountResponse("COMPANY-D", 1_000L, 15), service.discount("COMPANY-D"));
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
    void topMicroCacheCoalescesRedisReadsWithinTtlWindowAndSlicesByLimit() {
        Set<ZSetOperations.TypedTuple<String>> tuples = new LinkedHashSet<>();
        tuples.add(new DefaultTypedTuple<>("COMPANY-A", 300.0));
        tuples.add(new DefaultTypedTuple<>("COMPANY-B", 200.0));
        tuples.add(new DefaultTypedTuple<>("COMPANY-C", 100.0));
        when(zSetOperations.reverseRangeWithScores(KEY, 0, 99)).thenReturn(tuples);

        LeaderboardService cachedService = new LeaderboardService(redisTemplate, new DiscountService(), KEY, 500L);
        LeaderboardResponse top10 = cachedService.top(10);
        LeaderboardResponse top2 = cachedService.top(2);
        LeaderboardResponse top1 = cachedService.top(1);

        assertEquals(3, top10.entries().size());
        assertEquals(2, top2.limit());
        assertEquals(2, top2.entries().size());
        assertEquals(1, top1.limit());
        assertEquals("COMPANY-A", top1.entries().getFirst().companyId());
        verify(zSetOperations, times(1)).reverseRangeWithScores(KEY, 0, 99);
    }

    @Test
    void topMicroCacheCoalescesConcurrentColdStartReads() throws Exception {
        Set<ZSetOperations.TypedTuple<String>> tuples = new LinkedHashSet<>();
        tuples.add(new DefaultTypedTuple<>("COMPANY-A", 100.0));
        when(zSetOperations.reverseRangeWithScores(KEY, 0, 99)).thenAnswer(invocation -> {
            Thread.sleep(60);
            return tuples;
        });

        LeaderboardService cachedService = new LeaderboardService(redisTemplate, new DiscountService(), KEY, 500L);
        int threads = 4;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        try (var pool = Executors.newFixedThreadPool(threads)) {
            List<Future<LeaderboardResponse>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    barrier.await();
                    return cachedService.top(10);
                }));
            }
            for (Future<LeaderboardResponse> future : futures) {
                assertEquals(1, future.get().entries().size());
            }
        }

        verify(zSetOperations, times(1)).reverseRangeWithScores(KEY, 0, 99);
    }
}
