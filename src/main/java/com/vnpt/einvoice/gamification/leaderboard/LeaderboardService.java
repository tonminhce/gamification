package com.vnpt.einvoice.gamification.leaderboard;

import com.vnpt.einvoice.gamification.api.CompanySummaryResponse;
import com.vnpt.einvoice.gamification.api.DiscountResponse;
import com.vnpt.einvoice.gamification.api.LeaderboardEntry;
import com.vnpt.einvoice.gamification.api.LeaderboardResponse;
import com.vnpt.einvoice.gamification.api.PointsResponse;
import com.vnpt.einvoice.gamification.api.RankResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class LeaderboardService {

    private static final int CACHED_TOP_LIMIT = 100;

    private final StringRedisTemplate redisTemplate;
    private final DiscountService discountService;
    private final String leaderboardKey;
    private final long topCacheTtlNanos;
    private final DefaultRedisScript<List<?>> rankAndScoreScript;
    private final TopCacheSlot topSnapshot = new TopCacheSlot();

    public LeaderboardService(
            StringRedisTemplate redisTemplate,
            DiscountService discountService,
            String leaderboardKey
    ) {
        this(redisTemplate, discountService, leaderboardKey, 0L);
    }

    @Autowired
    @SuppressWarnings({"unchecked", "rawtypes"})
    public LeaderboardService(
            StringRedisTemplate redisTemplate,
            DiscountService discountService,
            @Value("${app.leaderboard.key}") String leaderboardKey,
            @Value("${app.leaderboard.top-cache-ttl-ms:50}") long topCacheTtlMs
    ) {
        this.redisTemplate = redisTemplate;
        this.discountService = discountService;
        this.leaderboardKey = leaderboardKey;
        this.topCacheTtlNanos = Math.max(0L, topCacheTtlMs) * 1_000_000L;
        this.rankAndScoreScript = new DefaultRedisScript<>();
        this.rankAndScoreScript.setLocation(new ClassPathResource("scripts/rank_and_score.lua"));
        this.rankAndScoreScript.setResultType((Class<List<?>>) (Class) List.class);
    }

    public LeaderboardResponse top(int limit) {
        if (topCacheTtlNanos <= 0L || limit > CACHED_TOP_LIMIT) {
            return loadTop(limit);
        }

        long now = System.nanoTime();
        CachedTop current = topSnapshot.snapshot;
        if (current != null && (now - current.loadedAtNanos()) < topCacheTtlNanos) {
            return slice(current.response(), limit);
        }

        // Cold-start synchronization prevents a Redis stampede before the first snapshot exists.
        if (current == null) {
            synchronized (topSnapshot.initLock) {
                current = topSnapshot.snapshot;
                if (current == null) {
                    LeaderboardResponse fresh = loadTop(CACHED_TOP_LIMIT);
                    topSnapshot.snapshot = new CachedTop(fresh, System.nanoTime());
                    return slice(fresh, limit);
                }
                return slice(current.response(), limit);
            }
        }

        // Single-flight stale-while-revalidate: one caller refreshes while concurrent readers get the cached snapshot.
        if (topSnapshot.refreshing.compareAndSet(false, true)) {
            try {
                LeaderboardResponse fresh = loadTop(CACHED_TOP_LIMIT);
                topSnapshot.snapshot = new CachedTop(fresh, System.nanoTime());
                return slice(fresh, limit);
            } finally {
                topSnapshot.refreshing.set(false);
            }
        }
        return slice(current.response(), limit);
    }

    private LeaderboardResponse slice(LeaderboardResponse fullTop, int limit) {
        List<LeaderboardEntry> all = fullTop.entries();
        if (all.size() <= limit) {
            return fullTop.limit() == limit ? fullTop : new LeaderboardResponse(limit, all);
        }
        return new LeaderboardResponse(limit, all.subList(0, limit));
    }

    private LeaderboardResponse loadTop(int limit) {
        Set<ZSetOperations.TypedTuple<String>> tuples = redisTemplate.opsForZSet()
                .reverseRangeWithScores(leaderboardKey, 0, limit - 1);

        if (tuples == null || tuples.isEmpty()) {
            return new LeaderboardResponse(limit, List.of());
        }

        List<LeaderboardEntry> entries = new ArrayList<>(tuples.size());
        long position = 0;
        long rank = 0;
        double previousScore = 0.0;
        boolean hasPrevious = false;
        for (ZSetOperations.TypedTuple<String> tuple : tuples) {
            position++;
            Double rawScore = tuple.getScore();
            double score = rawScore != null ? rawScore : 0.0;
            if (!hasPrevious || score != previousScore) {
                rank = position;
                hasPrevious = true;
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
        ScoreAndRank result = fetchScoreAndRank(companyId);
        return new RankResponse(companyId, result != null ? result.rank() : null);
    }

    public DiscountResponse discount(String companyId) {
        long points = points(companyId).points();
        return new DiscountResponse(companyId, points, discountService.discountPercent(points));
    }

    public CompanySummaryResponse summary(String companyId) {
        ScoreAndRank result = fetchScoreAndRank(companyId);
        if (result == null) {
            return new CompanySummaryResponse(companyId, 0L, null, 0);
        }
        return new CompanySummaryResponse(
                companyId,
                result.points(),
                result.rank(),
                discountService.discountPercent(result.points())
        );
    }

    private ScoreAndRank fetchScoreAndRank(String companyId) {
        List<?> raw = redisTemplate.execute(rankAndScoreScript, List.of(leaderboardKey), companyId);
        if (raw == null || raw.size() < 2) {
            return null;
        }
        long points = (long) Double.parseDouble(raw.get(0).toString());
        long higherScores = Long.parseLong(raw.get(1).toString());
        return new ScoreAndRank(points, higherScores + 1);
    }

    private record ScoreAndRank(long points, long rank) {
    }

    private record CachedTop(LeaderboardResponse response, long loadedAtNanos) {
    }

    private static final class TopCacheSlot {
        volatile CachedTop snapshot;
        final AtomicBoolean refreshing = new AtomicBoolean(false);
        final Object initLock = new Object();
    }
}
