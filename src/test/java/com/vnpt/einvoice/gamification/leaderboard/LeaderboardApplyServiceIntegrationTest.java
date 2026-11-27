package com.vnpt.einvoice.gamification.leaderboard;

import com.vnpt.einvoice.gamification.api.CompanySummaryResponse;
import com.vnpt.einvoice.gamification.api.RankResponse;
import com.vnpt.einvoice.gamification.event.CompanyRewardDelta;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class LeaderboardApplyServiceIntegrationTest {

    private static final String LEADERBOARD = "einv:leaderboard";
    private static final String PROCESSED_DELTAS = "einv:processed-deltas";
    private static final long TTL_SECONDS = 604_800L;

    @Container
    static GenericContainer<?> redis = new GenericContainer<>("redis:7.4-alpine")
            .withExposedPorts(6379);

    private LettuceConnectionFactory connectionFactory;
    private StringRedisTemplate redisTemplate;
    private LeaderboardApplyService service;
    private LeaderboardService leaderboardService;

    @BeforeEach
    void setUp() {
        connectionFactory = new LettuceConnectionFactory(new RedisStandaloneConfiguration(
                redis.getHost(),
                redis.getMappedPort(6379)
        ));
        connectionFactory.afterPropertiesSet();
        redisTemplate = new StringRedisTemplate(connectionFactory);
        redisTemplate.afterPropertiesSet();
        redisTemplate.execute((RedisCallback<Object>) connection -> {
            connection.serverCommands().flushDb();
            return null;
        });
        service = new LeaderboardApplyService(redisTemplate, LEADERBOARD, PROCESSED_DELTAS, TTL_SECONDS);
        leaderboardService = new LeaderboardService(redisTemplate, new DiscountService(), LEADERBOARD);
    }

    @AfterEach
    void tearDown() {
        connectionFactory.destroy();
    }

    @Test
    void appliesDeltasOnceWithBucketedTtlSet() {
        Instant windowStart = Instant.parse("2026-11-28T01:00:00Z");
        long epochDay = windowStart.getEpochSecond() / 86_400L;
        List<CompanyRewardDelta> deltas = List.of(
                delta("D-1", "COMPANY-A", 10, windowStart),
                delta("D-2", "COMPANY-A", 30, windowStart),
                delta("D-3", "COMPANY-B", 20, windowStart)
        );

        assertEquals(3, service.applyDeltas(deltas, "partition-0"));
        assertEquals(40.0, redisTemplate.opsForZSet().score(LEADERBOARD, "COMPANY-A"));
        assertEquals(20.0, redisTemplate.opsForZSet().score(LEADERBOARD, "COMPANY-B"));

        Long ttl = redisTemplate.getExpire(PROCESSED_DELTAS + ":" + epochDay);
        assertTrue(ttl != null && ttl > 0);

        assertEquals(0, service.applyDeltas(deltas, "partition-0"));
        assertEquals(40.0, redisTemplate.opsForZSet().score(LEADERBOARD, "COMPANY-A"));
    }

    @Test
    void skipsCheckpointedOffsetsAndHandlesControlRecordGaps() {
        Instant now = Instant.parse("2026-11-28T01:00:00Z");
        List<ConsumerRecord<String, CompanyRewardDelta>> firstPoll = List.of(
                record(delta("D-10", "COMPANY-A", 10, now), 10),
                // Offset 11 intentionally absent (simulating Kafka EOS control/abort gap)
                record(delta("D-12", "COMPANY-B", 20, now), 12)
        );
        assertEquals(2, service.applyPartitionBatch(firstPoll, "company-reward-deltas:0"));
        assertEquals("12", redisTemplate.opsForValue().get("einv:projector-checkpoint:company-reward-deltas:0"));

        // Re-deliver offset <= 12 even with a distinct deltaId; checkpoint blocks it.
        List<ConsumerRecord<String, CompanyRewardDelta>> redelivery = List.of(
                record(delta("D-11-NEW", "COMPANY-A", 999, now), 11),
                record(delta("D-12-NEW", "COMPANY-B", 999, now), 12)
        );
        assertEquals(0, service.applyPartitionBatch(redelivery, "company-reward-deltas:0"));
        assertEquals(10.0, redisTemplate.opsForZSet().score(LEADERBOARD, "COMPANY-A"));
        assertEquals(20.0, redisTemplate.opsForZSet().score(LEADERBOARD, "COMPANY-B"));

        // Offset 15 advances checkpoint and applies cleanly.
        assertEquals(1, service.applyPartitionBatch(
                List.of(record(delta("D-15", "COMPANY-A", 50, now), 15)),
                "company-reward-deltas:0"
        ));
        assertEquals(60.0, redisTemplate.opsForZSet().score(LEADERBOARD, "COMPANY-A"));
        assertEquals("15", redisTemplate.opsForValue().get("einv:projector-checkpoint:company-reward-deltas:0"));
    }

    @Test
    void recoversPendingMarkerAndRejectsPartialMismatch() {
        Instant now = Instant.parse("2026-11-28T01:00:00Z");
        long epochDay = now.getEpochSecond() / 86_400L;
        String bucketKey = PROCESSED_DELTAS + ":" + epochDay;

        // Seed PENDING marker as if a crash occurred before applyBatch ran.
        String pendingJson = """
                {"state":"PENDING","buckets":[{"key":"%s","ids":["D-CRASH-1"]}],"companies":[{"companyId":"COMPANY-R","deltaPoints":40,"scoreBefore":0}]}
                """.formatted(bucketKey).trim();
        redisTemplate.opsForValue().set("einv:delta-batch:company-reward-deltas:1", pendingJson);

        assertEquals(0, service.applyDeltas(List.of(delta("D-CRASH-1", "COMPANY-R", 40, now)), "company-reward-deltas:1"));
        assertEquals(40.0, redisTemplate.opsForZSet().score(LEADERBOARD, "COMPANY-R"));
        assertNull(redisTemplate.opsForValue().get("einv:delta-batch:company-reward-deltas:1"));

        // Now seed a conflicting PENDING marker whose scoreBefore matches neither current nor expected.
        String badMarker = """
                {"state":"PENDING","buckets":[{"key":"%s","ids":["D-BAD"]}],"companies":[{"companyId":"COMPANY-R","deltaPoints":10,"scoreBefore":5}]}
                """.formatted(bucketKey).trim();
        redisTemplate.opsForValue().set("einv:delta-batch:company-reward-deltas:1", badMarker);

        assertThrows(
                DataAccessException.class,
                () -> service.applyDeltas(List.of(delta("D-NEW", "COMPANY-R", 10, now)), "company-reward-deltas:1")
        );
    }

    @Test
    void computesCompetitionRankAndSummaryAtomically() {
        Instant now = Instant.parse("2026-11-28T01:00:00Z");
        service.applyDeltas(List.of(
                delta("D-1", "COMP-1", 1000, now),
                delta("D-2", "COMP-2", 500, now),
                delta("D-3", "COMP-3", 500, now),
                delta("D-4", "COMP-4", 100, now)
        ), "p0");

        assertEquals(new RankResponse("COMP-1", 1L), leaderboardService.rank("COMP-1"));
        assertEquals(new RankResponse("COMP-2", 2L), leaderboardService.rank("COMP-2"));
        assertEquals(new RankResponse("COMP-3", 2L), leaderboardService.rank("COMP-3"));
        assertEquals(new RankResponse("COMP-4", 4L), leaderboardService.rank("COMP-4"));
        assertEquals(new RankResponse("UNKNOWN", null), leaderboardService.rank("UNKNOWN"));

        assertEquals(
                new CompanySummaryResponse("COMP-1", 1000L, 1L, 15),
                leaderboardService.summary("COMP-1")
        );
        assertEquals(
                new CompanySummaryResponse("UNKNOWN", 0L, null, 0),
                leaderboardService.summary("UNKNOWN")
        );
    }

    private CompanyRewardDelta delta(String deltaId, String companyId, long points, Instant windowStart) {
        return new CompanyRewardDelta(
                deltaId,
                companyId,
                points,
                windowStart,
                windowStart.plusMillis(500),
                CompanyRewardDelta.SCHEMA_VERSION
        );
    }

    private ConsumerRecord<String, CompanyRewardDelta> record(CompanyRewardDelta delta, long offset) {
        return new ConsumerRecord<>("company-reward-deltas", 0, offset, delta.companyId(), delta);
    }
}
