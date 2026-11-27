package com.vnpt.einvoice.gamification.leaderboard;

import com.vnpt.einvoice.gamification.event.CompanyRewardDelta;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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

@Testcontainers
class LeaderboardApplyServiceIntegrationTest {

    private static final String LEADERBOARD = "einv:leaderboard";
    private static final String PROCESSED_DELTAS = "einv:processed-deltas";

    @Container
    static GenericContainer<?> redis = new GenericContainer<>("redis:7.4-alpine")
            .withExposedPorts(6379);

    private LettuceConnectionFactory connectionFactory;
    private StringRedisTemplate redisTemplate;
    private LeaderboardApplyService service;

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
            connection.flushDb();
            return null;
        });
        service = new LeaderboardApplyService(redisTemplate, LEADERBOARD, PROCESSED_DELTAS, 604_800);
    }

    @AfterEach
    void tearDown() {
        connectionFactory.destroy();
    }

    @Test
    void appliesDeltasOnceAndAggregatesByCompany() {
        List<CompanyRewardDelta> deltas = List.of(
                delta("D-1", "COMPANY-A", 10),
                delta("D-2", "COMPANY-A", 30),
                delta("D-3", "COMPANY-B", 20)
        );

        assertEquals(3, service.applyDeltas(deltas, "partition-0"));
        assertEquals(40.0, redisTemplate.opsForZSet().score(LEADERBOARD, "COMPANY-A"));
        assertEquals(20.0, redisTemplate.opsForZSet().score(LEADERBOARD, "COMPANY-B"));

        assertEquals(0, service.applyDeltas(deltas, "partition-0"));
        assertEquals(40.0, redisTemplate.opsForZSet().score(LEADERBOARD, "COMPANY-A"));
    }

    private CompanyRewardDelta delta(String deltaId, String companyId, long points) {
        Instant now = Instant.now();
        return new CompanyRewardDelta(
                deltaId,
                companyId,
                points,
                now.minusMillis(500),
                now,
                CompanyRewardDelta.SCHEMA_VERSION
        );
    }
}