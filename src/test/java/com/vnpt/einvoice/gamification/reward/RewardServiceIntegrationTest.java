package com.vnpt.einvoice.gamification.reward;

import com.vnpt.einvoice.gamification.event.InvoiceIssuedEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.dao.DataAccessException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
@Execution(ExecutionMode.SAME_THREAD)
class RewardServiceIntegrationTest {

    private static final String LEADERBOARD_KEY = "einv:leaderboard";
    private static final String PROCESSED_PREFIX = "einv:processed:";

    @Container
    static GenericContainer<?> redis = new GenericContainer<>("redis:7.4-alpine")
            .withExposedPorts(6379);

    private LettuceConnectionFactory connectionFactory;
    private StringRedisTemplate redisTemplate;
    private RewardService rewardService;

    @BeforeEach
    void setUp() {
        RedisStandaloneConfiguration configuration = new RedisStandaloneConfiguration(
                redis.getHost(),
                redis.getMappedPort(6379)
        );
        connectionFactory = new LettuceConnectionFactory(configuration);
        connectionFactory.afterPropertiesSet();
        redisTemplate = new StringRedisTemplate(connectionFactory);
        redisTemplate.afterPropertiesSet();
        redisTemplate.execute((RedisCallback<Object>) connection -> {
            connection.flushDb();
            return null;
        });
        rewardService = new RewardService(redisTemplate, LEADERBOARD_KEY, PROCESSED_PREFIX, 10);
    }

    @AfterEach
    void tearDown() {
        connectionFactory.destroy();
    }

    @Test
    void rewardsAnInvoiceOnlyOnce() {
        InvoiceIssuedEvent event = event("INV-1", "COMPANY-A");

        assertTrue(rewardService.applyReward(event));
        assertFalse(rewardService.applyReward(event));

        assertEquals(10.0, redisTemplate.opsForZSet().score(LEADERBOARD_KEY, "COMPANY-A"));
        String marker = redisTemplate.opsForValue().get(PROCESSED_PREFIX + "INV-1");
        assertTrue(marker != null && marker.contains("\"state\":\"APPLIED\""));
    }

    @Test
    void pipelinedBatchAppliesEachInvoiceOnlyOnce() {
        List<InvoiceIssuedEvent> events = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            events.add(event("INV-BATCH-" + i, "BATCH-COMPANY-" + (i % 10)));
        }
        events.addAll(new ArrayList<>(events.subList(0, 100)));

        List<Boolean> results = rewardService.applyRewards(events);

        assertEquals(200, results.stream().filter(Boolean::booleanValue).count());
        for (int company = 0; company < 10; company++) {
            assertEquals(200.0, redisTemplate.opsForZSet().score(
                    LEADERBOARD_KEY,
                    "BATCH-COMPANY-" + company
            ));
        }
    }

    @Test
    void concurrentDuplicatesAwardExactlyTenPoints() throws Exception {
        InvoiceIssuedEvent event = event("INV-CONCURRENT", "COMPANY-A");

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<Boolean>> results = new ArrayList<>();
            for (int i = 0; i < 32; i++) {
                results.add(executor.submit(() -> rewardService.applyReward(event)));
            }

            int appliedCount = 0;
            for (Future<Boolean> result : results) {
                if (result.get()) {
                    appliedCount++;
                }
            }
            assertEquals(1, appliedCount);
        }

        assertEquals(10.0, redisTemplate.opsForZSet().score(LEADERBOARD_KEY, "COMPANY-A"));
    }

    @Test
    void recoversWhenScriptFailedAfterWritingPendingMarker() {
        redisTemplate.opsForValue().set(
                PROCESSED_PREFIX + "INV-PARTIAL-BEFORE",
                "{\"state\":\"PENDING\",\"companyId\":\"COMPANY-A\",\"points\":10,\"scoreBefore\":0}"
        );

        assertTrue(rewardService.applyReward(event("INV-PARTIAL-BEFORE", "COMPANY-A")));
        assertEquals(10.0, redisTemplate.opsForZSet().score(LEADERBOARD_KEY, "COMPANY-A"));
    }

    @Test
    void recoversWhenScriptFailedAfterIncrementingScore() {
        redisTemplate.opsForZSet().add(LEADERBOARD_KEY, "COMPANY-A", 10.0);
        redisTemplate.opsForValue().set(
                PROCESSED_PREFIX + "INV-PARTIAL-AFTER",
                "{\"state\":\"PENDING\",\"companyId\":\"COMPANY-A\",\"points\":10,\"scoreBefore\":0}"
        );

        assertTrue(rewardService.applyReward(event("INV-PARTIAL-AFTER", "COMPANY-A")));
        assertEquals(10.0, redisTemplate.opsForZSet().score(LEADERBOARD_KEY, "COMPANY-A"));
    }

    @Test
    void invoiceIdIsTheGlobalBusinessIdempotencyKey() {
        assertTrue(rewardService.applyReward(event("INV-SAME", "COMPANY-A")));
        assertFalse(rewardService.applyReward(event("INV-SAME", "COMPANY-B")));

        assertNull(redisTemplate.opsForZSet().score(LEADERBOARD_KEY, "COMPANY-B"));
    }

    @Test
    void rejectsWrongLeaderboardTypeBeforeWritingTheIdempotencyKey() {
        redisTemplate.opsForValue().set(LEADERBOARD_KEY, "wrong-type");

        assertThrows(DataAccessException.class,
                () -> rewardService.applyReward(event("INV-TYPE", "COMPANY-A")));

        assertNull(redisTemplate.opsForValue().get(PROCESSED_PREFIX + "INV-TYPE"));
    }

    private InvoiceIssuedEvent event(String invoiceId, String companyId) {
        return new InvoiceIssuedEvent(
                "event-" + invoiceId,
                InvoiceIssuedEvent.TYPE,
                InvoiceIssuedEvent.SCHEMA_VERSION,
                invoiceId,
                companyId,
                Instant.now()
        );
    }
}
