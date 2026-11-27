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
import java.util.UUID;
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
    private static final String PROCESSED_SET = "einv:processed-invoices";

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
        rewardService = new RewardService(redisTemplate, LEADERBOARD_KEY, PROCESSED_SET, 10, 604_800);
    }

    @AfterEach
    void tearDown() {
        connectionFactory.destroy();
    }

    @Test
    void rewardsAnInvoiceOnlyOnce() {
        InvoiceIssuedEvent event = event("INV-1", "COMPANY-A");

        assertEquals(1, apply(event));
        assertEquals(0, apply(event));

        assertEquals(10.0, redisTemplate.opsForZSet().score(LEADERBOARD_KEY, "COMPANY-A"));
        assertTrue(redisTemplate.opsForSet().isMember(PROCESSED_SET, "INV-1"));
    }

    @Test
    void batchedScriptAppliesEachInvoiceOnlyOnce() {
        List<InvoiceIssuedEvent> events = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            events.add(event("INV-BATCH-" + i, "BATCH-COMPANY-" + (i % 10)));
        }
        events.addAll(new ArrayList<>(events.subList(0, 100)));

        int applied = rewardService.applyRewards(events);

        assertEquals(200, applied);
        for (int company = 0; company < 10; company++) {
            assertEquals(200.0, redisTemplate.opsForZSet().score(
                    LEADERBOARD_KEY,
                    "BATCH-COMPANY-" + company
            ));
        }
    }

    @Test
    void handlesFiveThousandEventBatch() {
        List<InvoiceIssuedEvent> events = new ArrayList<>();
        for (int i = 0; i < 5000; i++) {
            events.add(event("INV-LARGE-" + i, "LARGE-COMPANY-" + (i % 10)));
        }

        assertEquals(5000, rewardService.applyRewards(events));
        for (int company = 0; company < 10; company++) {
            assertEquals(5000.0, redisTemplate.opsForZSet().score(
                    LEADERBOARD_KEY,
                    "LARGE-COMPANY-" + company
            ));
        }
    }

    @Test
    void reloadsScriptAfterRedisScriptCacheIsFlushed() {
        assertEquals(1, apply(event("INV-BEFORE-FLUSH", "COMPANY-A")));
        redisTemplate.execute((RedisCallback<Object>) connection -> {
            connection.scriptFlush();
            return null;
        });

        assertEquals(1, apply(event("INV-AFTER-FLUSH", "COMPANY-A")));
        assertEquals(20.0, redisTemplate.opsForZSet().score(LEADERBOARD_KEY, "COMPANY-A"));
    }

    @Test
    void concurrentDuplicatesAwardExactlyTenPoints() throws Exception {
        InvoiceIssuedEvent event = event("INV-CONCURRENT", "COMPANY-A");

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<Boolean>> results = new ArrayList<>();
            for (int i = 0; i < 32; i++) {
                results.add(executor.submit(() -> apply(event) == 1));
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
                "einv:reward-batch:PARTIAL-BEFORE",
                "{\"state\":\"PENDING\",\"invoiceIds\":[\"INV-PARTIAL-BEFORE\"],\"companies\":[{\"companyId\":\"COMPANY-A\",\"delta\":10,\"scoreBefore\":0}]}"
        );

        assertEquals(0, rewardService.applyRewards(
                List.of(event("INV-PARTIAL-BEFORE", "COMPANY-A")),
                "PARTIAL-BEFORE"
        ));
        assertEquals(10.0, redisTemplate.opsForZSet().score(LEADERBOARD_KEY, "COMPANY-A"));
    }

    @Test
    void recoversWhenScriptFailedAfterIncrementingScore() {
        redisTemplate.opsForZSet().add(LEADERBOARD_KEY, "COMPANY-A", 10.0);
        redisTemplate.opsForValue().set(
                "einv:reward-batch:PARTIAL-AFTER",
                "{\"state\":\"PENDING\",\"invoiceIds\":[\"INV-PARTIAL-AFTER\"],\"companies\":[{\"companyId\":\"COMPANY-A\",\"delta\":10,\"scoreBefore\":0}]}"
        );

        assertEquals(0, rewardService.applyRewards(
                List.of(event("INV-PARTIAL-AFTER", "COMPANY-A")),
                "PARTIAL-AFTER"
        ));
        assertEquals(10.0, redisTemplate.opsForZSet().score(LEADERBOARD_KEY, "COMPANY-A"));
    }

    @Test
    void invoiceIdIsTheGlobalBusinessIdempotencyKey() {
        assertEquals(1, apply(event("INV-SAME", "COMPANY-A")));
        assertEquals(0, apply(event("INV-SAME", "COMPANY-B")));

        assertNull(redisTemplate.opsForZSet().score(LEADERBOARD_KEY, "COMPANY-B"));
    }

    @Test
    void rejectsWrongLeaderboardTypeBeforeWritingTheIdempotencyKey() {
        redisTemplate.opsForValue().set(LEADERBOARD_KEY, "wrong-type");

        assertThrows(DataAccessException.class,
                () -> apply(event("INV-TYPE", "COMPANY-A")));

        assertFalse(redisTemplate.opsForSet().isMember(PROCESSED_SET, "INV-TYPE"));
    }

    private int apply(InvoiceIssuedEvent event) {
        return rewardService.applyRewards(List.of(event), UUID.randomUUID().toString());
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
