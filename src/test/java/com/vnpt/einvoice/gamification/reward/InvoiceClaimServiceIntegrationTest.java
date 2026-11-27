package com.vnpt.einvoice.gamification.reward;

import com.vnpt.einvoice.gamification.event.InvoiceIssuedEvent;
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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class InvoiceClaimServiceIntegrationTest {

    private static final String PROCESSED_SET = "einv:processed-invoices";

    @Container
    static GenericContainer<?> redis = new GenericContainer<>("redis:7.4-alpine")
            .withExposedPorts(6379);

    private LettuceConnectionFactory connectionFactory;
    private StringRedisTemplate redisTemplate;
    private InvoiceClaimService service;

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
        service = new InvoiceClaimService(redisTemplate, PROCESSED_SET, 10, 604_800);
    }

    @AfterEach
    void tearDown() {
        connectionFactory.destroy();
    }

    @Test
    void claimsInvoicesOnceAndRecoversPendingMarker() {
        List<InvoiceIssuedEvent> events = List.of(
                event("INV-1", "COMPANY-A"),
                event("INV-2", "COMPANY-A")
        );
        InvoiceClaimResult first = service.claim(events, "partition-0", UUID.randomUUID().toString());

        assertFalse(first.recovered());
        assertEquals(1, first.companies().size());
        assertEquals(20, first.companies().getFirst().deltaPoints());
        assertEquals(2L, redisTemplate.opsForSet().size(PROCESSED_SET));

        InvoiceClaimResult recovered = service.claim(events, "partition-0", UUID.randomUUID().toString());
        assertTrue(recovered.recovered());
        assertEquals(first.batchUid(), recovered.batchUid());
        assertEquals(20, recovered.companies().getFirst().deltaPoints());

        service.complete("partition-0");
        InvoiceClaimResult afterComplete = service.claim(events, "partition-0", UUID.randomUUID().toString());
        assertTrue(afterComplete.companies().isEmpty());
    }

    @Test
    void concurrentClaimsAwardOneWinner() throws Exception {
        InvoiceIssuedEvent event = event("INV-CONCURRENT", "COMPANY-A");
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<InvoiceClaimResult>> futures = new ArrayList<>();
            for (int i = 0; i < 32; i++) {
                int partition = i;
                futures.add(executor.submit(() -> service.claim(
                        List.of(event),
                        "partition-" + partition,
                        UUID.randomUUID().toString()
                )));
            }

            long awardedPoints = 0;
            for (Future<InvoiceClaimResult> future : futures) {
                InvoiceClaimResult result = future.get();
                awardedPoints += result.companies().stream()
                        .mapToLong(ClaimedCompanyDelta::deltaPoints)
                        .sum();
            }
            assertEquals(10, awardedPoints);
        }
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