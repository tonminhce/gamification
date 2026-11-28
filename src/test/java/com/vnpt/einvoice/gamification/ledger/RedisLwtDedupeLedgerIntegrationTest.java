package com.vnpt.einvoice.gamification.ledger;

import com.vnpt.einvoice.gamification.event.CompanyRewardDelta;
import com.vnpt.einvoice.gamification.event.InvoiceIssuedEvent;
import com.vnpt.einvoice.gamification.metrics.RewardMetrics;
import com.vnpt.einvoice.gamification.streams.RewardTopology;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import tools.jackson.databind.json.JsonMapper;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class RedisLwtDedupeLedgerIntegrationTest {

    @Container
    static GenericContainer<?> redis = new GenericContainer<>("redis:7.4-alpine")
            .withExposedPorts(6379);

    @TempDir
    Path stateDir;

    private LettuceConnectionFactory connectionFactory;
    private StringRedisTemplate redisTemplate;

    @BeforeEach
    void setUp() {
        connectionFactory = new LettuceConnectionFactory(
                redis.getHost(),
                redis.getMappedPort(6379)
        );
        connectionFactory.afterPropertiesSet();
        redisTemplate = new StringRedisTemplate(connectionFactory);
        redisTemplate.afterPropertiesSet();
        redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
    }

    @AfterEach
    void tearDown() {
        if (connectionFactory != null) {
            connectionFactory.destroy();
        }
    }

    @Test
    void enforcesClaimedToEmittedStateMachineLeaseRecoveryAndConflictDetection() throws Exception {
        RedisLwtDedupeLedger ledger = new RedisLwtDedupeLedger(redisTemplate, 120L);
        Instant t0 = Instant.now();

        DedupeLedger.ClaimResult first = ledger.claim("INV-LWT-1", "COMPANY-A", "0:10", t0, Duration.ofMinutes(5));
        assertEquals(DedupeLedger.ClaimStatus.CLAIMED_NEW, first.status());
        assertTrue(first.shouldEmit());

        // Redelivery of exact same uncommitted offset recovers immediately before lease expires
        DedupeLedger.ClaimResult sameSourceRetry = ledger.claim("INV-LWT-1", "COMPANY-A", "0:10", t0, Duration.ofMinutes(5));
        assertEquals(DedupeLedger.ClaimStatus.RECOVERABLE_CLAIMED_UNEMITTED, sameSourceRetry.status());

        // Different source before lease expiry sees IN_FLIGHT_CLAIMED_BY_OTHER
        DedupeLedger.ClaimResult concurrentOther = ledger.claim("INV-LWT-1", "COMPANY-A", "0:11", t0, Duration.ofMinutes(5));
        assertEquals(DedupeLedger.ClaimStatus.IN_FLIGHT_CLAIMED_BY_OTHER, concurrentOther.status());
        assertFalse(concurrentOther.shouldEmit());

        // After claim lease expiry, recovery from a new offset succeeds
        Thread.sleep(150L);
        DedupeLedger.ClaimResult expiredLeaseTakeover = ledger.claim("INV-LWT-1", "COMPANY-A", "0:11", Instant.now(), Duration.ofMinutes(5));
        assertEquals(DedupeLedger.ClaimStatus.RECOVERABLE_CLAIMED_UNEMITTED, expiredLeaseTakeover.status());

        // Once EMITTED, replays are strictly duplicates and conflicting companyId is rejected
        ledger.markEmitted("INV-LWT-1", "0:11", Instant.now());
        assertEquals(
                DedupeLedger.ClaimStatus.DUPLICATE_EMITTED,
                ledger.claim("INV-LWT-1", "COMPANY-A", "0:12", Instant.now(), Duration.ofMinutes(5)).status()
        );
        DedupeLedger.ClaimResult conflict = ledger.claim("INV-LWT-1", "COMPANY-B", "0:13", Instant.now(), Duration.ofMinutes(5));
        assertEquals(DedupeLedger.ClaimStatus.CONFLICT, conflict.status());
        assertEquals("COMPANY-A", conflict.existingCompanyId());
    }

    @Test
    void topologyUsesExternalLedgerWithoutLocalDedupeRocksDbStore() {
        RedisLwtDedupeLedger ledger = new RedisLwtDedupeLedger(redisTemplate, 5_000L);
        Topology topology = RewardTopology.build(
                "invoice-events",
                "company-reward-deltas",
                "invoice-events.DLT",
                86_400_000L,
                10_000,
                500L,
                100L,
                RewardMetrics.noop(),
                ledger
        );

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "external-ledger-topology-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        Instant now = Instant.now();
        try (TopologyTestDriver driver = new TopologyTestDriver(topology, props, now)) {
            assertNull(driver.getWindowStore("invoice-dedupe-store"));

            JsonMapper mapper = JsonMapper.builder().build();
            JacksonJsonSerde<InvoiceIssuedEvent> invoiceSerde = new JacksonJsonSerde<>(InvoiceIssuedEvent.class, mapper).noTypeInfo();
            JacksonJsonSerde<CompanyRewardDelta> deltaSerde = new JacksonJsonSerde<>(CompanyRewardDelta.class, mapper).noTypeInfo();

            TestInputTopic<String, InvoiceIssuedEvent> input = driver.createInputTopic(
                    "invoice-events", Serdes.String().serializer(), invoiceSerde.serializer());
            TestOutputTopic<String, CompanyRewardDelta> deltas = driver.createOutputTopic(
                    "company-reward-deltas", Serdes.String().deserializer(), deltaSerde.deserializer());
            TestOutputTopic<String, InvoiceIssuedEvent> dlt = driver.createOutputTopic(
                    "invoice-events.DLT", Serdes.String().deserializer(), invoiceSerde.deserializer());

            input.pipeInput("INV-EXT-1", new InvoiceIssuedEvent("e1", InvoiceIssuedEvent.TYPE, 1, "INV-EXT-1", "COMPANY-X", now), now);
            input.pipeInput("INV-EXT-1", new InvoiceIssuedEvent("e2", InvoiceIssuedEvent.TYPE, 1, "INV-EXT-1", "COMPANY-X", now), now);
            input.pipeInput("INV-EXT-1", new InvoiceIssuedEvent("e3", InvoiceIssuedEvent.TYPE, 1, "INV-EXT-1", "COMPANY-Z", now), now);

            driver.advanceWallClockTime(Duration.ofSeconds(1));

            List<CompanyRewardDelta> emitted = deltas.readValuesToList();
            assertEquals(1, emitted.size());
            assertEquals("COMPANY-X", emitted.getFirst().companyId());
            assertEquals(10L, emitted.getFirst().deltaPoints());

            List<InvoiceIssuedEvent> dltRecords = dlt.readValuesToList();
            assertEquals(1, dltRecords.size());
            assertEquals("COMPANY-Z", dltRecords.getFirst().companyId());
        }
    }
}
