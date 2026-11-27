package com.vnpt.einvoice.gamification.streams;

import com.vnpt.einvoice.gamification.event.CompanyRewardDelta;
import com.vnpt.einvoice.gamification.event.InvoiceIssuedEvent;
import com.vnpt.einvoice.gamification.metrics.RewardMetrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class RewardTopologyTest {

    private static final Instant BASE_TIME = Instant.ofEpochMilli(1_700_000_000_000L);

    @TempDir
    Path stateDir;

    @Test
    void deduplicatesInvoicesAndAggregatesByCompanyWindow() {
        try (TopologyTestDriver driver = createDriver(86_400_000L)) {
            TestInputTopic<String, InvoiceIssuedEvent> input = inputTopic(driver);
            TestOutputTopic<String, CompanyRewardDelta> output = deltaTopic(driver);

            Instant now = BASE_TIME;
            input.pipeInput("INV-1", event("INV-1", "COMPANY-A", now), now);
            input.pipeInput("INV-1", event("INV-1", "COMPANY-A", now), now);
            input.pipeInput("INV-2", event("INV-2", "COMPANY-A", now), now);
            input.pipeInput("INV-3", event("INV-3", "COMPANY-B", now), now);

            driver.advanceWallClockTime(Duration.ofSeconds(1));

            List<CompanyRewardDelta> deltas = output.readKeyValuesToList().stream()
                    .map(keyValue -> keyValue.value)
                    .toList();
            assertEquals(2, deltas.size());
            assertEquals(20L, pointsFor(deltas, "COMPANY-A"));
            assertEquals(10L, pointsFor(deltas, "COMPANY-B"));
        }
    }

    @Test
    void routesInvalidEventsToDltAndContinuesValidOnes() {
        try (TopologyTestDriver driver = createDriver(86_400_000L)) {
            TestInputTopic<String, InvoiceIssuedEvent> input = inputTopic(driver);
            TestOutputTopic<String, CompanyRewardDelta> output = deltaTopic(driver);
            TestOutputTopic<String, InvoiceIssuedEvent> dlt = dltTopic(driver);

            Instant now = BASE_TIME;
            input.pipeInput("INV-BAD-TYPE", new InvoiceIssuedEvent("e1", "WRONG", 1, "INV-BAD-TYPE", "COMPANY-A", now), now);
            input.pipeInput("INV-BAD-VER", new InvoiceIssuedEvent("e2", InvoiceIssuedEvent.TYPE, 99, "INV-BAD-VER", "COMPANY-A", now), now);
            input.pipeInput("INV-GOOD", event("INV-GOOD", "COMPANY-A", now), now);

            driver.advanceWallClockTime(Duration.ofSeconds(1));

            assertEquals(2, dlt.readKeyValuesToList().size());
            List<CompanyRewardDelta> deltas = output.readValuesToList();
            assertEquals(1, deltas.size());
            assertEquals(10L, deltas.getFirst().deltaPoints());
        }
    }

    @Test
    void routesConflictingInvoiceCompanyToDltWithoutDoubleAward() {
        try (TopologyTestDriver driver = createDriver(86_400_000L)) {
            TestInputTopic<String, InvoiceIssuedEvent> input = inputTopic(driver);
            TestOutputTopic<String, CompanyRewardDelta> output = deltaTopic(driver);
            TestOutputTopic<String, InvoiceIssuedEvent> dlt = dltTopic(driver);

            Instant now = BASE_TIME;
            input.pipeInput("INV-CONFLICT", event("INV-CONFLICT", "COMPANY-A", now), now);
            input.pipeInput("INV-CONFLICT", event("INV-CONFLICT", "COMPANY-B", now), now);

            driver.advanceWallClockTime(Duration.ofSeconds(1));

            List<TestRecord<String, InvoiceIssuedEvent>> dltRecords = dlt.readRecordsToList();
            assertEquals(1, dltRecords.size());
            assertEquals("COMPANY-B", dltRecords.getFirst().value().companyId());
            assertEquals(
                    "INVOICE_COMPANY_CONFLICT",
                    new String(dltRecords.getFirst().headers().lastHeader(DedupeProcessor.CONFLICT_HEADER).value(), StandardCharsets.UTF_8)
            );

            List<CompanyRewardDelta> deltas = output.readValuesToList();
            assertEquals(1, deltas.size());
            assertEquals(10L, pointsFor(deltas, "COMPANY-A"));
            assertEquals(0L, pointsFor(deltas, "COMPANY-B"));
        }
    }

    @Test
    void lateEventForFlushedWindowGetsDistinctDeterministicDeltaId() {
        try (TopologyTestDriver driver = createDriver(86_400_000L)) {
            TestInputTopic<String, InvoiceIssuedEvent> input = inputTopic(driver);
            TestOutputTopic<String, CompanyRewardDelta> output = deltaTopic(driver);

            Instant windowStart = BASE_TIME;
            input.pipeInput("INV-1", event("INV-1", "COMPANY-A", windowStart), windowStart);
            driver.advanceWallClockTime(Duration.ofSeconds(1));

            input.pipeInput("INV-2", event("INV-2", "COMPANY-A", windowStart.plusMillis(100)), windowStart.plusMillis(100));
            driver.advanceWallClockTime(Duration.ofSeconds(1));

            List<CompanyRewardDelta> deltas = output.readValuesToList();
            assertEquals(2, deltas.size());
            assertNotEquals(deltas.get(0).deltaId(), deltas.get(1).deltaId());
            assertEquals(20L, pointsFor(deltas, "COMPANY-A"));
        }
    }

    @Test
    void expiresDedupeEntriesAfterRetentionMs() {
        try (TopologyTestDriver driver = createDriver(5_000L)) {
            TestInputTopic<String, InvoiceIssuedEvent> input = inputTopic(driver);
            TestOutputTopic<String, CompanyRewardDelta> output = deltaTopic(driver);

            Instant t0 = BASE_TIME;
            input.pipeInput("INV-TTL", event("INV-TTL", "COMPANY-A", t0), t0);
            input.pipeInput("INV-TTL", event("INV-TTL", "COMPANY-A", t0.plusSeconds(2)), t0.plusSeconds(2));

            // Advance stream high-water mark beyond the 5s retention window.
            input.pipeInput("INV-ADV", event("INV-ADV", "COMPANY-B", t0.plusSeconds(10)), t0.plusSeconds(10));
            input.pipeInput("INV-TTL", event("INV-TTL", "COMPANY-A", t0.plusSeconds(11)), t0.plusSeconds(11));

            driver.advanceWallClockTime(Duration.ofSeconds(15));

            List<CompanyRewardDelta> deltas = output.readValuesToList();
            assertEquals(20L, pointsFor(deltas, "COMPANY-A"));
            assertEquals(10L, pointsFor(deltas, "COMPANY-B"));
        }
    }

    private TopologyTestDriver createDriver(long retentionMs) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "topology-test");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, com.vnpt.einvoice.gamification.config.HighThroughputRocksDBConfig.class);
        properties.put(com.vnpt.einvoice.gamification.config.HighThroughputRocksDBConfig.BLOCK_CACHE_MB_CONFIG, 32);
        properties.put(com.vnpt.einvoice.gamification.config.HighThroughputRocksDBConfig.WRITE_BUFFER_MB_CONFIG, 16);

        return new TopologyTestDriver(
                RewardTopology.build(
                        "invoice-events",
                        "company-reward-deltas",
                        "invoice-events.DLT",
                        retentionMs,
                        10_000,
                        500L,
                        100L,
                        RewardMetrics.noop()
                ),
                properties,
                Instant.ofEpochMilli(1_700_000_000_000L)
        );
    }

    private TestInputTopic<String, InvoiceIssuedEvent> inputTopic(TopologyTestDriver driver) {
        JsonMapper mapper = JsonMapper.builder().build();
        JacksonJsonSerde<InvoiceIssuedEvent> invoiceSerde =
                new JacksonJsonSerde<>(InvoiceIssuedEvent.class, mapper).noTypeInfo();
        return driver.createInputTopic(
                "invoice-events",
                Serdes.String().serializer(),
                invoiceSerde.serializer()
        );
    }

    private TestOutputTopic<String, CompanyRewardDelta> deltaTopic(TopologyTestDriver driver) {
        JsonMapper mapper = JsonMapper.builder().build();
        JacksonJsonSerde<CompanyRewardDelta> deltaSerde =
                new JacksonJsonSerde<>(CompanyRewardDelta.class, mapper).noTypeInfo();
        return driver.createOutputTopic(
                "company-reward-deltas",
                Serdes.String().deserializer(),
                deltaSerde.deserializer()
        );
    }

    private TestOutputTopic<String, InvoiceIssuedEvent> dltTopic(TopologyTestDriver driver) {
        JsonMapper mapper = JsonMapper.builder().build();
        JacksonJsonSerde<InvoiceIssuedEvent> invoiceSerde =
                new JacksonJsonSerde<>(InvoiceIssuedEvent.class, mapper).noTypeInfo();
        return driver.createOutputTopic(
                "invoice-events.DLT",
                Serdes.String().deserializer(),
                invoiceSerde.deserializer()
        );
    }

    private long pointsFor(List<CompanyRewardDelta> deltas, String companyId) {
        return deltas.stream()
                .filter(delta -> delta.companyId().equals(companyId))
                .mapToLong(CompanyRewardDelta::deltaPoints)
                .sum();
    }

    private InvoiceIssuedEvent event(String invoiceId, String companyId, Instant now) {
        return new InvoiceIssuedEvent(
                "event-" + invoiceId + "-" + companyId,
                InvoiceIssuedEvent.TYPE,
                InvoiceIssuedEvent.SCHEMA_VERSION,
                invoiceId,
                companyId,
                now
        );
    }
}
