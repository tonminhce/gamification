package com.vnpt.einvoice.gamification.streams;

import com.vnpt.einvoice.gamification.config.HighThroughputRocksDBConfig;
import com.vnpt.einvoice.gamification.event.CompanyRewardDelta;
import com.vnpt.einvoice.gamification.event.InvoiceIssuedEvent;
import com.vnpt.einvoice.gamification.metrics.RewardMetrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
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
        try (TopologyTestDriver driver = createDriver(86_400_000L, 10_000)) {
            TestInputTopic<String, InvoiceIssuedEvent> input = inputTopic(driver);
            TestOutputTopic<String, CompanyRewardDelta> output = deltaTopic(driver);

            Instant now = BASE_TIME;
            input.pipeInput("INV-1", event("INV-1", "COMPANY-A", now), now);
            input.pipeInput("INV-1", event("INV-1", "COMPANY-A", now), now);
            input.pipeInput("INV-2", event("INV-2", "COMPANY-A", now), now);
            input.pipeInput("INV-3", event("INV-3", "COMPANY-B", now), now);

            driver.advanceWallClockTime(Duration.ofSeconds(1));

            List<CompanyRewardDelta> deltas = output.readValuesToList();
            assertEquals(2, deltas.size());
            assertEquals(20L, pointsFor(deltas, "COMPANY-A"));
            assertEquals(10L, pointsFor(deltas, "COMPANY-B"));
        }
    }

    @Test
    void routesInvalidAndNullOrMismatchedKeyEventsToDlt() {
        try (TopologyTestDriver driver = createDriver(86_400_000L, 10_000)) {
            TestInputTopic<String, InvoiceIssuedEvent> input = inputTopic(driver);
            TestOutputTopic<String, CompanyRewardDelta> output = deltaTopic(driver);
            TestOutputTopic<String, InvoiceIssuedEvent> dlt = dltTopic(driver);

            Instant now = BASE_TIME;
            input.pipeInput("INV-BAD-TYPE", new InvoiceIssuedEvent("e1", "WRONG", 1, "INV-BAD-TYPE", "COMPANY-A", now), now);
            input.pipeInput("INV-BAD-VER", new InvoiceIssuedEvent("e2", InvoiceIssuedEvent.TYPE, 99, "INV-BAD-VER", "COMPANY-A", now), now);
            input.pipeInput(null, event("INV-NULL-KEY", "COMPANY-A", now), now);
            input.pipeInput("WRONG-KEY", event("INV-MISMATCH", "COMPANY-A", now), now);
            input.pipeInput("INV-GOOD", event("INV-GOOD", "COMPANY-A", now), now);

            driver.advanceWallClockTime(Duration.ofSeconds(1));

            assertEquals(4, dlt.readKeyValuesToList().size());
            List<CompanyRewardDelta> deltas = output.readValuesToList();
            assertEquals(1, deltas.size());
            assertEquals(10L, deltas.getFirst().deltaPoints());
        }
    }

    @Test
    void routesConflictingInvoiceCompanyToDltWithoutDoubleAward() {
        try (TopologyTestDriver driver = createDriver(86_400_000L, 10_000)) {
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
        try (TopologyTestDriver driver = createDriver(86_400_000L, 10_000)) {
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
    void expiresDedupeEntriesAfterRetentionMsAndQuarantinesExpiredTimestamps() {
        try (TopologyTestDriver driver = createDriver(5_000L, 10_000)) {
            TestInputTopic<String, InvoiceIssuedEvent> input = inputTopic(driver);
            TestOutputTopic<String, CompanyRewardDelta> output = deltaTopic(driver);
            TestOutputTopic<String, InvoiceIssuedEvent> dlt = dltTopic(driver);

            Instant t0 = BASE_TIME;
            input.pipeInput("INV-TTL", event("INV-TTL", "COMPANY-A", t0), t0);
            input.pipeInput("INV-TTL", event("INV-TTL", "COMPANY-A", t0.plusSeconds(2)), t0.plusSeconds(2));

            // Advance stream high-water mark beyond the 5s retention window.
            input.pipeInput("INV-ADV", event("INV-ADV", "COMPANY-B", t0.plusSeconds(10)), t0.plusSeconds(10));

            // An out-of-order delayed record with timestamp < cutoff must be quarantined instead of double-awarding.
            input.pipeInput("INV-EXPIRED-LATE", event("INV-EXPIRED-LATE", "COMPANY-C", t0), t0);

            // Re-issuing INV-TTL at t0+11s (after retention expiry) is legitimately accepted again.
            input.pipeInput("INV-TTL", event("INV-TTL", "COMPANY-A", t0.plusSeconds(11)), t0.plusSeconds(11));

            driver.advanceWallClockTime(Duration.ofSeconds(15));

            List<TestRecord<String, InvoiceIssuedEvent>> dltRecords = dlt.readRecordsToList();
            assertEquals(1, dltRecords.size());
            assertEquals("INV-EXPIRED-LATE", dltRecords.getFirst().key());
            assertEquals(
                    "EXPIRED_INVOICE_TIMESTAMP",
                    new String(dltRecords.getFirst().headers().lastHeader(DedupeProcessor.CONFLICT_HEADER).value(), StandardCharsets.UTF_8)
            );

            List<CompanyRewardDelta> deltas = output.readValuesToList();
            assertEquals(20L, pointsFor(deltas, "COMPANY-A"));
            assertEquals(10L, pointsFor(deltas, "COMPANY-B"));
            assertEquals(0L, pointsFor(deltas, "COMPANY-C"));
        }
    }

    @Test
    void bloomBucketsCoverFullRetentionAcrossMultipleBucketTransitionsAndSaturation() {
        try (TopologyTestDriver driver = createDriver(10_000L, 1_024)) {
            TestInputTopic<String, InvoiceIssuedEvent> input = inputTopic(driver);
            TestOutputTopic<String, CompanyRewardDelta> output = deltaTopic(driver);

            // Insert INV-EARLY at t0, then advance through multiple 5s buckets inside the 10s retention window.
            Instant t0 = BASE_TIME;
            input.pipeInput("INV-EARLY", event("INV-EARLY", "COMPANY-A", t0), t0);
            input.pipeInput("INV-B1", event("INV-B1", "COMPANY-A", t0.plusSeconds(5)), t0.plusSeconds(5));
            input.pipeInput("INV-B2", event("INV-B2", "COMPANY-A", t0.plusSeconds(9)), t0.plusSeconds(9));

            // Replay INV-EARLY at t0+9.5s (still within 10s retention after 2 bucket steps) -> must deduplicate.
            input.pipeInput("INV-EARLY", event("INV-EARLY", "COMPANY-A", t0.plusMillis(9_500)), t0.plusMillis(9_500));

            // Saturate current bucket beyond maxSafeInsertions (2,048) and verify exact RocksDB fallback still deduplicates.
            for (int i = 0; i < 2_200; i++) {
                String id = "INV-SAT-" + i;
                input.pipeInput(id, event(id, "COMPANY-SAT", t0.plusMillis(9_600)), t0.plusMillis(9_600));
            }
            input.pipeInput("INV-SAT-0", event("INV-SAT-0", "COMPANY-SAT", t0.plusMillis(9_700)), t0.plusMillis(9_700));

            driver.advanceWallClockTime(Duration.ofSeconds(15));

            List<CompanyRewardDelta> deltas = output.readValuesToList();
            assertEquals(30L, pointsFor(deltas, "COMPANY-A"));
            assertEquals(22_000L, pointsFor(deltas, "COMPANY-SAT"));
        }
    }

    @Test
    void restartPreservesDedupeStateAndSequenceCounter() {
        long t0 = BASE_TIME.toEpochMilli();
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "restart-state-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());

        // Phase 1: persist one flushed window (advances seq -> 1) and one unflushed window to RocksDB on disk.
        MockProcessorContext<String, InvoiceIssuedEvent> dedupeCtx1 =
                new MockProcessorContext<>(props, new TaskId(0, 0), stateDir.toFile());
        MockProcessorContext<String, CompanyRewardDelta> windowCtx1 =
                new MockProcessorContext<>(props, new TaskId(0, 0), stateDir.toFile());
        WindowStore<String, String> dedupeStore1 = Stores.windowStoreBuilder(
                        Stores.persistentWindowStore(
                                DedupeProcessor.STORE, Duration.ofHours(24), Duration.ofHours(24), false),
                        Serdes.String(),
                        Serdes.String()
                )
                .withLoggingDisabled()
                .build();
        KeyValueStore<byte[], byte[]> windowStore1 = Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(WindowAggregatorProcessor.STORE),
                        Serdes.ByteArray(),
                        Serdes.ByteArray()
                )
                .withLoggingDisabled()
                .build();

        dedupeStore1.init(dedupeCtx1.getStateStoreContext(), dedupeStore1);
        windowStore1.init(windowCtx1.getStateStoreContext(), windowStore1);
        dedupeCtx1.addStateStore(dedupeStore1);
        windowCtx1.addStateStore(windowStore1);

        DedupeProcessor dedupe1 = new DedupeProcessor(86_400_000L, 10_000, RewardMetrics.noop());
        WindowAggregatorProcessor window1 = new WindowAggregatorProcessor(500L, 100L, RewardMetrics.noop());
        dedupe1.init(dedupeCtx1);
        window1.init(windowCtx1);

        dedupe1.process(new Record<>("INV-R1", event("INV-R1", "COMPANY-A", BASE_TIME), t0));
        window1.process(new Record<>("COMPANY-A", event("INV-R1", "COMPANY-A", BASE_TIME), t0));
        windowCtx1.scheduledPunctuators().getFirst().getPunctuator().punctuate(t0 + 1_000L);
        assertEquals(1, windowCtx1.forwarded().size());
        assertEquals("0:" + t0 + ":COMPANY-A:0", windowCtx1.forwarded().getFirst().record().value().deltaId());

        // Leave an unflushed window on disk before closing (simulating crash/restart mid-window).
        long unflushedTs = t0 + 10_000L;
        dedupe1.process(new Record<>("INV-UNFLUSHED", event("INV-UNFLUSHED", "COMPANY-A", Instant.ofEpochMilli(unflushedTs)), unflushedTs));
        window1.process(new Record<>("COMPANY-A", event("INV-UNFLUSHED", "COMPANY-A", Instant.ofEpochMilli(unflushedTs)), unflushedTs));

        dedupe1.close();
        window1.close();
        dedupeStore1.close();
        windowStore1.close();

        // Phase 2: reopen fresh processor instances against the persisted RocksDB directory on disk.
        MockProcessorContext<String, InvoiceIssuedEvent> dedupeCtx2 =
                new MockProcessorContext<>(props, new TaskId(0, 0), stateDir.toFile());
        MockProcessorContext<String, CompanyRewardDelta> windowCtx2 =
                new MockProcessorContext<>(props, new TaskId(0, 0), stateDir.toFile());
        WindowStore<String, String> dedupeStore2 = Stores.windowStoreBuilder(
                        Stores.persistentWindowStore(
                                DedupeProcessor.STORE, Duration.ofHours(24), Duration.ofHours(24), false),
                        Serdes.String(),
                        Serdes.String()
                )
                .withLoggingDisabled()
                .build();
        KeyValueStore<byte[], byte[]> windowStore2 = Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(WindowAggregatorProcessor.STORE),
                        Serdes.ByteArray(),
                        Serdes.ByteArray()
                )
                .withLoggingDisabled()
                .build();

        try {
            dedupeStore2.init(dedupeCtx2.getStateStoreContext(), dedupeStore2);
            windowStore2.init(windowCtx2.getStateStoreContext(), windowStore2);
            dedupeCtx2.addStateStore(dedupeStore2);
            windowCtx2.addStateStore(windowStore2);

            DedupeProcessor dedupe2 = new DedupeProcessor(86_400_000L, 10_000, RewardMetrics.noop());
            WindowAggregatorProcessor window2 = new WindowAggregatorProcessor(500L, 100L, RewardMetrics.noop());
            dedupe2.init(dedupeCtx2);
            window2.init(windowCtx2);

            // Replay of INV-R1 must hit the restored Bloom/RocksDB dedupe state and not forward.
            dedupe2.process(new Record<>("INV-R1", event("INV-R1", "COMPANY-A", BASE_TIME.plusSeconds(1)), t0 + 1_000L));
            assertEquals(0, dedupeCtx2.forwarded().size());

            // Same unflushed window receives a new invoice after restart -> merges with restored 10pts to emit 20pts with seq=1.
            dedupe2.process(new Record<>("INV-R2", event("INV-R2", "COMPANY-A", Instant.ofEpochMilli(unflushedTs + 100L)), unflushedTs + 100L));
            assertEquals(1, dedupeCtx2.forwarded().size());
            window2.process(new Record<>("COMPANY-A", dedupeCtx2.forwarded().getFirst().record().value(), unflushedTs + 100L));
            windowCtx2.scheduledPunctuators().getFirst().getPunctuator().punctuate(t0 + 20_000L);

            assertEquals(1, windowCtx2.forwarded().size());
            CompanyRewardDelta restoredDelta = windowCtx2.forwarded().getFirst().record().value();
            assertEquals("0:" + unflushedTs + ":COMPANY-A:1", restoredDelta.deltaId());
            assertEquals(20L, restoredDelta.deltaPoints());
        } finally {
            dedupeStore2.close();
            windowStore2.close();
        }
    }

    private TopologyTestDriver createDriver(long retentionMs, int bloomExpected) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "topology-test");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, HighThroughputRocksDBConfig.class);
        properties.put(HighThroughputRocksDBConfig.BLOCK_CACHE_MB_CONFIG, 32);
        properties.put(HighThroughputRocksDBConfig.WRITE_BUFFER_MB_CONFIG, 8);

        return new TopologyTestDriver(
                RewardTopology.build(
                        "invoice-events",
                        "company-reward-deltas",
                        "invoice-events.DLT",
                        retentionMs,
                        bloomExpected,
                        500L,
                        100L,
                        RewardMetrics.noop()
                ),
                properties,
                BASE_TIME
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
