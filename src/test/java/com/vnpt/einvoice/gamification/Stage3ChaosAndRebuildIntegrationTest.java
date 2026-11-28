package com.vnpt.einvoice.gamification;

import com.vnpt.einvoice.gamification.api.InvoiceBatchAcceptedResponse;
import com.vnpt.einvoice.gamification.api.InvoiceSimulationRequest;
import com.vnpt.einvoice.gamification.event.CompanyRewardDelta;
import com.vnpt.einvoice.gamification.event.InvoiceIssuedEvent;
import com.vnpt.einvoice.gamification.invoice.InvoiceEventPublisher;
import com.vnpt.einvoice.gamification.invoice.PublishCircuitBreaker;
import com.vnpt.einvoice.gamification.metrics.RewardMetrics;
import com.vnpt.einvoice.gamification.snapshot.LeaderboardSnapshot;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class Stage3ChaosAndRebuildIntegrationTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:4.4.0");

    @Container
    static GenericContainer<?> redis = new GenericContainer<>("redis:7.4-alpine")
            .withExposedPorts(6379);

    @TempDir
    static Path stateDir;

    @TempDir
    static Path snapshotDir;

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
        registry.add("app.kafka.partitions", () -> 2);
        registry.add("app.streams.threads", () -> 1);
        registry.add("app.streams.state-dir", () -> stateDir.toAbsolutePath().toString());
        registry.add("app.snapshot.dir", () -> snapshotDir.toAbsolutePath().toString());
        registry.add("app.streams.commit-interval-ms", () -> 100);
        registry.add("app.window.size-ms", () -> 150);
        registry.add("app.window.punctuation-ms", () -> 50);
        registry.add("app.leaderboard.top-cache-ttl-ms", () -> 0);
    }

    @LocalServerPort
    int port;

    @Autowired
    StringRedisTemplate redisTemplate;

    @Autowired
    KafkaTemplate<String, InvoiceIssuedEvent> kafkaTemplate;

    @Autowired
    RewardMetrics rewardMetrics;

    private final HttpClient http = HttpClient.newHttpClient();
    private final JsonMapper mapper = JsonMapper.builder().build();

    @Test
    void snapshotAndKafkaRebuildRecoversDisasterWipeAndIgnoresAbortedTransactions() throws Exception {
        // 1. Ingest initial batch before snapshot barrier
        assertEquals(202, postJson("/api/invoices/batch", Map.of("invoices", List.of(
                Map.of("invoiceId", "S3-B1-1", "companyId", "SNAP-CO-1"),
                Map.of("invoiceId", "S3-B1-2", "companyId", "SNAP-CO-1"),
                Map.of("invoiceId", "S3-B1-3", "companyId", "SNAP-CO-2")
        ))).statusCode());
        waitForPoints("SNAP-CO-1", 20L);
        waitForPoints("SNAP-CO-2", 10L);

        // 2. Capture consistent ZSET + per-partition offset manifest snapshot
        HttpResponse<String> snapResp = postEmpty("/api/admin/snapshots");
        assertEquals(200, snapResp.statusCode());
        LeaderboardSnapshot.SnapshotMetadata snapshot =
                mapper.readValue(snapResp.body(), LeaderboardSnapshot.SnapshotMetadata.class);
        assertEquals(30L, snapshot.totalPoints());
        assertEquals(2, snapshot.companyCount());
        assertTrue(snapshot.partitionOffsets().values().stream().anyMatch(offset -> offset >= 0L));
        assertEquals(snapshot.snapshotId(), getJson("/api/admin/snapshots/latest").path("snapshotId").asString());

        // Verify batched BROKER_ACK durability mode against real Kafka 4.4.0 broker acks=all
        InvoiceEventPublisher durablePublisher = new InvoiceEventPublisher(
                kafkaTemplate,
                "invoice-events",
                1000,
                rewardMetrics,
                PublishCircuitBreaker.noop(),
                InvoiceEventPublisher.AckMode.BROKER_ACK,
                5_000L
        );
        InvoiceBatchAcceptedResponse durableResp = durablePublisher.publishBatch(List.of(
                new InvoiceSimulationRequest("S3-DURABLE-1", "SNAP-CO-2"),
                new InvoiceSimulationRequest("S3-DURABLE-2", "SNAP-CO-2")
        ));
        assertEquals("DURABLE", durableResp.status());
        assertEquals(2, durableResp.acceptedCount());
        waitForPoints("SNAP-CO-2", 30L);

        // 3. Produce a Kafka transaction ABORT alongside a committed delta to verify read_committed & offset gap handling
        produceAbortedAndCommittedDeltas();
        waitForPoints("COMMIT-AFTER-ABORT", 50L);
        assertEquals(0L, getJson("/api/companies/ABORTED-CO/points").path("points").asLong());

        // 4. Ingest post-snapshot suffix invoices
        assertEquals(202, postJson("/api/invoices/batch", Map.of("invoices", List.of(
                Map.of("invoiceId", "S3-B2-1", "companyId", "SNAP-CO-1"),
                Map.of("invoiceId", "S3-B2-2", "companyId", "SNAP-CO-3")
        ))).statusCode());
        waitForPoints("SNAP-CO-1", 30L);
        waitForPoints("SNAP-CO-3", 10L);

        // 5. Simulate total Redis disaster wipe (FLUSHDB)
        redisTemplate.getConnectionFactory().getConnection().serverCommands().flushDb();
        assertEquals(0L, getJson("/api/companies/SNAP-CO-1/points").path("points").asLong());

        // 6. Execute snapshot + barrier Kafka suffix rebuild
        HttpResponse<String> rebuildResp = postEmpty("/api/admin/snapshots/rebuild?snapshotId=" + snapshot.snapshotId());
        assertEquals(200, rebuildResp.statusCode());
        LeaderboardSnapshot.RebuildReport report =
                mapper.readValue(rebuildResp.body(), LeaderboardSnapshot.RebuildReport.class);
        assertNotNull(report);
        assertEquals(snapshot.snapshotId(), report.snapshotId());
        assertEquals(false, report.coldReplayFromKafka());
        assertEquals(2, report.restoredCompanies());
        assertEquals(120L, report.totalPointsAfterRebuild());

        assertEquals(30L, getJson("/api/companies/SNAP-CO-1/points").path("points").asLong());
        assertEquals(30L, getJson("/api/companies/SNAP-CO-2/points").path("points").asLong());
        assertEquals(10L, getJson("/api/companies/SNAP-CO-3/points").path("points").asLong());
        assertEquals(50L, getJson("/api/companies/COMMIT-AFTER-ABORT/points").path("points").asLong());
        assertEquals(0L, getJson("/api/companies/ABORTED-CO/points").path("points").asLong());

        // 7. Replay pre-snapshot & suffix invoices after rebuild -> must not inflate points; fresh invoice adds +10
        assertEquals(202, postJson("/api/invoices/simulate", Map.of("invoiceId", "S3-B1-1", "companyId", "SNAP-CO-1")).statusCode());
        assertEquals(202, postJson("/api/invoices/simulate", Map.of("invoiceId", "S3-B2-1", "companyId", "SNAP-CO-1")).statusCode());
        assertEquals(202, postJson("/api/invoices/simulate", Map.of("invoiceId", "S3-POST-REBUILD", "companyId", "SNAP-POST")).statusCode());
        waitForPoints("SNAP-POST", 10L);
        assertEquals(30L, getJson("/api/companies/SNAP-CO-1/points").path("points").asLong());

        // 8. Simulate full cold rebuild directly from Kafka topic retention (no snapshot used)
        redisTemplate.getConnectionFactory().getConnection().serverCommands().flushDb();
        HttpResponse<String> coldResp = postEmpty("/api/admin/snapshots/rebuild?cold=true");
        assertEquals(200, coldResp.statusCode());
        LeaderboardSnapshot.RebuildReport coldReport =
                mapper.readValue(coldResp.body(), LeaderboardSnapshot.RebuildReport.class);
        assertEquals(true, coldReport.coldReplayFromKafka());
        assertEquals(130L, coldReport.totalPointsAfterRebuild());
        assertEquals(30L, getJson("/api/companies/SNAP-CO-1/points").path("points").asLong());
        assertEquals(10L, getJson("/api/companies/SNAP-POST/points").path("points").asLong());
        assertEquals(0L, getJson("/api/companies/ABORTED-CO/points").path("points").asLong());
    }

    private void produceAbortedAndCommittedDeltas() throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "chaos-abort-tx-" + System.nanoTime());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(
                props, new StringSerializer(), new StringSerializer())) {
            producer.initTransactions();

            producer.beginTransaction();
            CompanyRewardDelta aborted = new CompanyRewardDelta(
                    "chaos-aborted-1", "ABORTED-CO", 9_990L, Instant.now(), Instant.now(), 1);
            producer.send(new ProducerRecord<>("company-reward-deltas", "ABORTED-CO", mapper.writeValueAsString(aborted)));
            producer.flush();
            producer.abortTransaction();

            producer.beginTransaction();
            CompanyRewardDelta committed = new CompanyRewardDelta(
                    "chaos-committed-1", "COMMIT-AFTER-ABORT", 50L, Instant.now(), Instant.now(), 1);
            producer.send(new ProducerRecord<>("company-reward-deltas", "COMMIT-AFTER-ABORT", mapper.writeValueAsString(committed)));
            producer.commitTransaction();
        }
    }

    private void waitForPoints(String companyId, long expectedPoints) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(25).toNanos();
        long observed = -1;
        while (System.nanoTime() < deadline) {
            observed = getJson("/api/companies/" + companyId + "/points").path("points").asLong();
            if (observed == expectedPoints) {
                return;
            }
            Thread.sleep(50);
        }
        assertEquals(expectedPoints, observed, "Timed out waiting for points on " + companyId);
    }

    private HttpResponse<String> postJson(String path, Object payload) throws Exception {
        String json = mapper.writeValueAsString(payload);
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(5))
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> postEmpty(String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .timeout(Duration.ofSeconds(15))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private JsonNode getJson(String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build();
        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), "Unexpected status for GET " + path + ": " + response.body());
        return mapper.readTree(response.body());
    }
}
