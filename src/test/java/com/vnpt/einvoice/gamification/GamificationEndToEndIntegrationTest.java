package com.vnpt.einvoice.gamification;

import com.vnpt.einvoice.gamification.event.InvoiceIssuedEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class GamificationEndToEndIntegrationTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:4.4.0");

    @Container
    static GenericContainer<?> redis = new GenericContainer<>("redis:7.4-alpine")
            .withExposedPorts(6379);

    @TempDir
    static Path streamsStateDir;

    @LocalServerPort
    int port;

    private final HttpClient http = HttpClient.newHttpClient();
    private final JsonMapper mapper = JsonMapper.builder().build();

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
        registry.add("app.kafka.partitions", () -> 4);
        registry.add("app.streams.threads", () -> 2);
        registry.add("app.streams.commit-interval-ms", () -> 200);
        registry.add("app.streams.state-dir", () -> streamsStateDir.toAbsolutePath().toString());
        registry.add("app.streams.rocksdb-block-cache-mb", () -> 32);
        registry.add("app.streams.rocksdb-write-buffer-mb", () -> 16);
        registry.add("app.dedupe.bloom-expected-insertions", () -> 10_000);
        registry.add("app.window.size-ms", () -> 250);
        registry.add("app.window.punctuation-ms", () -> 50);
        registry.add("spring.kafka.listener.concurrency", () -> 4);
        registry.add("app.leaderboard.top-cache-ttl-ms", () -> 0);
    }

    @Test
    void fullPipelineEndToEndDedupeRankingConflictDltAndMetrics() throws Exception {
        // 1. Single invoice via REST
        HttpResponse<String> singleResp = postJson(
                "/api/invoices/simulate",
                Map.of("invoiceId", "E2E-SINGLE-1", "companyId", "E2E-GOLD")
        );
        assertEquals(202, singleResp.statusCode());

        // 2. Batch ingestion via REST: 9 more for E2E-GOLD (total 10 => 100 pts => 5% discount),
        //    plus 5 each for E2E-TIE-A and E2E-TIE-B (50 pts each => tied at rank 2).
        List<Map<String, String>> batch = new ArrayList<>();
        for (int i = 2; i <= 10; i++) {
            batch.add(Map.of("invoiceId", "E2E-GOLD-" + i, "companyId", "E2E-GOLD"));
        }
        for (int i = 1; i <= 5; i++) {
            batch.add(Map.of("invoiceId", "E2E-TIE-A-" + i, "companyId", "E2E-TIE-A"));
            batch.add(Map.of("invoiceId", "E2E-TIE-B-" + i, "companyId", "E2E-TIE-B"));
        }
        HttpResponse<String> batchResp = postJson("/api/invoices/batch", Map.of("invoices", batch));
        assertEquals(202, batchResp.statusCode());
        assertEquals(19, mapper.readTree(batchResp.body()).path("acceptedCount").asInt());

        // Wait for Streams window aggregation + Projector -> Redis convergence.
        waitForPoints("E2E-GOLD", 100L);
        waitForPoints("E2E-TIE-A", 50L);
        waitForPoints("E2E-TIE-B", 50L);

        // 3. Verify leaderboard competition ties, individual summary, and discount tiers
        JsonNode top = getJson("/api/leaderboard/top?limit=10");
        assertEquals(3, top.path("entries").size());
        assertEquals(1L, top.path("entries").get(0).path("rank").asLong());
        assertEquals("E2E-GOLD", top.path("entries").get(0).path("companyId").asString());
        assertEquals(2L, top.path("entries").get(1).path("rank").asLong());
        assertEquals(2L, top.path("entries").get(2).path("rank").asLong());

        JsonNode goldSummary = getJson("/api/companies/E2E-GOLD/summary");
        assertEquals(100L, goldSummary.path("points").asLong());
        assertEquals(1L, goldSummary.path("rank").asLong());
        assertEquals(5, goldSummary.path("discountPercent").asInt());

        JsonNode tieRank = getJson("/api/companies/E2E-TIE-A/rank");
        assertEquals(2L, tieRank.path("rank").asLong());

        // 4. Replay exact duplicates via both endpoints -> points must not increase
        postJson("/api/invoices/simulate", Map.of("invoiceId", "E2E-SINGLE-1", "companyId", "E2E-GOLD"));
        postJson("/api/invoices/batch", Map.of("invoices", List.of(
                Map.of("invoiceId", "E2E-GOLD-2", "companyId", "E2E-GOLD"),
                Map.of("invoiceId", "E2E-TIE-A-1", "companyId", "E2E-TIE-A")
        )));

        // 5. Send conflicting companyId for existing invoiceId + invalid event + malformed JSON directly to Kafka
        postJson("/api/invoices/simulate", Map.of("invoiceId", "E2E-SINGLE-1", "companyId", "E2E-HIJACKER"));
        sendInvalidAndPoisonEventsToKafka();

        // Trigger a fresh valid invoice so we prove the pipeline stays healthy after DLT and poison-pill records
        postJson("/api/invoices/simulate", Map.of("invoiceId", "E2E-FRESH-AFTER", "companyId", "E2E-FRESH"));
        waitForPoints("E2E-FRESH", 10L);

        // Confirm duplicate & conflict did NOT inflate points
        assertEquals(100L, getJson("/api/companies/E2E-GOLD/points").path("points").asLong());
        assertEquals(50L, getJson("/api/companies/E2E-TIE-A/points").path("points").asLong());
        assertEquals(0L, getJson("/api/companies/E2E-HIJACKER/points").path("points").asLong());

        // 6. Verify both invalid event and conflict reached invoice-events.DLT
        List<ConsumerRecord<String, String>> dltRecords = drainDltRecords(2);
        assertTrue(dltRecords.size() >= 2);
        boolean sawConflictHeader = dltRecords.stream().anyMatch(rec -> {
            var header = rec.headers().lastHeader("x-dlt-reason");
            return header != null && "INVOICE_COMPANY_CONFLICT".equals(new String(header.value(), StandardCharsets.UTF_8));
        });
        assertTrue(sawConflictHeader, "Expected conflict record with x-dlt-reason header in DLT");

        // 7. Verify Actuator health and Micrometer dedupe/projector metrics
        JsonNode health = getJson("/actuator/health");
        assertEquals("UP", health.path("status").asString());

        JsonNode dupMetric = getJson("/actuator/metrics/gamification.streams.dedupe.duplicate");
        assertTrue(dupMetric.path("measurements").get(0).path("value").asDouble() >= 3.0);

        JsonNode conflictMetric = getJson("/actuator/metrics/gamification.streams.dedupe.conflict");
        assertTrue(conflictMetric.path("measurements").get(0).path("value").asDouble() >= 1.0);
    }

    private void sendInvalidAndPoisonEventsToKafka() throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        try (KafkaProducer<String, String> rawProducer = new KafkaProducer<>(
                props,
                new StringSerializer(),
                new StringSerializer()
        )) {
            InvoiceIssuedEvent bad = new InvoiceIssuedEvent(
                    "bad-evt-1",
                    "UNKNOWN_TYPE",
                    1,
                    "E2E-BAD-1",
                    "E2E-BAD-CO",
                    Instant.now()
            );
            rawProducer.send(new ProducerRecord<>("invoice-events", "E2E-BAD-1", mapper.writeValueAsString(bad))).get();
            rawProducer.send(new ProducerRecord<>("invoice-events", "E2E-POISON", "{not-valid-json")).get();
        }
    }

    private List<ConsumerRecord<String, String>> drainDltRecords(int minExpected) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "e2e-dlt-verifier-" + System.nanoTime());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        List<ConsumerRecord<String, String>> collected = new ArrayList<>();
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("invoice-events.DLT"));
            while (collected.size() < minExpected && System.nanoTime() < deadline) {
                ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(250));
                polled.forEach(collected::add);
            }
        }
        return collected;
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
