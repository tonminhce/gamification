import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.vnpt.einvoice.gamification.event.InvoiceIssuedEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

public class KafkaLoadProducer {
    public static void main(String[] args) throws Exception {
        int total = args.length > 0 ? Integer.parseInt(args[0]) : 10_000;
        int companies = args.length > 1 ? Integer.parseInt(args[1]) : 20;
        String bootstrap = args.length > 2 ? args[2] : "localhost:9092";
        String topic = args.length > 3 ? args[3] : "invoice-events";
        String api = args.length > 4 ? args[4] : "http://localhost:8080";
        String runId = "DIRECT-" + System.currentTimeMillis();

        ObjectMapper mapper = JsonMapper.builder()
                .addModule(new JavaTimeModule())
                .build();
        HttpClient httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .build();

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 65_536);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67_108_864);

        JsonSerializer<InvoiceIssuedEvent> serializer = new JsonSerializer<>(mapper);
        serializer.setAddTypeInfo(true);
        List<Future<?>> futures = new ArrayList<>(total);
        long producerStart = System.nanoTime();
        try (KafkaProducer<String, InvoiceIssuedEvent> producer = new KafkaProducer<>(
                properties,
                new StringSerializer(),
                serializer
        )) {
            for (int i = 0; i < total; i++) {
                String invoiceId = runId + "-" + i;
                String companyId = runId + "-COMPANY-" + (i % companies);
                InvoiceIssuedEvent event = new InvoiceIssuedEvent(
                        UUID.randomUUID().toString(),
                        InvoiceIssuedEvent.TYPE,
                        InvoiceIssuedEvent.SCHEMA_VERSION,
                        invoiceId,
                        companyId,
                        Instant.now()
                );
                futures.add(producer.send(new ProducerRecord<>(topic, invoiceId, event)));
            }
            for (Future<?> future : futures) {
                future.get();
            }
        }
        long producerEnd = System.nanoTime();
        long pointsAtProducerEnd = totalPoints(httpClient, mapper, api);

        long expectedPoints = total * 10L;
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        long observedPoints;
        while (true) {
            observedPoints = totalPoints(httpClient, mapper, api);
            if (observedPoints >= expectedPoints || System.nanoTime() >= deadline) {
                break;
            }
            Thread.sleep(25);
        }
        long endToEndEnd = System.nanoTime();
        if (observedPoints != expectedPoints) {
            throw new IllegalStateException("Expected " + expectedPoints + " points, observed " + observedPoints);
        }

        for (int i = 0; i < 20; i++) {
            leaderboardRequest(httpClient, api);
        }
        List<Double> latencies = new ArrayList<>(200);
        for (int i = 0; i < 200; i++) {
            long start = System.nanoTime();
            leaderboardRequest(httpClient, api);
            latencies.add((System.nanoTime() - start) / 1_000_000.0);
        }
        latencies.sort(Comparator.naturalOrder());

        double producerSeconds = (producerEnd - producerStart) / 1_000_000_000.0;
        double endToEndSeconds = (endToEndEnd - producerStart) / 1_000_000_000.0;
        double drainSeconds = (endToEndEnd - producerEnd) / 1_000_000_000.0;
        long backlogAtProducerEnd = (expectedPoints - pointsAtProducerEnd) / 10L;
        double p95 = latencies.get((int) Math.ceil(latencies.size() * 0.95) - 1);

        System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(new Result(
                runId,
                total,
                companies,
                round(total / producerSeconds),
                round(total / endToEndSeconds),
                round(p95),
                expectedPoints,
                observedPoints,
                pointsAtProducerEnd,
                backlogAtProducerEnd,
                backlogAtProducerEnd > 0 ? round(backlogAtProducerEnd / drainSeconds) : null
        )));
    }

    private static long totalPoints(HttpClient client, ObjectMapper mapper, String api) throws Exception {
        JsonNode body = mapper.readTree(leaderboardRequest(client, api));
        long points = 0;
        for (JsonNode entry : body.path("entries")) {
            points += entry.path("points").asLong();
        }
        return points;
    }

    private static String leaderboardRequest(HttpClient client, String api) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(api + "/api/leaderboard/top?limit=100"))
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IllegalStateException("Leaderboard returned " + response.statusCode());
        }
        return response.body();
    }

    private static double round(double value) {
        return Math.round(value * 100.0) / 100.0;
    }

    private record Result(
            String runId,
            int total,
            int companies,
            double producerAckedEventsPerSecond,
            double directKafkaEndToEndEventsPerSecond,
            double leaderboardP95Ms,
            long expectedPoints,
            long observedPoints,
            long pointsAtProducerEnd,
            long backlogEventsAtProducerEnd,
            Double backlogDrainEventsPerSecond
    ) {
    }
}
