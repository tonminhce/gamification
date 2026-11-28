import com.vnpt.einvoice.gamification.event.InvoiceIssuedEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaLoadProducer {
    public static void main(String[] args) throws Exception {
        int total = args.length > 0 ? Integer.parseInt(args[0]) : 100_000;
        int companies = args.length > 1 ? Integer.parseInt(args[1]) : 20;
        String bootstrap = args.length > 2 ? args[2] : "localhost:9092";
        String topic = args.length > 3 ? args[3] : "invoice-events";
        String api = args.length > 4 ? args[4] : "http://localhost:8080";
        int duplicateReplayCount = args.length > 5 ? Integer.parseInt(args[5]) : Math.min(10_000, total);
        int concurrentReadThreads = args.length > 6 ? Integer.parseInt(args[6]) : 8;
        if (companies < 1 || companies > 100) {
            throw new IllegalArgumentException("companies must be between 1 and 100");
        }
        String runId = "DIRECT-" + System.currentTimeMillis();

        JsonMapper mapper = JsonMapper.builder().build();
        HttpClient httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .build();

        long baselinePoints = totalPoints(httpClient, mapper, api);

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 65_536);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67_108_864);

        JacksonJsonSerializer<InvoiceIssuedEvent> serializer =
                new JacksonJsonSerializer<InvoiceIssuedEvent>(mapper).noTypeInfo();

        AtomicBoolean loadRunning = new AtomicBoolean(true);
        List<Double> concurrentReadLatencies = new CopyOnWriteArrayList<>();
        ExecutorService readerPool = Executors.newFixedThreadPool(concurrentReadThreads);
        for (int r = 0; r < concurrentReadThreads; r++) {
            readerPool.submit(() -> {
                while (loadRunning.get()) {
                    try {
                        long t0 = System.nanoTime();
                        getString(httpClient, api + "/api/leaderboard/top?limit=100");
                        concurrentReadLatencies.add((System.nanoTime() - t0) / 1_000_000.0);
                        Thread.sleep(5);
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception ignored) {
                    }
                }
            });
        }

        List<Future<?>> futures = new ArrayList<>(total);
        long producerStart = System.nanoTime();
        try (KafkaProducer<String, InvoiceIssuedEvent> producer = new KafkaProducer<>(
                properties,
                new StringSerializer(),
                serializer
        )) {
            Instant batchTs = Instant.now();
            ThreadLocalRandom rng = ThreadLocalRandom.current();
            for (int i = 0; i < total; i++) {
                String invoiceId = runId + "-" + i;
                String companyId = runId + "-COMPANY-" + (i % companies);
                InvoiceIssuedEvent event = new InvoiceIssuedEvent(
                        fastUuid(rng),
                        InvoiceIssuedEvent.TYPE,
                        InvoiceIssuedEvent.SCHEMA_VERSION,
                        invoiceId,
                        companyId,
                        batchTs
                );
                futures.add(producer.send(new ProducerRecord<>(topic, invoiceId, event)));
            }
            for (Future<?> future : futures) {
                future.get();
            }
        }
        long producerEnd = System.nanoTime();
        long pointsAtProducerEnd = totalPoints(httpClient, mapper, api) - baselinePoints;

        long expectedAddedPoints = total * 10L;
        long expectedTotalPoints = baselinePoints + expectedAddedPoints;
        long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
        long observedTotalPoints;
        while (true) {
            observedTotalPoints = totalPoints(httpClient, mapper, api);
            if (observedTotalPoints >= expectedTotalPoints || System.nanoTime() >= deadline) {
                break;
            }
            Thread.sleep(25);
        }
        long endToEndEnd = System.nanoTime();
        loadRunning.set(false);
        readerPool.shutdownNow();

        if (observedTotalPoints != expectedTotalPoints) {
            throw new IllegalStateException("Expected " + expectedTotalPoints + " points, observed " + observedTotalPoints);
        }

        if (duplicateReplayCount > 0) {
            double dupBaseline = dedupeDuplicateCount(httpClient, mapper, api);
            List<Future<?>> dupFutures = new ArrayList<>(duplicateReplayCount);
            try (KafkaProducer<String, InvoiceIssuedEvent> dupProducer = new KafkaProducer<>(
                    properties,
                    new StringSerializer(),
                    new JacksonJsonSerializer<InvoiceIssuedEvent>(mapper).noTypeInfo()
            )) {
                Instant dupTs = Instant.now();
                ThreadLocalRandom rng = ThreadLocalRandom.current();
                for (int i = 0; i < duplicateReplayCount; i++) {
                    String invoiceId = runId + "-" + i;
                    String companyId = runId + "-COMPANY-" + (i % companies);
                    InvoiceIssuedEvent event = new InvoiceIssuedEvent(
                            fastUuid(rng),
                            InvoiceIssuedEvent.TYPE,
                            InvoiceIssuedEvent.SCHEMA_VERSION,
                            invoiceId,
                            companyId,
                            dupTs
                    );
                    dupFutures.add(dupProducer.send(new ProducerRecord<>(topic, invoiceId, event)));
                }
                for (Future<?> f : dupFutures) {
                    f.get();
                }
            }
            long dupDeadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (dedupeDuplicateCount(httpClient, mapper, api) < dupBaseline + duplicateReplayCount
                    && System.nanoTime() < dupDeadline) {
                Thread.sleep(50);
            }
            Thread.sleep(700);
            long afterDupPoints = totalPoints(httpClient, mapper, api);
            if (afterDupPoints != expectedTotalPoints) {
                throw new IllegalStateException("Duplicate replay inflated points: " + afterDupPoints);
            }
        }

        for (int i = 0; i < 20; i++) {
            getString(httpClient, api + "/api/leaderboard/top?limit=100");
        }
        List<Double> postDrainLatencies = new ArrayList<>(200);
        for (int i = 0; i < 200; i++) {
            long start = System.nanoTime();
            getString(httpClient, api + "/api/leaderboard/top?limit=100");
            postDrainLatencies.add((System.nanoTime() - start) / 1_000_000.0);
        }
        postDrainLatencies.sort(Comparator.naturalOrder());

        List<Double> sortedConcurrent = new ArrayList<>(concurrentReadLatencies);
        sortedConcurrent.sort(Comparator.naturalOrder());

        double producerSeconds = (producerEnd - producerStart) / 1_000_000_000.0;
        double endToEndSeconds = (endToEndEnd - producerStart) / 1_000_000_000.0;
        double drainSeconds = (endToEndEnd - producerEnd) / 1_000_000_000.0;
        long backlogAtProducerEnd = (expectedAddedPoints - pointsAtProducerEnd) / 10L;

        System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(new Result(
                runId,
                total,
                companies,
                round(total / producerSeconds),
                round(total / endToEndSeconds),
                round(percentile(postDrainLatencies, 0.95)),
                sortedConcurrent.isEmpty() ? null : round(percentile(sortedConcurrent, 0.95)),
                sortedConcurrent.isEmpty() ? null : round(percentile(sortedConcurrent, 0.99)),
                sortedConcurrent.size(),
                expectedAddedPoints,
                observedTotalPoints - baselinePoints,
                pointsAtProducerEnd,
                backlogAtProducerEnd,
                backlogAtProducerEnd > 0 ? round(backlogAtProducerEnd / drainSeconds) : null,
                duplicateReplayCount
        )));
    }

    private static String fastUuid(ThreadLocalRandom rng) {
        long msb = (rng.nextLong() & 0xFFFFFFFFFFFF0FFFL) | 0x0000000000004000L;
        long lsb = (rng.nextLong() & 0x3FFFFFFFFFFFFFFFL) | 0x8000000000000000L;
        return new UUID(msb, lsb).toString();
    }

    private static double percentile(List<Double> sorted, double p) {
        return sorted.get(Math.min(sorted.size() - 1, Math.max(0, (int) Math.ceil(sorted.size() * p) - 1)));
    }

    private static double dedupeDuplicateCount(HttpClient client, JsonMapper mapper, String api) throws Exception {
        JsonNode body = mapper.readTree(getString(client, api + "/actuator/metrics/gamification.streams.dedupe.duplicate"));
        return body.path("measurements").get(0).path("value").asDouble();
    }

    private static long totalPoints(HttpClient client, JsonMapper mapper, String api) throws Exception {
        JsonNode body = mapper.readTree(getString(client, api + "/api/leaderboard/top?limit=100"));
        long points = 0;
        for (JsonNode entry : body.path("entries")) {
            points += entry.path("points").asLong();
        }
        return points;
    }

    private static String getString(HttpClient client, String url) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IllegalStateException(url + " returned " + response.statusCode());
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
            double postDrainLeaderboardP95Ms,
            Double underLoadLeaderboardP95Ms,
            Double underLoadLeaderboardP99Ms,
            int underLoadReadSamples,
            long expectedAddedPoints,
            long observedAddedPoints,
            long pointsAtProducerEnd,
            long backlogEventsAtProducerEnd,
            Double backlogDrainEventsPerSecond,
            int duplicateReplayVerified
    ) {
    }
}
