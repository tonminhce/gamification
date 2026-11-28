import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class RestLoadProducer {
    public static void main(String[] args) throws Exception {
        int total = args.length > 0 ? Integer.parseInt(args[0]) : 10_000;
        int concurrency = args.length > 1 ? Integer.parseInt(args[1]) : 200;
        int companies = args.length > 2 ? Integer.parseInt(args[2]) : 20;
        String api = args.length > 3 ? args[3] : "http://localhost:8080";
        int batchSize = args.length > 4 ? Integer.parseInt(args[4]) : 1;
        if (companies < 1 || companies > 100) {
            throw new IllegalArgumentException("companies must be between 1 and 100");
        }
        String runId = "REST-" + System.currentTimeMillis();

        JsonMapper mapper = JsonMapper.builder().build();
        HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .build();

        long baselinePoints = totalPoints(client, mapper, api);
        String endpoint = batchSize > 1 ? "/api/invoices/batch" : "/api/invoices/simulate";

        List<byte[]> bodies = new ArrayList<>();
        if (batchSize <= 1) {
            for (int i = 0; i < total; i++) {
                String invoiceId = runId + "-" + i;
                String companyId = runId + "-COMPANY-" + (i % companies);
                bodies.add(mapper.writeValueAsBytes(Map.of(
                        "invoiceId", invoiceId,
                        "companyId", companyId
                )));
            }
        } else {
            for (int startIdx = 0; startIdx < total; startIdx += batchSize) {
                int endIdx = Math.min(total, startIdx + batchSize);
                List<Map<String, String>> invoices = new ArrayList<>(endIdx - startIdx);
                for (int i = startIdx; i < endIdx; i++) {
                    invoices.add(Map.of(
                            "invoiceId", runId + "-" + i,
                            "companyId", runId + "-COMPANY-" + (i % companies)
                    ));
                }
                bodies.add(mapper.writeValueAsBytes(Map.of("invoices", invoices)));
            }
        }

        int requestCount = bodies.size();
        AtomicInteger nextIndex = new AtomicInteger();
        AtomicInteger overload503Responses = new AtomicInteger();
        long start = System.nanoTime();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<Void>> futures = new ArrayList<>(Math.min(concurrency, requestCount));
            for (int worker = 0; worker < Math.min(concurrency, requestCount); worker++) {
                futures.add(executor.submit(() -> {
                    while (true) {
                        int index = nextIndex.getAndIncrement();
                        if (index >= requestCount) {
                            return null;
                        }
                        byte[] payload = bodies.get(index);
                        int attempts = 0;
                        while (true) {
                            HttpRequest request = HttpRequest.newBuilder(URI.create(api + endpoint))
                                    .timeout(Duration.ofSeconds(10))
                                    .header("content-type", "application/json")
                                    .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
                                    .build();
                            HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());
                            if (response.statusCode() == 202) {
                                break;
                            }
                            if (response.statusCode() == 503 && attempts < 20) {
                                overload503Responses.incrementAndGet();
                                attempts++;
                                Thread.sleep(50L + (attempts * 25L));
                                continue;
                            }
                            throw new IllegalStateException("Expected 202, received " + response.statusCode());
                        }
                    }
                }));
            }
            for (Future<Void> future : futures) {
                future.get();
            }
        }
        long publishEnd = System.nanoTime();

        long expectedTotal = baselinePoints + total * 10L;
        long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        long observedPoints;
        while (true) {
            observedPoints = totalPoints(client, mapper, api);
            if (observedPoints >= expectedTotal || System.nanoTime() >= deadline) {
                break;
            }
            Thread.sleep(25);
        }
        long end = System.nanoTime();
        if (observedPoints != expectedTotal) {
            throw new IllegalStateException("Expected " + expectedTotal + " points, observed " + observedPoints);
        }

        for (int i = 0; i < 20; i++) {
            leaderboardRequest(client, api);
        }
        List<Double> latencies = new ArrayList<>(200);
        for (int i = 0; i < 200; i++) {
            long sampleStart = System.nanoTime();
            leaderboardRequest(client, api);
            latencies.add((System.nanoTime() - sampleStart) / 1_000_000.0);
        }
        latencies.sort(Comparator.naturalOrder());

        double publishSeconds = (publishEnd - start) / 1_000_000_000.0;
        double endToEndSeconds = (end - start) / 1_000_000_000.0;
        double p95 = latencies.get((int) Math.ceil(latencies.size() * 0.95) - 1);

        System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(new Result(
                runId,
                total,
                batchSize,
                concurrency,
                companies,
                round(total / publishSeconds),
                round(total / endToEndSeconds),
                round(p95),
                overload503Responses.get(),
                total * 10L,
                observedPoints - baselinePoints
        )));
    }

    private static long totalPoints(HttpClient client, JsonMapper mapper, String api) throws Exception {
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
            int batchSize,
            int concurrency,
            int companies,
            double apiAcceptedEventsPerSecond,
            double restEndToEndEventsPerSecond,
            double leaderboardP95Ms,
            int overload503Responses,
            long expectedAddedPoints,
            long observedAddedPoints
    ) {
    }
}
