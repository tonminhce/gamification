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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implements the full Stage 3 Benchmark Contract:
 * separates offered, accepted, durable, processed, and applied rates alongside
 * freshness p50/p95/p99, concurrent leaderboard read tail latencies, overload retries,
 * dropped iterations, and duplicate replay verification.
 */
public class Stage3LoadBenchmark {

    public static void main(String[] args) throws Exception {
        int total = args.length > 0 ? Integer.parseInt(args[0]) : 300_000;
        int batchSize = args.length > 1 ? Integer.parseInt(args[1]) : 1_000;
        int concurrency = args.length > 2 ? Integer.parseInt(args[2]) : 24;
        int companies = args.length > 3 ? Integer.parseInt(args[3]) : 20;
        String api = args.length > 4 ? args[4] : "http://localhost:18080";
        int duplicateReplay = args.length > 5 ? Integer.parseInt(args[5]) : 2_000;
        int readerThreads = args.length > 6 ? Integer.parseInt(args[6]) : 8;

        if (companies < 1 || companies > 80) {
            throw new IllegalArgumentException("companies must be between 1 and 80 (leaving room in top-100 for probes)");
        }

        String runId = "S3-" + System.currentTimeMillis();
        JsonMapper mapper = JsonMapper.builder().build();
        HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .build();

        long baselineTopPoints = totalTopPoints(client, mapper, api);
        double baseAccepted = metricValue(client, mapper, api, "gamification.invoice.accepted");
        double baseDurable = metricValue(client, mapper, api, "gamification.invoice.published");
        double baseProcessed = metricValue(client, mapper, api, "gamification.streams.dedupe.unique");
        double baseDuplicate = metricValue(client, mapper, api, "gamification.streams.dedupe.duplicate");

        List<byte[]> batches = buildPayloads(mapper, runId, total, batchSize, companies);
        int requestCount = batches.size();

        AtomicBoolean loadActive = new AtomicBoolean(true);
        AtomicBoolean probingActive = new AtomicBoolean(true);
        List<Double> concurrentReadMs = new CopyOnWriteArrayList<>();
        List<Double> freshnessMs = new CopyOnWriteArrayList<>();
        AtomicInteger probeSeq = new AtomicInteger();
        AtomicInteger acceptedProbes = new AtomicInteger();

        try (ExecutorService readers = Executors.newFixedThreadPool(readerThreads);
             ExecutorService probeExecutor = Executors.newVirtualThreadPerTaskExecutor();
             ExecutorService writers = Executors.newVirtualThreadPerTaskExecutor()) {

            for (int r = 0; r < readerThreads; r++) {
                readers.submit(() -> {
                    while (loadActive.get()) {
                        try {
                            long t0 = System.nanoTime();
                            getJson(client, mapper, api + "/api/leaderboard/top?limit=100");
                            concurrentReadMs.add((System.nanoTime() - t0) / 1_000_000.0);
                            Thread.sleep(5);
                        } catch (InterruptedException ignored) {
                            Thread.currentThread().interrupt();
                            break;
                        } catch (Exception ignored) {
                        }
                    }
                });
            }

            Future<?> freshnessProbeTask = probeExecutor.submit(() -> {
                while (probingActive.get() && probeSeq.get() < 15) {
                    int idx = probeSeq.incrementAndGet();
                    String probeCompany = runId + "-PROBE-" + idx;
                    String probeInv = runId + "-PROBE-INV-" + idx;
                    try {
                        byte[] body = mapper.writeValueAsBytes(Map.of(
                                "invoiceId", probeInv,
                                "companyId", probeCompany
                        ));
                        long t0 = System.nanoTime();
                        HttpResponse<String> ack = postBytes(client, api + "/api/invoices/simulate", body);
                        if (ack.statusCode() == 202) {
                            acceptedProbes.incrementAndGet();
                            long probeDeadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
                            while (System.nanoTime() < probeDeadline) {
                                long pts = getJson(client, mapper, api + "/api/companies/" + probeCompany + "/points")
                                        .path("points").asLong();
                                if (pts == 10L) {
                                    freshnessMs.add((System.nanoTime() - t0) / 1_000_000.0);
                                    break;
                                }
                                Thread.sleep(20);
                            }
                        }
                        if (!probingActive.get()) {
                            break;
                        }
                        Thread.sleep(50);
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception ignored) {
                    }
                }
            });

            AtomicInteger nextBatch = new AtomicInteger();
            AtomicLong offeredEvents = new AtomicLong();
            AtomicLong acceptedEvents = new AtomicLong();
            AtomicInteger overload503 = new AtomicInteger();
            AtomicInteger droppedIterations = new AtomicInteger();

            long startNanos = System.nanoTime();
            List<Future<Void>> workerFutures = new ArrayList<>(Math.min(concurrency, requestCount));
            for (int w = 0; w < Math.min(concurrency, requestCount); w++) {
                workerFutures.add(writers.submit(() -> {
                    while (true) {
                        int idx = nextBatch.getAndIncrement();
                        if (idx >= requestCount) {
                            return null;
                        }
                        byte[] payload = batches.get(idx);
                        int batchLen = Math.min(batchSize, total - idx * batchSize);
                        offeredEvents.addAndGet(batchLen);
                        if (!sendWithJitteredRetry(client, api + "/api/invoices/batch", payload, overload503)) {
                            droppedIterations.incrementAndGet();
                        } else {
                            acceptedEvents.addAndGet(batchLen);
                        }
                    }
                }));
            }
            for (Future<Void> workerFuture : workerFutures) {
                workerFuture.get();
            }
            long offerEndNanos = System.nanoTime();
            probingActive.set(false);
            freshnessProbeTask.get();

            int probesSent = acceptedProbes.get();
            long expectedPoints = (total + probesSent) * 10L;
            long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
            long observedPoints;
            while (true) {
                observedPoints = totalTopPoints(client, mapper, api) - baselineTopPoints;
                if (observedPoints >= expectedPoints || System.nanoTime() >= deadline) {
                    break;
                }
                Thread.sleep(25);
            }
            long appliedEndNanos = System.nanoTime();
            loadActive.set(false);

            if (observedPoints != expectedPoints) {
                throw new IllegalStateException("Expected added points " + expectedPoints + " but observed " + observedPoints);
            }

            if (duplicateReplay > 0) {
                verifyDuplicates(client, mapper, api, runId, companies, duplicateReplay, baselineTopPoints + expectedPoints);
            }

            List<Double> postDrainReadMs = measurePostDrainReads(client, mapper, api);
            double afterAccepted = metricValue(client, mapper, api, "gamification.invoice.accepted") - baseAccepted;
            double afterDurable = metricValue(client, mapper, api, "gamification.invoice.published") - baseDurable;
            double afterProcessed = metricValue(client, mapper, api, "gamification.streams.dedupe.unique") - baseProcessed;
            double afterDuplicate = metricValue(client, mapper, api, "gamification.streams.dedupe.duplicate") - baseDuplicate;

            double offerSeconds = (offerEndNanos - startNanos) / 1_000_000_000.0;
            double e2eSeconds = (appliedEndNanos - startNanos) / 1_000_000_000.0;

            List<Double> sortedReads = sortedCopy(concurrentReadMs);
            List<Double> sortedFreshness = sortedCopy(freshnessMs);
            List<Double> sortedPostDrain = sortedCopy(postDrainReadMs);

            Map<String, Object> report = Map.ofEntries(
                    Map.entry("runId", runId),
                    Map.entry("offeredTotal", offeredEvents.get()),
                    Map.entry("acceptedTotal", acceptedEvents.get()),
                    Map.entry("probeEvents", probesSent),
                    Map.entry("offeredEventsPerSecond", round2(offeredEvents.get() / offerSeconds)),
                    Map.entry("acceptedEventsPerSecond", round2(afterAccepted / offerSeconds)),
                    Map.entry("durableEventsPerSecond", round2(afterDurable / e2eSeconds)),
                    Map.entry("processedEventsPerSecond", round2(afterProcessed / e2eSeconds)),
                    Map.entry("appliedEventsPerSecond", round2((total + probesSent) / e2eSeconds)),
                    Map.entry("freshnessSamples", sortedFreshness.size()),
                    Map.entry("freshnessP50Ms", round2(percentile(sortedFreshness, 0.50))),
                    Map.entry("freshnessP95Ms", round2(percentile(sortedFreshness, 0.95))),
                    Map.entry("freshnessP99Ms", round2(percentile(sortedFreshness, 0.99))),
                    Map.entry("underLoadReadSamples", sortedReads.size()),
                    Map.entry("underLoadReadP50Ms", round2(percentile(sortedReads, 0.50))),
                    Map.entry("underLoadReadP95Ms", round2(percentile(sortedReads, 0.95))),
                    Map.entry("underLoadReadP99Ms", round2(percentile(sortedReads, 0.99))),
                    Map.entry("postDrainReadP95Ms", round2(percentile(sortedPostDrain, 0.95))),
                    Map.entry("overload503Responses", overload503.get()),
                    Map.entry("droppedIterations", droppedIterations.get()),
                    Map.entry("expectedAddedPoints", expectedPoints),
                    Map.entry("observedAddedPoints", observedPoints),
                    Map.entry("duplicateReplayVerified", (long) afterDuplicate)
            );
            System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(report));
        }
    }

    private static List<byte[]> buildPayloads(
            JsonMapper mapper,
            String runId,
            int total,
            int batchSize,
            int companies
    ) {
        List<byte[]> batches = new ArrayList<>((total + batchSize - 1) / batchSize);
        for (int start = 0; start < total; start += batchSize) {
            int end = Math.min(total, start + batchSize);
            List<Map<String, String>> invoices = new ArrayList<>(end - start);
            for (int i = start; i < end; i++) {
                invoices.add(Map.of(
                        "invoiceId", runId + "-" + i,
                        "companyId", runId + "-CO-" + (i % companies)
                ));
            }
            batches.add(mapper.writeValueAsBytes(Map.of("invoices", invoices)));
        }
        return batches;
    }

    private static boolean sendWithJitteredRetry(
            HttpClient client,
            String uri,
            byte[] payload,
            AtomicInteger overload503
    ) throws Exception {
        for (int attempt = 0; attempt < 25; attempt++) {
            HttpResponse<String> response = postBytes(client, uri, payload);
            if (response.statusCode() == 202) {
                return true;
            }
            if (response.statusCode() == 503) {
                overload503.incrementAndGet();
                long baseMs = Math.min(800L, 40L * (1L << Math.min(4, attempt)));
                long jitterMs = ThreadLocalRandom.current().nextLong(10L, 40L);
                Thread.sleep(baseMs + jitterMs);
                continue;
            }
            throw new IllegalStateException("Unexpected HTTP status " + response.statusCode() + ": " + response.body());
        }
        return false;
    }

    private static void verifyDuplicates(
            HttpClient client,
            JsonMapper mapper,
            String api,
            String runId,
            int companies,
            int duplicateReplay,
            long expectedTotalPoints
    ) throws Exception {
        double dupBaseline = metricValue(client, mapper, api, "gamification.streams.dedupe.duplicate");
        List<byte[]> replayPayloads = new ArrayList<>();
        for (int start = 0; start < duplicateReplay; start += 500) {
            int end = Math.min(duplicateReplay, start + 500);
            List<Map<String, String>> invoices = new ArrayList<>(end - start);
            for (int i = start; i < end; i++) {
                invoices.add(Map.of(
                        "invoiceId", runId + "-" + i,
                        "companyId", runId + "-CO-" + (i % companies)
                ));
            }
            replayPayloads.add(mapper.writeValueAsBytes(Map.of("invoices", invoices)));
        }
        AtomicInteger dummy = new AtomicInteger();
        for (byte[] payload : replayPayloads) {
            sendWithJitteredRetry(client, api + "/api/invoices/batch", payload, dummy);
        }
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (metricValue(client, mapper, api, "gamification.streams.dedupe.duplicate") < dupBaseline + duplicateReplay
                && System.nanoTime() < deadline) {
            Thread.sleep(50);
        }
        Thread.sleep(600);
        long pointsAfter = totalTopPoints(client, mapper, api);
        if (pointsAfter != expectedTotalPoints) {
            throw new IllegalStateException("Duplicate replay altered points: expected=" + expectedTotalPoints + " actual=" + pointsAfter);
        }
    }

    private static List<Double> measurePostDrainReads(HttpClient client, JsonMapper mapper, String api) throws Exception {
        for (int i = 0; i < 20; i++) {
            getJson(client, mapper, api + "/api/leaderboard/top?limit=100");
        }
        List<Double> latencies = new ArrayList<>(200);
        for (int i = 0; i < 200; i++) {
            long t0 = System.nanoTime();
            getJson(client, mapper, api + "/api/leaderboard/top?limit=100");
            latencies.add((System.nanoTime() - t0) / 1_000_000.0);
        }
        return latencies;
    }

    private static HttpResponse<String> postBytes(HttpClient client, String uri, byte[] payload) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(uri))
                .timeout(Duration.ofSeconds(10))
                .header("content-type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
                .build();
        return client.send(req, HttpResponse.BodyHandlers.ofString());
    }

    private static JsonNode getJson(HttpClient client, JsonMapper mapper, String uri) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(uri))
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build();
        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new IllegalStateException("GET " + uri + " failed: status=" + resp.statusCode());
        }
        return mapper.readTree(resp.body());
    }

    private static long totalTopPoints(HttpClient client, JsonMapper mapper, String api) throws Exception {
        JsonNode root = getJson(client, mapper, api + "/api/leaderboard/top?limit=100");
        long sum = 0L;
        for (JsonNode entry : root.path("entries")) {
            sum += entry.path("points").asLong();
        }
        return sum;
    }

    private static double metricValue(HttpClient client, JsonMapper mapper, String api, String name) throws Exception {
        JsonNode node = getJson(client, mapper, api + "/actuator/metrics/" + name);
        return node.path("measurements").get(0).path("value").asDouble(0.0);
    }

    private static List<Double> sortedCopy(List<Double> values) {
        List<Double> copy = new ArrayList<>(values);
        copy.sort(Comparator.naturalOrder());
        return copy;
    }

    private static double percentile(List<Double> sorted, double q) {
        if (sorted.isEmpty()) {
            return 0.0;
        }
        int idx = Math.min(sorted.size() - 1, Math.max(0, (int) Math.ceil(q * sorted.size()) - 1));
        return sorted.get(idx);
    }

    private static double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
