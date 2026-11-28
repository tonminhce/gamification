package com.vnpt.einvoice.gamification.snapshot;

import com.vnpt.einvoice.gamification.event.CompanyRewardDelta;
import com.vnpt.einvoice.gamification.leaderboard.LeaderboardApplyService;
import com.vnpt.einvoice.gamification.metrics.RewardMetrics;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.DefaultTypedTuple;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Stage 3 disaster-recovery service implementing consistent ZSET + partition-offset manifest
 * snapshots and deterministic rebuilds from snapshot + {@code company-reward-deltas} replay.
 */
@Service
public class LeaderboardSnapshotService {

    private static final Logger log = LoggerFactory.getLogger(LeaderboardSnapshotService.class);
    private static final String CHECKPOINT_PREFIX = "einv:projector-checkpoint:";
    private static final String MARKER_PREFIX = "einv:delta-batch:";
    private static final long SECONDS_PER_DAY = 86_400L;
    private static final String CAPTURE_LUA = """
            local partitionCount = tonumber(ARGV[1])
            local checkpoints = {}
            for i = 1, partitionCount do
                checkpoints[i] = redis.call('GET', KEYS[1 + i]) or '-1'
            end
            local entries = redis.call('ZREVRANGE', KEYS[1], 0, -1, 'WITHSCORES')
            return { checkpoints, entries }
            """;

    private final StringRedisTemplate redisTemplate;
    private final LeaderboardApplyService applyService;
    private final KafkaConnectionDetails kafkaConnectionDetails;
    private final KafkaListenerEndpointRegistry listenerRegistry;
    private final RewardMetrics metrics;
    private final JsonMapper jsonMapper;
    private final String leaderboardKey;
    private final String deltaTopic;
    private final String processedDeltaPrefix;
    private final String consumerGroupId;
    private final int partitions;
    private final long markerTtlSeconds;
    private final Path snapshotDir;
    private final DefaultRedisScript<List<?>> captureScript;

    @Autowired
    public LeaderboardSnapshotService(
            StringRedisTemplate redisTemplate,
            LeaderboardApplyService applyService,
            KafkaConnectionDetails kafkaConnectionDetails,
            KafkaListenerEndpointRegistry listenerRegistry,
            RewardMetrics metrics,
            @Value("${app.leaderboard.key}") String leaderboardKey,
            @Value("${app.kafka.delta-topic}") String deltaTopic,
            @Value("${app.leaderboard.processed-delta-set}") String processedDeltaPrefix,
            @Value("${spring.kafka.consumer.group-id:einvoice-gamification}") String consumerGroupId,
            @Value("${app.kafka.partitions:12}") int partitions,
            @Value("${app.reward.batch-marker-ttl-seconds:604800}") long markerTtlSeconds,
            @Value("${app.snapshot.dir:./data/snapshots}") String snapshotDir
    ) {
        this.redisTemplate = redisTemplate;
        this.applyService = applyService;
        this.kafkaConnectionDetails = kafkaConnectionDetails;
        this.listenerRegistry = listenerRegistry;
        this.metrics = metrics;
        this.jsonMapper = JsonMapper.builder().build();
        this.leaderboardKey = leaderboardKey;
        this.deltaTopic = deltaTopic;
        this.processedDeltaPrefix = processedDeltaPrefix;
        this.consumerGroupId = consumerGroupId;
        this.partitions = Math.max(1, partitions);
        this.markerTtlSeconds = Math.max(60L, markerTtlSeconds);
        this.snapshotDir = Path.of(snapshotDir);
        this.captureScript = new DefaultRedisScript<>();
        this.captureScript.setScriptText(CAPTURE_LUA);
        this.captureScript.setResultType(rawListResultClass());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Class<List<?>> rawListResultClass() {
        return (Class<List<?>>) (Class) List.class;
    }

    public LeaderboardSnapshot.SnapshotMetadata captureSnapshot() {
        List<String> keys = new ArrayList<>(1 + partitions);
        keys.add(leaderboardKey);
        for (int p = 0; p < partitions; p++) {
            keys.add(CHECKPOINT_PREFIX + deltaTopic + ":" + p);
        }

        List<?> raw = redisTemplate.execute(captureScript, keys, Integer.toString(partitions));
        if (raw == null || raw.size() < 2) {
            throw new IllegalStateException("Snapshot Lua capture returned incomplete payload");
        }

        List<?> rawCheckpoints = (List<?>) raw.get(0);
        Map<Integer, Long> partitionOffsets = new TreeMap<>();
        boolean hasMissingCheckpoint = false;
        for (int p = 0; p < rawCheckpoints.size(); p++) {
            long cp = Long.parseLong(rawCheckpoints.get(p).toString());
            partitionOffsets.put(p, cp);
            if (cp < 0L) {
                hasMissingCheckpoint = true;
            }
        }
        if (hasMissingCheckpoint) {
            fillMissingCheckpointsFromCommittedOffsets(partitionOffsets);
        }

        List<?> rawEntries = (List<?>) raw.get(1);
        List<LeaderboardSnapshot.SnapshotEntry> entries = new ArrayList<>(rawEntries.size() / 2);
        long totalPoints = 0L;
        for (int i = 0; i + 1 < rawEntries.size(); i += 2) {
            String companyId = rawEntries.get(i).toString();
            long points = (long) Double.parseDouble(rawEntries.get(i + 1).toString());
            entries.add(new LeaderboardSnapshot.SnapshotEntry(companyId, points));
            totalPoints += points;
        }

        Map<Long, List<String>> deltaMarkers = collectRecentDeltaMarkers();
        Instant now = Instant.now();
        String snapshotId = "snap-" + now.toEpochMilli();
        LeaderboardSnapshot snapshot = new LeaderboardSnapshot(
                snapshotId,
                now,
                leaderboardKey,
                deltaTopic,
                partitions,
                partitionOffsets,
                entries,
                totalPoints,
                deltaMarkers
        );

        persistSnapshot(snapshot);
        metrics.recordSnapshotCaptured();
        log.info("Captured leaderboard snapshot id={} companies={} totalPoints={}",
                snapshotId, entries.size(), totalPoints);
        return snapshot.metadata();
    }

    public LeaderboardSnapshot.SnapshotMetadata latestSnapshotMetadata() {
        LeaderboardSnapshot latest = loadLatestSnapshot();
        return latest != null ? latest.metadata() : null;
    }

    public synchronized LeaderboardSnapshot.RebuildReport rebuild(String snapshotId, boolean coldReplayFromKafka) {
        long startNanos = System.nanoTime();
        LeaderboardSnapshot snapshot = null;
        if (!coldReplayFromKafka) {
            snapshot = (snapshotId == null || snapshotId.isBlank())
                    ? loadLatestSnapshot()
                    : loadSnapshot(snapshotId);
            if (snapshot == null) {
                throw new IllegalArgumentException("Snapshot not found: " + snapshotId);
            }
        }

        MessageListenerContainer container = stopProjectorContainer();
        try {
            clearLeaderboardAndMarkers();

            int restoredCompanies = 0;
            int restoredMarkers = 0;
            Map<Integer, Long> barrierOffsets = Map.of();

            if (snapshot != null) {
                restoredCompanies = restoreEntries(snapshot);
                restoreCheckpoints(snapshot);
                restoredMarkers = restoreDeltaMarkers(snapshot);
                barrierOffsets = snapshot.partitionOffsets();
            }

            ReplayStats replayStats = replayKafkaDeltasAfterBarrier(barrierOffsets);
            long totalPointsAfter = readLeaderboardTotalPoints();
            long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            metrics.recordSnapshotRebuilt();

            log.info("Leaderboard rebuild finished: snapshot={} cold={} restoredCompanies={} replayed={} applied={} totalPoints={} durationMs={}",
                    snapshot != null ? snapshot.snapshotId() : "NONE",
                    coldReplayFromKafka,
                    restoredCompanies,
                    replayStats.replayedRecords,
                    replayStats.appliedRecords,
                    totalPointsAfter,
                    durationMs);

            return new LeaderboardSnapshot.RebuildReport(
                    snapshot != null ? snapshot.snapshotId() : null,
                    coldReplayFromKafka,
                    restoredCompanies,
                    restoredMarkers,
                    replayStats.replayedRecords,
                    replayStats.appliedRecords,
                    totalPointsAfter,
                    durationMs
            );
        } finally {
            if (container != null && !container.isRunning()) {
                container.start();
            }
        }
    }

    private Map<Long, List<String>> collectRecentDeltaMarkers() {
        long currentDay = Instant.now().getEpochSecond() / SECONDS_PER_DAY;
        Map<Long, List<String>> result = new LinkedHashMap<>();
        long lookbackDays = Math.max(1L, markerTtlSeconds / SECONDS_PER_DAY);
        for (long day = currentDay - lookbackDays; day <= currentDay + 1L; day++) {
            String bucketKey = processedDeltaPrefix + ":" + day;
            List<String> ids = new ArrayList<>();
            ScanOptions options = ScanOptions.scanOptions().count(1000).build();
            try (Cursor<String> cursor = redisTemplate.opsForSet().scan(bucketKey, options)) {
                while (cursor.hasNext()) {
                    ids.add(cursor.next());
                }
            }
            if (!ids.isEmpty()) {
                result.put(day, ids);
            }
        }
        return result;
    }

    private void persistSnapshot(LeaderboardSnapshot snapshot) {
        try {
            Files.createDirectories(snapshotDir);
            Path target = snapshotDir.resolve(snapshot.snapshotId() + ".json");
            Path tmp = snapshotDir.resolve(snapshot.snapshotId() + ".json.tmp");
            byte[] bytes = jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(snapshot);
            Files.write(tmp, bytes);
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to write snapshot " + snapshot.snapshotId(), ex);
        }
    }

    private LeaderboardSnapshot loadLatestSnapshot() {
        if (!Files.exists(snapshotDir)) {
            return null;
        }
        try (Stream<Path> files = Files.list(snapshotDir)) {
            return files
                    .filter(p -> p.getFileName().toString().startsWith("snap-") && p.getFileName().toString().endsWith(".json"))
                    .max(Comparator.comparing(p -> p.getFileName().toString()))
                    .map(this::readSnapshotFile)
                    .orElse(null);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to list snapshots", ex);
        }
    }

    private LeaderboardSnapshot loadSnapshot(String snapshotId) {
        if (snapshotId.contains("..") || snapshotId.contains("/") || snapshotId.contains("\\")) {
            throw new IllegalArgumentException("Invalid snapshotId");
        }
        Path path = snapshotDir.resolve(snapshotId + ".json");
        if (!Files.exists(path)) {
            return null;
        }
        return readSnapshotFile(path);
    }

    private LeaderboardSnapshot readSnapshotFile(Path path) {
        try {
            return jsonMapper.readValue(Files.readAllBytes(path), LeaderboardSnapshot.class);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to read snapshot " + path, ex);
        }
    }

    private MessageListenerContainer stopProjectorContainer() {
        if (listenerRegistry == null) {
            return null;
        }
        MessageListenerContainer container = listenerRegistry.getListenerContainer("leaderboardProjector");
        if (container != null && container.isRunning()) {
            CountDownLatch stopped = new CountDownLatch(1);
            container.stop(stopped::countDown);
            try {
                if (!stopped.await(15, TimeUnit.SECONDS)) {
                    log.warn("Timed out waiting 15s for leaderboardProjector listener to stop before rebuild");
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
        return container;
    }

    private void clearLeaderboardAndMarkers() {
        redisTemplate.delete(leaderboardKey);
        for (int p = 0; p < partitions; p++) {
            String markerId = deltaTopic + ":" + p;
            redisTemplate.delete(MARKER_PREFIX + markerId);
            redisTemplate.delete(CHECKPOINT_PREFIX + markerId);
        }
        long currentDay = Instant.now().getEpochSecond() / SECONDS_PER_DAY;
        long lookbackDays = Math.max(1L, markerTtlSeconds / SECONDS_PER_DAY);
        for (long day = currentDay - lookbackDays; day <= currentDay + 1L; day++) {
            redisTemplate.delete(processedDeltaPrefix + ":" + day);
        }
    }

    private int restoreEntries(LeaderboardSnapshot snapshot) {
        List<LeaderboardSnapshot.SnapshotEntry> entries = snapshot.entries();
        if (entries.isEmpty()) {
            return 0;
        }
        for (int start = 0; start < entries.size(); start += 1000) {
            int end = Math.min(entries.size(), start + 1000);
            Set<ZSetOperations.TypedTuple<String>> batch = new LinkedHashSet<>(end - start);
            for (int i = start; i < end; i++) {
                LeaderboardSnapshot.SnapshotEntry entry = entries.get(i);
                batch.add(new DefaultTypedTuple<>(entry.companyId(), (double) entry.points()));
            }
            redisTemplate.opsForZSet().add(leaderboardKey, batch);
        }
        return entries.size();
    }

    private void restoreCheckpoints(LeaderboardSnapshot snapshot) {
        Duration ttl = Duration.ofSeconds(markerTtlSeconds);
        for (Map.Entry<Integer, Long> entry : snapshot.partitionOffsets().entrySet()) {
            if (entry.getValue() >= 0L) {
                String key = CHECKPOINT_PREFIX + deltaTopic + ":" + entry.getKey();
                redisTemplate.opsForValue().set(key, Long.toString(entry.getValue()), ttl);
            }
        }
    }

    private int restoreDeltaMarkers(LeaderboardSnapshot snapshot) {
        if (snapshot.processedDeltaIdsByDay() == null || snapshot.processedDeltaIdsByDay().isEmpty()) {
            return 0;
        }
        Duration ttl = Duration.ofSeconds(markerTtlSeconds);
        int count = 0;
        for (Map.Entry<Long, List<String>> entry : snapshot.processedDeltaIdsByDay().entrySet()) {
            List<String> ids = entry.getValue();
            if (ids == null || ids.isEmpty()) {
                continue;
            }
            String key = processedDeltaPrefix + ":" + entry.getKey();
            for (int start = 0; start < ids.size(); start += 1000) {
                int end = Math.min(ids.size(), start + 1000);
                String[] chunk = ids.subList(start, end).toArray(String[]::new);
                redisTemplate.opsForSet().add(key, chunk);
                count += chunk.length;
            }
            redisTemplate.expire(key, ttl);
        }
        return count;
    }

    private ReplayStats replayKafkaDeltasAfterBarrier(Map<Integer, Long> barrierOffsets) {
        List<String> servers = kafkaConnectionDetails.getBootstrapServers();
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", servers));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "leaderboard-rebuild-" + System.nanoTime());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5000);

        long replayed = 0L;
        int applied = 0;
        try (JacksonJsonSerde<CompanyRewardDelta> deltaSerde =
                     new JacksonJsonSerde<>(CompanyRewardDelta.class, jsonMapper).noTypeInfo();
             KafkaConsumer<String, CompanyRewardDelta> consumer =
                     new KafkaConsumer<>(props, new StringDeserializer(), deltaSerde.deserializer())) {
            List<PartitionInfo> infos = consumer.partitionsFor(deltaTopic);
            if (infos == null || infos.isEmpty()) {
                return new ReplayStats(0L, 0);
            }
            List<TopicPartition> topicPartitions = infos.stream()
                    .map(info -> new TopicPartition(deltaTopic, info.partition()))
                    .toList();
            consumer.assign(topicPartitions);

            Map<TopicPartition, Long> beginning = consumer.beginningOffsets(topicPartitions);
            Map<TopicPartition, Long> ending = consumer.endOffsets(topicPartitions);
            Map<TopicPartition, Long> pendingTargets = new LinkedHashMap<>();

            for (TopicPartition tp : topicPartitions) {
                long endOffset = ending.getOrDefault(tp, 0L);
                long startOffset = beginning.getOrDefault(tp, 0L);
                Long barrierOffset = barrierOffsets.get(tp.partition());
                long resumeOffset = (barrierOffset != null && barrierOffset >= 0L)
                        ? Math.max(startOffset, barrierOffset + 1L)
                        : startOffset;
                consumer.seek(tp, resumeOffset);
                if (resumeOffset < endOffset) {
                    pendingTargets.put(tp, endOffset);
                }
            }

            long idleDeadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (!pendingTargets.isEmpty()) {
                ConsumerRecords<String, CompanyRewardDelta> polled = consumer.poll(Duration.ofMillis(200));
                if (polled.isEmpty()) {
                    if (System.nanoTime() >= idleDeadline) {
                        break;
                    }
                } else {
                    idleDeadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
                    for (TopicPartition tp : polled.partitions()) {
                        Long targetEnd = pendingTargets.get(tp);
                        if (targetEnd == null) {
                            continue;
                        }
                        List<ConsumerRecord<String, CompanyRewardDelta>> partitionRecords = polled.records(tp);
                        List<ConsumerRecord<String, CompanyRewardDelta>> bounded = new ArrayList<>(partitionRecords.size());
                        for (ConsumerRecord<String, CompanyRewardDelta> record : partitionRecords) {
                            if (record.offset() < targetEnd && validDelta(record.value())) {
                                bounded.add(record);
                            }
                        }
                        if (!bounded.isEmpty()) {
                            replayed += bounded.size();
                            applied += applyService.applyPartitionBatch(bounded, deltaTopic + ":" + tp.partition());
                        }
                    }
                }
                pendingTargets.entrySet().removeIf(entry -> consumer.position(entry.getKey()) >= entry.getValue());
            }
        }
        return new ReplayStats(replayed, applied);
    }

    private void fillMissingCheckpointsFromCommittedOffsets(Map<Integer, Long> partitionOffsets) {
        if (consumerGroupId == null || consumerGroupId.isBlank() || kafkaConnectionDetails == null) {
            return;
        }
        List<String> servers = kafkaConnectionDetails.getBootstrapServers();
        if (servers == null || servers.isEmpty()) {
            return;
        }
        Map<String, Object> adminProps = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", servers),
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000,
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 3000
        );
        try (AdminClient admin = AdminClient.create(adminProps)) {
            Map<TopicPartition, OffsetAndMetadata> committed =
                    admin.listConsumerGroupOffsets(consumerGroupId).partitionsToOffsetAndMetadata().get(3, TimeUnit.SECONDS);
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : committed.entrySet()) {
                TopicPartition tp = entry.getKey();
                if (!deltaTopic.equals(tp.topic()) || entry.getValue() == null) {
                    continue;
                }
                long nextToConsume = entry.getValue().offset();
                if (nextToConsume > 0L) {
                    partitionOffsets.compute(tp.partition(), (p, existing) ->
                            (existing == null || existing < 0L) ? (nextToConsume - 1L) : existing);
                }
            }
        } catch (Exception ex) {
            log.debug("Could not inspect committed consumer group offsets for idle partitions", ex);
        }
    }

    private long readLeaderboardTotalPoints() {
        byte[] keyBytes = leaderboardKey.getBytes(StandardCharsets.UTF_8);
        Long total = redisTemplate.execute((RedisCallback<Long>) connection -> {
            long sum = 0L;
            var tuples = connection.zSetCommands().zRangeWithScores(keyBytes, 0, -1);
            if (tuples != null) {
                for (var tuple : tuples) {
                    if (tuple.getScore() != null) {
                        sum += tuple.getScore().longValue();
                    }
                }
            }
            return sum;
        });
        return total != null ? total : 0L;
    }

    private static boolean validDelta(CompanyRewardDelta delta) {
        return delta != null
                && delta.schemaVersion() == CompanyRewardDelta.SCHEMA_VERSION
                && delta.deltaId() != null
                && !delta.deltaId().isBlank()
                && delta.companyId() != null
                && !delta.companyId().isBlank()
                && delta.deltaPoints() > 0;
    }

    private record ReplayStats(long replayedRecords, int appliedRecords) {
    }
}
