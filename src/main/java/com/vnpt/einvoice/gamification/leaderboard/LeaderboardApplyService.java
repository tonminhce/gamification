package com.vnpt.einvoice.gamification.leaderboard;

import com.vnpt.einvoice.gamification.event.CompanyRewardDelta;
import com.vnpt.einvoice.gamification.metrics.RewardMetrics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Applies idempotent company reward delta batches to Redis using a single atomic Lua script
 * with per-partition PENDING recovery, partition offset checkpointing, and daily-bucketed TTL
 * processed-delta Sets.
 */
@Service
public class LeaderboardApplyService {

    private static final StringRedisSerializer SERIALIZER = StringRedisSerializer.UTF_8;
    private static final String MARKER_PREFIX = "einv:delta-batch:";
    private static final String CHECKPOINT_PREFIX = "einv:projector-checkpoint:";
    private static final long SECONDS_PER_DAY = 86_400L;

    private final StringRedisTemplate redisTemplate;
    private final byte[] script;
    private final String leaderboardKeyName;
    private final byte[] leaderboardKey;
    private final byte[] markerTtl;
    private final String processedDeltaPrefix;
    private final RewardMetrics metrics;
    private final AtomicBoolean leaderboardTypeValidated = new AtomicBoolean(false);
    private final Set<String> validatedBucketKeys = ConcurrentHashMap.newKeySet();
    private volatile String scriptSha;

    public LeaderboardApplyService(
            StringRedisTemplate redisTemplate,
            String leaderboardKey,
            String processedDeltaSet,
            long markerTtlSeconds
    ) {
        this(redisTemplate, leaderboardKey, processedDeltaSet, markerTtlSeconds, RewardMetrics.noop());
    }

    @Autowired
    public LeaderboardApplyService(
            StringRedisTemplate redisTemplate,
            @Value("${app.leaderboard.key}") String leaderboardKey,
            @Value("${app.leaderboard.processed-delta-set}") String processedDeltaSet,
            @Value("${app.reward.batch-marker-ttl-seconds}") long markerTtlSeconds,
            RewardMetrics metrics
    ) {
        this.redisTemplate = redisTemplate;
        this.leaderboardKeyName = leaderboardKey;
        this.leaderboardKey = SERIALIZER.serialize(leaderboardKey);
        this.markerTtl = SERIALIZER.serialize(Long.toString(markerTtlSeconds));
        this.processedDeltaPrefix = processedDeltaSet;
        this.metrics = metrics;

        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
        redisScript.setLocation(new ClassPathResource("scripts/apply_delta_batch.lua"));
        redisScript.setResultType(Long.class);
        this.script = redisScript.getScriptAsString().getBytes(StandardCharsets.UTF_8);
    }

    public int applyDeltas(List<CompanyRewardDelta> deltas, String markerId) {
        if (deltas.isEmpty()) {
            return 0;
        }
        List<DeltaEntry> entries = new ArrayList<>(deltas.size());
        for (CompanyRewardDelta delta : deltas) {
            entries.add(new DeltaEntry(-1L, delta));
        }
        return applyEntries(entries, markerId, -1L);
    }

    public int applyPartitionBatch(
            List<ConsumerRecord<String, CompanyRewardDelta>> records,
            String markerId
    ) {
        if (records.isEmpty()) {
            return 0;
        }
        List<DeltaEntry> entries = new ArrayList<>(records.size());
        long maxOffset = -1L;
        for (ConsumerRecord<String, CompanyRewardDelta> record : records) {
            if (record.offset() > maxOffset) {
                maxOffset = record.offset();
            }
            CompanyRewardDelta delta = record.value();
            if (delta != null) {
                entries.add(new DeltaEntry(record.offset(), delta));
            }
        }
        if (entries.isEmpty()) {
            return 0;
        }
        return applyEntries(entries, markerId, maxOffset);
    }

    private int applyEntries(List<DeltaEntry> entries, String markerId, long batchMaxOffset) {
        long startNanos = System.nanoTime();
        validateLeaderboardTypeOnce();

        Map<Long, Integer> bucketIndexByDay = new LinkedHashMap<>();
        List<String> bucketKeys = new ArrayList<>(2);
        long fallbackDay = Instant.now().getEpochSecond() / SECONDS_PER_DAY;

        for (DeltaEntry entry : entries) {
            Instant windowStart = entry.delta().windowStart();
            long epochDay = windowStart != null ? windowStart.getEpochSecond() / SECONDS_PER_DAY : fallbackDay;
            bucketIndexByDay.computeIfAbsent(epochDay, day -> {
                String bucketKey = processedDeltaPrefix + ":" + day;
                validateBucketTypeOnce(bucketKey);
                bucketKeys.add(bucketKey);
                return bucketKeys.size();
            });
        }

        int bucketCount = bucketKeys.size();
        int fixedKeyCount = 3 + bucketCount;
        int argCount = 3 + entries.size() * 5;
        byte[][] keysAndArguments = new byte[fixedKeyCount + argCount][];

        int pos = 0;
        keysAndArguments[pos++] = leaderboardKey;
        keysAndArguments[pos++] = SERIALIZER.serialize(MARKER_PREFIX + markerId);
        keysAndArguments[pos++] = SERIALIZER.serialize(CHECKPOINT_PREFIX + markerId);
        for (String bucketKey : bucketKeys) {
            keysAndArguments[pos++] = SERIALIZER.serialize(bucketKey);
        }

        keysAndArguments[pos++] = markerTtl;
        keysAndArguments[pos++] = SERIALIZER.serialize(Integer.toString(bucketCount));
        keysAndArguments[pos++] = SERIALIZER.serialize(Long.toString(batchMaxOffset));

        for (DeltaEntry entry : entries) {
            CompanyRewardDelta delta = entry.delta();
            Instant windowStart = delta.windowStart();
            long epochDay = windowStart != null ? windowStart.getEpochSecond() / SECONDS_PER_DAY : fallbackDay;
            int bucketIdx = bucketIndexByDay.get(epochDay);

            keysAndArguments[pos++] = SERIALIZER.serialize(Long.toString(entry.offset()));
            keysAndArguments[pos++] = SERIALIZER.serialize(delta.deltaId());
            keysAndArguments[pos++] = SERIALIZER.serialize(delta.companyId());
            keysAndArguments[pos++] = SERIALIZER.serialize(Long.toString(delta.deltaPoints()));
            keysAndArguments[pos++] = SERIALIZER.serialize(Integer.toString(bucketIdx));
        }

        int applied;
        try {
            applied = execute(fixedKeyCount, keysAndArguments, scriptSha());
        } catch (DataAccessException ex) {
            Throwable cause = ex.getMostSpecificCause();
            if (cause.getMessage() == null || !cause.getMessage().contains("NOSCRIPT")) {
                throw ex;
            }
            scriptSha = null;
            applied = execute(fixedKeyCount, keysAndArguments, scriptSha());
        }

        metrics.recordProjectorBatch(entries.size(), applied, System.nanoTime() - startNanos);
        return applied;
    }

    private int execute(int keyCount, byte[][] keysAndArguments, String sha) {
        Long applied = redisTemplate.execute((RedisCallback<Long>) connection ->
                connection.scriptingCommands().evalSha(
                        sha,
                        ReturnType.INTEGER,
                        keyCount,
                        keysAndArguments
                ));
        if (applied == null) {
            throw new IllegalStateException("Leaderboard delta script returned no result");
        }
        return applied.intValue();
    }

    private void validateLeaderboardTypeOnce() {
        if (leaderboardTypeValidated.get()) {
            return;
        }
        DataType leaderboardType = redisTemplate.type(leaderboardKeyName);
        if (leaderboardType != DataType.NONE && leaderboardType != DataType.ZSET) {
            throw new RedisSystemException("leaderboard key has unexpected type: " + leaderboardType, null);
        }
        leaderboardTypeValidated.set(true);
    }

    private void validateBucketTypeOnce(String bucketKey) {
        if (validatedBucketKeys.contains(bucketKey)) {
            return;
        }
        DataType processedType = redisTemplate.type(bucketKey);
        if (processedType != DataType.NONE && processedType != DataType.SET) {
            throw new RedisSystemException("processed delta set has unexpected type: " + processedType, null);
        }
        validatedBucketKeys.add(bucketKey);
    }

    private String scriptSha() {
        String sha = scriptSha;
        if (sha == null) {
            synchronized (this) {
                sha = scriptSha;
                if (sha == null) {
                    sha = redisTemplate.execute((RedisCallback<String>) connection ->
                            connection.scriptingCommands().scriptLoad(script)
                    );
                    if (sha == null) {
                        throw new IllegalStateException("Redis returned no script SHA");
                    }
                    scriptSha = sha;
                }
            }
        }
        return sha;
    }

    private record DeltaEntry(long offset, CompanyRewardDelta delta) {
    }
}
