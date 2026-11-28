package com.vnpt.einvoice.gamification.leaderboard;

import com.vnpt.einvoice.gamification.event.CompanyRewardDelta;
import com.vnpt.einvoice.gamification.metrics.RewardMetrics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
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
    private static final byte[][] CACHED_BUCKET_INDICES = buildBucketIndexCache(8);

    private final StringRedisTemplate redisTemplate;
    private final byte[] script;
    private final byte[] leaderboardKey;
    private final byte[] markerTtl;
    private final String processedDeltaPrefix;
    private final RewardMetrics metrics;
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
        return applyInternal(deltas.size(), deltas, null, markerId, -1L);
    }

    public int applyPartitionBatch(
            List<ConsumerRecord<String, CompanyRewardDelta>> records,
            String markerId
    ) {
        if (records.isEmpty()) {
            return 0;
        }
        long maxOffset = -1L;
        for (ConsumerRecord<String, CompanyRewardDelta> record : records) {
            if (record.offset() > maxOffset) {
                maxOffset = record.offset();
            }
        }
        return applyInternal(records.size(), null, records, markerId, maxOffset);
    }

    private int applyInternal(
            int count,
            List<CompanyRewardDelta> directDeltas,
            List<ConsumerRecord<String, CompanyRewardDelta>> records,
            String markerId,
            long batchMaxOffset
    ) {
        long startNanos = System.nanoTime();
        long fallbackDay = Instant.now().getEpochSecond() / SECONDS_PER_DAY;

        Map<Long, Integer> bucketIndexByDay = new LinkedHashMap<>(2);
        List<byte[]> bucketKeys = new ArrayList<>(2);
        int[] bucketIndices = new int[count];
        int validCount = 0;

        for (int i = 0; i < count; i++) {
            CompanyRewardDelta delta = directDeltas != null ? directDeltas.get(i) : records.get(i).value();
            if (delta == null) {
                continue;
            }
            Instant windowStart = delta.windowStart();
            long epochDay = windowStart != null ? windowStart.getEpochSecond() / SECONDS_PER_DAY : fallbackDay;
            Integer existingIdx = bucketIndexByDay.get(epochDay);
            int bucketIdx;
            if (existingIdx != null) {
                bucketIdx = existingIdx;
            } else {
                bucketKeys.add(SERIALIZER.serialize(processedDeltaPrefix + ":" + epochDay));
                bucketIdx = bucketKeys.size();
                bucketIndexByDay.put(epochDay, bucketIdx);
            }
            bucketIndices[validCount++] = bucketIdx;
        }

        if (validCount == 0) {
            return 0;
        }

        int fixedKeyCount = 3 + bucketKeys.size();
        byte[][] keysAndArgs = buildKeysAndArgs(
                fixedKeyCount,
                bucketKeys,
                markerId,
                batchMaxOffset,
                directDeltas,
                records,
                bucketIndices,
                validCount
        );

        int applied = executeWithNoscriptRetry(fixedKeyCount, keysAndArgs);
        metrics.recordProjectorBatch(validCount, applied, System.nanoTime() - startNanos);
        return applied;
    }

    private byte[][] buildKeysAndArgs(
            int fixedKeyCount,
            List<byte[]> bucketKeys,
            String markerId,
            long batchMaxOffset,
            List<CompanyRewardDelta> directDeltas,
            List<ConsumerRecord<String, CompanyRewardDelta>> records,
            int[] bucketIndices,
            int validCount
    ) {
        int argCount = 3 + validCount * 5;
        byte[][] keysAndArgs = new byte[fixedKeyCount + argCount][];

        int pos = 0;
        keysAndArgs[pos++] = leaderboardKey;
        keysAndArgs[pos++] = SERIALIZER.serialize(MARKER_PREFIX + markerId);
        keysAndArgs[pos++] = SERIALIZER.serialize(CHECKPOINT_PREFIX + markerId);
        for (byte[] bucketKey : bucketKeys) {
            keysAndArgs[pos++] = bucketKey;
        }

        keysAndArgs[pos++] = markerTtl;
        keysAndArgs[pos++] = bucketIndexBytes(bucketKeys.size());
        keysAndArgs[pos++] = SERIALIZER.serialize(Long.toString(batchMaxOffset));

        int idxPos = 0;
        int total = directDeltas != null ? directDeltas.size() : records.size();
        for (int i = 0; i < total; i++) {
            long offset;
            CompanyRewardDelta delta;
            if (directDeltas != null) {
                offset = -1L;
                delta = directDeltas.get(i);
            } else {
                ConsumerRecord<String, CompanyRewardDelta> record = records.get(i);
                offset = record.offset();
                delta = record.value();
            }
            if (delta == null) {
                continue;
            }
            keysAndArgs[pos++] = SERIALIZER.serialize(Long.toString(offset));
            keysAndArgs[pos++] = SERIALIZER.serialize(delta.deltaId());
            keysAndArgs[pos++] = SERIALIZER.serialize(delta.companyId());
            keysAndArgs[pos++] = SERIALIZER.serialize(Long.toString(delta.deltaPoints()));
            keysAndArgs[pos++] = bucketIndexBytes(bucketIndices[idxPos++]);
        }
        return keysAndArgs;
    }

    private int executeWithNoscriptRetry(int keyCount, byte[][] keysAndArgs) {
        try {
            return execute(keyCount, keysAndArgs, scriptSha());
        } catch (DataAccessException ex) {
            Throwable cause = ex.getMostSpecificCause();
            if (cause.getMessage() == null || !cause.getMessage().contains("NOSCRIPT")) {
                throw ex;
            }
            scriptSha = null;
            return execute(keyCount, keysAndArgs, scriptSha());
        }
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

    private static byte[] bucketIndexBytes(int index) {
        if (index >= 1 && index < CACHED_BUCKET_INDICES.length) {
            return CACHED_BUCKET_INDICES[index];
        }
        return SERIALIZER.serialize(Integer.toString(index));
    }

    private static byte[][] buildBucketIndexCache(int maxCached) {
        byte[][] cache = new byte[maxCached + 1][];
        for (int i = 1; i <= maxCached; i++) {
            cache[i] = SERIALIZER.serialize(Integer.toString(i));
        }
        return cache;
    }
}
