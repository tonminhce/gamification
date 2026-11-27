package com.vnpt.einvoice.gamification.reward;

import com.vnpt.einvoice.gamification.event.InvoiceIssuedEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
public class RewardService {

    private static final StringRedisSerializer SERIALIZER = StringRedisSerializer.UTF_8;
    private static final String BATCH_KEY_PREFIX = "einv:reward-batch:";

    private final StringRedisTemplate redisTemplate;
    private final byte[] applyRewardScript;
    private final String leaderboardKeyName;
    private final byte[] leaderboardKey;
    private final byte[] pointsPerInvoice;
    private final byte[] batchMarkerTtl;
    private final String processedSetName;
    private final byte[] processedSet;
    private volatile String scriptSha;

    public RewardService(
            StringRedisTemplate redisTemplate,
            @Value("${app.leaderboard.key}") String leaderboardKey,
            @Value("${app.reward.processed-set}") String processedSet,
            @Value("${app.reward.points-per-invoice}") long pointsPerInvoice,
            @Value("${app.reward.batch-marker-ttl-seconds}") long batchMarkerTtlSeconds
    ) {
        this.redisTemplate = redisTemplate;
        this.processedSetName = processedSet;
        this.processedSet = SERIALIZER.serialize(processedSet);
        this.leaderboardKeyName = leaderboardKey;
        this.leaderboardKey = SERIALIZER.serialize(leaderboardKey);
        this.pointsPerInvoice = SERIALIZER.serialize(Long.toString(pointsPerInvoice));
        this.batchMarkerTtl = SERIALIZER.serialize(Long.toString(batchMarkerTtlSeconds));

        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("scripts/apply_reward_batch.lua"));
        script.setResultType(Long.class);
        this.applyRewardScript = script.getScriptAsString().getBytes(StandardCharsets.UTF_8);
    }

    public int applyRewards(List<InvoiceIssuedEvent> events) {
        return applyRewards(events, UUID.randomUUID().toString());
    }

    public int applyRewards(List<InvoiceIssuedEvent> events, String batchId) {
        if (events.isEmpty()) {
            return 0;
        }

        validateKeyTypes();
        byte[] batchKey = SERIALIZER.serialize(BATCH_KEY_PREFIX + batchId);
        List<byte[]> arguments = new ArrayList<>(2 + events.size() * 2);
        arguments.add(pointsPerInvoice);
        arguments.add(batchMarkerTtl);
        for (InvoiceIssuedEvent event : events) {
            arguments.add(SERIALIZER.serialize(event.invoiceId()));
            arguments.add(SERIALIZER.serialize(event.companyId()));
        }
        byte[][] scriptArguments = arguments.toArray(byte[][]::new);

        try {
            return executeBatch(batchKey, scriptArguments, scriptSha());
        } catch (DataAccessException ex) {
            Throwable cause = ex.getMostSpecificCause();
            if (cause.getMessage() == null || !cause.getMessage().contains("NOSCRIPT")) {
                throw ex;
            }
            scriptSha = null;
            return executeBatch(batchKey, scriptArguments, scriptSha());
        }
    }

    private int executeBatch(byte[] batchKey, byte[][] arguments, String sha) {
        byte[][] keysAndArguments = new byte[arguments.length + 3][];
        keysAndArguments[0] = leaderboardKey;
        keysAndArguments[1] = batchKey;
        keysAndArguments[2] = processedSet;
        System.arraycopy(arguments, 0, keysAndArguments, 3, arguments.length);
        Long applied = redisTemplate.execute((RedisCallback<Long>) connection -> connection.evalSha(
                sha,
                ReturnType.INTEGER,
                3,
                keysAndArguments
        ));
        if (applied == null) {
            throw new IllegalStateException("Reward batch script returned no result");
        }
        return applied.intValue();
    }

    private void validateKeyTypes() {
        DataType leaderboardType = redisTemplate.type(leaderboardKeyName);
        if (leaderboardType != DataType.NONE && leaderboardType != DataType.ZSET) {
            throw new RedisSystemException("leaderboard key has unexpected type: " + leaderboardType, null);
        }
        DataType processedType = redisTemplate.type(processedSetName);
        if (processedType != DataType.NONE && processedType != DataType.SET) {
            throw new RedisSystemException("processed invoice set has unexpected type: " + processedType, null);
        }
    }

    private String scriptSha() {
        String sha = scriptSha;
        if (sha == null) {
            synchronized (this) {
                sha = scriptSha;
                if (sha == null) {
                    sha = redisTemplate.execute(
                            (RedisCallback<String>) connection -> connection.scriptLoad(applyRewardScript)
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
}
