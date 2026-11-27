package com.vnpt.einvoice.gamification.leaderboard;

import com.vnpt.einvoice.gamification.event.CompanyRewardDelta;
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

@Service
public class LeaderboardApplyService {

    private static final StringRedisSerializer SERIALIZER = StringRedisSerializer.UTF_8;
    private static final String MARKER_PREFIX = "einv:delta-batch:";

    private final StringRedisTemplate redisTemplate;
    private final byte[] script;
    private final String leaderboardKeyName;
    private final byte[] leaderboardKey;
    private final byte[] markerTtl;
    private final String processedDeltaSetName;
    private final byte[] processedDeltaSet;
    private volatile String scriptSha;

    public LeaderboardApplyService(
            StringRedisTemplate redisTemplate,
            @Value("${app.leaderboard.key}") String leaderboardKey,
            @Value("${app.leaderboard.processed-delta-set}") String processedDeltaSet,
            @Value("${app.reward.batch-marker-ttl-seconds}") long markerTtlSeconds
    ) {
        this.redisTemplate = redisTemplate;
        this.leaderboardKeyName = leaderboardKey;
        this.leaderboardKey = SERIALIZER.serialize(leaderboardKey);
        this.markerTtl = SERIALIZER.serialize(Long.toString(markerTtlSeconds));
        this.processedDeltaSetName = processedDeltaSet;
        this.processedDeltaSet = SERIALIZER.serialize(processedDeltaSet);

        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
        redisScript.setLocation(new ClassPathResource("scripts/apply_delta_batch.lua"));
        redisScript.setResultType(Long.class);
        this.script = redisScript.getScriptAsString().getBytes(StandardCharsets.UTF_8);
    }

    public int applyDeltas(List<CompanyRewardDelta> deltas, String markerId) {
        if (deltas.isEmpty()) {
            return 0;
        }

        validateKeyTypes();
        List<byte[]> arguments = new ArrayList<>(1 + deltas.size() * 3);
        arguments.add(markerTtl);
        for (CompanyRewardDelta delta : deltas) {
            arguments.add(SERIALIZER.serialize(delta.deltaId()));
            arguments.add(SERIALIZER.serialize(delta.companyId()));
            arguments.add(SERIALIZER.serialize(Long.toString(delta.deltaPoints())));
        }
        byte[][] keysAndArguments = new byte[arguments.size() + 3][];
        keysAndArguments[0] = leaderboardKey;
        keysAndArguments[1] = SERIALIZER.serialize(MARKER_PREFIX + markerId);
        keysAndArguments[2] = processedDeltaSet;
        System.arraycopy(arguments.toArray(byte[][]::new), 0, keysAndArguments, 3, arguments.size());

        try {
            return execute(keysAndArguments, scriptSha());
        } catch (DataAccessException ex) {
            Throwable cause = ex.getMostSpecificCause();
            if (cause.getMessage() == null || !cause.getMessage().contains("NOSCRIPT")) {
                throw ex;
            }
            scriptSha = null;
            return execute(keysAndArguments, scriptSha());
        }
    }

    private int execute(byte[][] keysAndArguments, String sha) {
        Long applied = redisTemplate.execute((RedisCallback<Long>) connection -> connection.evalSha(
                sha,
                ReturnType.INTEGER,
                3,
                keysAndArguments
        ));
        if (applied == null) {
            throw new IllegalStateException("Leaderboard delta script returned no result");
        }
        return applied.intValue();
    }

    private void validateKeyTypes() {
        DataType leaderboardType = redisTemplate.type(leaderboardKeyName);
        if (leaderboardType != DataType.NONE && leaderboardType != DataType.ZSET) {
            throw new RedisSystemException("leaderboard key has unexpected type: " + leaderboardType, null);
        }
        DataType processedType = redisTemplate.type(processedDeltaSetName);
        if (processedType != DataType.NONE && processedType != DataType.SET) {
            throw new RedisSystemException("processed delta set has unexpected type: " + processedType, null);
        }
    }

    private String scriptSha() {
        String sha = scriptSha;
        if (sha == null) {
            synchronized (this) {
                sha = scriptSha;
                if (sha == null) {
                    sha = redisTemplate.execute(
                            (RedisCallback<String>) connection -> connection.scriptLoad(script)
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