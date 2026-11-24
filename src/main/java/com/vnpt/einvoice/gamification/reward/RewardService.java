package com.vnpt.einvoice.gamification.reward;

import com.vnpt.einvoice.gamification.event.InvoiceIssuedEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@Service
public class RewardService {

    private static final StringRedisSerializer SERIALIZER = StringRedisSerializer.UTF_8;

    private final StringRedisTemplate redisTemplate;
    private final byte[] applyRewardScript;
    private final byte[] leaderboardKey;
    private final byte[] pointsPerInvoice;
    private final String processedKeyPrefix;

    public RewardService(
            StringRedisTemplate redisTemplate,
            @Value("${app.leaderboard.key}") String leaderboardKey,
            @Value("${app.reward.processed-key-prefix}") String processedKeyPrefix,
            @Value("${app.reward.points-per-invoice}") long pointsPerInvoice
    ) {
        this.redisTemplate = redisTemplate;
        this.processedKeyPrefix = processedKeyPrefix;
        this.leaderboardKey = SERIALIZER.serialize(leaderboardKey);
        this.pointsPerInvoice = SERIALIZER.serialize(Long.toString(pointsPerInvoice));

        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("scripts/apply_reward.lua"));
        script.setResultType(Long.class);
        this.applyRewardScript = script.getScriptAsString().getBytes(StandardCharsets.UTF_8);
    }

    public boolean applyReward(InvoiceIssuedEvent event) {
        return applyRewards(List.of(event)).getFirst();
    }

    public List<Boolean> applyRewards(List<InvoiceIssuedEvent> events) {
        if (events.isEmpty()) {
            return List.of();
        }

        List<Object> results = redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            for (InvoiceIssuedEvent event : events) {
                connection.eval(
                        applyRewardScript,
                        ReturnType.INTEGER,
                        2,
                        SERIALIZER.serialize(processedKeyPrefix + event.invoiceId()),
                        leaderboardKey,
                        SERIALIZER.serialize(event.invoiceId()),
                        SERIALIZER.serialize(event.companyId()),
                        pointsPerInvoice
                );
            }
            return null;
        });

        List<Boolean> applied = new ArrayList<>(results.size());
        for (Object result : results) {
            if (!(result instanceof Number value)) {
                throw new IllegalStateException("Reward script returned an unexpected result");
            }
            applied.add(value.longValue() == 1L);
        }
        return applied;
    }
}
