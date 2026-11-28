package com.vnpt.einvoice.gamification.ledger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Redis-backed atomic CAS {@link DedupeLedger} implementing the {@code CLAIMED -> EMITTED}
 * state machine via Lua so invoice deduplication is independent of Kafka partition counts.
 */
@Service
@ConditionalOnProperty(name = "app.dedupe.mode", havingValue = "external-ledger")
public class RedisLwtDedupeLedger implements DedupeLedger {

    private static final String KEY_PREFIX = "einv:dedupe-ledger:{";
    private static final String KEY_SUFFIX = "}";

    private final StringRedisTemplate redisTemplate;
    private final DefaultRedisScript<List<?>> script;
    private final long claimLeaseMs;

    public RedisLwtDedupeLedger(StringRedisTemplate redisTemplate, long claimLeaseMs) {
        this.redisTemplate = redisTemplate;
        this.claimLeaseMs = Math.max(100L, claimLeaseMs);
        this.script = new DefaultRedisScript<>();
        this.script.setLocation(new ClassPathResource("scripts/claim_invoice_lwt.lua"));
        this.script.setResultType(rawListResultClass());
    }

    @Autowired
    public RedisLwtDedupeLedger(
            StringRedisTemplate redisTemplate,
            @Value("${app.dedupe.claim-lease-ms:5000}") long claimLeaseMs,
            @SuppressWarnings("unused") @Value("${app.dedupe.mode:streams-rocksdb}") String mode
    ) {
        this(redisTemplate, claimLeaseMs);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Class<List<?>> rawListResultClass() {
        return (Class<List<?>>) (Class) List.class;
    }

    @Override
    public ClaimResult claim(String invoiceId, String companyId, String sourceId, Instant occurredAt, Duration ttl) {
        long nowMs = System.currentTimeMillis();
        long ttlMs = Math.max(1_000L, ttl != null ? ttl.toMillis() : 86_400_000L);
        List<?> result = redisTemplate.execute(
                script,
                List.of(ledgerKey(invoiceId)),
                "CLAIM",
                companyId,
                sourceId,
                Long.toString(nowMs),
                Long.toString(ttlMs),
                Long.toString(claimLeaseMs)
        );
        if (result == null || result.isEmpty()) {
            throw new IllegalStateException("Dedupe ledger CLAIM returned empty result for invoiceId=" + invoiceId);
        }
        int code = Integer.parseInt(result.getFirst().toString());
        String existingCompany = result.size() > 1 && result.get(1) != null ? result.get(1).toString() : "";
        return switch (code) {
            case 1 -> new ClaimResult(ClaimStatus.CLAIMED_NEW, existingCompany);
            case 2 -> new ClaimResult(ClaimStatus.DUPLICATE_EMITTED, existingCompany);
            case 3 -> new ClaimResult(ClaimStatus.RECOVERABLE_CLAIMED_UNEMITTED, existingCompany);
            case 4 -> new ClaimResult(ClaimStatus.CONFLICT, existingCompany);
            case 6 -> new ClaimResult(ClaimStatus.IN_FLIGHT_CLAIMED_BY_OTHER, existingCompany);
            default -> throw new IllegalStateException("Unexpected ledger CLAIM code=" + code);
        };
    }

    @Override
    public void markEmitted(String invoiceId, String sourceId, Instant emittedAt) {
        long nowMs = emittedAt != null ? emittedAt.toEpochMilli() : System.currentTimeMillis();
        redisTemplate.execute(
                script,
                List.of(ledgerKey(invoiceId)),
                "EMIT",
                "",
                sourceId,
                Long.toString(nowMs),
                "0",
                Long.toString(claimLeaseMs)
        );
    }

    private static String ledgerKey(String invoiceId) {
        return KEY_PREFIX + invoiceId + KEY_SUFFIX;
    }
}
