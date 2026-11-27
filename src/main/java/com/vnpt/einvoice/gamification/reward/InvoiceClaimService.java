package com.vnpt.einvoice.gamification.reward;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
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

@Service
public class InvoiceClaimService {

    private static final StringRedisSerializer SERIALIZER = StringRedisSerializer.UTF_8;
    private static final String MARKER_PREFIX = "einv:invoice-claim:";

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final byte[] script;
    private final byte[] markerTtl;
    private final byte[] pointsPerInvoice;
    private final String processedSetName;
    private final byte[] processedSet;
    private volatile String scriptSha;

    public InvoiceClaimService(
            StringRedisTemplate redisTemplate,
            @Value("${app.reward.processed-set}") String processedSet,
            @Value("${app.reward.points-per-invoice}") long pointsPerInvoice,
            @Value("${app.reward.batch-marker-ttl-seconds}") long markerTtlSeconds
    ) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = JsonMapper.builder().addModule(new JavaTimeModule()).build();
        this.markerTtl = SERIALIZER.serialize(Long.toString(markerTtlSeconds));
        this.pointsPerInvoice = SERIALIZER.serialize(Long.toString(pointsPerInvoice));
        this.processedSetName = processedSet;
        this.processedSet = SERIALIZER.serialize(processedSet);

        DefaultRedisScript<String> redisScript = new DefaultRedisScript<>();
        redisScript.setLocation(new ClassPathResource("scripts/claim_invoice_batch.lua"));
        redisScript.setResultType(String.class);
        this.script = redisScript.getScriptAsString().getBytes(StandardCharsets.UTF_8);
    }

    public InvoiceClaimResult claim(List<InvoiceIssuedEvent> events, String markerId, String batchUid) {
        validateProcessedSetType();
        List<byte[]> arguments = new ArrayList<>(3 + events.size() * 2);
        arguments.add(markerTtl);
        arguments.add(SERIALIZER.serialize(batchUid));
        arguments.add(pointsPerInvoice);
        for (InvoiceIssuedEvent event : events) {
            arguments.add(SERIALIZER.serialize(event.invoiceId()));
            arguments.add(SERIALIZER.serialize(event.companyId()));
        }
        byte[][] keysAndArguments = new byte[arguments.size() + 2][];
        keysAndArguments[0] = SERIALIZER.serialize(MARKER_PREFIX + markerId);
        keysAndArguments[1] = processedSet;
        System.arraycopy(arguments.toArray(byte[][]::new), 0, keysAndArguments, 2, arguments.size());

        try {
            return parse(execute(keysAndArguments, scriptSha()));
        } catch (DataAccessException ex) {
            Throwable cause = ex.getMostSpecificCause();
            if (cause.getMessage() == null || !cause.getMessage().contains("NOSCRIPT")) {
                throw ex;
            }
            scriptSha = null;
            return parse(execute(keysAndArguments, scriptSha()));
        }
    }

    public void complete(String markerId) {
        redisTemplate.delete(MARKER_PREFIX + markerId);
    }

    private byte[] execute(byte[][] keysAndArguments, String sha) {
        byte[] result = redisTemplate.execute((RedisCallback<byte[]>) connection -> connection.evalSha(
                sha,
                ReturnType.VALUE,
                2,
                keysAndArguments
        ));
        if (result == null) {
            throw new IllegalStateException("Invoice claim script returned no result");
        }
        return result;
    }

    private InvoiceClaimResult parse(byte[] payload) {
        try {
            JsonNode root = objectMapper.readTree(payload);
            List<ClaimedCompanyDelta> companies = new ArrayList<>();
            for (JsonNode company : root.path("companies")) {
                companies.add(new ClaimedCompanyDelta(
                        company.path("companyId").asText(),
                        company.path("deltaPoints").asLong()
                ));
            }
            return new InvoiceClaimResult(
                    root.path("batchUid").asText(),
                    root.path("recovered").asBoolean(),
                    companies
            );
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to parse invoice claim result", ex);
        }
    }

    private void validateProcessedSetType() {
        DataType type = redisTemplate.type(processedSetName);
        if (type != DataType.NONE && type != DataType.SET) {
            throw new RedisSystemException("processed invoice set has unexpected type: " + type, null);
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