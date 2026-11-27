package com.vnpt.einvoice.gamification.streams;

import com.vnpt.einvoice.gamification.event.CompanyRewardDelta;
import com.vnpt.einvoice.gamification.event.InvoiceIssuedEvent;
import com.vnpt.einvoice.gamification.metrics.RewardMetrics;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Aggregates deduplicated invoice points per company over fixed event-time windows.
 *
 * <p>Maintains an in-memory window index synchronized with the transactional RocksDB store so
 * {@code process()} avoids RocksDB reads and {@code flush()} inspects only expired windows in O(ready)
 * instead of scanning the full store. Emits deterministic, sequence-numbered delta IDs so EOS rollbacks
 * reproduce identical IDs while late arrivals receive distinct sequence numbers.
 */
public class WindowAggregatorProcessor implements Processor<String, InvoiceIssuedEvent, String, CompanyRewardDelta> {

    static final String STORE = "company-window-store";
    static final long DEFAULT_WINDOW_MS = 500L;
    static final long DEFAULT_PUNCTUATION_MS = 100L;
    static final long POINTS_PER_INVOICE = 10L;

    private static final byte SEQ_PREFIX = 0x00;
    private static final byte WINDOW_PREFIX = 0x01;
    private static final byte[] SEQ_KEY = new byte[]{SEQ_PREFIX};

    private final long windowMs;
    private final long punctuationMs;
    private final RewardMetrics metrics;

    private ProcessorContext<String, CompanyRewardDelta> context;
    private KeyValueStore<byte[], byte[]> store;
    private int taskPartition;
    private long nextEmitSeq;
    private final NavigableMap<Long, Map<String, Long>> activeWindows = new TreeMap<>();

    public WindowAggregatorProcessor(long windowMs, long punctuationMs, RewardMetrics metrics) {
        this.windowMs = Math.max(10L, windowMs);
        this.punctuationMs = Math.max(10L, punctuationMs);
        this.metrics = Objects.requireNonNull(metrics);
    }

    @Override
    public void init(ProcessorContext<String, CompanyRewardDelta> context) {
        this.context = context;
        this.store = context.getStateStore(STORE);
        this.taskPartition = context.taskId().partition();
        this.activeWindows.clear();
        this.nextEmitSeq = 0L;

        try (KeyValueIterator<byte[], byte[]> iterator = store.all()) {
            while (iterator.hasNext()) {
                KeyValue<byte[], byte[]> entry = iterator.next();
                if (entry.key == null || entry.key.length == 0 || entry.value == null || entry.value.length < Long.BYTES) {
                    continue;
                }
                if (entry.key[0] == SEQ_PREFIX) {
                    this.nextEmitSeq = bytesToLong(entry.value);
                } else if (entry.key[0] == WINDOW_PREFIX && entry.key.length > 1 + Long.BYTES) {
                    long windowStart = decodeWindowStart(entry.key);
                    String companyId = decodeCompanyId(entry.key);
                    long points = bytesToLong(entry.value);
                    activeWindows.computeIfAbsent(windowStart, ignored -> new HashMap<>())
                            .put(companyId, points);
                }
            }
        }

        context.schedule(
                Duration.ofMillis(punctuationMs),
                PunctuationType.WALL_CLOCK_TIME,
                this::flush
        );
    }

    @Override
    public void process(Record<String, InvoiceIssuedEvent> record) {
        if (record.key() == null || record.value() == null) {
            return;
        }
        String companyId = record.key();
        long windowStart = Math.floorDiv(record.timestamp(), windowMs) * windowMs;
        long updatedPoints = activeWindows
                .computeIfAbsent(windowStart, ignored -> new HashMap<>())
                .merge(companyId, POINTS_PER_INVOICE, Long::sum);
        store.put(encodeWindowKey(windowStart, companyId), longToBytes(updatedPoints));
    }

    private void flush(long now) {
        if (activeWindows.isEmpty()) {
            return;
        }
        long maxReadyStart = now - windowMs;
        boolean emittedAny = false;
        Iterator<Map.Entry<Long, Map<String, Long>>> windowIterator =
                activeWindows.headMap(maxReadyStart, true).entrySet().iterator();

        while (windowIterator.hasNext()) {
            Map.Entry<Long, Map<String, Long>> windowEntry = windowIterator.next();
            long windowStart = windowEntry.getKey();
            long windowEnd = windowStart + windowMs;
            Instant startInstant = Instant.ofEpochMilli(windowStart);
            Instant endInstant = Instant.ofEpochMilli(windowEnd);

            for (Map.Entry<String, Long> companyEntry : windowEntry.getValue().entrySet()) {
                String companyId = companyEntry.getKey();
                long points = companyEntry.getValue();
                long seq = nextEmitSeq++;
                String deltaId = taskPartition + ":" + windowStart + ":" + companyId + ":" + seq;

                CompanyRewardDelta delta = new CompanyRewardDelta(
                        deltaId,
                        companyId,
                        points,
                        startInstant,
                        endInstant,
                        CompanyRewardDelta.SCHEMA_VERSION
                );
                store.delete(encodeWindowKey(windowStart, companyId));
                context.forward(new Record<>(companyId, delta, windowEnd));
                metrics.recordWindowEmitted(points);
                emittedAny = true;
            }
            windowIterator.remove();
        }

        if (emittedAny) {
            store.put(SEQ_KEY, longToBytes(nextEmitSeq));
        }
    }

    private static byte[] encodeWindowKey(long windowStart, String companyId) {
        byte[] companyBytes = companyId.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(1 + Long.BYTES + companyBytes.length);
        buffer.put(WINDOW_PREFIX);
        buffer.putLong(windowStart);
        buffer.put(companyBytes);
        return buffer.array();
    }

    private static long decodeWindowStart(byte[] key) {
        return ByteBuffer.wrap(key, 1, Long.BYTES).getLong();
    }

    private static String decodeCompanyId(byte[] key) {
        return new String(key, 1 + Long.BYTES, key.length - 1 - Long.BYTES, StandardCharsets.UTF_8);
    }

    private static byte[] longToBytes(long value) {
        return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
    }

    private static long bytesToLong(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getLong();
    }
}
