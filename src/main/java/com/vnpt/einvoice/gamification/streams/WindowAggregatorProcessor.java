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
 * <p>Maintains an in-memory window index synchronized with a caching-enabled RocksDB store so
 * {@code process()} avoids RocksDB reads and byte-key re-encoding, while {@code flush()} inspects
 * only expired windows in O(ready). Emits deterministic sequence-numbered delta IDs so EOS rollbacks
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
    private final NavigableMap<Long, Map<String, WindowAccum>> activeWindows = new TreeMap<>();

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
                    this.nextEmitSeq = decodeLong(entry.value, 0);
                } else if (entry.key[0] == WINDOW_PREFIX && entry.key.length > 1 + Long.BYTES) {
                    long windowStart = decodeLong(entry.key, 1);
                    String companyId = decodeCompanyId(entry.key);
                    long points = decodeLong(entry.value, 0);
                    activeWindows.computeIfAbsent(windowStart, ignored -> new HashMap<>())
                            .put(companyId, new WindowAccum(points, entry.key));
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
        Map<String, WindowAccum> companyMap = activeWindows.computeIfAbsent(windowStart, ignored -> new HashMap<>());
        WindowAccum accum = companyMap.get(companyId);
        if (accum == null) {
            accum = new WindowAccum(POINTS_PER_INVOICE, encodeWindowKey(windowStart, companyId));
            companyMap.put(companyId, accum);
        } else {
            accum.points += POINTS_PER_INVOICE;
        }
        store.put(accum.storeKey, encodeLong(accum.points));
    }

    private void flush(long now) {
        if (activeWindows.isEmpty()) {
            return;
        }
        long maxReadyStart = now - windowMs;
        boolean emittedAny = false;
        Iterator<Map.Entry<Long, Map<String, WindowAccum>>> windowIterator =
                activeWindows.headMap(maxReadyStart, true).entrySet().iterator();

        while (windowIterator.hasNext()) {
            Map.Entry<Long, Map<String, WindowAccum>> windowEntry = windowIterator.next();
            long windowStart = windowEntry.getKey();
            long windowEnd = windowStart + windowMs;
            Instant startInstant = Instant.ofEpochMilli(windowStart);
            Instant endInstant = Instant.ofEpochMilli(windowEnd);

            for (Map.Entry<String, WindowAccum> companyEntry : windowEntry.getValue().entrySet()) {
                String companyId = companyEntry.getKey();
                WindowAccum accum = companyEntry.getValue();
                long seq = nextEmitSeq++;
                String deltaId = taskPartition + ":" + windowStart + ":" + companyId + ":" + seq;

                CompanyRewardDelta delta = new CompanyRewardDelta(
                        deltaId,
                        companyId,
                        accum.points,
                        startInstant,
                        endInstant,
                        CompanyRewardDelta.SCHEMA_VERSION
                );
                store.delete(accum.storeKey);
                context.forward(new Record<>(companyId, delta, windowEnd));
                metrics.recordWindowEmitted(accum.points);
                emittedAny = true;
            }
            windowIterator.remove();
        }

        if (emittedAny) {
            store.put(SEQ_KEY, encodeLong(nextEmitSeq));
        }
    }

    private static byte[] encodeWindowKey(long windowStart, String companyId) {
        byte[] companyBytes = companyId.getBytes(StandardCharsets.UTF_8);
        byte[] out = new byte[1 + Long.BYTES + companyBytes.length];
        out[0] = WINDOW_PREFIX;
        writeLong(out, 1, windowStart);
        System.arraycopy(companyBytes, 0, out, 1 + Long.BYTES, companyBytes.length);
        return out;
    }

    private static String decodeCompanyId(byte[] key) {
        return new String(key, 1 + Long.BYTES, key.length - 1 - Long.BYTES, StandardCharsets.UTF_8);
    }

    private static byte[] encodeLong(long value) {
        byte[] out = new byte[Long.BYTES];
        writeLong(out, 0, value);
        return out;
    }

    private static void writeLong(byte[] dst, int offset, long value) {
        for (int i = Long.BYTES - 1; i >= 0; i--) {
            dst[offset + i] = (byte) (value & 0xFFL);
            value >>>= 8;
        }
    }

    private static long decodeLong(byte[] src, int offset) {
        long value = 0L;
        for (int i = 0; i < Long.BYTES; i++) {
            value = (value << 8) | (src[offset + i] & 0xFFL);
        }
        return value;
    }

    private static final class WindowAccum {
        long points;
        final byte[] storeKey;

        WindowAccum(long points, byte[] storeKey) {
            this.points = points;
            this.storeKey = storeKey;
        }
    }
}
