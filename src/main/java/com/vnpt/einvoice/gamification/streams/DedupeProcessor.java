package com.vnpt.einvoice.gamification.streams;

import com.vnpt.einvoice.gamification.event.InvoiceIssuedEvent;
import com.vnpt.einvoice.gamification.metrics.RewardMetrics;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * Partition-local exact invoice deduplicator backed by a bounded-retention RocksDB WindowStore
 * and a two-generation rotating primitive Bloom filter negative cache.
 *
 * <p>First-seen invoices skip RocksDB lookups on Bloom miss; Bloom hits always verify against the
 * persistent store so false positives never drop valid invoices. Conflicts (same invoiceId for a
 * different companyId) are flagged via {@link #CONFLICT_HEADER} for DLT quarantine without double award.
 */
public class DedupeProcessor implements Processor<String, InvoiceIssuedEvent, String, InvoiceIssuedEvent> {

    private static final Logger log = LoggerFactory.getLogger(DedupeProcessor.class);

    static final String STORE = "invoice-dedupe-store";
    static final String CONFLICT_HEADER = "x-dlt-reason";
    static final byte[] CONFLICT_VALUE = "INVOICE_COMPANY_CONFLICT".getBytes(StandardCharsets.UTF_8);

    private final long retentionMs;
    private final int bloomExpectedInsertions;
    private final RewardMetrics metrics;

    private ProcessorContext<String, InvoiceIssuedEvent> context;
    private WindowStore<String, String> store;
    private RotatingBloomFilter seen;
    private long maxObservedTimestamp = Long.MIN_VALUE;

    public DedupeProcessor(long retentionMs, int bloomExpectedInsertions, RewardMetrics metrics) {
        this.retentionMs = Math.max(1L, retentionMs);
        this.bloomExpectedInsertions = Math.max(1_024, bloomExpectedInsertions);
        this.metrics = Objects.requireNonNull(metrics);
    }

    @Override
    public void init(ProcessorContext<String, InvoiceIssuedEvent> context) {
        this.context = context;
        this.store = context.getStateStore(STORE);
        this.seen = new RotatingBloomFilter(bloomExpectedInsertions, Math.max(1L, retentionMs / 2));

        // Single chronological pass restores the Bloom cache and recovers maxObservedTimestamp.
        long restoredLatest = Long.MIN_VALUE;
        try (KeyValueIterator<Windowed<String>, String> iterator = store.all()) {
            while (iterator.hasNext()) {
                KeyValue<Windowed<String>, String> entry = iterator.next();
                long ts = entry.key.window().start();
                if (ts > restoredLatest) {
                    restoredLatest = ts;
                }
                seen.add(entry.key.key(), ts);
            }
        }
        if (restoredLatest != Long.MIN_VALUE) {
            this.maxObservedTimestamp = restoredLatest;
        }
    }

    @Override
    public void process(Record<String, InvoiceIssuedEvent> record) {
        String invoiceId = record.key();
        InvoiceIssuedEvent event = record.value();
        if (invoiceId == null || event == null) {
            return;
        }

        long eventTs = Math.max(0L, record.timestamp());
        if (eventTs > maxObservedTimestamp) {
            maxObservedTimestamp = eventTs;
        }
        long cutoff = Math.max(0L, maxObservedTimestamp - retentionMs);

        if (!seen.mightContain(invoiceId)) {
            metrics.recordBloomNegativeSkip();
            claimAndForward(record, invoiceId, event.companyId(), eventTs);
            return;
        }

        metrics.recordBloomHit();
        String existingCompany = findActiveClaim(invoiceId, cutoff, maxObservedTimestamp);
        if (existingCompany == null) {
            claimAndForward(record, invoiceId, event.companyId(), eventTs);
            return;
        }

        if (existingCompany.equals(event.companyId())) {
            metrics.recordDedupeDuplicate();
            return;
        }

        metrics.recordDedupeConflict();
        log.warn(
                "Invoice conflict rejected: invoiceId={} existingCompany={} incomingCompany={}",
                invoiceId,
                existingCompany,
                event.companyId()
        );
        RecordHeaders conflictHeaders = new RecordHeaders(record.headers());
        conflictHeaders.remove(CONFLICT_HEADER);
        conflictHeaders.add(CONFLICT_HEADER, CONFLICT_VALUE);
        context.forward(
                new Record<>(invoiceId, event, record.timestamp(), conflictHeaders),
                RewardTopology.CONFLICT_DLT_SINK
        );
    }

    private void claimAndForward(
            Record<String, InvoiceIssuedEvent> record,
            String invoiceId,
            String companyId,
            long eventTs
    ) {
        store.put(invoiceId, companyId, eventTs);
        seen.add(invoiceId, eventTs);
        metrics.recordDedupeUnique();
        context.forward(record, RewardTopology.REKEY_BY_COMPANY);
    }

    private String findActiveClaim(String invoiceId, long cutoff, long upperBound) {
        try (WindowStoreIterator<String> iterator = store.fetch(invoiceId, cutoff, upperBound)) {
            if (iterator.hasNext()) {
                return iterator.next().value;
            }
        }
        return null;
    }

    /**
     * Two-generation rotating Bloom filter backed by primitive long arrays.
     * Prevents bit saturation over unbounded runtimes without per-element deletion.
     */
    private static final class RotatingBloomFilter {

        private final int wordCount;
        private final int bitMask;
        private final int maxInsertionsPerGen;
        private final long rotationIntervalMs;

        private long[] currentWords;
        private long[] previousWords;
        private int currentInsertions;
        private long currentGenStartTs = Long.MIN_VALUE;

        RotatingBloomFilter(int expectedInsertionsPerGen, long rotationIntervalMs) {
            int rawBits = Math.max(1024, expectedInsertionsPerGen * 10);
            int bitCapacity = Integer.highestOneBit(rawBits - 1) << 1;
            if (bitCapacity <= 0) {
                bitCapacity = 1 << 26;
            }
            this.bitMask = bitCapacity - 1;
            this.wordCount = bitCapacity >>> 6;
            this.maxInsertionsPerGen = Math.max(512, expectedInsertionsPerGen);
            this.rotationIntervalMs = Math.max(1L, rotationIntervalMs);
            this.currentWords = new long[wordCount];
            this.previousWords = new long[wordCount];
        }

        void add(String key, long eventTs) {
            if (currentGenStartTs == Long.MIN_VALUE) {
                currentGenStartTs = eventTs;
            } else if (currentInsertions >= maxInsertionsPerGen
                    || (eventTs - currentGenStartTs) >= rotationIntervalMs) {
                rotate(eventTs);
            }

            long h1 = mix64(key.hashCode());
            long h2 = mix64(h1 ^ 0x9E3779B97F4A7C15L);
            setBit(currentWords, (int) h1 & bitMask);
            setBit(currentWords, (int) (h1 + h2) & bitMask);
            setBit(currentWords, (int) (h1 + 2 * h2) & bitMask);
            currentInsertions++;
        }

        boolean mightContain(String key) {
            long h1 = mix64(key.hashCode());
            long h2 = mix64(h1 ^ 0x9E3779B97F4A7C15L);
            int p1 = (int) h1 & bitMask;
            int p2 = (int) (h1 + h2) & bitMask;
            int p3 = (int) (h1 + 2 * h2) & bitMask;
            return containsAll(currentWords, p1, p2, p3)
                    || containsAll(previousWords, p1, p2, p3);
        }

        private void rotate(long eventTs) {
            long[] recycled = previousWords;
            Arrays.fill(recycled, 0L);
            previousWords = currentWords;
            currentWords = recycled;
            currentInsertions = 0;
            currentGenStartTs = eventTs;
        }

        private static void setBit(long[] words, int bitIndex) {
            words[bitIndex >>> 6] |= (1L << bitIndex);
        }

        private static boolean containsAll(long[] words, int p1, int p2, int p3) {
            return ((words[p1 >>> 6] >>> p1) & 1L) != 0L
                    && ((words[p2 >>> 6] >>> p2) & 1L) != 0L
                    && ((words[p3 >>> 6] >>> p3) & 1L) != 0L;
        }

        private static long mix64(long z) {
            z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
            z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
            return z ^ (z >>> 31);
        }
    }
}
