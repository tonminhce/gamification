package com.vnpt.einvoice.gamification.streams;

import com.vnpt.einvoice.gamification.event.InvoiceIssuedEvent;
import com.vnpt.einvoice.gamification.ledger.DedupeLedger;
import com.vnpt.einvoice.gamification.metrics.RewardMetrics;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

/**
 * Partition-local exact invoice deduplicator backed by a bounded-retention RocksDB WindowStore
 * and a 3-generation time-bucketed primitive Bloom filter negative cache.
 *
 * <p>First-seen invoices skip RocksDB lookups on Bloom miss; Bloom hits (and saturated buckets)
 * always verify against the persistent store so false positives never drop valid invoices.
 * Out-of-retention timestamps and invoice-company conflicts are quarantined to DLT without award.
 */
public class DedupeProcessor implements Processor<String, InvoiceIssuedEvent, String, InvoiceIssuedEvent> {

    private static final Logger log = LoggerFactory.getLogger(DedupeProcessor.class);

    static final String STORE = "invoice-dedupe-store";
    static final String CONFLICT_HEADER = "x-dlt-reason";
    static final byte[] CONFLICT_VALUE = "INVOICE_COMPANY_CONFLICT".getBytes(StandardCharsets.UTF_8);
    static final byte[] EXPIRED_VALUE = "EXPIRED_INVOICE_TIMESTAMP".getBytes(StandardCharsets.UTF_8);

    private final long retentionMs;
    private final int bloomExpectedInsertions;
    private final RewardMetrics metrics;
    private final DedupeLedger externalLedger;

    private ProcessorContext<String, InvoiceIssuedEvent> context;
    private WindowStore<String, String> store;
    private TimeBucketedBloomFilter seen;
    private long maxObservedTimestamp = Long.MIN_VALUE;

    public DedupeProcessor(long retentionMs, int bloomExpectedInsertions, RewardMetrics metrics) {
        this(retentionMs, bloomExpectedInsertions, metrics, null);
    }

    public DedupeProcessor(
            long retentionMs,
            int bloomExpectedInsertions,
            RewardMetrics metrics,
            DedupeLedger externalLedger
    ) {
        this.retentionMs = Math.max(1_000L, retentionMs);
        this.bloomExpectedInsertions = Math.max(1_024, bloomExpectedInsertions);
        this.metrics = Objects.requireNonNull(metrics);
        this.externalLedger = externalLedger;
    }

    @Override
    public void init(ProcessorContext<String, InvoiceIssuedEvent> context) {
        this.context = context;
        this.maxObservedTimestamp = Long.MIN_VALUE;
        if (externalLedger != null) {
            this.store = null;
            this.seen = null;
            return;
        }
        this.store = context.getStateStore(STORE);
        this.seen = new TimeBucketedBloomFilter(bloomExpectedInsertions, retentionMs);

        try (KeyValueIterator<Windowed<String>, String> iterator = store.all()) {
            while (iterator.hasNext()) {
                KeyValue<Windowed<String>, String> entry = iterator.next();
                long ts = entry.key.window().start();
                if (ts > maxObservedTimestamp) {
                    maxObservedTimestamp = ts;
                }
                seen.add(entry.key.key(), ts);
            }
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

        if (eventTs < cutoff) {
            metrics.recordDedupeExpired();
            log.warn(
                    "Expired invoice rejected before store write: invoiceId={} eventTs={} cutoff={}",
                    invoiceId,
                    eventTs,
                    cutoff
            );
            forwardToDlt(record, EXPIRED_VALUE);
            return;
        }

        if (externalLedger != null) {
            processWithExternalLedger(record, invoiceId, event, eventTs);
            return;
        }

        if (!seen.mightContain(invoiceId, cutoff)) {
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
        forwardToDlt(record, CONFLICT_VALUE);
    }

    private void processWithExternalLedger(
            Record<String, InvoiceIssuedEvent> record,
            String invoiceId,
            InvoiceIssuedEvent event,
            long eventTs
    ) {
        String sourceId = resolveSourceId(event);
        DedupeLedger.ClaimResult claim = externalLedger.claim(
                invoiceId,
                event.companyId(),
                sourceId,
                Instant.ofEpochMilli(eventTs),
                Duration.ofMillis(retentionMs)
        );
        switch (claim.status()) {
            case CLAIMED_NEW -> {
                metrics.recordDedupeUnique();
                context.forward(record, RewardTopology.REKEY_BY_COMPANY);
                externalLedger.markEmitted(invoiceId, sourceId, Instant.now());
            }
            case RECOVERABLE_CLAIMED_UNEMITTED -> {
                metrics.recordDedupeLedgerRecovered();
                context.forward(record, RewardTopology.REKEY_BY_COMPANY);
                externalLedger.markEmitted(invoiceId, sourceId, Instant.now());
            }
            case DUPLICATE_EMITTED, IN_FLIGHT_CLAIMED_BY_OTHER -> metrics.recordDedupeDuplicate();
            case CONFLICT -> {
                metrics.recordDedupeConflict();
                log.warn(
                        "Invoice conflict rejected by external ledger: invoiceId={} existingCompany={} incomingCompany={}",
                        invoiceId,
                        claim.existingCompanyId(),
                        event.companyId()
                );
                forwardToDlt(record, CONFLICT_VALUE);
            }
        }
    }

    private String resolveSourceId(InvoiceIssuedEvent event) {
        RecordMetadata metadata = context.recordMetadata().orElse(null);
        if (metadata != null) {
            return metadata.partition() + ":" + metadata.offset();
        }
        return event.eventId() != null ? event.eventId() : "local";
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

    private void forwardToDlt(Record<String, InvoiceIssuedEvent> record, byte[] reason) {
        RecordHeaders dltHeaders = new RecordHeaders(record.headers());
        dltHeaders.remove(CONFLICT_HEADER);
        dltHeaders.add(CONFLICT_HEADER, reason);
        context.forward(
                new Record<>(record.key(), record.value(), record.timestamp(), dltHeaders),
                RewardTopology.CONFLICT_DLT_SINK
        );
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
     * Three-bucket time-partitioned Bloom filter with 64-bit FNV-1a character hashing and
     * count-saturation fallback. Guarantees zero false negatives over the full retention window
     * even under non-chronological RocksDB restores or insertion spikes exceeding sizing hints.
     */
    private static final class TimeBucketedBloomFilter {

        private static final int BUCKET_COUNT = 3;
        private static final long MAX_BITS_PER_BUCKET = 1L << 29;

        private final long bucketMs;
        private final int bitMask;
        private final int maxSafeInsertions;
        private final BloomBucket[] buckets = new BloomBucket[BUCKET_COUNT];
        private long highestBucketId = Long.MIN_VALUE;

        TimeBucketedBloomFilter(int expectedInsertionsPerGen, long retentionMs) {
            this.bucketMs = Math.max(1L, (retentionMs + 1L) / 2L);
            long rawBits = Math.max(1024L, (long) expectedInsertionsPerGen * 10L);
            long capacity = Long.highestOneBit(rawBits - 1L) << 1;
            if (capacity <= 0L || capacity > MAX_BITS_PER_BUCKET) {
                capacity = MAX_BITS_PER_BUCKET;
            }
            this.bitMask = (int) (capacity - 1L);
            int wordCount = (int) (capacity >>> 6);
            this.maxSafeInsertions = (int) Math.min(Integer.MAX_VALUE, Math.max(1_024L, (long) expectedInsertionsPerGen * 2L));
            for (int i = 0; i < BUCKET_COUNT; i++) {
                buckets[i] = new BloomBucket(wordCount);
            }
        }

        void add(String key, long eventTs) {
            long targetBucketId = eventTs / bucketMs;
            if (targetBucketId > highestBucketId) {
                highestBucketId = targetBucketId;
            }
            long minRetainedBucket = highestBucketId - (BUCKET_COUNT - 1);
            if (targetBucketId < minRetainedBucket) {
                return;
            }

            BloomBucket bucket = resolveOrAssignBucket(targetBucketId, minRetainedBucket);
            if (bucket == null || bucket.saturated) {
                return;
            }
            if (bucket.insertions >= maxSafeInsertions) {
                bucket.saturated = true;
                return;
            }

            long h1 = hash64(key);
            long h2 = mix64(h1 ^ 0x9E3779B97F4A7C15L);
            setBit(bucket.words, (int) h1 & bitMask);
            setBit(bucket.words, (int) (h1 + h2) & bitMask);
            setBit(bucket.words, (int) (h1 + 2L * h2) & bitMask);
            bucket.insertions++;
        }

        boolean mightContain(String key, long cutoffTs) {
            if (highestBucketId == Long.MIN_VALUE) {
                return false;
            }
            long minActiveBucket = Math.max(0L, cutoffTs / bucketMs);
            long h1 = hash64(key);
            long h2 = mix64(h1 ^ 0x9E3779B97F4A7C15L);
            int p1 = (int) h1 & bitMask;
            int p2 = (int) (h1 + h2) & bitMask;
            int p3 = (int) (h1 + 2L * h2) & bitMask;

            for (BloomBucket bucket : buckets) {
                if (bucket.bucketId >= minActiveBucket) {
                    if (bucket.saturated || containsAll(bucket.words, p1, p2, p3)) {
                        return true;
                    }
                }
            }
            return false;
        }

        private BloomBucket resolveOrAssignBucket(long targetBucketId, long minRetainedBucket) {
            BloomBucket reusable = null;
            for (BloomBucket bucket : buckets) {
                if (bucket.bucketId == targetBucketId) {
                    return bucket;
                }
                if (reusable == null && bucket.bucketId < minRetainedBucket) {
                    reusable = bucket;
                }
            }
            if (reusable != null) {
                reusable.reset(targetBucketId);
            }
            return reusable;
        }

        private static void setBit(long[] words, int bitIndex) {
            words[bitIndex >>> 6] |= (1L << bitIndex);
        }

        private static boolean containsAll(long[] words, int p1, int p2, int p3) {
            return ((words[p1 >>> 6] >>> p1) & 1L) != 0L
                    && ((words[p2 >>> 6] >>> p2) & 1L) != 0L
                    && ((words[p3 >>> 6] >>> p3) & 1L) != 0L;
        }

        private static long hash64(String key) {
            long hash = 0xCBF29CE484222325L;
            int len = key.length();
            for (int i = 0; i < len; i++) {
                hash ^= key.charAt(i);
                hash *= 0x100000001B3L;
            }
            return mix64(hash);
        }

        private static long mix64(long z) {
            z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
            z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
            return z ^ (z >>> 31);
        }

        private static final class BloomBucket {
            long bucketId = Long.MIN_VALUE;
            final long[] words;
            int insertions;
            boolean saturated;

            BloomBucket(int wordCount) {
                this.words = new long[wordCount];
            }

            void reset(long newBucketId) {
                Arrays.fill(words, 0L);
                this.bucketId = newBucketId;
                this.insertions = 0;
                this.saturated = false;
            }
        }
    }
}
