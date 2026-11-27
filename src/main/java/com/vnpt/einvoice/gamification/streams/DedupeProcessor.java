package com.vnpt.einvoice.gamification.streams;

import com.vnpt.einvoice.gamification.event.InvoiceIssuedEvent;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.BitSet;

public class DedupeProcessor implements Processor<String, InvoiceIssuedEvent, String, InvoiceIssuedEvent> {

    static final String STORE = "invoice-dedupe-store";

    private ProcessorContext<String, InvoiceIssuedEvent> context;
    private KeyValueStore<String, Long> store;
    private InvoiceBloomFilter seen;

    @Override
    public void init(ProcessorContext<String, InvoiceIssuedEvent> context) {
        this.context = context;
        this.store = context.getStateStore(STORE);
        this.seen = new InvoiceBloomFilter(4_000_000);
        try (KeyValueIterator<String, Long> iterator = store.all()) {
            while (iterator.hasNext()) {
                seen.add(iterator.next().key);
            }
        }
    }

    @Override
    public void process(Record<String, InvoiceIssuedEvent> record) {
        if (record.key() == null) {
            return;
        }
        if (!seen.mightContain(record.key()) || store.get(record.key()) == null) {
            store.put(record.key(), record.timestamp());
            seen.add(record.key());
            context.forward(record);
        }
    }

    private static final class InvoiceBloomFilter {

        private final BitSet bits;
        private final int mask;

        InvoiceBloomFilter(int expectedInsertions) {
            int size = Integer.highestOneBit(Math.max(64, expectedInsertions * 10 - 1)) << 1;
            this.mask = size - 1;
            this.bits = new BitSet(size);
        }

        void add(String key) {
            int first = mix(key.hashCode());
            bits.set(first & mask);
            bits.set(mix(first) & mask);
        }

        boolean mightContain(String key) {
            int first = mix(key.hashCode());
            return bits.get(first & mask) && bits.get(mix(first) & mask);
        }

        private static int mix(int value) {
            value ^= value >>> 16;
            value *= 0x85EBCA6B;
            value ^= value >>> 13;
            value *= 0xC2B2AE35;
            return value ^ (value >>> 16);
        }
    }
}
