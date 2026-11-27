package com.vnpt.einvoice.gamification.streams;

import com.vnpt.einvoice.gamification.event.CompanyRewardDelta;
import com.vnpt.einvoice.gamification.event.InvoiceIssuedEvent;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class WindowAggregatorProcessor implements Processor<String, InvoiceIssuedEvent, String, CompanyRewardDelta> {

    static final String STORE = "company-window-store";
    static final long WINDOW_MS = 500L;
    static final long POINTS_PER_INVOICE = 10L;

    private ProcessorContext<String, CompanyRewardDelta> context;
    private KeyValueStore<String, Long> store;

    @Override
    public void init(ProcessorContext<String, CompanyRewardDelta> context) {
        this.context = context;
        this.store = context.getStateStore(STORE);
        context.schedule(
                Duration.ofMillis(250),
                PunctuationType.WALL_CLOCK_TIME,
                this::flush
        );
    }

    @Override
    public void process(Record<String, InvoiceIssuedEvent> record) {
        if (record.key() == null || record.value() == null) {
            return;
        }
        long windowStart = Math.floorDiv(record.timestamp(), WINDOW_MS) * WINDOW_MS;
        String key = storeKey(record.key(), windowStart);
        Long current = store.get(key);
        store.put(key, (current == null ? 0L : current) + POINTS_PER_INVOICE);
    }

    private void flush(long now) {
        List<KeyValue<String, Long>> ready = new ArrayList<>();
        try (KeyValueIterator<String, Long> iterator = store.all()) {
            while (iterator.hasNext()) {
                KeyValue<String, Long> entry = iterator.next();
                ParsedKey parsed = parseKey(entry.key);
                if (parsed != null && parsed.windowStart + WINDOW_MS <= now) {
                    ready.add(entry);
                }
            }
        }

        for (KeyValue<String, Long> entry : ready) {
            ParsedKey parsed = parseKey(entry.key);
            if (parsed == null) {
                continue;
            }
            long windowEnd = parsed.windowStart + WINDOW_MS;
            CompanyRewardDelta delta = new CompanyRewardDelta(
                    UUID.randomUUID() + ":" + parsed.companyId + ":" + parsed.windowStart,
                    parsed.companyId,
                    entry.value,
                    Instant.ofEpochMilli(parsed.windowStart),
                    Instant.ofEpochMilli(windowEnd),
                    CompanyRewardDelta.SCHEMA_VERSION
            );
            store.delete(entry.key);
            context.forward(new Record<>(parsed.companyId, delta, windowEnd));
        }
    }

    private String storeKey(String companyId, long windowStart) {
        return companyId.length() + ":" + companyId + ":" + windowStart;
    }

    private ParsedKey parseKey(String key) {
        try {
            int lengthEnd = key.indexOf(':');
            int companyLength = Integer.parseInt(key.substring(0, lengthEnd));
            int companyStart = lengthEnd + 1;
            String companyId = key.substring(companyStart, companyStart + companyLength);
            long windowStart = Long.parseLong(key.substring(companyStart + companyLength + 1));
            return new ParsedKey(companyId, windowStart);
        } catch (RuntimeException ex) {
            return null;
        }
    }

    private record ParsedKey(String companyId, long windowStart) {
    }
}