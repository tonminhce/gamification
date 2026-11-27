package com.vnpt.einvoice.gamification.streams;

import com.vnpt.einvoice.gamification.event.InvoiceIssuedEvent;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

public class DedupeProcessor implements Processor<String, InvoiceIssuedEvent, String, InvoiceIssuedEvent> {

    static final String STORE = "invoice-dedupe-store";

    private ProcessorContext<String, InvoiceIssuedEvent> context;
    private KeyValueStore<String, Long> store;

    @Override
    public void init(ProcessorContext<String, InvoiceIssuedEvent> context) {
        this.context = context;
        this.store = context.getStateStore(STORE);
    }

    @Override
    public void process(Record<String, InvoiceIssuedEvent> record) {
        if (record.key() == null || store.get(record.key()) != null) {
            return;
        }
        store.put(record.key(), record.timestamp());
        context.forward(record);
    }
}