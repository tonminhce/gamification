package com.vnpt.einvoice.gamification.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.vnpt.einvoice.gamification.event.CompanyRewardDelta;
import com.vnpt.einvoice.gamification.event.InvoiceIssuedEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.kafka.support.serializer.JsonSerde;

public final class RewardTopology {

    static final String DEDUPE_PROCESSOR = "invoice-dedupe";
    static final String AGGREGATOR_PROCESSOR = "company-window-aggregate";

    private RewardTopology() {
    }

    public static Topology build(String sourceTopic, String deltaTopic, String dltTopic) {
        ObjectMapper mapper = JsonMapper.builder()
                .addModule(new JavaTimeModule())
                .build();
        JsonSerde<InvoiceIssuedEvent> invoiceSerde =
                new JsonSerde<>(InvoiceIssuedEvent.class, mapper).noTypeInfo();
        JsonSerde<CompanyRewardDelta> deltaSerde =
                new JsonSerde<>(CompanyRewardDelta.class, mapper);

        StreamsBuilder builder = new StreamsBuilder();
        StoreBuilder<?> dedupeStore = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(DedupeProcessor.STORE),
                Serdes.String(),
                Serdes.Long()
        );
        StoreBuilder<?> windowStore = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(WindowAggregatorProcessor.STORE),
                Serdes.String(),
                Serdes.Long()
        );
        builder.addStateStore(dedupeStore);
        builder.addStateStore(windowStore);

        KStream<String, InvoiceIssuedEvent> source = builder
                .stream(sourceTopic, Consumed.with(Serdes.String(), invoiceSerde));

        source.filter((key, event) -> !valid(event) && event != null)
                .to(dltTopic, Produced.with(Serdes.String(), invoiceSerde));

        source.filter((key, event) -> valid(event))
                .process(
                        DedupeProcessor::new,
                        Named.as(DEDUPE_PROCESSOR),
                        DedupeProcessor.STORE
                )
                .selectKey((invoiceId, event) -> event.companyId())
                .process(
                        WindowAggregatorProcessor::new,
                        Named.as(AGGREGATOR_PROCESSOR),
                        WindowAggregatorProcessor.STORE
                )
                .to(deltaTopic, Produced.with(Serdes.String(), deltaSerde));

        return builder.build();
    }

    private static boolean valid(InvoiceIssuedEvent event) {
        return event != null
                && InvoiceIssuedEvent.TYPE.equals(event.eventType())
                && event.schemaVersion() == InvoiceIssuedEvent.SCHEMA_VERSION
                && event.invoiceId() != null
                && !event.invoiceId().isBlank()
                && event.companyId() != null
                && !event.companyId().isBlank();
    }
}