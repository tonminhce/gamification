package com.vnpt.einvoice.gamification.streams;

import com.vnpt.einvoice.gamification.event.CompanyRewardDelta;
import com.vnpt.einvoice.gamification.event.InvoiceIssuedEvent;
import com.vnpt.einvoice.gamification.metrics.RewardMetrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;

public final class RewardTopology {

    static final String DEDUPE_PROCESSOR = "invoice-dedupe";
    static final String REKEY_BY_COMPANY = "rekey-by-company";
    static final String AGGREGATOR_PROCESSOR = "company-window-aggregate";
    static final String CONFLICT_DLT_SINK = "conflict-dlt-sink";
    static final long DEFAULT_RETENTION_MS = Duration.ofHours(24).toMillis();
    static final int DEFAULT_BLOOM_EXPECTED = 4_000_000;

    private RewardTopology() {
    }

    public static Topology build(String sourceTopic, String deltaTopic, String dltTopic) {
        return build(
                sourceTopic,
                deltaTopic,
                dltTopic,
                DEFAULT_RETENTION_MS,
                DEFAULT_BLOOM_EXPECTED,
                WindowAggregatorProcessor.DEFAULT_WINDOW_MS,
                WindowAggregatorProcessor.DEFAULT_PUNCTUATION_MS,
                RewardMetrics.noop()
        );
    }

    public static Topology build(
            String sourceTopic,
            String deltaTopic,
            String dltTopic,
            long dedupeRetentionMs,
            int bloomExpectedInsertions,
            long windowSizeMs,
            long punctuationMs,
            RewardMetrics metrics
    ) {
        JsonMapper mapper = JsonMapper.builder().build();
        JacksonJsonSerde<InvoiceIssuedEvent> invoiceSerde =
                new JacksonJsonSerde<>(InvoiceIssuedEvent.class, mapper).noTypeInfo();
        JacksonJsonSerde<CompanyRewardDelta> deltaSerde =
                new JacksonJsonSerde<>(CompanyRewardDelta.class, mapper).noTypeInfo();

        long effectiveRetentionMs = Math.max(1_000L, dedupeRetentionMs);
        Duration retention = Duration.ofMillis(effectiveRetentionMs);
        StoreBuilder<?> dedupeStore = Stores.windowStoreBuilder(
                Stores.persistentWindowStore(
                        DedupeProcessor.STORE,
                        retention,
                        retention,
                        false
                ),
                Serdes.String(),
                Serdes.String()
        );
        StoreBuilder<?> windowStore = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(WindowAggregatorProcessor.STORE),
                Serdes.ByteArray(),
                Serdes.ByteArray()
        ).withCachingEnabled();

        StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(dedupeStore);
        builder.addStateStore(windowStore);

        KStream<String, InvoiceIssuedEvent> source = builder
                .stream(sourceTopic, Consumed.with(Serdes.String(), invoiceSerde));

        source.split()
                .branch(
                        (key, event) -> event != null && !validRecord(key, event),
                        Branched.withConsumer(invalid ->
                                invalid.to(dltTopic, Produced.with(Serdes.String(), invoiceSerde)))
                )
                .branch(
                        RewardTopology::validRecord,
                        Branched.withConsumer(validEvents -> validEvents
                                .process(
                                        () -> new DedupeProcessor(
                                                effectiveRetentionMs,
                                                bloomExpectedInsertions,
                                                metrics
                                        ),
                                        Named.as(DEDUPE_PROCESSOR),
                                        DedupeProcessor.STORE
                                )
                                .selectKey((invoiceId, event) -> event.companyId(), Named.as(REKEY_BY_COMPANY))
                                .process(
                                        () -> new WindowAggregatorProcessor(
                                                windowSizeMs,
                                                punctuationMs,
                                                metrics
                                        ),
                                        Named.as(AGGREGATOR_PROCESSOR),
                                        WindowAggregatorProcessor.STORE
                                )
                                .to(deltaTopic, Produced.with(Serdes.String(), deltaSerde))
                        )
                );

        Topology topology = builder.build();
        topology.addSink(
                CONFLICT_DLT_SINK,
                dltTopic,
                Serdes.String().serializer(),
                invoiceSerde.serializer(),
                DEDUPE_PROCESSOR
        );
        return topology;
    }

    private static boolean validRecord(String key, InvoiceIssuedEvent event) {
        return event != null
                && InvoiceIssuedEvent.TYPE.equals(event.eventType())
                && event.schemaVersion() == InvoiceIssuedEvent.SCHEMA_VERSION
                && event.invoiceId() != null
                && !event.invoiceId().isBlank()
                && event.companyId() != null
                && !event.companyId().isBlank()
                && key != null
                && key.equals(event.invoiceId());
    }
}
