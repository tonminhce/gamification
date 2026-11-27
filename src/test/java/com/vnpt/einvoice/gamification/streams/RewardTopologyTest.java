package com.vnpt.einvoice.gamification.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.vnpt.einvoice.gamification.event.CompanyRewardDelta;
import com.vnpt.einvoice.gamification.event.InvoiceIssuedEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RewardTopologyTest {

    @TempDir
    Path stateDir;

    @Test
    void deduplicatesInvoicesAndAggregatesByCompanyWindow() {
        ObjectMapper mapper = JsonMapper.builder()
                .addModule(new JavaTimeModule())
                .build();
        JsonSerde<InvoiceIssuedEvent> invoiceSerde =
                new JsonSerde<>(InvoiceIssuedEvent.class, mapper).noTypeInfo();
        JsonSerde<CompanyRewardDelta> deltaSerde =
                new JsonSerde<>(CompanyRewardDelta.class, mapper).noTypeInfo();

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "topology-test");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        try (TopologyTestDriver driver = new TopologyTestDriver(
                RewardTopology.build("invoice-events", "company-reward-deltas", "invoice-events.DLT"),
                properties,
                Instant.now()
        )) {
            TestInputTopic<String, InvoiceIssuedEvent> input = driver.createInputTopic(
                    "invoice-events",
                    Serdes.String().serializer(),
                    invoiceSerde.serializer()
            );
            TestOutputTopic<String, CompanyRewardDelta> output = driver.createOutputTopic(
                    "company-reward-deltas",
                    Serdes.String().deserializer(),
                    deltaSerde.deserializer()
            );

            Instant now = Instant.now();
            input.pipeInput("INV-1", event("INV-1", "COMPANY-A", now));
            input.pipeInput("INV-1", event("INV-1", "COMPANY-A", now));
            input.pipeInput("INV-2", event("INV-2", "COMPANY-B", now));

            driver.advanceWallClockTime(Duration.ofSeconds(1));

            List<CompanyRewardDelta> deltas = output.readKeyValuesToList().stream()
                    .map(keyValue -> keyValue.value)
                    .toList();
            assertEquals(2, deltas.size());
            assertEquals(10, deltas.stream()
                    .filter(delta -> delta.companyId().equals("COMPANY-A"))
                    .mapToLong(CompanyRewardDelta::deltaPoints)
                    .sum());
            assertEquals(10, deltas.stream()
                    .filter(delta -> delta.companyId().equals("COMPANY-B"))
                    .mapToLong(CompanyRewardDelta::deltaPoints)
                    .sum());
        }
    }

    private InvoiceIssuedEvent event(String invoiceId, String companyId, Instant now) {
        return new InvoiceIssuedEvent(
                "event-" + invoiceId + "-" + companyId,
                InvoiceIssuedEvent.TYPE,
                InvoiceIssuedEvent.SCHEMA_VERSION,
                invoiceId,
                companyId,
                now
        );
    }
}