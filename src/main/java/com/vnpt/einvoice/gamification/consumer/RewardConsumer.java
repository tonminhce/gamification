package com.vnpt.einvoice.gamification.consumer;

import com.vnpt.einvoice.gamification.event.InvoiceIssuedEvent;
import com.vnpt.einvoice.gamification.reward.RewardService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
public class RewardConsumer {

    private static final Logger log = LoggerFactory.getLogger(RewardConsumer.class);

    private final RewardService rewardService;
    private final KafkaTemplate<String, InvoiceIssuedEvent> kafkaTemplate;
    private final String dltTopic;

    public RewardConsumer(
            RewardService rewardService,
            KafkaTemplate<String, InvoiceIssuedEvent> kafkaTemplate,
            @Value("${app.kafka.dlt-topic}") String dltTopic
    ) {
        this.rewardService = rewardService;
        this.kafkaTemplate = kafkaTemplate;
        this.dltTopic = dltTopic;
    }

    @KafkaListener(topics = "${app.kafka.topic}", batch = "true")
    public void consume(List<ConsumerRecord<String, InvoiceIssuedEvent>> records) {
        if (records.isEmpty()) {
            return;
        }

        List<InvoiceIssuedEvent> validEvents = new ArrayList<>(records.size());
        for (ConsumerRecord<String, InvoiceIssuedEvent> record : records) {
            InvoiceIssuedEvent event = record.value();
            if (valid(event)) {
                validEvents.add(event);
            } else {
                sendToDlt(event);
            }
        }

        if (validEvents.isEmpty()) {
            return;
        }
        ConsumerRecord<String, InvoiceIssuedEvent> first = records.getFirst();
        String batchMarkerId = first.topic() + ":" + first.partition();
        rewardService.applyRewards(validEvents, batchMarkerId);
    }

    private boolean valid(InvoiceIssuedEvent event) {
        return event != null
                && InvoiceIssuedEvent.TYPE.equals(event.eventType())
                && event.schemaVersion() == InvoiceIssuedEvent.SCHEMA_VERSION
                && event.invoiceId() != null
                && !event.invoiceId().isBlank()
                && event.companyId() != null
                && !event.companyId().isBlank();
    }

    private void sendToDlt(InvoiceIssuedEvent event) {
        if (event == null) {
            log.warn("Skipping null invoice event without DLT payload");
            return;
        }
        String key = event.companyId() == null ? "unknown" : event.companyId();
        try {
            kafkaTemplate.send(dltTopic, key, event).get(2, TimeUnit.SECONDS);
        } catch (Exception ex) {
            if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new IllegalStateException("Failed to publish invalid invoice event to DLT", ex);
        }
    }
}