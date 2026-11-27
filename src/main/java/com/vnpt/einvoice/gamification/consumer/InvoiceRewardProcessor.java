package com.vnpt.einvoice.gamification.consumer;

import com.vnpt.einvoice.gamification.event.CompanyRewardDelta;
import com.vnpt.einvoice.gamification.event.InvoiceIssuedEvent;
import com.vnpt.einvoice.gamification.reward.ClaimedCompanyDelta;
import com.vnpt.einvoice.gamification.reward.InvoiceClaimResult;
import com.vnpt.einvoice.gamification.reward.InvoiceClaimService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Component
public class InvoiceRewardProcessor {

    private static final Logger log = LoggerFactory.getLogger(InvoiceRewardProcessor.class);

    private final InvoiceClaimService claimService;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String deltaTopic;
    private final String dltTopic;

    public InvoiceRewardProcessor(
            InvoiceClaimService claimService,
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${app.kafka.delta-topic}") String deltaTopic,
            @Value("${app.kafka.dlt-topic}") String dltTopic
    ) {
        this.claimService = claimService;
        this.kafkaTemplate = kafkaTemplate;
        this.deltaTopic = deltaTopic;
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
        String markerId = first.topic() + ":" + first.partition();
        for (int attempt = 0; attempt < 2; attempt++) {
            InvoiceClaimResult result = claimService.claim(validEvents, markerId, UUID.randomUUID().toString());
            if (result.companies().isEmpty()) {
                return;
            }
            publishDeltas(result);
            claimService.complete(markerId);
            if (!result.recovered()) {
                return;
            }
        }
        throw new IllegalStateException("Claim batch still pending after recovery");
    }

    private void publishDeltas(InvoiceClaimResult result) {
        Instant windowEnd = Instant.now();
        Instant windowStart = windowEnd.minusMillis(500);
        for (ClaimedCompanyDelta company : result.companies()) {
            CompanyRewardDelta delta = new CompanyRewardDelta(
                    result.batchUid() + ":" + company.companyId(),
                    company.companyId(),
                    company.deltaPoints(),
                    windowStart,
                    windowEnd,
                    CompanyRewardDelta.SCHEMA_VERSION
            );
            try {
                kafkaTemplate.send(deltaTopic, company.companyId(), delta).get(2, TimeUnit.SECONDS);
            } catch (Exception ex) {
                if (ex instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                throw new IllegalStateException("Failed to publish company reward delta", ex);
            }
        }
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