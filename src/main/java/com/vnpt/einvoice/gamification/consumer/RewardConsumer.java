package com.vnpt.einvoice.gamification.consumer;

import com.vnpt.einvoice.gamification.event.InvoiceIssuedEvent;
import com.vnpt.einvoice.gamification.exception.InvalidInvoiceEventException;
import com.vnpt.einvoice.gamification.reward.RewardService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class RewardConsumer {

    private final RewardService rewardService;

    public RewardConsumer(RewardService rewardService) {
        this.rewardService = rewardService;
    }

    @KafkaListener(topics = "${app.kafka.topic}", batch = "true")
    public void consume(List<ConsumerRecord<String, InvoiceIssuedEvent>> records) {
        List<InvoiceIssuedEvent> events = records.stream()
                .map(ConsumerRecord::value)
                .toList();
        events.forEach(this::validate);
        rewardService.applyRewards(events);
    }

    private void validate(InvoiceIssuedEvent event) {
        if (event == null
                || !InvoiceIssuedEvent.TYPE.equals(event.eventType())
                || event.schemaVersion() != InvoiceIssuedEvent.SCHEMA_VERSION
                || event.invoiceId() == null
                || event.invoiceId().isBlank()
                || event.companyId() == null
                || event.companyId().isBlank()) {
            throw new InvalidInvoiceEventException("Invalid INVOICE_ISSUED event");
        }
    }
}
