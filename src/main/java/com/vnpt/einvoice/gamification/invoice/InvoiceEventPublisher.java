package com.vnpt.einvoice.gamification.invoice;

import com.vnpt.einvoice.gamification.api.InvoiceAcceptedResponse;
import com.vnpt.einvoice.gamification.api.InvoiceBatchAcceptedResponse;
import com.vnpt.einvoice.gamification.api.InvoiceSimulationRequest;
import com.vnpt.einvoice.gamification.event.InvoiceIssuedEvent;
import com.vnpt.einvoice.gamification.exception.EventPublishException;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Service
public class InvoiceEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(InvoiceEventPublisher.class);

    private final KafkaTemplate<String, InvoiceIssuedEvent> kafkaTemplate;
    private final String topic;

    public InvoiceEventPublisher(
            KafkaTemplate<String, InvoiceIssuedEvent> kafkaTemplate,
            @Value("${app.kafka.topic}") String topic
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public InvoiceAcceptedResponse publish(InvoiceSimulationRequest request) {
        String eventId = UUID.randomUUID().toString();
        InvoiceIssuedEvent event = new InvoiceIssuedEvent(
                eventId,
                InvoiceIssuedEvent.TYPE,
                InvoiceIssuedEvent.SCHEMA_VERSION,
                request.invoiceId(),
                request.companyId(),
                Instant.now()
        );

        try {
            kafkaTemplate.send(topic, request.companyId(), event).whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Asynchronous Kafka publish failed for invoiceId={}", request.invoiceId(), ex);
                }
            });
        } catch (KafkaException ex) {
            throw new EventPublishException("Kafka rejected the invoice event", ex);
        }

        return new InvoiceAcceptedResponse(
                request.invoiceId(),
                request.companyId(),
                eventId,
                "PUBLISHING"
        );
    }

    public InvoiceBatchAcceptedResponse publishBatch(List<InvoiceSimulationRequest> requests) {
        for (InvoiceSimulationRequest request : requests) {
            publish(request);
        }
        return new InvoiceBatchAcceptedResponse(requests.size(), "PUBLISHING");
    }
}
