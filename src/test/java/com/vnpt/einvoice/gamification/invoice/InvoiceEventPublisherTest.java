package com.vnpt.einvoice.gamification.invoice;

import com.vnpt.einvoice.gamification.api.InvoiceAcceptedResponse;
import com.vnpt.einvoice.gamification.api.InvoiceSimulationRequest;
import com.vnpt.einvoice.gamification.event.InvoiceIssuedEvent;
import com.vnpt.einvoice.gamification.exception.EventPublishException;
import com.vnpt.einvoice.gamification.invoice.InvoiceEventPublisher;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class InvoiceEventPublisherTest {

    @Mock
    KafkaTemplate<String, InvoiceIssuedEvent> kafkaTemplate;

    @Test
    void publishesWithCompanyIdAsKafkaKey() {
        when(kafkaTemplate.send(eq("invoice-events"), eq("COMPANY-A"), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        InvoiceEventPublisher publisher = new InvoiceEventPublisher(kafkaTemplate, "invoice-events");

        InvoiceAcceptedResponse response = publisher.publish(
                new InvoiceSimulationRequest("INV-1", "COMPANY-A")
        );

        ArgumentCaptor<InvoiceIssuedEvent> captor = ArgumentCaptor.forClass(InvoiceIssuedEvent.class);
        org.mockito.Mockito.verify(kafkaTemplate)
                .send(eq("invoice-events"), eq("COMPANY-A"), captor.capture());
        assertEquals("INV-1", captor.getValue().invoiceId());
        assertEquals(InvoiceIssuedEvent.TYPE, captor.getValue().eventType());
        assertEquals("PUBLISHING", response.status());
    }

    @Test
    void mapsSynchronousKafkaFailureToApiFailure() {
        when(kafkaTemplate.send(eq("invoice-events"), eq("COMPANY-A"), any()))
                .thenThrow(new KafkaException("broker unavailable"));
        InvoiceEventPublisher publisher = new InvoiceEventPublisher(kafkaTemplate, "invoice-events");

        assertThrows(EventPublishException.class,
                () -> publisher.publish(new InvoiceSimulationRequest("INV-1", "COMPANY-A")));
    }
}
