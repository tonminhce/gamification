package com.vnpt.einvoice.gamification.invoice;

import com.vnpt.einvoice.gamification.api.InvoiceAcceptedResponse;
import com.vnpt.einvoice.gamification.api.InvoiceBatchAcceptedResponse;
import com.vnpt.einvoice.gamification.api.InvoiceSimulationRequest;
import com.vnpt.einvoice.gamification.event.InvoiceIssuedEvent;
import com.vnpt.einvoice.gamification.exception.EventPublishException;
import com.vnpt.einvoice.gamification.exception.OverloadException;
import com.vnpt.einvoice.gamification.metrics.RewardMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class InvoiceEventPublisherTest {

    @Mock
    KafkaTemplate<String, InvoiceIssuedEvent> kafkaTemplate;

    @Test
    void publishesWithInvoiceIdAsKafkaKey() {
        when(kafkaTemplate.send(eq("invoice-events"), eq("INV-1"), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        InvoiceEventPublisher publisher = new InvoiceEventPublisher(kafkaTemplate, "invoice-events");

        InvoiceAcceptedResponse response = publisher.publish(
                new InvoiceSimulationRequest("INV-1", "COMPANY-A")
        );

        ArgumentCaptor<InvoiceIssuedEvent> captor = ArgumentCaptor.forClass(InvoiceIssuedEvent.class);
        org.mockito.Mockito.verify(kafkaTemplate)
                .send(eq("invoice-events"), eq("INV-1"), captor.capture());
        assertEquals("INV-1", captor.getValue().invoiceId());
        assertEquals(InvoiceIssuedEvent.TYPE, captor.getValue().eventType());
        assertEquals("PUBLISHING", response.status());
    }

    @Test
    void publishesEveryInvoiceInABatch() {
        when(kafkaTemplate.send(eq("invoice-events"), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        InvoiceEventPublisher publisher = new InvoiceEventPublisher(kafkaTemplate, "invoice-events");
        List<InvoiceSimulationRequest> requests = List.of(
                new InvoiceSimulationRequest("INV-1", "COMPANY-A"),
                new InvoiceSimulationRequest("INV-2", "COMPANY-B")
        );

        InvoiceBatchAcceptedResponse response = publisher.publishBatch(requests);

        verify(kafkaTemplate, times(2)).send(eq("invoice-events"), any(), any());
        assertEquals(2, response.acceptedCount());
        assertEquals("PUBLISHING", response.status());
    }

    @Test
    void mapsSynchronousKafkaFailureToApiFailure() {
        when(kafkaTemplate.send(eq("invoice-events"), eq("INV-1"), any()))
                .thenThrow(new KafkaException("broker unavailable"));
        InvoiceEventPublisher publisher = new InvoiceEventPublisher(kafkaTemplate, "invoice-events");

        assertThrows(EventPublishException.class,
                () -> publisher.publish(new InvoiceSimulationRequest("INV-1", "COMPANY-A")));
    }

    @Test
    void rejectsRequestsWithOverloadExceptionWhenInFlightCapIsReached() {
        CompletableFuture<org.springframework.kafka.support.SendResult<String, InvoiceIssuedEvent>> pending =
                new CompletableFuture<>();
        when(kafkaTemplate.send(eq("invoice-events"), any(), any())).thenReturn(pending);
        InvoiceEventPublisher publisher = new InvoiceEventPublisher(
                kafkaTemplate,
                "invoice-events",
                1,
                RewardMetrics.noop()
        );

        publisher.publish(new InvoiceSimulationRequest("INV-1", "COMPANY-A"));
        assertThrows(OverloadException.class,
                () -> publisher.publish(new InvoiceSimulationRequest("INV-2", "COMPANY-A")));

        pending.complete(null);
        InvoiceAcceptedResponse afterRelease = publisher.publish(new InvoiceSimulationRequest("INV-3", "COMPANY-A"));
        assertEquals("PUBLISHING", afterRelease.status());
    }

    @Test
    void publishBatchRejectsWithOverloadExceptionWithoutLeakingPermits() {
        CompletableFuture<SendResult<String, InvoiceIssuedEvent>> p1 = new CompletableFuture<>();
        CompletableFuture<SendResult<String, InvoiceIssuedEvent>> p2 = new CompletableFuture<>();
        when(kafkaTemplate.send(eq("invoice-events"), any(), any()))
                .thenReturn(p1)
                .thenReturn(p2)
                .thenReturn(CompletableFuture.completedFuture(null));

        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        InvoiceEventPublisher publisher = new InvoiceEventPublisher(
                kafkaTemplate, "invoice-events", 2, new RewardMetrics(registry)
        );

        publisher.publishBatch(List.of(
                new InvoiceSimulationRequest("INV-1", "COMPANY-A"),
                new InvoiceSimulationRequest("INV-2", "COMPANY-B")
        ));

        assertThrows(OverloadException.class, () -> publisher.publishBatch(List.of(
                new InvoiceSimulationRequest("INV-3", "COMPANY-C")
        )));
        assertEquals(1.0, registry.counter("gamification.invoice.rejected.overload").count());

        p1.complete(null);
        p2.complete(null);

        InvoiceBatchAcceptedResponse recovered = publisher.publishBatch(List.of(
                new InvoiceSimulationRequest("INV-3", "COMPANY-C"),
                new InvoiceSimulationRequest("INV-4", "COMPANY-D")
        ));
        assertEquals(2, recovered.acceptedCount());
    }

    @Test
    void publishBatchPartialKafkaFailureReleasesPermitsAndTracksExactCounts() {
        CompletableFuture<SendResult<String, InvoiceIssuedEvent>> pendingFirst = new CompletableFuture<>();
        when(kafkaTemplate.send(eq("invoice-events"), any(), any()))
                .thenReturn(pendingFirst)
                .thenThrow(new KafkaException("broker send failed"))
                .thenReturn(CompletableFuture.completedFuture(null));

        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        InvoiceEventPublisher publisher = new InvoiceEventPublisher(
                kafkaTemplate, "invoice-events", 3, new RewardMetrics(registry)
        );

        assertThrows(EventPublishException.class, () -> publisher.publishBatch(List.of(
                new InvoiceSimulationRequest("INV-1", "COMPANY-A"),
                new InvoiceSimulationRequest("INV-2", "COMPANY-B"),
                new InvoiceSimulationRequest("INV-3", "COMPANY-C")
        )));

        assertEquals(1.0, registry.counter("gamification.invoice.accepted").count());
        assertEquals(2.0, registry.counter("gamification.invoice.publish.failed").count());
        // Permits stay held until the already-dispatched in-flight future settles
        assertThrows(OverloadException.class,
                () -> publisher.publish(new InvoiceSimulationRequest("INV-4", "COMPANY-D")));

        pendingFirst.complete(null);
        assertEquals(1.0, registry.counter("gamification.invoice.published").count());

        InvoiceBatchAcceptedResponse afterRecovery = publisher.publishBatch(List.of(
                new InvoiceSimulationRequest("INV-5", "COMPANY-A"),
                new InvoiceSimulationRequest("INV-6", "COMPANY-B"),
                new InvoiceSimulationRequest("INV-7", "COMPANY-C")
        ));
        assertEquals(3, afterRecovery.acceptedCount());
        assertEquals(4.0, registry.counter("gamification.invoice.accepted").count());
        assertEquals(4.0, registry.counter("gamification.invoice.published").count());
        assertEquals(2.0, registry.counter("gamification.invoice.publish.failed").count());
    }

    @Test
    void publishBatchRecordsAsyncFailureAndReleasesPermits() {
        when(kafkaTemplate.send(eq("invoice-events"), eq("INV-1"), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(kafkaTemplate.send(eq("invoice-events"), eq("INV-2"), any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("async broker error")));

        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        InvoiceEventPublisher publisher = new InvoiceEventPublisher(
                kafkaTemplate, "invoice-events", 2, new RewardMetrics(registry)
        );

        InvoiceBatchAcceptedResponse response = publisher.publishBatch(List.of(
                new InvoiceSimulationRequest("INV-1", "COMPANY-A"),
                new InvoiceSimulationRequest("INV-2", "COMPANY-B")
        ));

        assertEquals(2, response.acceptedCount());
        assertEquals(2.0, registry.counter("gamification.invoice.accepted").count());
        assertEquals(1.0, registry.counter("gamification.invoice.published").count());
        assertEquals(1.0, registry.counter("gamification.invoice.publish.failed").count());

        when(kafkaTemplate.send(eq("invoice-events"), eq("INV-3"), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        assertEquals("PUBLISHING", publisher.publish(new InvoiceSimulationRequest("INV-3", "COMPANY-C")).status());
    }
}
