package com.vnpt.einvoice.gamification.invoice;

import com.vnpt.einvoice.gamification.api.InvoiceAcceptedResponse;
import com.vnpt.einvoice.gamification.api.InvoiceBatchAcceptedResponse;
import com.vnpt.einvoice.gamification.api.InvoiceSimulationRequest;
import com.vnpt.einvoice.gamification.event.InvoiceIssuedEvent;
import com.vnpt.einvoice.gamification.exception.EventPublishException;
import com.vnpt.einvoice.gamification.exception.OverloadException;
import com.vnpt.einvoice.gamification.metrics.RewardMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PublishCircuitBreakerTest {

    @Mock
    KafkaTemplate<String, InvoiceIssuedEvent> kafkaTemplate;

    @Test
    void tripsOpenAfterConsecutiveFailuresAndClosesAfterHalfOpenProbes() throws Exception {
        PublishCircuitBreaker breaker = new PublishCircuitBreaker(true, 3, 120L, 2);
        assertEquals(PublishCircuitBreaker.State.CLOSED, breaker.currentState());
        assertTrue(breaker.allowRequest());

        breaker.recordFailure();
        breaker.recordFailure();
        assertEquals(PublishCircuitBreaker.State.CLOSED, breaker.currentState());

        breaker.recordFailure();
        assertEquals(PublishCircuitBreaker.State.OPEN, breaker.currentState());
        assertFalse(breaker.allowRequest());

        Thread.sleep(150L);
        assertTrue(breaker.allowRequest());
        assertEquals(PublishCircuitBreaker.State.HALF_OPEN, breaker.currentState());
        assertTrue(breaker.allowRequest());
        assertFalse(breaker.allowRequest());

        breaker.recordSuccess();
        assertEquals(PublishCircuitBreaker.State.HALF_OPEN, breaker.currentState());
        breaker.recordSuccess();
        assertEquals(PublishCircuitBreaker.State.CLOSED, breaker.currentState());
    }

    @Test
    void halfOpenFailureImmediatelyReopensCircuit() throws Exception {
        PublishCircuitBreaker breaker = new PublishCircuitBreaker(true, 1, 100L, 2);
        breaker.recordFailure();
        assertEquals(PublishCircuitBreaker.State.OPEN, breaker.currentState());

        Thread.sleep(120L);
        assertTrue(breaker.allowRequest());
        assertEquals(PublishCircuitBreaker.State.HALF_OPEN, breaker.currentState());

        breaker.recordFailure();
        assertEquals(PublishCircuitBreaker.State.OPEN, breaker.currentState());
    }

    @Test
    void brokerAckModeWaitsForKafkaDurabilityAndShedsWhenCircuitOpens() {
        when(kafkaTemplate.send(eq("invoice-events"), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        PublishCircuitBreaker breaker = new PublishCircuitBreaker(true, 2, 5_000L, 1);
        InvoiceEventPublisher publisher = new InvoiceEventPublisher(
                kafkaTemplate,
                "invoice-events",
                10,
                new RewardMetrics(registry),
                breaker,
                InvoiceEventPublisher.AckMode.BROKER_ACK,
                500L
        );

        InvoiceAcceptedResponse single = publisher.publish(new InvoiceSimulationRequest("INV-1", "CO-1"));
        assertEquals("DURABLE", single.status());

        InvoiceBatchAcceptedResponse batch = publisher.publishBatch(List.of(
                new InvoiceSimulationRequest("INV-2", "CO-1"),
                new InvoiceSimulationRequest("INV-3", "CO-2")
        ));
        assertEquals("DURABLE", batch.status());
        assertEquals(2, batch.acceptedCount());
    }

    @Test
    void openCircuitRejectsPublishWithoutCallingKafkaOrLeakingPermits() {
        CompletableFuture<SendResult<String, InvoiceIssuedEvent>> failed1 =
                CompletableFuture.failedFuture(new RuntimeException("broker down 1"));
        CompletableFuture<SendResult<String, InvoiceIssuedEvent>> failed2 =
                CompletableFuture.failedFuture(new RuntimeException("broker down 2"));
        when(kafkaTemplate.send(eq("invoice-events"), any(), any()))
                .thenReturn(failed1)
                .thenReturn(failed2);

        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        PublishCircuitBreaker breaker = new PublishCircuitBreaker(true, 2, 5_000L, 1);
        InvoiceEventPublisher publisher = new InvoiceEventPublisher(
                kafkaTemplate,
                "invoice-events",
                2,
                new RewardMetrics(registry),
                breaker,
                InvoiceEventPublisher.AckMode.BROKER_ACK,
                500L
        );

        assertThrows(EventPublishException.class,
                () -> publisher.publish(new InvoiceSimulationRequest("INV-1", "CO-1")));
        assertThrows(EventPublishException.class,
                () -> publisher.publish(new InvoiceSimulationRequest("INV-2", "CO-1")));

        assertEquals(PublishCircuitBreaker.State.OPEN, publisher.circuitBreakerState());
        assertThrows(OverloadException.class,
                () -> publisher.publish(new InvoiceSimulationRequest("INV-3", "CO-1")));
        assertEquals(1.0, registry.counter("gamification.ingest.circuit.rejected").count());

        assertThrows(OverloadException.class,
                () -> publisher.publishBatch(List.of(new InvoiceSimulationRequest("INV-4", "CO-1"))));
        verifyNoMoreInteractions(kafkaTemplate);
    }
}
