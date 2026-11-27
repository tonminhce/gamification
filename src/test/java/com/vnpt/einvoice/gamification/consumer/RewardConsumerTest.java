package com.vnpt.einvoice.gamification.consumer;

import com.vnpt.einvoice.gamification.event.InvoiceIssuedEvent;
import com.vnpt.einvoice.gamification.reward.RewardService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RewardConsumerTest {

    @Mock
    RewardService rewardService;

    @Mock
    KafkaTemplate<String, InvoiceIssuedEvent> kafkaTemplate;

    RewardConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new RewardConsumer(rewardService, kafkaTemplate, "invoice-events.DLT");
    }

    @Test
    void processesTheWholeBatchBeforeItCanBeCommitted() {
        InvoiceIssuedEvent first = event("INV-1");
        InvoiceIssuedEvent second = event("INV-2");

        consumer.consume(List.of(record(first, 10), record(second, 11)));

        verify(rewardService).applyRewards(List.of(first, second), "invoice-events:0");
    }

    @Test
    void propagatesBatchFailureSoKafkaDoesNotCommit() {
        doThrow(new DataAccessException("Redis unavailable") { })
                .when(rewardService).applyRewards(anyList(), anyString());

        assertThrows(DataAccessException.class, () -> consumer.consume(List.of(
                record(event("INV-1"), 10),
                record(event("INV-2"), 11)
        )));
    }

    @Test
    void sendsInvalidEventToDltAndProcessesValidOnes() {
        InvoiceIssuedEvent invalid = new InvoiceIssuedEvent(
                "event-bad",
                "WRONG_TYPE",
                1,
                "INV-BAD",
                "COMPANY-A",
                Instant.now()
        );
        InvoiceIssuedEvent valid = event("INV-GOOD");
        when(kafkaTemplate.send(eq("invoice-events.DLT"), eq("COMPANY-A"), any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        consumer.consume(List.of(record(invalid, 10), record(valid, 11)));

        verify(kafkaTemplate).send("invoice-events.DLT", "COMPANY-A", invalid);
        verify(rewardService).applyRewards(List.of(valid), "invoice-events:0");
    }

    private InvoiceIssuedEvent event(String invoiceId) {
        return new InvoiceIssuedEvent(
                "event-" + invoiceId,
                InvoiceIssuedEvent.TYPE,
                InvoiceIssuedEvent.SCHEMA_VERSION,
                invoiceId,
                "COMPANY-A",
                Instant.now()
        );
    }

    private ConsumerRecord<String, InvoiceIssuedEvent> record(InvoiceIssuedEvent event, long offset) {
        return new ConsumerRecord<>("invoice-events", 0, offset, event.companyId(), event);
    }
}