package com.vnpt.einvoice.gamification.consumer;

import com.vnpt.einvoice.gamification.event.InvoiceIssuedEvent;
import com.vnpt.einvoice.gamification.reward.RewardService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessException;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class RewardConsumerTest {

    @Mock
    RewardService rewardService;

    @InjectMocks
    RewardConsumer consumer;

    @Test
    void processesTheWholeBatchBeforeItCanBeCommitted() {
        InvoiceIssuedEvent first = event("INV-1");
        InvoiceIssuedEvent second = event("INV-2");

        consumer.consume(List.of(record(first), record(second)));

        verify(rewardService).applyRewards(List.of(first, second));
    }

    @Test
    void propagatesBatchFailureSoKafkaDoesNotCommit() {
        doThrow(new DataAccessException("Redis unavailable") { })
                .when(rewardService).applyRewards(anyList());

        assertThrows(DataAccessException.class, () -> consumer.consume(List.of(
                record(event("INV-1")),
                record(event("INV-2"))
        )));
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

    private ConsumerRecord<String, InvoiceIssuedEvent> record(InvoiceIssuedEvent event) {
        return new ConsumerRecord<>("invoice-events", 0, 0L, event.companyId(), event);
    }
}
