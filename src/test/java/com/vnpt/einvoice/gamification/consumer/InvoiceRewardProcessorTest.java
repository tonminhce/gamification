package com.vnpt.einvoice.gamification.consumer;

import com.vnpt.einvoice.gamification.event.CompanyRewardDelta;
import com.vnpt.einvoice.gamification.event.InvoiceIssuedEvent;
import com.vnpt.einvoice.gamification.reward.ClaimedCompanyDelta;
import com.vnpt.einvoice.gamification.reward.InvoiceClaimResult;
import com.vnpt.einvoice.gamification.reward.InvoiceClaimService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class InvoiceRewardProcessorTest {

    @Mock
    InvoiceClaimService claimService;

    @Mock
    KafkaTemplate<String, Object> kafkaTemplate;

    InvoiceRewardProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new InvoiceRewardProcessor(
                claimService,
                kafkaTemplate,
                "company-reward-deltas",
                "invoice-events.DLT"
        );
    }

    @Test
    void publishesDeltaAndCompletesClaim() {
        when(claimService.claim(any(), eq("invoice-events:0"), anyString()))
                .thenReturn(new InvoiceClaimResult(
                        "batch-1",
                        false,
                        List.of(new ClaimedCompanyDelta("COMPANY-A", 20))
                ));
        when(kafkaTemplate.send(eq("company-reward-deltas"), eq("COMPANY-A"), any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        processor.consume(List.of(record(event("INV-1"), 10), record(event("INV-2"), 11)));

        verify(kafkaTemplate).send(eq("company-reward-deltas"), eq("COMPANY-A"), any(CompanyRewardDelta.class));
        verify(claimService).complete("invoice-events:0");
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