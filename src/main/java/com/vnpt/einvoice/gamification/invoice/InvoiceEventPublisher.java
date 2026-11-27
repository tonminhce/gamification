package com.vnpt.einvoice.gamification.invoice;

import com.vnpt.einvoice.gamification.api.InvoiceAcceptedResponse;
import com.vnpt.einvoice.gamification.api.InvoiceBatchAcceptedResponse;
import com.vnpt.einvoice.gamification.api.InvoiceSimulationRequest;
import com.vnpt.einvoice.gamification.event.InvoiceIssuedEvent;
import com.vnpt.einvoice.gamification.exception.EventPublishException;
import com.vnpt.einvoice.gamification.exception.OverloadException;
import com.vnpt.einvoice.gamification.metrics.RewardMetrics;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class InvoiceEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(InvoiceEventPublisher.class);
    private static final int DEFAULT_MAX_IN_FLIGHT = 200_000;

    private final KafkaTemplate<String, InvoiceIssuedEvent> kafkaTemplate;
    private final String topic;
    private final Semaphore inFlightPermits;
    private final RewardMetrics metrics;

    public InvoiceEventPublisher(
            KafkaTemplate<String, InvoiceIssuedEvent> kafkaTemplate,
            String topic
    ) {
        this(kafkaTemplate, topic, DEFAULT_MAX_IN_FLIGHT, RewardMetrics.noop());
    }

    @Autowired
    public InvoiceEventPublisher(
            KafkaTemplate<String, InvoiceIssuedEvent> kafkaTemplate,
            @Value("${app.kafka.topic}") String topic,
            @Value("${app.ingest.max-in-flight:200000}") int maxInFlight,
            RewardMetrics metrics
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
        this.inFlightPermits = new Semaphore(Math.max(1, maxInFlight));
        this.metrics = metrics;
    }

    public InvoiceAcceptedResponse publish(InvoiceSimulationRequest request) {
        if (!inFlightPermits.tryAcquire()) {
            metrics.recordRejectedOverload(1);
            throw new OverloadException("Ingestion queue saturated; retry shortly");
        }

        String eventId = fastUuid();
        InvoiceIssuedEvent event = new InvoiceIssuedEvent(
                eventId,
                InvoiceIssuedEvent.TYPE,
                InvoiceIssuedEvent.SCHEMA_VERSION,
                request.invoiceId(),
                request.companyId(),
                Instant.now()
        );

        long startNanos = System.nanoTime();
        try {
            kafkaTemplate.send(topic, event.invoiceId(), event).whenComplete((result, ex) -> {
                inFlightPermits.release();
                if (ex != null) {
                    metrics.recordPublishFailed();
                    log.error("Asynchronous Kafka publish failed for invoiceId={}", event.invoiceId(), ex);
                } else {
                    metrics.recordPublished(System.nanoTime() - startNanos);
                }
            });
        } catch (KafkaException ex) {
            inFlightPermits.release();
            metrics.recordPublishFailed();
            throw new EventPublishException("Kafka rejected the invoice event", ex);
        } catch (RuntimeException ex) {
            inFlightPermits.release();
            throw ex;
        }

        metrics.recordAccepted(1);
        return new InvoiceAcceptedResponse(
                request.invoiceId(),
                request.companyId(),
                eventId,
                "PUBLISHING"
        );
    }

    public InvoiceBatchAcceptedResponse publishBatch(List<InvoiceSimulationRequest> requests) {
        int count = requests.size();
        if (!inFlightPermits.tryAcquire(count)) {
            metrics.recordRejectedOverload(count);
            throw new OverloadException("Ingestion queue saturated for batch; retry shortly");
        }

        Instant batchNow = Instant.now();
        long batchStartNanos = System.nanoTime();
        AtomicInteger remaining = new AtomicInteger(count);
        AtomicInteger succeeded = new AtomicInteger();
        int dispatched = 0;

        try {
            for (InvoiceSimulationRequest request : requests) {
                InvoiceIssuedEvent event = new InvoiceIssuedEvent(
                        fastUuid(),
                        InvoiceIssuedEvent.TYPE,
                        InvoiceIssuedEvent.SCHEMA_VERSION,
                        request.invoiceId(),
                        request.companyId(),
                        batchNow
                );
                kafkaTemplate.send(topic, event.invoiceId(), event).whenComplete((result, ex) -> {
                    if (ex != null) {
                        metrics.recordPublishFailed();
                        log.error("Asynchronous Kafka publish failed for invoiceId={}", event.invoiceId(), ex);
                    } else {
                        succeeded.incrementAndGet();
                    }
                    if (remaining.decrementAndGet() == 0) {
                        inFlightPermits.release(count);
                        int ok = succeeded.get();
                        if (ok > 0) {
                            metrics.recordBatchPublished(ok, System.nanoTime() - batchStartNanos);
                        }
                    }
                });
                dispatched++;
            }
        } catch (KafkaException ex) {
            int unsent = count - dispatched;
            if (unsent > 0 && remaining.addAndGet(-unsent) == 0) {
                inFlightPermits.release(count);
            }
            metrics.recordPublishFailed();
            throw new EventPublishException("Kafka rejected the invoice batch", ex);
        } catch (RuntimeException ex) {
            int unsent = count - dispatched;
            if (unsent > 0 && remaining.addAndGet(-unsent) == 0) {
                inFlightPermits.release(count);
            }
            throw ex;
        }

        metrics.recordAccepted(count);
        return new InvoiceBatchAcceptedResponse(count, "PUBLISHING");
    }

    private static String fastUuid() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        long msb = (random.nextLong() & 0xFFFFFFFFFFFF0FFFL) | 0x0000000000004000L;
        long lsb = (random.nextLong() & 0x3FFFFFFFFFFFFFFFL) | 0x8000000000000000L;
        return new UUID(msb, lsb).toString();
    }
}
