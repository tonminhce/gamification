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
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Service
public class InvoiceEventPublisher {

    public enum AckMode {
        ASYNC,
        BROKER_ACK;

        public static AckMode parse(String raw) {
            if (raw != null && raw.trim().toLowerCase(Locale.ROOT).equals("broker-ack")) {
                return BROKER_ACK;
            }
            return ASYNC;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(InvoiceEventPublisher.class);
    private static final int DEFAULT_MAX_IN_FLIGHT = 200_000;
    private static final long DEFAULT_ACK_TIMEOUT_MS = 5_000L;

    private final KafkaTemplate<String, InvoiceIssuedEvent> kafkaTemplate;
    private final String topic;
    private final Semaphore inFlightPermits;
    private final RewardMetrics metrics;
    private final PublishCircuitBreaker circuitBreaker;
    private final AckMode ackMode;
    private final long ackTimeoutMs;

    public InvoiceEventPublisher(
            KafkaTemplate<String, InvoiceIssuedEvent> kafkaTemplate,
            String topic
    ) {
        this(kafkaTemplate, topic, DEFAULT_MAX_IN_FLIGHT, RewardMetrics.noop());
    }

    public InvoiceEventPublisher(
            KafkaTemplate<String, InvoiceIssuedEvent> kafkaTemplate,
            String topic,
            int maxInFlight,
            RewardMetrics metrics
    ) {
        this(kafkaTemplate, topic, maxInFlight, metrics, PublishCircuitBreaker.noop(), AckMode.ASYNC, DEFAULT_ACK_TIMEOUT_MS);
    }

    public InvoiceEventPublisher(
            KafkaTemplate<String, InvoiceIssuedEvent> kafkaTemplate,
            String topic,
            int maxInFlight,
            RewardMetrics metrics,
            PublishCircuitBreaker circuitBreaker,
            AckMode ackMode,
            long ackTimeoutMs
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
        this.inFlightPermits = new Semaphore(Math.max(1, maxInFlight));
        this.metrics = metrics;
        this.circuitBreaker = circuitBreaker != null ? circuitBreaker : PublishCircuitBreaker.noop();
        this.ackMode = ackMode != null ? ackMode : AckMode.ASYNC;
        this.ackTimeoutMs = Math.max(100L, ackTimeoutMs);
    }

    @Autowired
    public InvoiceEventPublisher(
            KafkaTemplate<String, InvoiceIssuedEvent> kafkaTemplate,
            @Value("${app.kafka.topic}") String topic,
            @Value("${app.ingest.max-in-flight:200000}") int maxInFlight,
            RewardMetrics metrics,
            @Value("${app.ingest.ack-mode:async}") String ackMode,
            @Value("${app.ingest.ack-timeout-ms:5000}") long ackTimeoutMs,
            @Value("${app.ingest.circuit-breaker.enabled:true}") boolean cbEnabled,
            @Value("${app.ingest.circuit-breaker.failure-threshold:20}") int cbFailureThreshold,
            @Value("${app.ingest.circuit-breaker.open-duration-ms:5000}") long cbOpenDurationMs,
            @Value("${app.ingest.circuit-breaker.half-open-max-probes:5}") int cbHalfOpenProbes
    ) {
        this(
                kafkaTemplate,
                topic,
                maxInFlight,
                metrics,
                new PublishCircuitBreaker(cbEnabled, cbFailureThreshold, cbOpenDurationMs, cbHalfOpenProbes),
                AckMode.parse(ackMode),
                ackTimeoutMs
        );
    }

    public PublishCircuitBreaker.State circuitBreakerState() {
        return circuitBreaker.currentState();
    }

    public AckMode ackMode() {
        return ackMode;
    }

    public InvoiceAcceptedResponse publish(InvoiceSimulationRequest request) {
        if (!circuitBreaker.allowRequest()) {
            metrics.recordCircuitRejected(1);
            throw new OverloadException("Kafka publish circuit breaker is OPEN; retry shortly");
        }
        if (!inFlightPermits.tryAcquire()) {
            metrics.recordRejectedOverload(1);
            throw new OverloadException("Ingestion queue saturated; retry shortly");
        }

        InvoiceIssuedEvent event = buildEvent(request, Instant.now());
        long startNanos = System.nanoTime();
        AtomicBoolean circuitRecorded = new AtomicBoolean(false);
        CompletableFuture<SendResult<String, InvoiceIssuedEvent>> future;
        try {
            future = kafkaTemplate.send(topic, event.invoiceId(), event);
            future.whenComplete((result, ex) -> {
                inFlightPermits.release();
                if (ex != null) {
                    if (circuitRecorded.compareAndSet(false, true)) {
                        circuitBreaker.recordFailure();
                    }
                    metrics.recordPublishFailed();
                    log.error("Asynchronous Kafka publish failed for invoiceId={}", event.invoiceId(), ex);
                } else {
                    if (circuitRecorded.compareAndSet(false, true)) {
                        circuitBreaker.recordSuccess();
                    }
                    metrics.recordPublished(System.nanoTime() - startNanos);
                }
            });
        } catch (KafkaException ex) {
            inFlightPermits.release();
            if (circuitRecorded.compareAndSet(false, true)) {
                circuitBreaker.recordFailure();
            }
            metrics.recordPublishFailed();
            throw new EventPublishException("Kafka rejected the invoice event", ex);
        } catch (Throwable t) {
            inFlightPermits.release();
            if (circuitRecorded.compareAndSet(false, true)) {
                circuitBreaker.recordFailure();
            }
            throw t;
        }

        if (ackMode == AckMode.BROKER_ACK) {
            awaitSingleAck(future, circuitRecorded);
        }

        metrics.recordAccepted(1);
        return new InvoiceAcceptedResponse(
                request.invoiceId(),
                request.companyId(),
                event.eventId(),
                ackMode == AckMode.BROKER_ACK ? "DURABLE" : "PUBLISHING"
        );
    }

    public InvoiceBatchAcceptedResponse publishBatch(List<InvoiceSimulationRequest> requests) {
        int count = requests.size();
        if (!circuitBreaker.allowRequest()) {
            metrics.recordCircuitRejected(count);
            throw new OverloadException("Kafka publish circuit breaker is OPEN; retry shortly");
        }
        if (!inFlightPermits.tryAcquire(count)) {
            metrics.recordRejectedOverload(count);
            throw new OverloadException("Ingestion queue saturated for batch; retry shortly");
        }

        Instant batchNow = Instant.now();
        long batchStartNanos = System.nanoTime();
        AtomicInteger remaining = new AtomicInteger(count);
        AtomicInteger succeeded = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        AtomicReference<Throwable> firstAsyncError = new AtomicReference<>();
        AtomicBoolean circuitRecorded = new AtomicBoolean(false);
        List<CompletableFuture<SendResult<String, InvoiceIssuedEvent>>> futures =
                ackMode == AckMode.BROKER_ACK ? new ArrayList<>(count) : null;
        int dispatched = 0;

        try {
            for (InvoiceSimulationRequest request : requests) {
                InvoiceIssuedEvent event = buildEvent(request, batchNow);
                CompletableFuture<SendResult<String, InvoiceIssuedEvent>> future =
                        kafkaTemplate.send(topic, event.invoiceId(), event);
                future.whenComplete((result, ex) -> {
                    if (ex != null) {
                        failed.incrementAndGet();
                        firstAsyncError.compareAndSet(null, ex);
                    } else {
                        succeeded.incrementAndGet();
                    }
                    if (remaining.decrementAndGet() == 0) {
                        finalizeBatchCompletion(count, succeeded.get(), failed.get(), firstAsyncError.get(), batchStartNanos, circuitRecorded);
                    }
                });
                if (futures != null) {
                    futures.add(future);
                }
                dispatched++;
            }
        } catch (KafkaException ex) {
            if (circuitRecorded.compareAndSet(false, true)) {
                circuitBreaker.recordFailure();
            }
            recordPartialDispatchFailure(count, dispatched, remaining, succeeded, failed, firstAsyncError, batchStartNanos, circuitRecorded);
            throw new EventPublishException("Kafka rejected the invoice batch", ex);
        } catch (Throwable t) {
            if (circuitRecorded.compareAndSet(false, true)) {
                circuitBreaker.recordFailure();
            }
            recordPartialDispatchFailure(count, dispatched, remaining, succeeded, failed, firstAsyncError, batchStartNanos, circuitRecorded);
            throw t;
        }

        if (futures != null) {
            awaitBatchAck(futures, circuitRecorded);
        }

        metrics.recordAccepted(count);
        return new InvoiceBatchAcceptedResponse(
                count,
                ackMode == AckMode.BROKER_ACK ? "DURABLE" : "PUBLISHING"
        );
    }

    private void awaitSingleAck(CompletableFuture<?> future, AtomicBoolean circuitRecorded) {
        try {
            future.get(ackTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            if (circuitRecorded.compareAndSet(false, true)) {
                circuitBreaker.recordFailure();
            }
            throw new EventPublishException("Interrupted while awaiting Kafka broker acknowledgment", ex);
        } catch (TimeoutException | ExecutionException ex) {
            if (circuitRecorded.compareAndSet(false, true)) {
                circuitBreaker.recordFailure();
            }
            throw new EventPublishException("Kafka broker acknowledgment failed", ex);
        }
    }

    private void awaitBatchAck(
            List<CompletableFuture<SendResult<String, InvoiceIssuedEvent>>> futures,
            AtomicBoolean circuitRecorded
    ) {
        try {
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                    .get(ackTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            if (circuitRecorded.compareAndSet(false, true)) {
                circuitBreaker.recordFailure();
            }
            throw new EventPublishException("Interrupted while awaiting Kafka batch durability", ex);
        } catch (TimeoutException | ExecutionException ex) {
            if (circuitRecorded.compareAndSet(false, true)) {
                circuitBreaker.recordFailure();
            }
            throw new EventPublishException("Kafka batch durability acknowledgment failed", ex);
        }
    }

    private void recordPartialDispatchFailure(
            int totalCount,
            int dispatched,
            AtomicInteger remaining,
            AtomicInteger succeeded,
            AtomicInteger failed,
            AtomicReference<Throwable> firstAsyncError,
            long batchStartNanos,
            AtomicBoolean circuitRecorded
    ) {
        if (dispatched > 0) {
            metrics.recordAccepted(dispatched);
        }
        int unsent = totalCount - dispatched;
        if (unsent > 0) {
            metrics.recordPublishFailed(unsent);
            if (remaining.addAndGet(-unsent) == 0) {
                finalizeBatchCompletion(totalCount, succeeded.get(), failed.get(), firstAsyncError.get(), batchStartNanos, circuitRecorded);
            }
        }
    }

    private void finalizeBatchCompletion(
            int permitsToRelease,
            int okCount,
            int asyncFailCount,
            Throwable sampleError,
            long batchStartNanos,
            AtomicBoolean circuitRecorded
    ) {
        inFlightPermits.release(permitsToRelease);
        if (okCount > 0) {
            if (asyncFailCount == 0 && circuitRecorded.compareAndSet(false, true)) {
                circuitBreaker.recordSuccess();
            }
            metrics.recordBatchPublished(okCount, System.nanoTime() - batchStartNanos);
        }
        if (asyncFailCount > 0) {
            if (circuitRecorded.compareAndSet(false, true)) {
                circuitBreaker.recordFailure();
            }
            metrics.recordPublishFailed(asyncFailCount);
            log.error("Asynchronous Kafka batch publish failed for {} invoice(s)", asyncFailCount, sampleError);
        }
    }

    private static InvoiceIssuedEvent buildEvent(InvoiceSimulationRequest request, Instant timestamp) {
        return new InvoiceIssuedEvent(
                fastUuid(),
                InvoiceIssuedEvent.TYPE,
                InvoiceIssuedEvent.SCHEMA_VERSION,
                request.invoiceId(),
                request.companyId(),
                timestamp
        );
    }

    private static String fastUuid() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        long msb = (random.nextLong() & 0xFFFFFFFFFFFF0FFFL) | 0x0000000000004000L;
        long lsb = (random.nextLong() & 0x3FFFFFFFFFFFFFFFL) | 0x8000000000000000L;
        return new UUID(msb, lsb).toString();
    }
}
