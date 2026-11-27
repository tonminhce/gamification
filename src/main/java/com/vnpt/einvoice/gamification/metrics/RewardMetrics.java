package com.vnpt.einvoice.gamification.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Centralized pipeline metrics covering REST ingress, Streams deduplication,
 * window aggregation, and Redis leaderboard projection.
 */
@Component
public class RewardMetrics {

    private final Counter invoicesAccepted;
    private final Counter invoicesPublished;
    private final Counter invoicesPublishFailed;
    private final Counter invoicesRejectedOverload;
    private final Timer publishLatency;

    private final Counter dedupeUnique;
    private final Counter dedupeDuplicate;
    private final Counter dedupeConflict;
    private final Counter bloomNegativeSkip;
    private final Counter bloomHit;

    private final Counter windowsEmitted;
    private final Counter windowPointsEmitted;

    private final Counter projectorBatches;
    private final Counter projectorDeltasApplied;
    private final Counter projectorDeltasSkipped;
    private final Timer projectorApplyLatency;

    public RewardMetrics(MeterRegistry registry) {
        this.invoicesAccepted = registry.counter("gamification.invoice.accepted");
        this.invoicesPublished = registry.counter("gamification.invoice.published");
        this.invoicesPublishFailed = registry.counter("gamification.invoice.publish.failed");
        this.invoicesRejectedOverload = registry.counter("gamification.invoice.rejected.overload");
        this.publishLatency = Timer.builder("gamification.invoice.publish.latency")
                .register(registry);

        this.dedupeUnique = registry.counter("gamification.streams.dedupe.unique");
        this.dedupeDuplicate = registry.counter("gamification.streams.dedupe.duplicate");
        this.dedupeConflict = registry.counter("gamification.streams.dedupe.conflict");
        this.bloomNegativeSkip = registry.counter("gamification.streams.dedupe.bloom_skip");
        this.bloomHit = registry.counter("gamification.streams.dedupe.bloom_hit");

        this.windowsEmitted = registry.counter("gamification.streams.window.emitted");
        this.windowPointsEmitted = registry.counter("gamification.streams.window.points");

        this.projectorBatches = registry.counter("gamification.projector.batches");
        this.projectorDeltasApplied = registry.counter("gamification.projector.deltas.applied");
        this.projectorDeltasSkipped = registry.counter("gamification.projector.deltas.skipped");
        this.projectorApplyLatency = Timer.builder("gamification.projector.apply.latency")
                .publishPercentileHistogram()
                .register(registry);
    }

    public static RewardMetrics noop() {
        return new RewardMetrics(new SimpleMeterRegistry());
    }

    public void recordAccepted(int count) {
        invoicesAccepted.increment(count);
    }

    public void recordPublished(long elapsedNanos) {
        invoicesPublished.increment();
        publishLatency.record(elapsedNanos, TimeUnit.NANOSECONDS);
    }

    public void recordBatchPublished(int count, long elapsedNanos) {
        invoicesPublished.increment(count);
        publishLatency.record(elapsedNanos, TimeUnit.NANOSECONDS);
    }

    public void recordPublishFailed() {
        invoicesPublishFailed.increment();
    }

    public void recordRejectedOverload(int count) {
        invoicesRejectedOverload.increment(count);
    }

    public void recordDedupeUnique() {
        dedupeUnique.increment();
    }

    public void recordDedupeDuplicate() {
        dedupeDuplicate.increment();
    }

    public void recordDedupeConflict() {
        dedupeConflict.increment();
    }

    public void recordBloomNegativeSkip() {
        bloomNegativeSkip.increment();
    }

    public void recordBloomHit() {
        bloomHit.increment();
    }

    public void recordWindowEmitted(long points) {
        windowsEmitted.increment();
        windowPointsEmitted.increment(points);
    }

    public void recordProjectorBatch(int totalDeltas, int appliedDeltas, long elapsedNanos) {
        projectorBatches.increment();
        projectorDeltasApplied.increment(appliedDeltas);
        int skipped = Math.max(0, totalDeltas - appliedDeltas);
        if (skipped > 0) {
            projectorDeltasSkipped.increment(skipped);
        }
        projectorApplyLatency.record(elapsedNanos, TimeUnit.NANOSECONDS);
    }
}
