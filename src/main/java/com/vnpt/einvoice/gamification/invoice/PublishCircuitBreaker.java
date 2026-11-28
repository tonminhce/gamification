package com.vnpt.einvoice.gamification.invoice;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Low-overhead sliding failure circuit breaker protecting REST ingress from stalled Kafka brokers.
 * Fast path on healthy traffic incurs only volatile/atomic reads; sustained failures trip to
 * {@code OPEN} for a cooldown window and recover via bounded {@code HALF_OPEN} probes.
 */
public class PublishCircuitBreaker {

    public enum State {
        CLOSED,
        OPEN,
        HALF_OPEN
    }

    private final boolean enabled;
    private final int failureThreshold;
    private final long openDurationNanos;
    private final int halfOpenMaxProbes;

    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
    private final AtomicInteger consecutiveFailures = new AtomicInteger();
    private final AtomicInteger halfOpenInFlight = new AtomicInteger();
    private final AtomicInteger halfOpenSuccesses = new AtomicInteger();
    private volatile long openedAtNanos;

    public PublishCircuitBreaker(
            boolean enabled,
            int failureThreshold,
            long openDurationMs,
            int halfOpenMaxProbes
    ) {
        this.enabled = enabled;
        this.failureThreshold = Math.max(1, failureThreshold);
        this.openDurationNanos = Math.max(100L, openDurationMs) * 1_000_000L;
        this.halfOpenMaxProbes = Math.max(1, halfOpenMaxProbes);
    }

    public static PublishCircuitBreaker noop() {
        return new PublishCircuitBreaker(false, 20, 5_000L, 5);
    }

    public boolean allowRequest() {
        if (!enabled) {
            return true;
        }
        State current = state.get();
        if (current == State.CLOSED) {
            return true;
        }
        if (current == State.OPEN) {
            if ((System.nanoTime() - openedAtNanos) < openDurationNanos) {
                return false;
            }
            if (state.compareAndSet(State.OPEN, State.HALF_OPEN)) {
                halfOpenSuccesses.set(0);
                halfOpenInFlight.set(1);
                return true;
            }
            current = state.get();
        }
        if (current == State.HALF_OPEN) {
            while (true) {
                int inFlight = halfOpenInFlight.get();
                if (inFlight >= halfOpenMaxProbes) {
                    return false;
                }
                if (halfOpenInFlight.compareAndSet(inFlight, inFlight + 1)) {
                    return true;
                }
            }
        }
        return true;
    }

    public void recordSuccess() {
        if (!enabled) {
            return;
        }
        if (consecutiveFailures.get() != 0) {
            consecutiveFailures.set(0);
        }
        if (state.get() == State.HALF_OPEN) {
            halfOpenInFlight.updateAndGet(v -> Math.max(0, v - 1));
            if (halfOpenSuccesses.incrementAndGet() >= halfOpenMaxProbes
                    && state.compareAndSet(State.HALF_OPEN, State.CLOSED)) {
                consecutiveFailures.set(0);
                halfOpenInFlight.set(0);
            }
        }
    }

    public void recordFailure() {
        if (!enabled) {
            return;
        }
        if (state.get() == State.HALF_OPEN) {
            halfOpenInFlight.updateAndGet(v -> Math.max(0, v - 1));
            if (state.compareAndSet(State.HALF_OPEN, State.OPEN)) {
                openedAtNanos = System.nanoTime();
            }
            return;
        }
        if (consecutiveFailures.incrementAndGet() >= failureThreshold
                && state.compareAndSet(State.CLOSED, State.OPEN)) {
            openedAtNanos = System.nanoTime();
        }
    }

    public State currentState() {
        return state.get();
    }
}
