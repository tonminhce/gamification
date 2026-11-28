package com.vnpt.einvoice.gamification.ledger;

import java.time.Duration;
import java.time.Instant;

/**
 * Partition-independent invoice deduplication ledger supporting a recoverable
 * {@code CLAIMED -> EMITTED} state machine so input topics can rescale partitions
 * without local state migration.
 */
public interface DedupeLedger {

    ClaimResult claim(String invoiceId, String companyId, String sourceId, Instant occurredAt, Duration ttl);

    void markEmitted(String invoiceId, String sourceId, Instant emittedAt);

    enum ClaimStatus {
        CLAIMED_NEW,
        RECOVERABLE_CLAIMED_UNEMITTED,
        DUPLICATE_EMITTED,
        CONFLICT,
        IN_FLIGHT_CLAIMED_BY_OTHER
    }

    record ClaimResult(ClaimStatus status, String existingCompanyId) {
        public boolean shouldEmit() {
            return status == ClaimStatus.CLAIMED_NEW || status == ClaimStatus.RECOVERABLE_CLAIMED_UNEMITTED;
        }
    }
}
