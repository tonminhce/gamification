-- Production Stage 3 external LWT ledger schema for ScyllaDB / Apache Cassandra.
-- Partition key is stable invoice_id so Kafka topic partition scaling (64 -> 128+)
-- never loses deduplication history within the retention TTL window.

CREATE KEYSPACE IF NOT EXISTS einvoice_dedupe
WITH replication = {
    'class': 'NetworkTopologyStrategy',
    'replication_factor': 3
};

CREATE TABLE IF NOT EXISTS einvoice_dedupe.invoice_claims (
    invoice_id text,
    company_id text,
    state text,         -- CLAIMED | EMITTED
    source_id text,     -- partition:offset or producer idempotency token
    claimed_at timestamp,
    emitted_at timestamp,
    PRIMARY KEY ((invoice_id))
) WITH compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_unit': 'HOURS',
    'compaction_window_size': '1'
}
AND default_time_to_live = 86400
AND gc_grace_seconds = 3600;

-- Claim new invoice atomically (CAS / Paxos LWT):
-- INSERT INTO einvoice_dedupe.invoice_claims (invoice_id, company_id, state, source_id, claimed_at)
-- VALUES (?, ?, 'CLAIMED', ?, toTimestamp(now())) IF NOT EXISTS;
--
-- Promote CLAIMED -> EMITTED after delta emission:
-- UPDATE einvoice_dedupe.invoice_claims SET state = 'EMITTED', emitted_at = toTimestamp(now())
-- WHERE invoice_id = ? IF state = 'CLAIMED' AND source_id = ?;
