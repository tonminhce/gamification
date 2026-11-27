# Phase 4 — Theory and interview preparation

This guide refers to the code in this POC. Line numbers are from the current source tree.

## 1. Event-Driven Architecture

**What is it?**  
Components communicate by publishing events rather than calling each other directly. The producer knows the event contract, not the reward implementation.

**Why do we need it here?**  
Invoice issuance should not wait for leaderboard maintenance. Kafka also allows reward processing to scale and recover independently.

**Where is it implemented?**  
`src/main/java/com/vnpt/einvoice/gamification/invoice/InvoiceEventPublisher.java:47` publishes the event. The Kafka Streams topology in `src/main/java/com/vnpt/einvoice/gamification/streams/RewardTopology.java` consumes it, with `DedupeProcessor` owning the idempotency check.

**Interviewer questions**

- Why not update Redis directly in the invoice controller?
- What happens if reward processing is down?
- Is the event a command or a fact?

**Senior answer**  
“`INVOICE_ISSUED` is a fact that already happened. The controller publishes that fact and returns after Kafka accepts it. If rewards are down, events remain in Kafka and are processed later. This decouples availability and scaling, at the cost of eventual consistency and more operational moving parts.”

## 2. Asynchronous Processing

**What is it?**  
Work completes after the request returns, rather than inside the request thread.

**Why do we need it here?**  
A slow Redis or Kafka consumer should not block invoice issuance. The API queues the Kafka send asynchronously and does not wait for reward completion.

**Where is it implemented?**  
`InvoiceEventPublisher.java` attaches a completion callback to the Kafka send. The REST controller returns `202 Accepted` with status `PUBLISHING`; the consumer runs independently.

**Interviewer questions**

- Does `202 Accepted` mean points are already visible?
- What is the consistency model?
- What would make the API slower?

**Senior answer**  
“No, `202` with `PUBLISHING` means the API queued the send, not that Kafka or Redis completed it. Reads are eventually consistent. The callback logs delivery failures, and clients retry with the same `invoiceId`. For production I would add an outbox or delivery-status store so accepted requests cannot be lost between the API and broker.”

## 3. Kafka Producer and Consumer

**What is it?**  
A producer writes records to a topic. A consumer reads records, processes them, and manages offsets.

**Why do we need it here?**  
The producer transports invoice facts. The consumer applies reward logic and reads the leaderboard separately.

**Where is it implemented?**  
Producer settings: `src/main/resources/application.yml:14`. Typed producer factory: `src/main/java/com/vnpt/einvoice/gamification/config/KafkaConfig.java`. Consumer: the Streams topology in `src/main/java/com/vnpt/einvoice/gamification/streams/RewardTopology.java`; the leaderboard projector is a second consumer in `LeaderboardProjector.java`.

**Interviewer questions**

- Why is the Kafka key `invoiceId`?
- What does `acks=all` guarantee?
- What does producer idempotence guarantee?

**Senior answer**  
“The key is `invoiceId` because invoice identity is the dedupe key. Keying by company would put duplicate invoices for the same company on one partition, but a duplicate retry could still hash to the same partition only if the key matches the business identity; keying by invoiceId makes the guarantee explicit. The trade-off is that one company's events can span partitions, so the topology re-keys to `companyId` for window aggregation. `acks=all` waits for all in-sync replicas; with one replica in this POC, it effectively waits for the broker. Producer idempotence prevents duplicates caused by producer retries, but it does not stop a user or upstream system from publishing the same business invoice twice.”

## 4. Kafka Consumer Groups and Partitions

**What is it?**  
A topic is split into ordered partitions. A consumer group divides partitions among consumers so each partition is processed by one group member at a time.

**Why do we need it here?**  
Twelve partitions let six Streams threads share invoice processing, and twelve projector listeners apply delta partitions in parallel.

**Where is it implemented?**  
Streams threads: `src/main/resources/application.yml:56`. Projector concurrency: `src/main/resources/application.yml:41`. Topic creation: `src/main/java/com/vnpt/einvoice/gamification/config/KafkaConfig.java`.

**Interviewer questions**

- Can two consumers read the same partition?
- What happens when a consumer joins or leaves?
- Why not create 100 partitions immediately?

**Senior answer**  
“Within one group, a partition has one owner at a time. A rebalance reassigns it after membership changes. More instances than partitions leave some consumers idle. Too many partitions also increase metadata, ordering, and operational complexity, so partition count should be driven by measured parallelism needs.”

## 5. Consumer Offsets and At-Least-Once Delivery

**What is it?**  
An offset is the position of the next record to read in a partition. At-least-once means a record is not lost, but it may be processed more than once.

**Why do we need it here?**  
A crash after Redis succeeds but before the offset commits causes redelivery. The reward must remain correct.

**Where is it implemented?**  
Streams uses `exactly_once_v2` in `src/main/java/com/vnpt/einvoice/gamification/config/RewardStreamsConfig.java:39`, so input offsets and state/output commit together. The projector disables auto-commit (`src/main/resources/application.yml:29`) and uses batch acknowledgment (`src/main/resources/application.yml:40`); the listener returns only after Redis succeeds in `LeaderboardProjector.java`.

**Interviewer questions**

- What happens if the app crashes after `ZINCRBY` but before commit?
- Why not auto-commit?
- What does committing a batch offset mean?

**Senior answer**  
“For the invoice topic, the Streams transaction is not committed, so the input is reprocessed after restart; the dedupe state store is restored from its changelog and rejects the duplicate invoice IDs. For the projector, the delta batch is redelivered; Lua sees the existing delta Set entries and PENDING marker, so it does not add points again. Auto-commit can acknowledge records before processing finishes, so it is disabled. A committed batch offset means every record before that position completed successfully; otherwise the consumer must retry.”

## 6. Idempotency

**What is it?**  
Repeating the same operation has the same effect as performing it once.

**Why do we need it here?**  
Kafka retries, producer retries, user retries, and batch redelivery can all repeat `INV-1001`.

**Where is it implemented?**  
`src/main/java/com/vnpt/einvoice/gamification/streams/DedupeProcessor.java` checks an in-heap Bloom filter and then the persistent RocksDB `invoice-dedupe-store`; the state is changelog-backed and committed with the same Streams transaction as the emitted delta. The projector separately deduplicates delta IDs in `einv:processed-deltas`. The topology test and live restart replay are in `src/test/java/com/vnpt/einvoice/gamification/streams/RewardTopologyTest.java`.

**Interviewer questions**

- Why use `invoiceId` instead of `eventId`?
- Permanent key or TTL?
- What if the same invoice ID arrives for another company?

**Senior answer**  
“`invoiceId` is the business identity; `eventId` changes on every publish. The current POC keeps keys without a TTL because an old duplicate must never award again, and the state store is restored from the changelog after a restart. The Bloom filter only avoids a RocksDB read for first-seen invoices; a filter hit still checks the store, so it can produce false positives but never false negatives. The cost is unbounded storage — at 1M/s that is 86.4 billion IDs per day, so production needs a documented retention window or an external ledger. A TTL is acceptable only if the business explicitly accepts re-award after expiry. If the same invoice ID arrives for another company, the first claim wins; the second is treated as a duplicate and is not awarded. That is correct for an invoice identity but should be alerted as a data-quality signal.”

## 7. Atomicity

**What is it?**  
A group of changes appears indivisible to other clients.

**Why do we need it here?**  
Checking “not processed” and incrementing points must be one indivisible decision.

**Where is it implemented?**  
Invoice dedupe and window state are updated in one Streams transaction (`exactly_once_v2` in `RewardStreamsConfig.java`). Leaderboard application is in `src/main/resources/scripts/apply_delta_batch.lua`.

**Interviewer questions**

- Is a Redis Lua script a database transaction with rollback?
- What happens on a script error?
- Why validate key types first?

**Senior answer**  
“Redis runs the script without interleaving other clients, so it is atomic for concurrency. It is not a rollback transaction: a runtime error does not undo earlier writes. I validate key types before writing. The projector script uses `PENDING` markers and pre-update scores so retries can distinguish a missing write from a write that succeeded before the marker was cleared. On the Kafka side, Streams commits the dedupe write, window state and delta output in one transaction, so a crash cannot leave an invoice marked as processed without its delta being visible to a `read_committed` consumer. Redis itself is still outside the Kafka transaction; inconsistent partial Redis state is rejected for reconciliation, and the batch is not acknowledged.”

## 8. Race Conditions

**What is it?**  
Correctness depends on timing between concurrent operations.

**Why do we need it here?**  
Two consumers must never both see “invoice not processed” and both award 10 points. Redis `GET`/`SET`/`ZINCRBY` as separate calls in the projector have the same problem for delta application.

**Where is it implemented?**  
The invoice race is removed by Kafka Streams partition ownership: one task owns each input partition, and the dedupe store is local to that task, so no other consumer can process the same invoice concurrently. On the Redis side, `src/main/resources/scripts/apply_delta_batch.lua` performs the delta dedupe and score update atomically.

**Interviewer questions**

- Show the failing interleaving.
- Would a Java lock solve it?
- What about multiple application instances?

**Senior answer**  
“With separate Redis calls, consumer A reads no marker, consumer B reads no marker, then both write the marker and increment. A local Java lock only protects one JVM and would fail after horizontal scaling. Moving invoice dedupe into the partition-owner state store removes the cross-instance race: Kafka guarantees one active task per partition, and Streams transactions make the state write and delta emit atomic. The remaining shared boundary is Redis, where the projector uses one Lua execution for the check and the update.”

## 9. Redis Sorted Sets

**What is it?**  
A Redis data structure that maps unique members to numeric scores and keeps them ordered by score.

**Why do we need it here?**  
We need points, top-N ranking, and individual rank without scanning every company.

**Where is it implemented?**  
Score update: `src/main/resources/scripts/apply_delta_batch.lua`. Top list: `src/main/java/com/vnpt/einvoice/gamification/leaderboard/LeaderboardService.java:36`. Individual competition rank: `LeaderboardService.java`.

**Interviewer questions**

- Why not a Hash or Set?
- How are ties handled?
- What if a company has no score?

**Senior answer**  
“A Set has no scores or rank. A Hash can store points but requires a scan and sort to build a leaderboard. A Sorted Set maintains both score and order. Equal scores receive the same competition rank in both endpoints. The top list derives ranks from adjacent equal scores; the company endpoint counts higher scores with `ZCOUNT`. `ZREVRANK` is useful for ordinal positions but does not match the competition-rank semantics required here. Missing members return 0 points and no rank.”

## 10. Redis Lua Scripting

**What is it?**  
Server-side code executed atomically inside Redis.

**Why do we need it here?**  
It removes network round trips and prevents other clients from interleaving the idempotency check with the score update.

**Where is it implemented?**  
`apply_delta_batch.lua` for leaderboard updates, loaded and executed by `LeaderboardApplyService.java`. Invoice dedupe no longer uses Lua; it is a RocksDB state-store lookup in `DedupeProcessor.java`.

**Interviewer questions**

- Why not `MULTI/EXEC`?
- Is Lua a scalability risk?
- What would you monitor?

**Senior answer**  
“Lua is short enough for this POC and works on dedicated keys. `MULTI/EXEC` could group commands, but Lua lets the marker state decide whether to increment, finalize, or reject an inconsistent partial update. I removed the invoice claim from Redis because a Lua script on one shared Set does not scale with partition count; a partition-local state store with a changelog is a better fit for exact dedupe. Long scripts block Redis, so script duration is a key metric. I would monitor script errors, latency, memory, AOF health, and eviction.”

## 11. Time Complexity

**What is it?**  
How operation cost grows with data size.

**Why do we need it here?**  
The leaderboard must stay fast as company count grows.

**Where is it implemented?**  
`ZINCRBY` in `apply_delta_batch.lua`; top range at `LeaderboardService.java:36`; rank in `LeaderboardService.java`.

**Interviewer questions**

- Complexity of each Redis command?
- Why is top N not O(N)?
- Does a Lua script change the complexity?

**Senior answer**  
“`ZINCRBY` is O(log N), `ZREVRANK` is O(log N), `ZCOUNT` is O(log N), `ZSCORE` is O(1), and `ZREVRANGE WITHSCORES` for M results is O(log N + M). String `GET` and `SET` are O(1). Lua does not remove those costs; it packages them into one atomic execution. The top endpoint avoids M extra rank calls by deriving competition ranks from the sorted result.”

## 12. Throughput vs. Latency

**What is it?**  
Throughput is work completed per second. Latency is time for one request.

**Why do we need it here?**  
The design targets high event throughput and low leaderboard latency, but optimizing one can hurt the other.

**Where is it implemented?**  
Streams `exactly_once_v2`, window aggregation and the delta projector: `src/main/java/com/vnpt/einvoice/gamification/config/RewardStreamsConfig.java`, `src/main/java/com/vnpt/einvoice/gamification/streams/WindowAggregatorProcessor.java` and `LeaderboardApplyService`. Benchmarks: `scripts/KafkaLoadProducer.java`, `scripts/RestLoadProducer.java`, and `scripts/load-test.mjs`.

**Interviewer questions**

- Why did batch acknowledgment improve throughput?
- Can caching top N improve latency?
- What is the trade-off?

**Senior answer**  
“Batch commits, asynchronous publishing, producer batching, window aggregation and one Redis Lua call per projector batch raised the direct unique-event result from 5,603 events/s in the earliest per-event pipeline to 102,541–122,030 events/s end-to-end on three 1,000,000-event Stage 2B runs. I use 1M-event runs for the gate because a 100k-event run is dominated by the fixed window and drain latency. A top-N cache could reduce read latency further, but it introduces staleness and invalidation work. I would only add it after measuring a read bottleneck.”

## 13. p95 Latency

**What is it?**  
The latency below which 95% of observations fall. It exposes tail behavior that an average can hide.

**Why do we need it here?**  
The target is approximately 5 ms p95, not just a good average.

**Where is it implemented?**  
`scripts/KafkaLoadProducer.java` and `scripts/load-test.mjs` calculate p95 from 200 sequential leaderboard requests after warm-up.

**Interviewer questions**

- Why p95 instead of average?
- Is this benchmark a production SLA test?
- What creates tail latency?

**Senior answer**  
“The optimized Stage 2B runs measured 1.28–3.00 ms p95 after ingest drained across three 1M-event runs; this is not measured under concurrent load, so it is not a production guarantee. Tail latency can come from JVM pauses, GC, Redis contention, network scheduling, Kafka rebalances, or the 500 ms window flush. A real test needs production-like data, sustained load, multiple runs, and percentile reporting under concurrent reads and writes.”

## 14. Horizontal Scaling

**What is it?**  
Adding more application instances to share work.

**Why do we need it here?**  
Reward consumption can scale independently from the REST API.

**Where is it implemented?**  
Streams threads in `src/main/resources/application.yml:56`; projector concurrency in `src/main/resources/application.yml:40`.

**Interviewer questions**

- How many consumers are useful with twelve partitions?
- Does adding instances improve Redis unlimitedly?
- What breaks first?

**Senior answer**  
“Six Streams threads share twelve input partitions; more than twelve active tasks would leave consumers idle. The projector has twelve listeners, one per delta partition. Adding instances provides failover and more partition parallelism, but it cannot exceed the partition count. Redis remains a shared single-threaded dependency in this POC, so consumer scaling eventually shifts the bottleneck to Redis. I would increase partitions and instances together, then measure Redis CPU, script latency, and memory.”

## 15. System Design Trade-offs

**What is it?**  
Choosing which guarantees and operational costs matter for the workload.

**Why do we need it here?**  
The POC deliberately chooses simplicity, at-least-once delivery, permanent idempotency, and direct Redis reads.

**Where is it implemented?**  
The choices are visible in `application.yml`, `RewardStreamsConfig.java`, `apply_delta_batch.lua`, and `LeaderboardService.java`.

**Interviewer questions**

- Why not extend Kafka transactions to Redis?
- Why is there a DLT in the POC?
- What would you change for production?

**Senior answer**  
“Kafka transactions do not include an external Redis write, so they would not by themselves make the business effect exactly once. Streams uses `exactly_once_v2` internally for Kafka-to-Kafka correctness, and Redis idempotency covers the external sink. Invalid events go to `invoice-events.DLT`, but poison records that fail after valid processing still rely on retry. The POC retries indefinitely to avoid data loss, which also means a persistent Redis outage can block a partition. Production needs bounded retries, alerting, runbooks, Redis persistence/HA, backups, and a defined idempotency retention policy.”

## 16. Exactly-Once Semantics and Its Limits

**What is it?**  
A processing system appears to apply each event exactly once despite failures and retries.

**Why do we need it here?**  
It is a common interview follow-up because Kafka and Redis are separate systems.

**Where is it implemented?**  
There is no end-to-end exactly-once claim. Streams runs with `exactly_once_v2` (`RewardStreamsConfig.java`), so Kafka offsets, dedupe/window state and delta output commit atomically. Business idempotency at the Redis sink is implemented in `apply_delta_batch.lua` plus the delta Set. Producer idempotence is enabled at `src/main/resources/application.yml:21`.

**Interviewer questions**

- Is producer idempotence exactly once?
- Would Kafka transactions solve the Redis update?
- What guarantee does this POC actually provide?

**Senior answer**  
“Producer idempotence only deduplicates producer retries. Streams transactions give exactly-once for Kafka-to-Kafka processing — input offsets, RocksDB state and delta output — but Redis is outside that transaction. This POC therefore provides exactly-once business processing through the delta topic and an idempotent Redis sink: the projector can be redelivered, but delta IDs prevent a second award. The observable business result is exactly one award per invoice under the tested failure model, yet I would not label the whole pipeline exactly once.”

# Phase 5 — STAR interview story

## Situation

VNPT's e-Invoice platform handles invoice-related events for many companies. The gamification feature needs to award points for successful invoices and rank companies in near real time without allowing retries to create duplicate rewards.

## Task

Build a small, reliable event-driven POC that separates invoice issuance from reward processing, updates points atomically, and keeps leaderboard reads fast. Stage 2B uses a Kafka Streams dedupe state store, 500 ms window aggregation, a reward-delta topic and a Redis projector. The target is 100,000 events/s and the longer-term architecture targets 1M/s. These are targets to measure rather than claims.

## Action

I used one Spring Boot application with a two-stage reward pipeline. The invoice API supports single and batch requests and queues `INVOICE_ISSUED` events asynchronously, keyed by `invoiceId`. A Kafka Streams topology with `exactly_once_v2` claims each invoice in a changelog-backed RocksDB store with a Bloom negative cache, drops duplicates, re-keys by `companyId`, and aggregates points in 500 ms windows. It commits state and output every 500 ms and emits one `CompanyRewardDelta` per company and window to a second Kafka topic. A separate projector consumes that topic with `read_committed`, deduplicates delta IDs, coalesces by company, and applies one `ZINCRBY` per company using a recoverable Lua batch. Leaderboard reads use descending Sorted Set ranges, score lookups, and competition-rank counting.

I tested discount boundaries, duplicate delivery, topology dedupe and windowing, DLT filtering, delta projector dedupe/aggregation, malformed REST input, an application restart with a replayed invoice, and the full two-hop flow. I tuned commit interval and Streams threads against measured runs, upgraded the broker to Kafka 4.4.0 after a 4.1.0 transaction-coordinator bug shut down the client, and ran three 1,000,000-event direct Kafka benchmarks plus a backlog-only drain test.

## Result

On the local Docker POC, the optimized Stage 2B runs measured 102,541.84, 120,862.70 and 122,030.32 unique end-to-end events/s — all above 100k. Leaderboard p95 was 1.28–3.00 ms post-drain, and every run produced exactly 10,000,000 points. Duplicate replay produced no delta. The 3M-key dedupe store restored in 42 seconds; a pre-restart invoice produced no new delta, and a fresh invoice still awarded 10 points.

These are local POC results, not VNPT production measurements. The roadmap's 15-minute sustained gate, concurrent-read p95, freshness and failover drills are not tested. The dedupe store has no retention policy, and one company's deltas are not repartitioned into a single partition. Stage 3 addresses partition scaling, exact-state expiry and a 1M/s capacity run.

## Natural 1–2 minute English interview explanation

“I built a small VNPT e-Invoice gamification POC to practice the backend design rather than claim a production system. The invoice API validates the request, queues an `INVOICE_ISSUED` event asynchronously, and returns 202 with publishing status, so reward processing does not block invoice issuance.

The reward pipeline has two stages. A Kafka Streams topology uses a partitioned RocksDB state store and a Bloom negative cache to deduplicate invoice IDs, aggregates points over 500 ms windows, and publishes company deltas to a second Kafka topic with exactly-once processing. A separate projector reads that topic with `read_committed`, deduplicates delta IDs, coalesces by company, and updates the leaderboard with one atomic Lua call per batch. That handles Kafka redelivery, producer retries, restarts, and concurrent consumers without double awards.

Leaderboard reads use Redis Sorted Sets directly, so top-N, points, and rank do not require scanning a database. Discounts are calculated from the current score.

I verified duplicate and restart behavior and ran three 1,000,000-event direct Kafka benchmarks. The runs measured 102,541, 120,863 and 122,030 unique end-to-end events per second, all above the 100k gate, with p95 between 1.28 and 3.00 ms post-drain and exact point totals. I would still not present that as 15-minute sustained production capacity: the sustained gate, concurrent-read latency and failover drills are the next measurements. Stage 3 then targets partition scaling, exact-state retention and a measured 1M/s capacity run.”
