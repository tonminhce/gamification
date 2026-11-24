# Phase 4 — Theory and interview preparation

This guide refers to the code in this POC. Line numbers are from the current source tree.

## 1. Event-Driven Architecture

**What is it?**  
Components communicate by publishing events rather than calling each other directly. The producer knows the event contract, not the reward implementation.

**Why do we need it here?**  
Invoice issuance should not wait for leaderboard maintenance. Kafka also allows reward processing to scale and recover independently.

**Where is it implemented?**  
`src/main/java/com/vnpt/einvoice/gamification/invoice/InvoiceEventPublisher.java:44` publishes the event. `src/main/java/com/vnpt/einvoice/gamification/consumer/RewardConsumer.java:21` consumes it.

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
Producer settings: `src/main/resources/application.yml:14`. Typed producer factory: `src/main/java/com/vnpt/einvoice/gamification/config/KafkaConfig.java`. Consumer: `RewardConsumer.java:21`.

**Interviewer questions**

- Why is the Kafka key `companyId`?
- What does `acks=all` guarantee?
- What does producer idempotence guarantee?

**Senior answer**  
“The key keeps one company's events in one partition, preserving their order. `acks=all` waits for all in-sync replicas; with one replica in this POC, it effectively waits for the broker. Producer idempotence prevents duplicates caused by producer retries, but it does not stop a user or upstream system from publishing the same business invoice twice.”

## 4. Kafka Consumer Groups and Partitions

**What is it?**  
A topic is split into ordered partitions. A consumer group divides partitions among consumers so each partition is processed by one group member at a time.

**Why do we need it here?**  
Three partitions allow three consumer threads to process different companies in parallel.

**Where is it implemented?**  
Group and concurrency: `src/main/resources/application.yml:22` and `src/main/resources/application.yml:34`. Topic creation: `src/main/java/com/vnpt/einvoice/gamification/config/KafkaConfig.java`.

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
Auto commit is disabled at `src/main/resources/application.yml:25`. Batch acknowledgment is configured at `src/main/resources/application.yml:33`. The listener returns only after the full batch succeeds at `RewardConsumer.java:21`.

**Interviewer questions**

- What happens if the app crashes after `ZINCRBY` but before commit?
- Why not auto-commit?
- What does committing a batch offset mean?

**Senior answer**  
“The batch is redelivered. Lua sees the existing invoice marker and does not add points again. Auto-commit can acknowledge records before processing finishes, so I disabled it. A committed batch offset means every record before that position completed successfully; otherwise the consumer must retry.”

## 6. Idempotency

**What is it?**  
Repeating the same operation has the same effect as performing it once.

**Why do we need it here?**  
Kafka retries, producer retries, user retries, and batch redelivery can all repeat `INV-1001`.

**Where is it implemented?**  
`src/main/resources/scripts/apply_reward.lua` stores an `APPLIED` marker and recovers `PENDING` markers. `RewardService.java:37` invokes the script. The concurrency and partial-write tests are in `src/test/java/com/vnpt/einvoice/gamification/reward/RewardServiceIntegrationTest.java`.

**Interviewer questions**

- Why use `invoiceId` instead of `eventId`?
- Permanent key or TTL?
- What if the same invoice ID arrives for another company?

**Senior answer**  
“`invoiceId` is the business identity; `eventId` changes on every publish. Permanent keys are safest for this POC because an old duplicate can never award again. The cost is unbounded memory. A TTL is acceptable only if the business explicitly accepts re-award after expiry. The marker is `PENDING` while the score update is in progress and `APPLIED` after success, so a retry can finish an interrupted update without awarding twice.”

## 7. Atomicity

**What is it?**  
A group of changes appears indivisible to other clients.

**Why do we need it here?**  
Checking “not processed” and incrementing points must be one indivisible decision.

**Where is it implemented?**  
The complete operation is in `src/main/resources/scripts/apply_reward.lua:1`.

**Interviewer questions**

- Is a Redis Lua script a database transaction with rollback?
- What happens on a script error?
- Why validate key types first?

**Senior answer**  
“Redis runs the script without interleaving other clients, so it is atomic for concurrency. It is not a rollback transaction: a runtime error does not undo earlier writes. I validate key types before writing and use a `PENDING` marker containing the pre-update score. On retry, the script can tell whether `ZINCRBY` happened and either finish or finalize the marker. Company-keyed ordering and batch retry prevent another event for that company from interleaving before recovery. Inconsistent state is rejected for reconciliation, and the batch is not acknowledged.”

## 8. Race Conditions

**What is it?**  
Correctness depends on timing between concurrent operations.

**Why do we need it here?**  
Separate `GET`, `SET`, and `ZINCRBY` calls allow two consumers to both see “not processed” and both add 10 points.

**Where is it implemented?**  
The race is removed by the single script in `src/main/resources/scripts/apply_reward.lua`. The 32-thread test in `RewardServiceIntegrationTest.java` exercises it against real Redis.

**Interviewer questions**

- Show the failing interleaving.
- Would a Java lock solve it?
- What about multiple application instances?

**Senior answer**  
“Consumer A reads no marker, consumer B reads no marker, then both write the marker and increment. A local Java lock only protects one JVM and would fail after horizontal scaling. The check and update must live in the shared Redis boundary, which is why I used Lua.”

## 9. Redis Sorted Sets

**What is it?**  
A Redis data structure that maps unique members to numeric scores and keeps them ordered by score.

**Why do we need it here?**  
We need points, top-N ranking, and individual rank without scanning every company.

**Where is it implemented?**  
Score update: `src/main/resources/scripts/apply_reward.lua`. Top list: `src/main/java/com/vnpt/einvoice/gamification/leaderboard/LeaderboardService.java:36`. Individual competition rank: `LeaderboardService.java`.

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
`src/main/resources/scripts/apply_reward.lua`; loaded and executed by `RewardService.java:37`.

**Interviewer questions**

- Why not `MULTI/EXEC`?
- Is Lua a scalability risk?
- What would you monitor?

**Senior answer**  
“Lua is short enough for this POC and works on dedicated keys. `MULTI/EXEC` could group commands, but Lua lets the marker state decide whether to increment, finalize, or reject an inconsistent partial update. Long scripts block Redis, so script duration is a key metric. I would monitor script errors, latency, memory, AOF health, and eviction.”

## 11. Time Complexity

**What is it?**  
How operation cost grows with data size.

**Why do we need it here?**  
The leaderboard must stay fast as company count grows.

**Where is it implemented?**  
`ZINCRBY` at `apply_reward.lua:16`; top range at `LeaderboardService.java:36`; rank at `LeaderboardService.java:64`.

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
Kafka batching and Redis pipelining: `src/main/resources/application.yml` and `RewardService.java`. Benchmarks: `scripts/KafkaLoadProducer.java`, `scripts/RestLoadProducer.java`, and `scripts/load-test.mjs`.

**Interviewer questions**

- Why did batch acknowledgment improve throughput?
- Can caching top N improve latency?
- What is the trade-off?

**Senior answer**  
“Batch commits, asynchronous publishing, producer batching, and Redis pipelining raised the direct unique-event result from 5,603 to 11,514 events/s in the final run. Repeated direct runs ranged from 11,514 to 13,623 events/s. A top-N cache could reduce read latency further, but it introduces staleness and invalidation work. I would only add it after measuring a read bottleneck.”

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
“The final direct Kafka run measured 2.89 ms p95, and repeated runs ranged from 1.00 to 2.89 ms, but that is not a production guarantee. Tail latency can come from JVM pauses, GC, Redis contention, network scheduling, Kafka rebalances, or large responses. A real test needs production-like data, sustained load, multiple runs, and percentile reporting under concurrent reads and writes.”

## 14. Horizontal Scaling

**What is it?**  
Adding more application instances to share work.

**Why do we need it here?**  
Reward consumption can scale independently from the REST API.

**Where is it implemented?**  
Consumer group and concurrency are in `src/main/resources/application.yml:22` and `src/main/resources/application.yml:34`.

**Interviewer questions**

- How many consumers are useful with three partitions?
- Does adding instances improve Redis unlimitedly?
- What breaks first?

**Senior answer**  
“At most three consumers actively own partitions in this topic. More instances provide failover but no extra partition parallelism. Redis remains a shared single-threaded dependency in this POC, so consumer scaling eventually shifts the bottleneck to Redis. I would increase partitions and instances together, then measure Redis CPU, script latency, and memory.”

## 15. System Design Trade-offs

**What is it?**  
Choosing which guarantees and operational costs matter for the workload.

**Why do we need it here?**  
The POC deliberately chooses simplicity, at-least-once delivery, permanent idempotency, and direct Redis reads.

**Where is it implemented?**  
The choices are visible in `application.yml`, `apply_reward.lua`, and `LeaderboardService.java`.

**Interviewer questions**

- Why no Kafka transactions?
- Why no dead-letter topic in the POC?
- What would you change for production?

**Senior answer**  
“Kafka transactions do not include an external Redis write, so they would not by themselves make the business effect exactly once. Redis idempotency is the key correctness mechanism. The POC retries indefinitely to avoid data loss, which also means a poison event can block its partition. Production needs bounded retries, a DLT, alerting, runbooks, Redis persistence/HA, backups, and a defined idempotency retention policy.”

## 16. Exactly-Once Semantics and Its Limits

**What is it?**  
A processing system appears to apply each event exactly once despite failures and retries.

**Why do we need it here?**  
It is a common interview follow-up because Kafka and Redis are separate systems.

**Where is it implemented?**  
There is no end-to-end exactly-once claim. Producer idempotence is enabled at `src/main/resources/application.yml:17`; business idempotency is implemented in `apply_reward.lua`.

**Interviewer questions**

- Is producer idempotence exactly once?
- Would Kafka transactions solve the Redis update?
- What guarantee does this POC actually provide?

**Senior answer**  
“Producer idempotence only deduplicates producer retries. Kafka transactions can provide exactly-once for Kafka-to-Kafka workflows, but Redis is outside that transaction. This POC provides at-least-once event delivery plus idempotent reward application. The observable business result is exactly one award per invoice under the tested failure model, but I would not label the whole pipeline exactly once.”

# Phase 5 — STAR interview story

## Situation

VNPT's e-Invoice platform handles invoice-related events for many companies. The gamification feature needs to award points for successful invoices and rank companies in near real time without allowing retries to create duplicate rewards.

## Task

Build a small, reliable event-driven POC that separates invoice issuance from reward processing, updates points atomically, and keeps leaderboard reads fast. The design targets 10,000 events/s and about 5 ms p95 leaderboard latency, but those are targets to measure rather than claims.

## Action

I used one Spring Boot application. The invoice API validates the request and queues an `INVOICE_ISSUED` event asynchronously using `companyId` as the Kafka key. A consumer group processes three partitions with batch acknowledgments, so offsets are committed only after the whole batch succeeds. The consumer sends up to 500 Lua executions through one Redis pipeline; each script remains atomic and stores a recoverable `PENDING`/`APPLIED` invoice marker. If a script fails between the marker write and score update, the retry uses the stored pre-update score to finish safely. Leaderboard reads use descending Sorted Set ranges, score lookups, and competition-rank counting. Discounts are derived dynamically from points.

I tested discount boundaries, duplicate delivery, 32 concurrent virtual threads against real Redis, a 300-record pipeline with 100 duplicates, both partial-write recovery points, malformed REST input, and the full REST-to-Kafka-to-Redis flow. I also ran repeated 10,000-event direct Kafka benchmarks and a separate REST duplicate benchmark.

## Result

On the local Docker POC, the final direct Kafka run measured 23,703.57 producer-acked events/s, 11,513.81 unique end-to-end events/s, and 2.89 ms leaderboard p95. Four direct runs ranged from 11,513.81 to 13,622.93 events/s with 1.00–2.89 ms p95. All 100,000 expected points were present. In the separate REST test, 1,000 duplicate deliveries left points unchanged and Kafka lag returned to zero.

These are local POC results, not VNPT production measurements. The 10,000 Kafka events/s and about 5 ms p95 targets were met in the direct benchmark. The next production work would be an outbox or delivery-status store, more partitions and consumers, Redis capacity planning, and an idempotency-key retention strategy.

## Natural 1–2 minute English interview explanation

“I built a small VNPT e-Invoice gamification POC to practice the backend design rather than claim a production system. The invoice API validates the request, queues an `INVOICE_ISSUED` event asynchronously, and returns 202 with publishing status, so reward processing does not block invoice issuance.

The consumer processes events from three partitions. It uses batch acknowledgments, meaning Kafka offsets are committed only after the whole batch succeeds, and sends the Redis Lua executions through a pipeline. The important correctness boundary is still each atomic Lua script: it records a recoverable invoice marker and increments the company's Sorted Set score by 10. If a script fails between the marker write and the score update, the retry can finish safely without double awards. That handles Kafka redelivery, producer retries, and concurrent consumers.

Leaderboard reads use Redis Sorted Sets directly, so top-N, points, and rank do not require scanning a database. Discounts are calculated from the current score.

I verified duplicate and concurrent behavior with real Redis and ran repeated 10,000-event direct Kafka benchmarks. Locally, the final run measured 11,513.81 unique end-to-end events per second and 2.89 ms p95 leaderboard latency. The separate REST duplicate test was also correct and Kafka lag returned to zero. Both performance targets were met in the local POC, but I would not present those numbers as production capacity. The next steps would be an outbox, more partitions, Redis capacity planning, and a production policy for idempotency-key retention.”
