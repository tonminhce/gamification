# Roadmap POC → 100k/s → 1M/s

## Nguyên tắc

Mỗi stage phải có **exit criteria đo được**. Không tăng partition, consumer hoặc Redis node trước khi xác định bottleneck bằng profile. Không gộp tối ưu hiệu năng với thay đổi correctness; mọi thay đổi kiến trúc phải giữ test retry, replay và failover.

## Stage 0 — Baseline đã hoàn thành

- Java 25, Spring Boot 4.2, Kafka 4.1, Redis 7.4.
- Async producer, batch acknowledgment, Redis pipeline, recoverable Lua marker.
- 30 JUnit 5 tests.
- Local direct Kafka benchmark: 11.513,81 unique events/s end-to-end, p95 2,89 ms; bốn run dao động 11.513–13.623 events/s.

**Kết luận:** POC đúng, nhưng kiến trúc per-event chưa được chứng minh là phù hợp cho 1M/s.

## Stage 1 — 100k events/s

**Trạng thái:** code đã hoàn thành; gate 100k/s chưa đạt ổn định. Best local run 88.647,90 events/s với p95 1,91 ms; repeated run 58.332,27 events/s với p95 4,31 ms trên host chia sẻ tài nguyên.

### Công việc

- [ ] Thêm `POST /api/invoices/batch`, giới hạn 100–1.000 event/request.
- [ ] Định nghĩa event schema versioning và correlation/trace ID.
- [ ] Tăng Kafka lên 12–24 partitions, RF3, `min.insync.replicas=2`.
- [ ] Tune producer `batch.size`, `linger.ms`, `compression.type`, buffer và `max.block.ms`.
- [ ] Consumer micro-batch 1.000–5.000 records; Redis pipeline có giới hạn.
- [ ] Dedupe hot window bằng Redis marker TTL hoặc state store đơn giản; ghi rõ retention.
- [ ] Thêm metrics: accepted/durable/processed/applied events, produce latency, consume lag, Redis script latency, hot key, dedupe hit ratio.
- [ ] Load test phân bố nhiều company, duplicate ratio 0%, 1%, 10%, burst 2× trong 60 giây và soak 4 giờ.

### Exit criteria

- [ ] ≥100.000 durable unique events/s trong 15 phút liên tục.
- [ ] Processed và applied event rate đạt ≥99,5% durable rate trong cửa sổ 15 phút; freshness p95 ≤1 giây; lag trở về 0 sau burst 2× trong 60 giây.
- [ ] Leaderboard p95 ≤5 ms tại 1.000 read requests/s.
- [ ] Không tăng điểm trùng ở mọi duplicate scenario.
- [ ] Memory tăng không quá 5% sau soak 4 giờ và trở về baseline sau GC.
- [ ] Không có unbounded queue hoặc connection pool.

## Stage 2 — 300k events/s và tách correctness state

**Trạng thái:** Stage 2A và Stage 2B đã code xong. Pipeline hiện là `invoice-events → Kafka Streams dedupe/aggregate 500 ms → company-reward-deltas → leaderboard projector → Redis`. Exact invoice dedupe nằm trong RocksDB state store có changelog, có Bloom negative cache; không còn Redis Set. Sau tối ưu, ba run 1.000.000 event liên tiếp đạt **102.541,84 / 120.862,70 / 122.030,32 events/s end-to-end**, p95 1,28–3,00 ms post-drain, đúng 10.000.000 điểm mỗi run. Replay sau restart không tạo delta mới; state 3M keys restore mất 42 giây. Gate 300k/s chưa đạt; gate 100k/s mới đạt cho run ngắn 1M event, chưa phải 15 phút sustained.

**Đã làm:** topic delta, Streams `exactly_once_v2`, RocksDB dedupe + changelog + Bloom filter, window 500 ms, commit interval 500 ms, projector `read_committed`, batch Lua với delta Set và PENDING marker, 23 test pass, broker Kafka 4.4.0 (fix KAFKA-19888), Streams start sau khi topic được tạo.

**Chưa làm:** 64 partitions/RF3, `deltaId` deterministic, repartition topic theo `companyId`, retention/expiry cho dedupe store, dedicated Redis, outbox, chaos test, rebuild drill, capacity gate 300k/s, soak 15 phút và p95 dưới concurrent read.

**Đã đo và loại:** 100 ms commit interval (102,7k/122,3k/119,2k), 12 Streams threads (103,5k/120,2k, p95 tệ hơn); 500 ms commit + 6 threads được giữ.

### Công việc

- [ ] Thêm topic `company-reward-deltas` với `deltaId` deterministic. *(topic có; `deltaId` hiện là UUID + company + windowStart, chưa deterministic — còn thiếu)*
- [x] Reward processor aggregate theo time-window 100–500 ms.
- [x] Chọn state store sau spike: Kafka Streams + RocksDB.
- [x] Với Kafka Streams: publish delta và commit offset/state bằng transaction.
- [ ] Với external LWT: implement state machine `CLAIMED → EMITTED` và outbox/delta intent; không tuyên bố Kafka EOS.
- [ ] Repartition output theo `companyId`; projector coalesce cùng company trước Redis; freeze output topology hoặc drain/fence/migrate pending trước khi đổi partition count. *(projector coalesce đã có; repartition topic và freeze procedure chưa có)*
- [x] Projector dùng `isolation.level=read_committed`, Lua atomic với source `deltaId` markers. *(marker hiện theo `topic:partition`, chưa phải hash range; chưa có xử lý offset gap cho transaction control)*
- [ ] Dedicated leaderboard Redis; đo write rate và marker operation rate sau coalesce.
- [ ] Transactional outbox cho REST acceptance nếu invoice service chưa có.
- [ ] Chaos test: crash giữa claim và emit, abort transaction, Redis failover, consumer rebalance. *(mới có restart + replay thủ công)*
- [ ] Kiểm thử restoration state store dưới tải và rebuild leaderboard từ snapshot + manifest offset theo từng partition. *(restore sau restart đã chạy một lần, chưa dưới tải, chưa rebuild drill)*

### Exit criteria

- [ ] ≥300.000 durable/processed/applied unique events/s trong 30 phút.
- [ ] Freshness p95 ≤1 giây.
- [ ] Exactly-once business effect trong retention window, kể cả crash và replay.
- [ ] State restore không làm mất điểm; catch-up hoàn tất trong ≤5 phút ở workload 300k/s.
- [ ] Redis write + marker operation rate nằm dưới 50–60% safe capacity đo được tại p95 ≤5 ms.
- [ ] Delta retention ≥7 ngày, snapshot hằng ngày kèm offset từng partition và source-delta dedupe set/reconstruction procedure, marker TTL ≥7 ngày; rebuild drill hoàn tất ≤15 phút.

## Stage 3 — 1M events/s

### Công việc

- [ ] Kafka 64–128 partitions; sizing broker theo tổng `input + delta output + changelog + transaction markers + internal topics + replication/compaction + consumer fetch + standby/restore egress`; network/disk ≤60% safe capacity.
- [ ] Scale reward processor theo partition; warm standby/state restore strategy; không tăng partitions khi local dedupe còn trong retention nếu chưa migrate/re-shard state.
- [ ] Exact dedupe store phân vùng theo stable `invoiceId` và expiry; ưu tiên external ledger nếu cần đổi partition count; time bucket chỉ là field lưu trữ.
- [ ] Hot/cold retention policy; archive audit không nằm trên hot path.
- [ ] Ingress autoscaling theo in-flight work, produce latency và downstream pressure.
- [ ] Load shedding, retry quota, jittered backoff, circuit breaker.
- [ ] Distributed load generators; báo cáo `dropped_iterations`.
- [ ] Capacity run 1M/s trong 30 phút; soak 24 giờ ở 1M/s nếu muốn tuyên bố sustained 24 giờ.
- [ ] Soak 24 giờ ở 70% capacity nếu chỉ tuyên bố 30 phút capacity và 24 giờ stability ở 700k/s.
- [ ] Failover/recovery drill và cost model per million durable accepted events.

### Exit criteria

- [ ] Durable accepted ≥1.000.000 events/s trong 30 phút.
- [ ] Processed ≥1.000.000 events/s và applied event-equivalent rate ≥1.000.000/s trong cùng cửa sổ.
- [ ] Leaderboard p95 ≤5 ms tại 10.000 read requests/s; p99 ≤10 ms.
- [ ] Freshness p95 ≤1 giây.
- [ ] Zero duplicate point increase trong retry/replay/failover test; so sánh expected ledger theo `invoiceId`.
- [ ] Không dropped iteration do load generator; overload được shed có kiểm soát.
- [ ] Hot Redis key p95 ≤10 ms cho write path và ≤5 ms cho leaderboard read path.
- [ ] RPO/RTO đã chốt và đo: accepted event RPO 0 sau commit; leaderboard RPO ≤5 phút/RTO ≤15 phút.
- [ ] Produce p95 ≤50 ms và consume p95 ≤100 ms cho mọi partition; không có partition lag >5 giây; memory growth ≤5% sau soak.

## Benchmark contract

Báo cáo phải tách rõ:

- **Offered rate:** tốc độ client cố gửi.
- **Accepted rate:** request được API chấp nhận.
- **Durable rate:** event đã nằm trong Kafka/outbox.
- **Processed rate:** event đã được dedupe và phát delta.
- **Applied rate:** điểm đã visible ở Redis.
- **Correctness:** duplicate không đổi tổng điểm.
- **Tail latency:** p95/p99 leaderboard và freshness.
- **Generator health:** CPU, network, `dropped_iterations`.

Không dùng một run ngắn để tuyên bố sustained capacity. Sau spike/peak test phải có soak test dài và failover test.

## Fault matrix tối thiểu

| Fault | Expected result |
|---|---|
| Producer retry cùng invoice | Một reward duy nhất |
| Consumer crash trước commit | Redelivery, không tăng trùng |
| Crash sau claim trước emit delta | Claim được phục hồi, delta phát đúng một lần |
| Kafka transaction abort | Delta aborted không visible với `read_committed` |
| Projector crash sau `ZINCRBY` | Retry nhận diện `PENDING`, không tăng lần hai |
| Outbox republish cùng `deltaId` ở offset mới | Source marker bỏ qua, không tăng lần hai |
| Transaction control/aborted offset gap | Checkpoint dùng consumer position/LSO, không stall |
| Redis failover | Read model rebuild từ snapshot + deltas |
| State store rebalance | Restore/catch-up trong ngân sách đã đo |
| Retry storm | Retry quota/load shedding giữ hệ thống ổn định |

## Rủi ro chính

| Rủi ro | Giảm thiểu |
|---|---|
| Hot company/key partition | Key bằng hash invoiceId, output repartition theo company, đo key skew |
| State store quá lớn | Retention window ngắn, archive lạnh, đo 86,4B IDs/ngày ở mức 1M/s |
| Redis trở thành hot ZSET | Aggregation + coalesce trước Redis, dedicated leaderboard, sharded top-K nếu cần |
| Retry storm | Retry quota, jitter, load shedding, bounded queue |
| Benchmark chỉ đo producer/durable | Acceptance test đo đến Redis visible và correctness |
| Thêm topic làm tăng freshness lag | Window ngắn, đo p95 freshness, tune theo SLO |
