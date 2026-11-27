# ADR-0001 — Kiến trúc xử lý 1 triệu invoice events/giây

- **Status:** Proposed — chờ vượt qua các capacity gate
- **Date:** 2026-11-26
- **Decision owner:** Backend/Platform team
- **Related:** `DEEP_RESEARCH_1M_EVENTS_ARCHITECTURE.md`, `docs/roadmap/1m-events-roadmap.md`

## Context

POC hiện tại đã chứng minh đường Java 25/Spring Boot 4 → Kafka → Redis Lua → Sorted Set hoạt động đúng và đạt khoảng 11,5k unique events/s end-to-end với leaderboard p95 2,89 ms trên máy local. Mục tiêu mới là 1 triệu events/s cho toàn bộ REST → Kafka → Redis, leaderboard p95 khoảng 5 ms, idempotency chính xác theo business window và cho phép scale-out.

Kiến trúc “mỗi invoice → một Lua script → một Redis marker” chưa được chứng minh là phù hợp ở 1M/s. Stage 1 đã thay bằng batch API, 12 partitions và một Lua batch script dùng Redis Set. Stage 2A đã thêm reward-delta topic và leaderboard projector; partitioned exact state store/time-window aggregation vẫn là Stage 2B. Ở tốc độ đó, mỗi event vẫn cần exact lookup, marker state và thao tác cập nhật. Retention 24 giờ tạo ra 86,4 tỷ invoice IDs, còn Redis command execution vẫn tuần tự. Vì vậy cần thay đổi đơn vị xử lý, không chỉ tăng batch.

## Decision

### 1. Dùng batch ingestion làm đường chính

Giữ endpoint single-event cho tương thích POC, nhưng thêm `POST /api/invoices/batch` cho tối đa 100–1.000 invoice mỗi request. Mục tiêu được đo bằng **events/s**, không phải HTTP requests/s.

API trả `202` sau khi sự kiện được nhận vào cơ chế bền vững: transactional outbox ở phía invoice service hoặc Kafka producer acknowledgment theo chính sách vận hành. Không trả `202` chỉ sau khi đưa event vào in-memory queue nếu yêu cầu là không mất event đã chấp nhận.

### 2. Tách topic đầu vào và topic reward delta

- `invoice-events`: key bằng hash của `invoiceId` để tránh hot company partition; thứ tự theo company không bắt buộc vì phép thưởng cộng gộp và idempotent.
- `company-reward-deltas`: key bằng `companyId`, chứa `deltaId`, `deltaPoints`, `windowStart`, `windowEnd`, `schemaVersion`.
- Mỗi `deltaId` xác định đúng một company trong một window. Không dùng một `batchId` chung cho nhiều company nằm trên nhiều partition.
- Bắt đầu với 64 partitions cho `invoice-events`, RF3, `min.insync.replicas=2`, `acks=all`; điều chỉnh sau benchmark phân bố khóa thực tế.

### 3. Reward processor thực hiện exact dedupe và aggregation

Processor tiêu thụ `invoice-events` theo micro-batch hoặc time-window 100–500 ms:

1. Tra cứu/claim `invoiceId` trong state store phân vùng.
2. Với event mới, cộng `10` vào map `companyId → deltaPoints` trong batch.
3. Publish `company-reward-deltas` với `deltaId` deterministic.
4. Commit offset/state theo đúng ranh giới transaction của state store đang chọn.

Có hai nhánh triển khai:

**Nhánh A — Kafka Streams + local RocksDB**

- Kafka transaction bao phủ offset, local state changelog và output topic.
- State store phân vùng theo task; standby replica và changelog restore phải được đo.
- Trong retention window, không tăng partition count nếu chưa migrate/re-shard state. Retry sau 64→128 partitions có thể không còn gặp claim cũ và award trùng.
- Đây là nhánh ưu tiên cho POC/private deployment; Stage 3 nên dùng external stable ledger nếu cần đổi partition count thường xuyên.

**Nhánh B — Cassandra/Scylla exact ledger**

- LWT `IF NOT EXISTS` claim không tham gia Kafka transaction.
- Claim và outbox/delta intent phải nằm trong cùng partition transaction hoặc state machine `CLAIMED → EMITTED`.
- Dùng `deltaId` deterministic để publish lại an toàn; crash giữa publish và mark `EMITTED` được xử lý bằng idempotent projector.
- Không được mô tả nhánh này là Kafka exactly-once.

`invoiceId` phải có stable lookup key. Time bucket chỉ là field/phân vùng lưu trữ, không được làm key khiến retry qua ngày trở thành một invoice mới. Cùng `invoiceId` với `eventHash` khác phải bị từ chối/quarantine, không award lần hai.

### 4. Repartition và coalesce theo company trước Redis

`company-reward-deltas` được key bằng `companyId`, nhờ đó mọi delta của một company đi vào cùng một output partition. Projector/aggregator coalesce các delta cùng company trong batch trước khi ghi Redis. Trong pending/retry horizon, không được tăng output partition count nếu chưa drain/fence owner cũ và migrate/clear pending state; nếu không company có thể chuyển partition khi batch cũ chưa recover.

Write model sau coalesce:

```text
redis_writes/s ≈ sum(distinct company per output partition per projector batch) / batch_seconds
```

Nếu chưa coalesce, 64 input partitions × 100 companies mỗi 100 ms có thể tạo khoảng 64.000 preliminary delta records/s. Sau repartition và coalesce, 100 companies toàn cục chỉ còn khoảng 1.000 `ZINCRBY` writes/s. Redis operation rate phải cộng thêm source-delta marker operations cho mỗi preliminary delta và batch marker operations cho mỗi projector batch.

### 5. Leaderboard projector idempotent theo source delta và partition checkpoint

Projector phải đặt `isolation.level=read_committed` nếu consume topic transactional. Mỗi consumer thread xử lý một output partition tuần tự; nếu batch listener nhận nhiều partition, phải group theo partition trước khi apply.

Định danh projector batch:

```text
projectorBatchId = hash(topic, partition, firstOffset, lastOffset, schemaVersion)
```

Projector dùng cả hai cơ chế:

- `einv:source-delta:{deltaId}`: marker TTL cho từng source delta, chống republish ở offset mới.
- `einv:projector-batch:{partition}`: trạng thái `PENDING`/`APPLIED`, offset range và danh sách company + `scoreBefore` để phục hồi lỗi giữa Lua write.

Một Lua script cho toàn bộ batch của một partition thực hiện:

1. Nếu có `PENDING`, phục hồi trước bằng danh sách `deltaId` và `scoreBefore`/`scoreAfter` đã lưu.
2. Bỏ qua record có offset ≤ `lastAppliedOffset`; không yêu cầu offset liên tục vì transaction control record và aborted record tạo gap hợp lệ.
3. Với record còn lại, bỏ qua `deltaId` đã có source marker; coalesce delta mới theo company.
4. Nếu không có delta mới, chỉ cập nhật checkpoint `lastAppliedOffset = max(lastAppliedOffset, batchEnd)`.
5. Nếu có delta mới, lưu `PENDING` cùng danh sách `deltaId`, company, delta và `scoreBefore`; apply `ZINCRBY`, đánh dấu source delta, rồi finalize batch/checkpoint.

Repoll có thể trùng offset hoặc đổi batch boundary; source marker và checkpoint xử lý được. Republish cùng `deltaId` ở offset mới bị source marker chặn. `read_committed` chỉ expose record hợp lệ; consumer position/LSO được dùng làm mốc tiến trình, không dùng phép cộng offset liên tục.

Source-delta marker TTL phải ≥ outbox republish/replay horizon, ví dụ 1 giờ nếu outbox retry tối đa 15 phút và replay 45 phút. Đây là retention khác với invoice idempotency window. Nếu dùng Redis Cluster, leaderboard, source marker và projector marker phải cùng hash tag.

Snapshot phải kèm manifest `topic → partition → lastAppliedOffset`, không chỉ một `last_delta_id` toàn cục. Snapshot được tạo tại cùng barrier offset với projector; replay bắt đầu sau offset của từng partition. Snapshot bundle phải khôi phục được source-delta dedupe set qua barrier, hoặc quét lại delta log từ retention start tới barrier để dựng lại markers trước khi replay suffix. Delta retention phải ≥ cả replay horizon và source-marker horizon; nếu không, republish cũ có thể tăng điểm lại sau rebuild.

### 6. Không tuyên bố end-to-end exactly-once

Kiến trúc cung cấp:

- Kafka Streams exactly-once cho offset/state/output bên trong Kafka ở nhánh A.
- At-least-once delivery cho REST/outbox, external ledger và Redis projector.
- Exactly-once **business effect** cho mỗi `invoiceId` trong retention window nhờ exact claim.
- Idempotent Redis application theo source `deltaId` và `projectorBatchId`/partition checkpoint.

Kafka transaction không bao gồm Redis. Bloom filter chỉ được dùng để tối ưu negative lookup, không được dùng để kết luận duplicate.

### 7. SLO và capacity gate

- Durable accepted events: ≥1.000.000/s trong 30 phút ổn định.
- Processed events: ≥1.000.000/s trong cùng cửa sổ.
- Applied reward deltas: đủ để mọi invoice đã processed đều visible ở Redis; applied event rate ≥1.000.000/s khi đo theo event tương đương.
- Leaderboard p95 ≤5 ms tại giả định 10.000 read requests/s; p99 ≤10 ms.
- Freshness p95 ≤1 giây từ Kafka accept đến Redis visible.
- Không tăng điểm trùng khi retry, replay, rebalance hoặc failover.
- Báo cáo offered rate, accepted rate, durable rate, processed rate, applied rate, failures, `dropped_iterations`, Kafka lag và Redis hot-key latency.

RPO/RTO đề xuất để chốt trước Stage 3:

- Accepted invoice event: RPO 0 sau Kafka/outbox commit.
- Leaderboard read model: RPO ≤5 phút nhờ rebuild từ snapshot + deltas; RTO ≤15 phút.
- Exact dedupe state: RPO 0 trong retention window nếu state store/ledger được replicate đúng cấu hình.

## Alternatives considered

| Phương án | Kết luận | Lý do |
|---|---|---|
| Tăng số consumer và batch của Lua hiện tại | Chưa phù hợp, chưa được chứng minh cho 1M/s | Vẫn một script/một lookup cho mỗi event; Redis tuần tự và cardinality quá lớn |
| Một Redis key vĩnh viễn cho mỗi invoice | Từ chối | 86,4 tỷ IDs/ngày ở 1M/s, chưa tính index/replica/overhead |
| Kafka transactions bao gồm Redis | Không khả thi | Redis không tham gia transaction Kafka |
| Bloom filter là nguồn dedupe cuối | Từ chối | False positive có thể loại nhầm invoice hợp lệ |
| Sharded leaderboard ngay từ đầu | Hoãn | Aggregation/coalesce có thể giữ write rate dưới capacity một dedicated ZSET |
| REST single-event 1M requests/s | Không ưu tiên | Chi phí HTTP/TLS/ingress rất cao; batch API đạt cùng event rate với ít request hơn |
| External LWT được coi là Kafka transaction | Từ chối | Cần state machine/outbox riêng và không được tuyên bố EOS Kafka |

## Consequences

### Tích cực

- Giảm số Redis write theo số company active sau repartition/coalesce.
- Tách correctness state khỏi read model; Redis có thể rebuild từ snapshot + deltas.
- Có thể scale consumer theo partition và thay state store mà không đổi API leaderboard.
- Dễ kiểm thử replay, lag và recovery hơn kiến trúc ghi trực tiếp.

### Tiêu cực

- Thêm topic, processing hop và state store.
- Leaderboard có eventual consistency trong cửa sổ aggregation.
- State store có dung lượng, restoration và vận hành phức tạp.
- Cần benchmark thật trên broker, network, storage và dedupe workload; chưa có bằng chứng production cho 1M/s.

## Open questions

- Số company active và phân bố điểm theo company là bao nhiêu?
- Kích thước event trung bình/p95 và payload tối đa?
- Business idempotency window là 15 phút, 1 giờ, 24 giờ hay 7 ngày?
- Read QPS của leaderboard và yêu cầu exact global rank cho mọi company?
- Invoice service đã có database transaction/outbox hay chưa?
- Cloud managed hay on-prem; budget network/storage và RPO/RTO?
