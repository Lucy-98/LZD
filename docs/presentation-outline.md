# LZD Uplift Feature Platform — Technical Presentation

Deck mục tiêu: 7 slide, 8–10 phút. Trọng tâm là đóng góp Data Engineering;
API và uplift model chỉ xuất hiện như integration boundary.

## Slide 1 — Problem Statement: Voucher đúng người, đúng thời điểm

**Thông điệp chính:** Bài toán không phải “ai sẽ mua”, mà là “ai sẽ mua thêm
nhờ voucher”. Dữ liệu sai hoặc chậm khiến quyết định uplift đúng về thuật toán
nhưng sai khi vận hành.

- Voucher đại trà làm tăng chi phí và giảm biên lợi nhuận.
- Batch-only bỏ lỡ intent gần realtime; realtime-only thiếu lịch sử ổn định.
- Training/serving skew có thể tạo prediction hợp lệ về kỹ thuật nhưng sai nghĩa.
- Thành công tạo ra quyết định voucher có thể kiểm chứng, rollback và quan sát.

**Visual:** Before/after flow: dữ liệu phân mảnh → versioned feature platform →
quyết định voucher có kiểm soát.

## Slide 2 — Vai trò của tôi: Data Engineering backbone

**Thông điệp chính:** Tôi xây dựng đường dữ liệu và các contract giúp API/model
có dữ liệu đúng, đủ, mới và tương thích.

- Kafka/CSV ingestion, DLQ và immutable MinIO lake.
- Reconstruction + dbt Bronze/Silver/Gold.
- Offline/PIT training dataset và Redis online feature store.
- Airflow orchestration, quality gates, version activation/rollback.
- Prometheus/Grafana/Loki và PostgreSQL audit.

**Boundary:** FastAPI và uplift model là consumer/integration components, không
phải trọng tâm ownership trong phần trình bày.

## Slide 3 — End-to-end Data Architecture

**Thông điệp chính:** Một backbone nối lịch sử, streaming, training và serving
qua các contract có version.

- Historical CSV + Kafka → MinIO raw.
- DuckDB/dbt → Bronze, Silver, Gold.
- Gold → 71 batch fields; raw events → 12 realtime PIT fields.
- DAG40 → versioned Redis; training mart → ML workflow.
- Monitoring bao phủ ingestion, DQ, sync và serving boundary.

**Visual:** ảnh `architecture-target.drawio`, crop tập trung zones 1–6.

## Slide 4 — Khó nhất #1: Reconstruct và chống training/serving skew

**Thông điệp chính:** Feature không chỉ cần đúng công thức mà còn đúng thời điểm.

- Reconstruction tạo lại 30F baseline từ dữ liệu ẩn danh và giữ provenance.
- `feature_ts` là cutoff; event sau cutoff không được đi vào training row.
- Offline PIT dùng cùng bucket 5 phút/1 giờ như online Redis.
- Ordered feature contract và defaults được kiểm tra bằng test.

**Visual:** dbt lineage + query mẫu của `training_dataset` và
`feat_user_realtime_pit`.

## Slide 5 — Khó nhất #2: Feature Store replay-safe và zero-downtime

**Thông điệp chính:** Retry không được biến thành double-count hoặc mixed version.

- MinIO object identity = topic/partition/offset-range + SHA-256.
- Lua cập nhật counter và dedup marker `event_id` trong một transaction.
- 32 deterministic shards; validation 500 users trước activation.
- Atomic `fs:meta:active_version`, rollback và giữ hai version gần nhất.

**Visual:** RedisInsight active pointer + version status + batch/realtime keys.

## Slide 6 — Reliability: Quality gates và observability

**Thông điệp chính:** “DAG xanh” chưa đủ; dữ liệu phải chứng minh được chất lượng.

- dbt schema/not-null/unique/business tests.
- Row count, checksum, freshness và online/offline parity.
- Model/feature/spec compatibility tuple trước readiness/deployment.
- Metrics: lag, DLQ, dedup, late drop, cache miss, p99 latency.
- Structured logs và audit cho run/version/model correlation.

**Visual:** Grafana Pipeline Overview + Data Quality & Skew; Airflow DAG40/50.

## Slide 7 — Kết quả và khả năng mở rộng

**Thông điệp chính:** Repo tạo ra một nền tảng feature có thể tái lập, vận hành
và mở rộng chứ không chỉ một demo model.

- 71 batch + 12 realtime fields, baseline model contract 30F.
- Versioned cutover/rollback, replay-safe ingestion và PIT parity.
- Full automated tests pass; fail-closed khi artifact/contract sai.
- Có thể mở rộng sang campaign ranking, churn intervention hoặc next-best-action.

**Closing:** Đóng góp DE biến dữ liệu rời rạc thành một production path có
contract, kiểm soát và bằng chứng vận hành.

## Checklist ảnh cần capture

- `01-architecture.png`: export draw.io, chữ đọc rõ ở 16:9.
- `02-airflow-pipeline.png`: DAG60/20/40/50 cùng run mới, không dùng run cũ.
- `03-minio-offset-range.png`: breadcrumb và object offset-range rõ ràng.
- `04-dbt-pit-query.png`: query + column counts, không lộ credential.
- `05-redis-versioning.png`: active pointer, status metadata, batch và RT key.
- `06-grafana-overview.png`: time range có traffic; p99/freshness/DQ có dữ liệu.
- `07-tests.png`: terminal full pytest summary.
- Tùy chọn `08-ready-tuple.png`: JSON `/ready`, dùng như integration evidence.

Trước khi chụp: dùng cùng một feature version/run trong mọi ảnh; ẩn password,
token, hostname nội bộ và user identifier thật.
