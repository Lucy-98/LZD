# Architecture Review & Risk Assessment

**Hệ thống:** LZD Uplift Feature Platform
**Ngày review:** 2026-08-05
**Phạm vi:** toàn bộ repo tại thời điểm review
**Số liệu nền đo được:** 1.108.338 user (926.669 train + 181.669 test), 89 field/user

> Review này viết theo chuẩn production. Hệ thống hiện là bản local mô phỏng,
> nên một số điểm trừ (HA, backup) là **cố ý chấp nhận**. Chúng vẫn được liệt kê
> vì câu hỏi đặt ra là *"nếu đây là hệ thống của công ty tôi"*.

---

## 1. Business Alignment

### Kiến trúc có phục vụ đúng bài toán không

Có, ở phần cốt lõi. Yêu cầu *"quyết định < 100ms, không query warehouse mỗi
lần"* được đáp ứng đúng bằng precompute + Redis lookup. Uplift (chứ không phải
propensity) được phản ánh đúng qua `is_treat`/`label` và cấu trúc training set.

### Over-engineering

| Thành phần | Đánh giá |
|---|---|
| **MLflow** | **Thừa ở giai đoạn này.** Chưa có model nào. Kéo theo 1 Postgres DB, 1 bucket, 768MB RAM, 1 image build. Bật khi teammate bắt đầu train, không sớm hơn |
| **RedisInsight** | Thừa — `redis-cli` làm được mọi việc đang cần. Tốn 512MB |
| **32 shard** cho 1.1M user | Quá nhiều. Mỗi shard ~35K dòng, chi phí điều phối > lợi ích song song. **8 shard là đủ**, và giảm luôn vấn đề quét lặp ở mục 8 |
| **4 dashboard Grafana** | Hợp lý cho mục tiêu đào tạo, nhưng dashboard 03 (infra) trùng phần lớn với dashboard có sẵn của cộng đồng |
| **dbt** | **Không thừa** — chính nó tạo lineage và test. Giữ |

### Business requirement CHƯA được phản ánh

Đây là nhóm thiếu sót đáng lo nhất, vì chúng không phải bug mà là **khoảng
trống**:

1. **Không có ràng buộc ngân sách.** Nghiệp vụ phát voucher luôn có budget
   ngày/tháng. Kiến trúc hiện tại quyết định độc lập từng user theo ngưỡng cố
   định `UPLIFT_DECISION_THRESHOLD=0.02` → không có cơ chế "hết ngân sách thì
   dừng", không có top-k selection. Thực tế bài toán là **knapsack**, không phải
   threshold.
2. **Không có chống lạm dụng.** Cùng một user gọi `/decide` 100 lần sẽ nhận
   `SEND_VOUCHER` 100 lần. Không có idempotency key, không có "đã phát rồi".
3. **Không có vòng phản hồi.** Không ghi lại *thực tế* user có dùng voucher và
   có mua không. `ops.inference_log` ghi quyết định nhưng không có bảng kết quả
   → **không đo được uplift thật, không retrain có giám sát được**. Đây là lỗ
   hổng nghiêm trọng nhất về mặt business: hệ thống không tự biết nó đúng hay sai.
4. **Không có holdout/A-B.** Muốn chứng minh model tạo ra giá trị thì phải giữ
   một nhóm control ngay trên production. Không có cơ chế đó.

### Chi phí vận hành

24 container, ~15GB RAM giới hạn cho 1.1M user. Với quy mô này, một job Python
đơn giản + Redis là đủ. **Chi phí không tương xứng nếu xét thuần chức năng** —
nhưng tương xứng nếu mục tiêu là đào tạo và làm khuôn cho hệ thống thật. Cần nói
rõ mục tiêu nào để không bị đánh giá sai.

---

## 2. Data Governance

| Hạng mục | Trạng thái | Ghi chú |
|---|---|---|
| Single Source of Truth | 🟢 **Tốt** | DuckDB là sự thật, Redis là bản sao — nêu rõ và thực thi trong code |
| Data Ownership | 🔴 **Không có** | Không có bảng/metadata nào ghi ai sở hữu feature nào. `feature_spec.yml` có `group` nhưng không có `owner` |
| Data Lineage | 🟡 **Một phần** | dbt cho lineage **trong** warehouse. Đứt ở 2 đầu: Kafka→MinIO và DuckDB→Redis→quyết định. Không có OpenLineage/Marquez |
| Data Catalog | 🔴 **Không có** | `dbt docs` chưa từng được generate/serve. Không ai tra được "feature f42 nghĩa là gì" — mà 83/89 feature là ẩn danh |
| Data Contract | 🟢 **Tốt** | `feature_spec.yml` + `assert_spec_contract` là contract thật, có cưỡng chế |
| Data Retention | 🔴 **Thiếu nghiêm trọng** | Xem bên dưới |
| Data Versioning | 🟡 **Một phần** | Feature có version. **Training data KHÔNG** |
| Auditability | 🟡 | `ops.*` tốt cho pipeline. Không audit được ai đọc/sửa gì |
| Reproducibility | 🔴 **Hỏng** | Xem bên dưới |

### 🔴 G-1 · Không thể tái hiện kết quả train

`marts.training_dataset` là materialization `table` — **bị drop và tạo lại mỗi
ngày** (`dbt/models/marts/training_dataset.sql`). MLflow ghi metric và param,
nhưng không ghi snapshot dữ liệu.

**Hậu quả:** ba tháng sau, model production cho kết quả lạ. Bạn muốn dựng lại
đúng tập train hôm đó để điều tra — **không thể**. Dữ liệu đã bị ghi đè 90 lần.
Model không audit được, không phản biện được, không tuân thủ được nếu có yêu cầu
giải trình.

**Khắc phục:** ghi snapshot bất biến ra MinIO trước khi train:
`s3://lakehouse/training_snapshots/dt=YYYY-MM-DD/` + log đường dẫn + checksum
vào MLflow. Chi phí thấp, giá trị rất cao.

### 🔴 G-2 · Không có retention cho bảng vận hành

`ops.inference_log` ghi **một dòng mỗi request**, không partition, không purge.
Ở 1.000 QPS = 86 triệu dòng/ngày. Postgres sẽ phình tới khi hết đĩa, và khi hết
đĩa thì **Airflow chết theo** (dùng chung instance).

`ops.pipeline_run`, `ops.dq_result` cũng tăng vĩnh viễn.

**Khắc phục:** partition theo ngày + job `DROP PARTITION` sau N ngày; hoặc đẩy
`inference_log` sang Kafka→lake thay vì ghi thẳng Postgres.

### 🟡 G-3 · Lineage đứt đúng chỗ quan trọng nhất

Khi một quyết định phát voucher bị nghi ngờ, bạn cần trả lời: *feature nào,
version nào, từ dòng dữ liệu nào, do job nào ghi*. Hiện tại `inference_log` có
`feature_version` — tốt — nhưng từ đó ngược lên tới event Kafka gốc thì không có
đường đi. Không có correlation ID xuyên suốt producer → consumer → sync → API.

---

## 3. Data Quality

| Chiều | Có kiểm tra? | Ở đâu |
|---|---|---|
| Completeness | 🟢 | `null_rates()`, `min_row_count`, row-count offline vs online |
| Accuracy | 🟡 | So mẫu Redis vs DuckDB. Không có ground truth để đối chiếu |
| Consistency | 🟢 | `validate_sync` + DAG 50 định kỳ |
| Timeliness | 🟢 | `feature_store_age_seconds` + alert 26h |
| Validity | 🟡 | `validate_event` ở ingest; dbt `accepted_values`. **Không có range check trên f0..f82** |
| Uniqueness | 🟢 | dbt test `unique` trên `event_id`, `user_id` |

### 🔴 Q-1 · Không có kiểm tra phân bố feature (silent corruption)

Đây là kiểu hỏng nguy hiểm nhất trong ML: **không có gì báo lỗi cả**.

Giả sử upstream đổi đơn vị `f30` từ USD sang cent. Kết quả:
- `not_null` pass ✓
- row count pass ✓
- offline/online consistency pass ✓ (cả hai bên đều sai như nhau)
- Model vẫn trả về số ✓
- **Uplift score sai hoàn toàn, voucher phát nhầm người, không alert nào nổ**

Hiện chỉ có `null_rate` cho 20 feature đầu và `feature_mean` được đẩy lên
Prometheus nhưng **không có ngưỡng nào so sánh với hôm qua**.

**Khắc phục:** thêm DQ check phân bố — min/max/mean/stddev/p50/p99 từng feature,
so với cửa sổ 7 ngày, lệch > k×σ thì cảnh báo. Đây là việc nên làm **trước** khi
làm bất cứ thứ gì khác trong danh sách này.

### 🟡 Q-2 · Điểm sinh dữ liệu bẩn đã được xử lý tốt

Công bằng mà nói: dedup theo `event_id` (`stg_app_events.sql`) và dedup theo
`(user_id, dt)` (`stg_user_snapshot.sql`) là đúng chỗ và đúng cách. DLQ có ghi
`reason`. Đây là phần mạnh.

### 🟡 Q-3 · DLQ là ngõ cụt

Message vào `app.user.events.dlq.v1` rồi **nằm đó tới khi hết retention 7 ngày**.
Không có consumer, không có công cụ replay, không có quy trình xử lý. Dữ liệu bị
mất vĩnh viễn mà vẫn "có vẻ" đã được xử lý vì đã vào DLQ.

---

## 4. Distributed Systems Review

| Thuộc tính | Đánh giá |
|---|---|
| Idempotency | 🟢 Thiết kế tốt: version deterministic + `HSET` + shard marker |
| Eventual Consistency | 🟢 Mô hình rõ ràng, ranh giới được nêu tường minh |
| Failure Recovery | 🟢 Resume theo shard, có công cụ `resync_pending` |
| Retry Safety | 🟢 Task retry an toàn vì idempotent |
| Backpressure | 🔴 **Không có** |
| Scalability | 🔴 Xem mục 13 |
| Fault Isolation | 🟢 Sync hỏng không ảnh hưởng serving — điểm mạnh nhất của thiết kế |

### 🔴 D-1 · Chạy lại DAG 40 trong cùng ngày ghi thẳng vào namespace ĐANG PHỤC VỤ

**Đây là lỗi thiết kế nghiêm trọng nhất trong toàn hệ thống.**

`make_version()` sinh version từ `logical_date` → chạy ngày 2026-08-05 luôn cho
`v20260805`. Rất tốt cho idempotency. Nhưng:

```
01:30  DAG 40 chạy → ghi v20260805 → validate → SET active_version = v20260805
10:00  Phát hiện mart sai, chạy lại DAG 20 rồi DAG 40
       → prepare_sync mở lại CHÍNH v20260805 — tức namespace ĐANG phục vụ traffic
       → 32 shard ghi đè trực tiếp lên dữ liệu người dùng đang đọc
```

Toàn bộ giá trị của blue-green bị vô hiệu: trong 5–10 phút sync, user nhận **hỗn
hợp** feature cũ và mới. Nếu validate fail giữa chừng, không có gì để rollback —
bản cũ đã bị ghi đè mất rồi.

`force_full_resync` còn tệ hơn: nó xoá shard marker nhưng **không xoá key**, nên
user đã biến mất khỏi snapshot vẫn còn dữ liệu cũ nằm lại.

**Khắc phục:** version phải gồm cả nội dung, không chỉ ngày:
`v20260805.<8 ký tự đầu checksum>`. Cùng dữ liệu → cùng version (giữ idempotency);
dữ liệu đổi → version mới → ghi vào namespace mới → swap → giữ nguyên blue-green.

### 🔴 D-2 · Backfill sẽ kích hoạt dữ liệu cũ lên production

`activate_version()` (`sync.py:310`) `SET` con trỏ **vô điều kiện**. Backfill
ngày 2026-07-01 hôm nay → `SET active_version = v20260701` → serving phục vụ
feature một tháng tuổi. Không có kiểm tra "version mới phải mới hơn version đang
active".

**Khắc phục:** chặn activate nếu `logical_date < ngày của active version`, trừ
khi có cờ `--force-activate` tường minh.

### 🔴 D-3 · Không có backpressure ở consumer

Nếu Redis chậm (fork khi ghi AOF, hoặc gần đầy bộ nhớ), `_flush()` chặn. Khi thời
gian giữa hai lần `poll()` vượt `max.poll.interval.ms` (300s), Kafka **đá consumer
ra khỏi group** → rebalance → consumer mới nhận partition → lại chậm → lại bị đá.
**Vòng lặp rebalance**, lag tăng vô hạn, không tự thoát.

**Khắc phục:** timeout cứng cho `_flush()`, giảm batch khi phát hiện chậm, và
alert trên `kafka_consumergroup_rebalance` (chưa được theo dõi).

### 🔴 D-4 · Deadlock DuckDB giữa reader và writer

DuckDB cho phép **một** process mở read-write, **hoặc** nhiều process mở
read-only — không đồng thời cả hai.

Pool `duckdb_writer=1` chỉ giới hạn *writer*. Nhưng DAG 40 chạy 4 shard task
song song, mỗi task là một process giữ handle **read-only**. Nếu DAG 20 (dbt,
cần write) khởi động trong lúc đó → **dbt fail ngay**, không phải chờ.

Hiện tại hai DAG cách nhau 30 phút nên chưa gặp. Chỉ cần một lần trigger tay là
gặp. Và khi dữ liệu tăng, DAG 20 chạy quá 30 phút thì gặp mỗi ngày.

**Khắc phục:** đưa cả reader vào cùng một pool, hoặc dùng Airflow Dataset để DAG
40 chỉ chạy khi DAG 20 phát tín hiệu hoàn tất.

### 🟡 D-5 · Race giữa prepare và shard đã được chặn đúng

Ghi nhận điểm tốt: `prepare_sync` chốt checksum, `validate_sync` tính lại và so
→ phát hiện được dữ liệu đổi giữa chừng. Đây là thiết kế đúng.

---

## 5. Kafka Review

| Hạng mục | Đánh giá |
|---|---|
| Topic design | 🟢 Có version trong tên (`.v1`) — đúng |
| Partition strategy | 🟡 3 partition; đủ hiện tại, thiếu khi scale |
| Key strategy | 🟢 `user_id` — đúng cho bài toán này |
| Retention | 🟡 48h; xem K-2 |
| DLQ design | 🟡 Có ghi `reason`, nhưng là ngõ cụt (Q-3) |
| Consumer group | 🟡 Một group, một instance |
| Ordering | 🟢 Hiểu đúng và tận dụng đúng |

### 🔴 K-1 · Replication factor = 1

`KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1`, một broker. Mất broker = **mất toàn
bộ dữ liệu chưa consume**, và mất luôn offset của consumer group. Không phục hồi
được.

Chấp nhận được ở local. Ở production là điểm chết tuyệt đối.

### 🟡 K-2 · Retention 48h < khả năng phục hồi cần thiết

Nếu MinIO mất dữ liệu (mục 14), khả năng phát lại chỉ còn 48h. Mọi event cũ hơn
mất vĩnh viễn → mất luôn feature realtime lịch sử → tập train có lỗ hổng.

**Khắc phục:** ít nhất 7 ngày cho topic gốc, và coi MinIO raw là bản sao lưu
chính thức (có versioning + replication).

### 🔴 K-3 · Không có schema registry

`schema_version` tồn tại trong payload nhưng **không có gì cưỡng chế**.
`validate_event()` chỉ kiểm tra 4 trường bắt buộc. Producer thêm/xoá/đổi kiểu
trường là consumer im lặng bỏ qua hoặc ghi `None` vào parquet → cột đổi kiểu →
dbt fail hoặc tệ hơn: ép kiểu ngầm và sai âm thầm.

**Khắc phục:** Schema Registry + Avro/Protobuf, hoặc tối thiểu validate JSON
Schema và từ chối `schema_version` lạ.

### 🟡 K-4 · Hotspot partition — rủi ro thấp hiện tại

Producer cố ý tạo phân bố 80/20 trên 20.000 user. Hash trải qua 3 partition nên
lệch tải partition không đáng kể. **Nhưng** ở production thật với vài user cực
lớn (seller lớn, bot), key `user_id` sẽ tạo hotspot rõ rệt. Cần theo dõi
`kafka_topic_partition_current_offset` theo từng partition — dashboard 02 đã có.

---

## 6. Feature Store Review

Đây là phần được đầu tư nhất và cũng là phần chắc nhất.

| Hạng mục | Đánh giá |
|---|---|
| Offline/online consistency | 🟢 Validate trước khi activate + kiểm tra định kỳ |
| Point-in-time correctness | 🟢 `e.event_ts < s.feature_ts`, cửa sổ khớp từng ô 5 phút |
| Feature skew | 🟢 Ba lớp bảo vệ: spec contract, PIT, validate |
| Freshness | 🟢 Có metric + alert |
| Versioning | 🟡 Có, nhưng hỏng khi rerun cùng ngày (D-1) |

### 🔴 F-1 · Redis không đủ bộ nhớ cho tập dữ liệu thật

**Tính toán trên số liệu đo được:**

```
1.108.338 user × 89 field
Hash listpack (89 < 128 nên dùng mã hoá nén)
≈ 17 byte/field × 89  ≈ 1.5 KB
+ key + overhead      ≈ 0.1 KB
                      ─────────
                        1.6 KB/user
× 1.108.338           ≈ 1.8 GB  cho MỘT version
× 2 version giữ lại   ≈ 3.5 GB
+ overlay realtime    ≈ 3.7 GB
```

Cấu hình hiện tại: `REDIS_MAXMEMORY=512mb`, `mem_limit: 900m`,
policy `noeviction`.

**Thiếu khoảng 7 lần.** Sync sẽ fail với `OOM command not allowed` ở khoảng 30%
tiến độ (~350K user). Với `noeviction` thì nó fail rõ ràng — tốt hơn là im lặng
xoá key — nhưng **DAG 40 sẽ không bao giờ chạy xong trên dữ liệu đầy đủ**.

Đây là lý do `row_limit=200000` trong hướng dẫn "chạy thử" thực ra là **điều
kiện bắt buộc**, không phải tuỳ chọn. Tài liệu đang nói sai.

**Khắc phục, theo thứ tự ưu tiên:**
1. Nâng `REDIS_MAXMEMORY` lên 4gb và `mem_limit` lên 5g (cần máy ≥ 16GB)
2. Hoặc giảm số feature đẩy lên online — thực tế model hiếm khi cần cả 83 feature
3. Hoặc nén: đóng gói feature vector thành một chuỗi msgpack/float32 thay vì 89
   field → giảm khoảng 4–5 lần, đổi lại mất khả năng đọc từng field
4. Sửa tài liệu ngay lập tức để không ai bất ngờ

### 🟡 F-2 · Validate lấy mẫu từ nhầm phía

`validate_sync()` lấy mẫu user bằng `SCAN` **trên Redis**, rồi tra ngược sang
DuckDB. Nghĩa là user bị bỏ sót hoàn toàn khỏi Redis **không bao giờ lọt vào
mẫu**.

Công bằng mà nói: kiểm tra row count (`row_diff_ratio <= 0.001`) vẫn bắt được
trường hợp bỏ sót hàng loạt. Nên đây là điểm yếu ở lớp thứ hai, không phải lỗ
hổng chết người. Nhưng lấy mẫu từ phía DuckDB thì vừa đơn giản hơn vừa mạnh hơn.

Thêm nữa: `SCAN` trả về key theo thứ tự bucket băm, **không ngẫu nhiên** → mẫu
bị lệch có hệ thống về phía một nhóm key nhất định.

### 🟡 F-3 · Checksum chỉ phủ 5/89 feature

`offline_store.checksum()` lấy `SUM` của 5 feature đầu. Feature thứ 6 trở đi đổi
giá trị mà số dòng không đổi → checksum không phát hiện.

---

## 7. Redis Review

Ngoài F-1 ở trên:

### 🔴 R-1 · `count_keys()` quét toàn bộ keyspace trong đường nóng của consumer

`stream_consumer.py:153` — trong **mỗi** lần flush micro-batch:

```python
REALTIME_OVERLAY_KEYS.set(self.store.count_keys(self.store.spec.realtime_key("*")))
```

`count_keys` chạy `SCAN` toàn bộ keyspace. Với 1.1M key batch + overlay, đó là
hàng nghìn vòng `SCAN` mỗi 30 giây, chỉ để cập nhật **một gauge dùng cho biểu đồ**.

**Hậu quả:** consumer chậm hẳn, Redis tốn CPU vô ích, và chính nó góp phần gây
ra kịch bản backpressure D-3.

**Khắc phục:** bỏ hẳn khỏi `_flush`. Lấy số này từ `redis_exporter` (đã cấu hình
sẵn `REDIS_EXPORTER_CHECK_KEY_PATTERNS`) hoặc chỉ tính trong DAG 50.

### 🟡 R-2 · `/decide/batch` không giới hạn kích thước

`BatchDecideRequest.user_ids: list[str]` — không có `max_items`. Một request với
1 triệu user_id sẽ tạo pipeline 2 triệu lệnh → Redis đơn luồng bị chiếm dụng →
**toàn bộ traffic khác đứng hình**. Vector DoS đơn giản.

**Khắc phục:** `Field(max_length=1000)` và chia lô nội bộ.

### 🟢 R-3 · Những điểm làm đúng

Ghi nhận: `SCAN` thay `KEYS`, `UNLINK` thay `DEL`, Lua cho thao tác đa khoá,
`noeviction` có lý do rõ ràng, TTL trên overlay, cửa sổ trượt theo ô. Đây là
phần code chín nhất trong repo.

---

## 8. Data Warehouse Review

### 🔴 W-1 · Chia shard bằng `hash() % N` gây quét lặp N lần

`offline_store.iter_shard()` sinh:

```sql
SELECT ... FROM marts.feat_user_serving WHERE (hash(user_id) % 32) = 7
```

DuckDB không có index trên biểu thức này → **quét toàn bảng cho mỗi shard**.
32 shard = 32 lần quét bảng 1.1M dòng × 89 cột. Độ phức tạp `O(N × S)` thay vì
`O(N)`.

**Khắc phục:** thêm cột `shard_id` tính sẵn trong dbt và sắp xếp bảng theo nó
(để DuckDB dùng được zone map / min-max pruning); hoặc export một lần ra 32 file
parquet rồi mỗi task đọc file của mình.

### Khi nào kiến trúc này hết đủ

| Ngưỡng | Dấu hiệu nhận biết | Việc phải làm |
|---|---|---|
| **~5M user** | DAG 20 chạy > 30 phút, chạm lịch DAG 40 → D-4 kích hoạt | Đổi `training_dataset` sang `incremental` |
| **~10M user** | Redis > 16GB/version; rebuild mart > 1h | Redis Cluster hoặc nén feature vector |
| **~50M user hoặc >1 team** | Tranh chấp khoá DuckDB liên tục; phải xếp hàng chờ ghi | Chuyển sang BigQuery/Snowflake — DuckDB đã hết vai trò |
| **Cần đọc song song khi ghi** | Query ad-hoc bị chặn | Đây là ngưỡng **cứng** của DuckDB, không tối ưu được |

**Dấu hiệu sớm nhất cần theo dõi:** thời gian chạy DAG 20 (`airflow_dagrun_duration_success_seconds`).
Khi nó vượt 50% khoảng cách tới DAG 40, bắt đầu di chuyển — đừng đợi tới lúc va chạm.

---

## 9. Airflow Review

### 🔴 A-1 · Phụ thuộc giữa DAG 20 và DAG 40 chỉ dựa vào... đồng hồ

DAG 20 chạy 01:00, DAG 40 chạy 01:30. **Không có ràng buộc phụ thuộc nào cả** —
không `ExternalTaskSensor`, không Dataset, không trigger.

**Hậu quả khi DAG 20 chạy quá 30 phút:**
- Trường hợp tốt: DAG 40 fail vì tranh chấp khoá DuckDB (D-4)
- Trường hợp xấu: DAG 20 đã xong `feat_user_serving` nhưng đang chạy
  `training_dataset` → DAG 40 đọc mart nửa cũ nửa mới và **sync thành công** →
  dữ liệu sai lên production mà mọi check đều xanh

Trường hợp xấu nguy hiểm hơn nhiều vì nó im lặng.

**Khắc phục:** Airflow Dataset (`outlets`/`inlets`) — DAG 40 chỉ chạy khi DAG 20
phát tín hiệu. Đây là sửa đổi nhỏ, giá trị lớn.

### 🟡 A-2 · Không có chiến lược backfill

`catchup=False` ở mọi DAG. Backfill phải làm tay, và khi làm sẽ trúng D-2
(kích hoạt dữ liệu cũ). Không có tài liệu nào nói backfill thế nào cho an toàn.

### 🟡 A-3 · Callback ghi Postgres đồng bộ ở mọi task

`on_execute_callback` + `on_success_callback` mỗi cái mở một kết nối Postgres
mới. Với DAG 40 (32 shard × 2 callback) = 64 kết nối được tạo và huỷ mỗi lần
chạy. Chưa gây vấn đề ở quy mô này, nhưng là cùng loại lỗi với cái đã sửa ở
`/decide` — chỉ khác là chưa nằm trên đường có SLA.

### 🟢 A-4 · Làm đúng

DAG mỏng, logic nằm trong `src/` nên test được không cần Airflow. Dynamic task
mapping dùng đúng chỗ. `trigger_rule=ALL_DONE` cho `publish_dbt_results` là
quyết định tinh tế và đúng.

---

## 10. ML Platform Review

| Hạng mục | Đánh giá |
|---|---|
| Reproducibility | 🔴 Hỏng (G-1) |
| Model versioning | 🟡 Có MLflow Registry nhưng chưa dùng |
| Feature versioning | 🟢 Tốt |
| Model rollback | 🔴 Không có |
| Experiment tracking | 🟡 Khung có, chưa chạy |

### 🔴 M-1 · Không có đường rollback model

Feature có `active_version` + rollback dưới 5 giây. **Model thì không có gì
tương đương.** `/admin/reload-model` nạp bản mới nhưng không có cách quay lại
bản trước ngoài việc đổi alias trong MLflow rồi gọi lại — không có metric nào
theo dõi, không có quy trình, không có ai viết ra.

Bất đối xứng này là lỗi thiết kế: đã bỏ công làm blue-green cho feature thì phải
làm tương đương cho model.

### 🔴 M-2 · Không có gate chất lượng khi promote

`promote_model()` là chỗ trống. Không có ràng buộc "AUUC phải tốt hơn model đang
chạy mới được lên Production". Teammate rất dễ điền vào đó một lệnh promote vô
điều kiện — và thế là bất kỳ model nào train xong cũng lên thẳng production.

### 🟡 M-3 · Nguy cơ leakage còn sót

PIT join xử lý đúng phần feature realtime. Nhưng `feat_user_behaviour` tính từ
`stg_app_events` với `where event_ts >= now() - interval '30 days'` — dùng
`now()` **chứ không phải `feature_ts`**. Với dữ liệu lịch sử, cửa sổ 30 ngày này
được neo vào thời điểm *chạy dbt*, không phải thời điểm snapshot của từng dòng.

Ở bản local, `feature_ts = now()` lúc seed nên hai cái trùng nhau và không lộ ra.
Với dữ liệu nhiều ngày thật, đây là **leakage**: feature batch chứa thông tin
sau thời điểm ra quyết định.

**Khắc phục:** neo cửa sổ vào `feature_ts` của từng dòng, giống hệt cách
`feat_user_realtime_pit.sql` đang làm.

### 🟡 M-4 · Model đang là stub nhưng không có gì báo động

`StubModel` trả về score giả với `model_version="stub-0"`. `/store/info` có cờ
`is_stub`. Nhưng **không có alert nào** cho "production đang chạy bằng model
giả". Ở một hệ thống thật, đây là kịch bản dễ xảy ra nhất khi deploy sai cấu
hình MLflow.

---

## 11. Security Review

Đây là hạng mục yếu nhất. Với bản local thì chấp nhận được; đưa lên bất kỳ mạng
nào chung là không.

### 🔴 S-1 · Client tự chỉ định giá trị feature — lỗ hổng nghiệp vụ

```python
# app.py — DecideRequest
context: dict[str, Any] = Field(default_factory=dict)
...
merged, missing = spec.merge(batch_raw, {**realtime, **req.context})
```

`context` **ghi đè mọi feature** và có độ ưu tiên cao nhất. Bất kỳ ai gọi API đều
có thể gửi:

```json
{"user_id": "U0000123", "context": {"rt_order_1h": 999, "f30": 0.99}}
```

để tự ép model trả về uplift cao và **tự cấp voucher cho mình**. Đây không phải
lỗi lý thuyết — đó là đường tấn công trực tiếp vào ngân sách khuyến mãi.

**Khắc phục:** whitelist đúng những feature client được phép gửi (nên là **rỗng**),
hoặc bỏ hẳn `context`. Nếu cần tín hiệu tức thời từ client thì phải ký và xác thực.

### 🔴 S-2 · Redis không mật khẩu, cổng mở ra host

Không có `requirepass`, port 6379 published. Bất kỳ ai truy cập được máy đó đều
`FLUSHALL` được toàn bộ feature store, hoặc sửa feature của bất kỳ user nào.
Cùng tình trạng: Kafka (không auth/TLS), Postgres (published), MinIO.

### 🔴 S-3 · Fernet key công khai trong `.env.example`

`AIRFLOW_FERNET_KEY=ZmDfcTF7_60GrrY167zsiPd67pEvs0aGOv2oasOM1Pg=` được ghi
thẳng vào file mẫu. Ai copy nguyên si (đa số sẽ làm vậy) thì **toàn bộ mật khẩu
connection trong Airflow có thể bị giải mã bởi bất kỳ ai đọc được repo**.

**Khắc phục:** để trống trong file mẫu + script `init` tự sinh; thêm kiểm tra
từ chối khởi động nếu phát hiện key mẫu.

### 🔴 S-4 · Một tài khoản cho tất cả

- Postgres: một user `lzd` là superuser cho cả `airflow`, `mlflow`, `pipeline`
  → service ghi `inference_log` **đọc được bảng connection của Airflow**, tức là
  leo thang tới mọi credential khác
- MinIO: root credential dùng làm AWS key cho mọi service, không có policy giới hạn
- Grafana: `admin/admin`

### 🔴 S-5 · API không xác thực

`/decide`, `/decide/batch`, `/admin/reload-model` — không auth, không rate limit.
`/admin/reload-model` cho phép bất kỳ ai buộc service nạp lại model.

### 🟡 S-6 · Vấn đề tuân thủ

`ops.inference_log` lưu quyết định gắn với user, vô thời hạn, không có đường xoá
theo yêu cầu (GDPR "right to erasure"). `user_id` là giả danh nhưng vẫn là dữ
liệu cá nhân theo hầu hết khung pháp lý. Không có mã hoá khi lưu, không có audit
log truy cập.

---

## 12. Observability Review

Phần này mạnh về bề rộng, yếu ở một số điểm mù cụ thể.

### 🔴 O-1 · `push_to_gateway` dùng PUT — push sau xoá metric của push trước

Đã xác minh: `prometheus_client.push_to_gateway` dùng **PUT**, tức là *thay thế
toàn bộ* metric trong nhóm `job`. Trong `sync.py:314` và `sync.py:324` có **hai
lệnh push liên tiếp cùng `job="feature_sync"`**:

```python
push_batch_metrics(PUSH_JOB, {"feature_sync_duration_seconds": ..., ...})  # dòng 314
push_batch_metrics(PUSH_JOB, {"feature_store_active_version_info": 1})     # dòng 324 → XOÁ cái trên
```

**Hậu quả:** panel *"Thời gian sync gần nhất"* và *"Rows/giây khi ghi Redis"*
trên dashboard 01 **luôn trống**, và không ai biết vì sao. Cùng lỗi này lặp lại ở
`prepare_sync` và trong DAG 50.

**Khắc phục:** đổi sang `pushadd_to_gateway` (POST, chỉ thêm/cập nhật), hoặc gộp
tất cả metric vào một lần push.

### 🔴 O-2 · Không có alert khi DAG ngừng chạy hẳn

`AirflowSchedulerHeartbeatMissing` chỉ bắt scheduler chết. Nếu ai đó **pause DAG
40**, scheduler vẫn khoẻ, mọi metric vẫn xanh, và feature từ từ cũ đi. Có
`FeatureStoreStale` sau 26h — nhưng đó là phát hiện *hậu quả*, không phải
*nguyên nhân*, và chậm hơn một chu kỳ kinh doanh.

**Khắc phục:** alert `time() - airflow_dag_last_success_timestamp{dag_id="40..."} > 90000`.

### 🔴 O-3 · Không có correlation ID xuyên hệ thống

Không thể lần một request `/decide` ngược về event Kafka đã tạo ra feature của
nó. Khi có sự cố dữ liệu, việc điều tra phải làm thủ công bằng cách đối chiếu
timestamp.

### 🟡 O-4 · Điểm mù còn lại

| Không theo dõi | Vì sao quan trọng |
|---|---|
| `lzd_inference_log_dropped_total` | Có metric nhưng không có alert — mất log âm thầm |
| Kafka consumer rebalance | Chính là dấu hiệu sớm của D-3 |
| Model đang là stub | M-4 |
| Phân bố feature theo ngày | Q-1 — điểm mù nghiêm trọng nhất |
| Dung lượng đĩa Postgres/MinIO | G-2 dẫn tới hết đĩa |
| Tỷ lệ `NO_DECISION` | Model hỏng nhưng API vẫn 200 |

### 🟢 O-5 · Làm đúng

Log JSON có trường `event` ổn định, `ops.*` cho phép truy vấn SQL, tách histogram
tổng và histogram Redis riêng để khoanh vùng chậm, `publish_dbt_results` chạy cả
khi test fail. Đây là những quyết định tốt.

---

## 13. Capacity Planning

### Dữ liệu × 10 (11M user)

| Thành phần | Kết quả |
|---|---|
| **Redis** | 💀 **Chết đầu tiên** — 18GB/version, 36GB cho 2 version |
| DuckDB dbt | 🟡 Rebuild mart 20–40 phút → va chạm DAG 40 (D-4, A-1) |
| Sync 32 shard | 🔴 `O(N×S)` → 32 × 11M dòng quét (W-1) |
| Kafka | 🟢 Không đổi (phụ thuộc traffic, không phụ thuộc số user) |
| MinIO | 🟢 Chỉ là dung lượng đĩa |

**Redesign cần thiết:** nén feature vector hoặc Redis Cluster; `incremental` cho mart.

### Dữ liệu × 100 (110M user)

| Thành phần | Kết quả |
|---|---|
| **DuckDB** | 💀 Hết vai trò — một node không kham nổi, và single-writer chặn mọi thứ |
| **Redis** | 💀 180GB/version — phải chuyển sang store phân tán (Aerospike/ScyllaDB) hoặc chỉ giữ user active |
| Sync | 💀 Phải đổi hoàn toàn: export song song ra parquet rồi nạp bằng nhiều worker |
| Airflow | 🟡 LocalExecutor hết đủ → Celery/Kubernetes executor |

**Đây là ngưỡng phải viết lại**, không phải tinh chỉnh.

### Request × 100

Hiện tại thực tế ~0 QPS. Giả sử lên 10.000 QPS:

| Thành phần | Kết quả |
|---|---|
| FastAPI | 🟢 Stateless → nhân bản ngang, dễ |
| **Redis** | 🔴 ~100K ops/s trên một node; 10K QPS × 2 HGETALL × 89 field → **nghẽn băng thông trước khi nghẽn CPU** → cần read replica |
| **Postgres `inference_log`** | 💀 864 triệu dòng/ngày — chết chắc (G-2) |
| Pushgateway | 🟢 Không liên quan đường serving |

**Bottleneck thật là kích thước payload**, không phải số lệnh: mỗi lookup kéo về
89 field. Đây là lý do thứ hai để nén feature vector.

---

## 14. Disaster Recovery

**Trạng thái chung: không có backup nào cả.** Mọi state nằm trong Docker volume;
`docker compose down -v` xoá sạch. `scripts/stack.ps1 reset` có hỏi xác nhận —
đó là biện pháp bảo vệ duy nhất đang tồn tại.

| Kịch bản | Phục hồi được? | RTO | RPO | Đủ chưa |
|---|---|---|---|---|
| **Redis mất sạch** | 🟢 Có — chạy lại DAG 40 từ DuckDB | 10–60 phút | **0** cho batch; ~1h overlay (tự lành) | 🟡 Trong lúc rebuild, serving trả toàn giá trị default và **không có gì chặn** việc đó. Cần chế độ "fail closed" |
| **Kafka mất 1 broker** | 🔴 Không — RF=1, một broker | ∞ | Toàn bộ dữ liệu chưa consume | 🔴 Không chấp nhận được ở production |
| **DuckDB hỏng** | 🟢 Có — dựng lại từ MinIO qua DAG 00 + 20 | 1–2 giờ | 0 (raw nằm ở object storage) | 🟢 Đây là điểm mạnh của kiến trúc medallion |
| **Airflow down** | 🟢 Serving chạy tiếp bằng version hiện tại | tuỳ | 0 | 🟢 Suy giảm êm; alert stale sau 26h |
| **MinIO mất dữ liệu** | 🟡 Một phần — `user_snapshot` dựng lại từ `data/*.csv`; `app_events` chỉ phát lại được 48h | 2–4 giờ | **48h** cho event | 🔴 Mất event lịch sử = mất feature realtime lịch sử = tập train có lỗ hổng vĩnh viễn |

### Thiếu sót về quy trình

1. Không có backup Postgres (`ops.*` + metadata Airflow + MLflow registry mất là
   mất hẳn — **kể cả model registry**)
2. MinIO không bật versioning, không replication
3. Không có diễn tập khôi phục nào được ghi lại (RUNBOOK có bài diễn tập *sự cố*,
   không có bài diễn tập *khôi phục thảm hoạ*)
4. Không định nghĩa RTO/RPO mục tiêu ở đâu → không có chuẩn để đánh giá

---

## 15. Final Assessment

### Điểm số (thang 1–10, chấm theo chuẩn production)

| Hạng mục | Điểm | Lý do |
|---|---|---|
| **Architecture** | **7** | Pattern chọn đúng và hiểu sâu. Trừ điểm vì D-1 (rerun ghi vào namespace sống) và A-1 (phụ thuộc bằng đồng hồ) là lỗi tầng thiết kế |
| **Reliability** | **5** | Cô lập lỗi trong sync rất tốt; nhưng không backup, Kafka RF=1, không backpressure |
| **Scalability** | **4** | Redis thiếu ~7 lần bộ nhớ cho chính dữ liệu đang có; sync `O(N×S)`; DuckDB single-writer |
| **Maintainability** | **8** | Hạng mục mạnh nhất: một contract duy nhất, DAG mỏng, 30 test, tài liệu tốt |
| **Governance** | **4** | Không tái hiện được kết quả train; không catalog, không ownership, không retention |
| **Security** | **2** | Client tự chỉ định feature được; không auth ở đâu cả; Fernet key công khai |
| **MLOps Maturity** | **5** | Chống skew tốt hơn phần lớn hệ thống thật; nhưng không repro, không rollback model, không quality gate |

**Trung bình có trọng số: 5.0** — nền móng tốt, chưa sẵn sàng production.

### Bảng rủi ro

| # | Risk | Severity | Probability | Impact | Recommendation |
|---|---|---|---|---|---|
| 1 | **S-1** Client ghi đè feature qua `context` → tự cấp voucher | 🔴 Critical | Cao | Thất thoát ngân sách khuyến mãi trực tiếp | Bỏ `context` hoặc whitelist rỗng mặc định |
| 2 | **F-1** Redis thiếu ~7× bộ nhớ cho dữ liệu thật | 🔴 Critical | **Chắc chắn** | DAG 40 không bao giờ chạy xong trên dữ liệu đầy đủ | Nâng lên 4–5GB, hoặc nén feature vector; sửa tài liệu ngay |
| 3 | **D-1** Rerun cùng ngày ghi vào namespace đang phục vụ | 🔴 Critical | Cao (mọi lần rerun) | Vô hiệu blue-green; user nhận feature nửa cũ nửa mới; mất khả năng rollback | Version = ngày + checksum nội dung |
| 4 | **G-1** Không tái hiện được tập train | 🔴 High | Chắc chắn | Model không audit/phản biện được | Snapshot bất biến ra MinIO + log vào MLflow |
| 5 | **Q-1** Không phát hiện được dịch chuyển phân bố feature | 🔴 High | Trung bình | Model hỏng âm thầm, mọi check vẫn xanh | DQ check phân bố so với cửa sổ 7 ngày |
| 6 | **A-1** DAG 40 phụ thuộc DAG 20 chỉ bằng đồng hồ | 🔴 High | Cao khi dữ liệu tăng | Sync mart nửa vời và **báo thành công** | Airflow Dataset thay cho lịch cố định |
| 7 | **D-2** Backfill kích hoạt dữ liệu cũ lên production | 🔴 High | Trung bình | Serving phục vụ feature hàng tháng tuổi | Chặn activate nếu cũ hơn version hiện tại |
| 8 | **M-3** Cửa sổ 30 ngày neo vào `now()` thay vì `feature_ts` | 🟠 High | Cao (khi có dữ liệu nhiều ngày) | Leakage → model đẹp offline, kém online | Neo vào `feature_ts` như model PIT |
| 9 | **O-1** Push metric xoá lẫn nhau | 🟠 Medium | Chắc chắn | Panel trống, mất khả năng quan sát chính job quan trọng nhất | Đổi sang `pushadd_to_gateway` |
| 10 | **R-1** `count_keys()` quét toàn keyspace mỗi flush | 🟠 Medium | Chắc chắn | Consumer chậm, CPU Redis phí, góp phần gây D-3 | Bỏ khỏi hot path, lấy từ exporter |
| 11 | **S-2/S-4** Không auth ở Redis/Kafka/API; dùng chung root credential | 🟠 High | Cao nếu rời máy local | Xoá sạch feature store, leo thang credential | requirepass, service account riêng, auth cho API |
| 12 | **D-4** Reader DuckDB chặn writer | 🟠 Medium | Trung bình | dbt fail giữa chừng, mart dở dang | Đưa reader vào cùng pool hoặc dùng Dataset |
| 13 | **K-1** Kafka RF=1 | 🟠 High | Thấp ở local | Mất toàn bộ event chưa consume | RF≥3 khi lên production |
| 14 | **G-2** `inference_log` tăng vô hạn | 🟠 Medium | Cao theo thời gian | Hết đĩa Postgres → **Airflow chết theo** | Partition theo ngày + purge |
| 15 | **M-1/M-2** Không rollback model, không quality gate | 🟠 Medium | Cao khi có model | Model kém lên thẳng production, không lùi được | Alias + so sánh AUUC trước khi promote |
| 16 | **S-3** Fernet key công khai trong file mẫu | 🟠 Medium | Cao | Giải mã được mọi credential trong Airflow | Để trống + tự sinh lúc `init` |
| 17 | **R-2** `/decide/batch` không giới hạn kích thước | 🟡 Medium | Thấp | DoS Redis đơn luồng | `max_length=1000` |
| 18 | **Q-3/K-3** DLQ là ngõ cụt; không schema registry | 🟡 Medium | Trung bình | Mất dữ liệu âm thầm khi schema đổi | Consumer replay DLQ + schema registry |

Không liệt kê: các điểm chỉ là "chưa tối ưu" mà không có kịch bản hỏng cụ thể.

---

## "Nếu đây là hệ thống của công ty tôi, tôi sẽ yêu cầu sửa 10 vấn đề nào trước?"

Sắp theo **rủi ro × xác suất ÷ công sức**, không theo mức độ thú vị.

### Tuần 1 — chặn máu

**1. Bỏ trường `context` khỏi `/decide`** *(30 phút)*
Đang có đường cho bất kỳ ai tự cấp voucher cho mình. Không cần bàn thêm, xoá là
xong. Nếu tương lai cần tín hiệu từ client thì thiết kế lại có ký và whitelist.

**2. Sửa cấu hình Redis và sửa tài liệu** *(1 giờ)*
`REDIS_MAXMEMORY=4gb`, `mem_limit: 5g`. Quan trọng không kém: **sửa README** —
hiện tài liệu ngụ ý chạy được dữ liệu đầy đủ, thực tế không. Một con số sai
trong tài liệu làm hỏng niềm tin vào toàn bộ phần còn lại.

**3. Version = ngày + checksum nội dung** *(nửa ngày)*
Sửa `make_version()` để nhận thêm checksum. Cùng dữ liệu → cùng version (giữ
idempotency); dữ liệu đổi → namespace mới (giữ blue-green). Đây là sửa nhỏ nhất
mang lại giá trị lớn nhất trong danh sách.

**4. Chặn activate version cũ hơn version đang chạy** *(1 giờ)*
Ba dòng code, chặn đứng cả một loại sự cố production.

### Tuần 2 — chặn hỏng âm thầm

**5. DQ check phân bố feature** *(1–2 ngày)*
min/max/mean/stddev từng feature, so với cửa sổ 7 ngày, lệch quá k×σ thì cảnh
báo. Đây là loại lỗi mà **mọi check hiện tại đều không bắt được** và cũng là loại
gây thiệt hại lâu nhất trước khi bị phát hiện.

**6. Snapshot tập train bất biến** *(nửa ngày)*
Ghi parquet ra `s3://lakehouse/training_snapshots/dt=.../` trước khi train, log
đường dẫn + checksum vào MLflow. Không có cái này thì không có model governance,
chỉ có model.

**7. Thay lịch cố định bằng Airflow Dataset** *(nửa ngày)*
DAG 40 chỉ chạy khi DAG 20 phát tín hiệu. Xử lý luôn cả A-1 lẫn D-4. Việc "DAG
báo thành công trong khi dữ liệu sai" là loại lỗi tệ nhất — tệ hơn fail hẳn.

**8. Sửa `now()` thành `feature_ts` trong `feat_user_behaviour`** *(1 giờ)*
Bản local chưa lộ vì mọi `feature_ts` đều bằng `now()`. Có dữ liệu nhiều ngày là
thành leakage thật. Sửa lúc rẻ, đừng đợi lúc phải giải thích với business vì sao
model offline 0.85 mà online 0.61.

### Tuần 3 — vá quan sát và vận hành

**9. Sửa `push_to_gateway` → `pushadd_to_gateway` + alert DAG ngừng chạy** *(2 giờ)*
Hiện các panel quan trọng nhất của dashboard feature-sync đang trống mà không ai
biết, và một DAG bị pause thì im lặng suốt 26 giờ. Công cụ giám sát mà tự nó sai
thì tệ hơn không có, vì nó tạo cảm giác an toàn giả.

**10. Retention cho `ops.inference_log` + backup Postgres** *(1 ngày)*
Partition theo ngày, purge sau 30 ngày, `pg_dump` hằng ngày ra MinIO. Postgres
đang giữ metadata Airflow **và** MLflow registry — mất nó là mất luôn mọi model
đã đăng ký. Hiện không có bản sao nào.

---

### Ba việc cố ý **không** đưa vào top 10

- **Kafka RF=3 / HA**: đúng nhưng vô nghĩa trên một máy. Xử lý khi lên hạ tầng thật.
- **Auth toàn hệ thống**: cần thiết, nhưng nếu stack chỉ chạy localhost thì rủi ro
  thực tế thấp hơn 10 việc ở trên. Làm ngay khi có ý định đưa lên mạng chung.
- **Thay DuckDB**: chưa tới ngưỡng. Theo dõi thời gian chạy DAG 20 làm chỉ báo,
  đừng đổi sớm chỉ vì lo xa.

### Ghi nhận

Không phải chỗ nào cũng có vấn đề. Ba thứ dưới đây tốt hơn mức trung bình của các
hệ thống production tôi từng review:

1. **`feature_spec.yml` là contract có cưỡng chế** — hầu hết hệ thống chỉ có tài
   liệu mô tả feature rồi trôi khỏi thực tế sau ba tháng. Ở đây pipeline fail nếu
   lệch.
2. **Point-in-time join làm đúng, kể cả phần cửa sổ trượt khớp tới từng ô 5 phút**
   giữa Redis và dbt. Đây là chỗ đa số làm sai và không bao giờ biết.
3. **Validate trước khi activate** — sync hỏng không chạm tới người dùng. Nhiều
   hệ thống thật ghi thẳng rồi cầu nguyện.

Nền móng đúng. Vấn đề nằm ở vận hành và quy mô, không ở tư duy thiết kế — đó là
loại vấn đề sửa được.
