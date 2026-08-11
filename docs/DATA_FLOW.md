# Dữ liệu đi như thế nào — đi theo từng bước

Tài liệu này bám đúng thứ tự dữ liệu chạy trong hệ thống. Mỗi bước có:
**làm gì → file code nào → nhìn ở đâu để kiểm chứng**.

---

## Bước 0 · Dataset gốc

`data/full_trainset.csv` (476 MB) và `full_testset.csv` — dataset DESCN
(Lazada voucher distribution, [paper](2207.09920v3.pdf)).

```
data_id, label, is_treat, f0, f1, ..., f82
train_0, 0,     0,        0,  365, ...
```

- `label` = 1 nếu user có conversion
- `is_treat` = 1 nếu user **được phát voucher** (treatment)
- `f0..f82` = feature ẩn danh

Không có `user_id` → ta sinh ra: `train_123` → `U0000123`. Nhờ vậy user trong
dataset và user trong stream event **là cùng một tập** → feature batch và
feature realtime của cùng một người gặp nhau được trên Redis.

---

## Bước 1 · CSV → Data Lake (MinIO)

**DAG `00_bootstrap_lake`** → `src/lzd_pipeline/ingestion/seed_loader.py`

DuckDB đọc CSV rồi `COPY ... TO 's3://...' (FORMAT PARQUET)` — không nạp hết
vào RAM. Kết quả:

```
s3://lakehouse/raw/user_snapshot/dt=2026-08-05/train.parquet
s3://lakehouse/raw/user_snapshot/dt=2026-08-05/test.parquet
```

**Kiểm chứng:** MinIO Console → bucket `lakehouse` → duyệt thư mục.

> Trong production thật, bước này không tồn tại — bảng nguồn đã nằm sẵn trên
> BigQuery do team khác ghi. Ta chỉ đọc.

---

## Bước 2 · App → Kafka (luồng realtime)

**`src/lzd_pipeline/ingestion/event_producer.py`** (container `event-producer`)

Sinh event theo **phiên**: `app_open → page_view → search → add_to_cart →
voucher_claim → order`, phân bố lệch 80/20 giống thực tế.

```json
{"event_id":"...", "user_id":"U0000123", "event_type":"add_to_cart",
 "event_ts":1780000000.5, "session_id":"...", "platform":"android",
 "price":24.5, "quantity":2}
```

Hai chi tiết quan trọng:

- **Key = `user_id`** → mọi event của một user vào cùng partition → đảm bảo thứ tự.
- `PRODUCER_CORRUPT_RATE=0.01` → 1% event bị làm hỏng có chủ đích, để bạn thấy
  DLQ và alert hoạt động thật.

**Kiểm chứng:** Kafka UI → topic `app.user.events.v1` → tab *Messages*.

---

## Bước 3 · Kafka → Lake + Redis overlay

**`src/lzd_pipeline/ingestion/stream_consumer.py`** (container `stream-consumer`)

Một micro-batch (2000 record hoặc 30 giây, cái nào đến trước):

```
poll → validate → ghi parquet lên MinIO → cập nhật Redis rt:u:* → COMMIT OFFSET
                                                                   └─ bước cuối
```

**Vì sao commit cuối cùng:** nếu container chết giữa chừng, offset chưa commit
→ batch đó được đọc lại → **at-least-once**, không mất dữ liệu.
Đổi lại có thể trùng, xử lý ở bước sau:

| Nơi | Cách khử trùng |
|---|---|
| Lake | `stg_app_events.sql` dedup theo `event_id` |
| Redis overlay | counter có thể cộng dư → overlay chỉ là **tín hiệu gần đúng**; con số chính xác lấy từ batch feature (nguồn sự thật) |

Event hỏng schema → topic `app.user.events.dlq.v1` kèm header `reason`.

### `rt_events_1h` phải thật sự là "1 giờ vừa rồi"

Cách làm ngây thơ — `HINCRBY` vào một field rồi `EXPIRE` lại key sau mỗi lần
ghi — **sai**: với user hoạt động liên tục, TTL bị đẩy lùi mãi nên key không
bao giờ hết hạn và counter cộng dồn cả ngày. Lúc train dbt tính đúng 1 giờ
(thấy 12), lúc serve đọc counter tích luỹ (thấy 400). Đó chính là
training/serving skew, chỉ khác là nằm ở nhánh realtime.

Cách đang dùng: chia giờ thành **12 ô 5 phút**, mỗi ô là một field riêng
`rt_events_1h|<mốc ô>`. Ghi = cộng vào ô hiện tại + xoá ô đã rớt khỏi cửa sổ
(cùng một script Lua, atomic). Đọc = cộng các ô còn trong cửa sổ.

`feat_user_realtime_pit.sql` làm tròn `feature_ts` về đầu ô 5 phút **y hệt**
`rt_bucket_start()` bên Python — nếu dùng `feature_ts - interval '1 hour'` thì
hai bên vẫn lệch nhau tới 5 phút dữ liệu.

> Đây cũng là lý do batch và realtime **không thể** cộng trùng: `merge()` ghi
> đè theo tên field chứ không cộng, và `hist_order_cnt_30d` (batch, 30 ngày)
> với `rt_order_1h` (realtime, 1 giờ) là hai cột khác nhau. Model nhận cả hai
> như hai feature độc lập — và nhận đúng như vậy cả lúc train.

**Kiểm chứng:**
- Kafka UI → consumer group `feature-stream-consumer` → cột *Lag*
- MinIO → `lakehouse/raw/app_events/dt=.../hour=.../part-*.parquet`
- `docker compose exec redis redis-cli HGETALL rt:u:U0000123`
- Grafana dashboard **02**

---

## Bước 4 · Lake → DuckDB → dbt (RAW → CLEANED → BUSINESS READY)

**DAG `20_build_features_dbt`** → `dbt/models/`

```
source raw.user_snapshot ──► stg_user_snapshot ──┐
source raw.app_events    ──► stg_app_events ──┬──┼─► feat_user_behaviour ──┐
                                              │  │                          ├─► feat_user_serving   ⭐ (=> Redis)
                                              └──┴─► feat_user_realtime_pit ┘            │
                                                                                          └─► training_dataset
```

| Model | Vai trò |
|---|---|
| `stg_user_snapshot` | ép kiểu, khử trùng theo `(user_id, dt)` |
| `stg_app_events` | khử trùng theo `event_id` — biến at-least-once thành exactly-once |
| `feat_user_behaviour` | feature 30 ngày: `hist_order_cnt_30d`, `hist_gmv_30d`, ... |
| `feat_user_serving` | **BUSINESS READY** — chính là bảng được sync lên Redis |
| `feat_user_realtime_pit` | feature realtime tính **point-in-time** (mục dưới) |
| `training_dataset` | serving features + realtime PIT + `label` + `is_treat` |

### Point-in-time — chống rò rỉ nhãn

Lúc **serve**, `rt_events_1h` = "1 giờ trước thời điểm request".
Nếu lúc **train** ta tính trên toàn bộ lịch sử, model học trên phân bố khác hẳn.

`feat_user_realtime_pit.sql` join event với điều kiện:

```sql
and e.event_ts <  s.feature_ts                               -- chỉ event TRƯỚC snapshot
and e.event_ts >= s.feature_ts - interval '3600 seconds'      -- trong cửa sổ trượt
```

Công thức trong SQL này **phải khớp** với `EVENT_TO_COUNTER` trong
`stream_consumer.py` và `realtime_features` trong `feature_spec.yml`.

### Hàng rào chống skew

Task `assert_spec_contract` so cột của mart với `feature_spec.yml`.
Thiếu cột → **fail ngay**, không để sync đẩy lên Redis một bộ feature khác với
lúc train.

**Kiểm chứng:** Airflow → task log của `dbt_run`; Grafana dashboard **04**.

---

## Bước 5 · DuckDB → Redis ⭐ (trái tim hệ thống)

**DAG `40_sync_features_to_redis`** → `src/lzd_pipeline/features/sync.py`

```
prepare_sync            khoá row_count + checksum, mở phiên version v20260805
      ▼
sync_shard[0..31]       32 task song song, mỗi shard = hash(user_id) % 32
      ▼                 shard đã DONE → bỏ qua (idempotent)
validate_sync           so mẫu Redis vs DuckDB + row count + checksum
      ▼                 FAIL → dừng, active_version KHÔNG đổi
activate_version        SET fs:meta:active_version = v20260805   ← 1 lệnh, atomic
      ▼
cleanup_old_versions    xoá version quá cũ (giữ 2 bản)
```

### Key layout trên Redis

| Key | Kiểu | Ý nghĩa |
|---|---|---|
| `fs:{version}:u:{user_id}` | HASH | feature batch của 1 user |
| `rt:u:{user_id}` | HASH | overlay realtime — field dạng `rt_events_1h\|<mốc ô 5 phút>` |
| `fs:meta:active_version` | STRING | **con trỏ** tới version đang phục vụ |
| `fs:meta:{v}:status` | HASH | trạng thái + số liệu của lần sync |
| `fs:meta:{v}:shards` | SET | shard đã ghi xong (đánh dấu idempotency) |
| `fs:meta:versions` | ZSET | version → timestamp, dùng để GC |

### Vì sao phải có `active_version`

Ghi đè trực tiếp lên key đang phục vụ → giữa lúc sync, một phần user dùng
feature mới, một phần dùng cũ → **serving không nhất quán**.

Ghi vào namespace mới rồi `SET` một key con trỏ → toàn bộ traffic chuyển sang
phiên bản mới trong **một lệnh atomic**. Lỗi thì trỏ ngược lại — rollback tức
thì, không cần ghi lại dữ liệu.

**Kiểm chứng:**
- Grafana dashboard **01** (tiến độ shard, bảng audit, báo cáo validate)
- `SELECT * FROM ops.feature_sync_shard WHERE feature_version='v20260805'`
- `redis-cli GET fs:meta:active_version`

---

## Bước 6 · Redis → FastAPI → quyết định

**`src/lzd_pipeline/serving/app.py`** — `POST /decide {"user_id": "U0000123"}`

```
EVALSHA read_merged  ──►  GET active_version
                          HGETALL fs:{v}:u:{uid}     1 RTT, atomic, ~1-3ms
                          HGETALL rt:u:{uid}
gộp ô 5 phút → giá trị cửa sổ 1h
merge theo feature_spec.yml, thiếu → default
model.predict_uplift()                              ← TODO teammate
score >= 0.02 ? SEND_VOUCHER : NO_VOUCHER
```

**Vì sao phải dùng Lua chứ không gọi 2 lệnh.** Bản đầu làm 2 bước: `GET
active_version` rồi mới `HGETALL`. Vừa tốn gấp đôi RTT, vừa có khe hở — giữa 2
bước, DAG sync có thể activate version mới rồi GC version cũ, request đang dở
đọc trúng namespace vừa bị xoá và báo cache miss oan. Lua chạy nguyên khối trên
Redis nên không chèn được gì vào giữa.

Thứ tự ưu tiên khi merge: **context client > realtime overlay > batch > default**.

Mỗi request ghi 1 dòng `ops.inference_log` (feature_version + model_version +
latency + cache_hit) → về sau đo được skew thật giữa lúc train và lúc serve.

**Kiểm chứng:**
```powershell
curl http://localhost:8000/features/U0000123      # xem đúng feature model nhận
python scripts/load_test.py --rps 20 --duration 60
```
Grafana dashboard **00**, hàng *SERVING*.

---

## Bước 7 · Training (khung cho teammate)

**DAG `30_train_uplift_model`** → `src/lzd_pipeline/training/`

`dataset.py` đã xong: `load_training_frame()` trả về đúng bộ feature theo
`feature_spec.yml` — **cùng thứ tự** với lúc serve.

`train.py` cần điền `build_model()` / `fit_model()` / `evaluate()`.
Đường ống MLflow (log param, metric, `feature_list.json`, đăng ký model) đã nối sẵn.

---

## Bản đồ metric → dashboard

| Nguồn | Cách vào Prometheus | Dashboard |
|---|---|---|
| producer / consumer / API | tự expose `/metrics` | 00, 02 |
| Task Airflow (batch) | push lên Pushgateway | 00, 01, 04 |
| Airflow nội bộ | statsd → statsd-exporter | 03 |
| Redis / Postgres / Kafka / MinIO | exporter riêng | 01, 02, 03 |
| Bảng `ops.*` | postgres-exporter custom queries + datasource Postgres | 00, 01, 04 |
| Log mọi container | Promtail → Loki | tất cả (panel Logs) |
