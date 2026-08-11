# LZD Uplift — Production Data Pipeline (offline training + online serving)

Hệ thống feature store hai nhánh cho bài toán **phát voucher bằng uplift model**,
mô phỏng đúng pattern production của Lazada trên Docker local.

| Nhánh | Đường đi | SLA |
|---|---|---|
| **Offline (training)** | MinIO (parquet) → DuckDB + dbt → `marts.training_dataset` → train → MLflow | hàng ngày |
| **Online (serving)** | DuckDB → **sync job** → Redis → FastAPI đọc merged features | lookup < 10ms, decision < 100ms |
| **Realtime overlay** | App → Kafka → stream-consumer → Redis (`rt:u:*`, TTL 1h) | < 30s |

> DuckDB đóng vai **BigQuery** (nguồn sự thật), Redis đóng vai **online store** (bản sao phục vụ đọc).

---

## 1. Kiến trúc

```
 Web/Mobile ─► Kafka ─► stream-consumer ─┬─► MinIO  raw/app_events/*.parquet
                                         └─► Redis  rt:u:{user}       (overlay, TTL 1h)
                                                        ▲
 data/*.csv ─► MinIO raw/user_snapshot ──► DuckDB ──► dbt ──► marts.feat_user_serving
                                                        │              │
                                                        │       [DAG 40 sync, versioned]
                                                        │              ▼
                                                        │      Redis fs:{v}:u:{user}
                                                        │              │
                                              marts.training_dataset   │
                                                        │              ▼
                                                     train ──► MLflow ──► FastAPI /decide
```

Sơ đồ gốc: [docs/img/architecture.png](docs/img/architecture.png) ·
Giải thích từng bước: [docs/DATA_FLOW.md](docs/DATA_FLOW.md)

> **Mới vào dự án?** Đọc [docs/KIEN_THUC.md](docs/KIEN_THUC.md) trước — giải
> thích từng công nghệ dùng để làm gì, nguyên tắc thiết kế đằng sau, lộ trình
> học 4 tuần và 12 câu tự kiểm tra.

---

## 2. Chạy lần đầu

> **Chưa `docker compose up` gì cả** — code đã sẵn sàng, bạn chủ động bật khi muốn.

```powershell
# 0) chuẩn bị (tạo .env, thư mục, build image)
.\scripts\stack.ps1 init

# 1) bật hạ tầng + observability
.\scripts\stack.ps1 up

# 2) đợi ~1-2 phút rồi kiểm tra
.\scripts\stack.ps1 health

# 3) bật streaming + serving + mlflow
.\scripts\stack.ps1 up-all
```

Bash/WSL: `make init && make up && make up-all`

### Thứ tự chạy pipeline (trong Airflow UI — http://localhost:8080)

| # | DAG | Làm gì | Khi nào |
|---|---|---|---|
| 1 | `00_bootstrap_lake` | Nạp CSV → MinIO parquet | trigger tay, **1 lần** (đặt `row_limit=200000` để chạy thử nhanh) |
| 2 | `20_build_features_dbt` | dbt: raw → cleaned → business-ready | trigger tay lần đầu, sau đó 01:00 hằng ngày |
| 3 | `40_sync_features_to_redis` | Đẩy feature lên Redis (versioned) | 01:30 hằng ngày |
| 4 | `50_data_quality` | Kiểm tra freshness/consistency | mỗi 30 phút |
| — | `10_ingest_stream_to_lake` | Giám sát luồng stream | mỗi giờ |
| — | `30_train_uplift_model` | **Khung cho teammate** | đang pause |
| — | `99_ops_toolbox` | Rollback / chaos / resync | trigger tay |

Kiểm tra sau khi sync xong:

```powershell
curl http://localhost:8000/store/info
curl http://localhost:8000/features/U0000123     # xem đúng feature model sẽ nhận
python scripts/load_test.py --rps 20 --duration 60
```

---

## 3. Giao diện quan sát

| UI | URL | Dùng để |
|---|---|---|
| **Grafana** | http://localhost:3000 (admin/admin) | 4 dashboard + log Loki, xem mục 4 |
| **Airflow** | http://localhost:8080 (admin/admin) | DAG, task log, Gantt, retry |
| **Kafka UI** | http://localhost:8082 | topic, message, consumer lag, DLQ |
| **MinIO** | http://localhost:9001 (minioadmin/minioadmin123) | duyệt file parquet trong lake |
| **MLflow** | http://localhost:5000 | experiment, metric, model registry |
| **Prometheus** | http://localhost:9090 | metric thô + alert rules |
| **RedisInsight** | http://localhost:5540 | duyệt key `fs:*`, `rt:*` bằng tay |
| **Swagger API** | http://localhost:8000/docs | thử `/decide`, `/features/{id}` |

---

## 4. Dashboard Grafana (provision sẵn, không cần import tay)

| Dashboard | Trả lời câu hỏi |
|---|---|
| `00 · End-to-End Pipeline Overview` | Dữ liệu đang chạy tới đâu? Có tắc ở đâu không? |
| `01 · Feature Sync (DuckDB → Redis)` | Sync tới shard nào, offline vs online có lệch không, version nào đang phục vụ |
| `02 · Streaming (Kafka → Lake → Redis)` | Lag theo partition, DLQ, độ trễ đầu-cuối |
| `03 · Airflow & Infra` | Task pass/fail, pool slot, RAM Redis, connection Postgres |
| `04 · Data Quality & Skew` | Null rate, freshness, offline mean vs online mean |

Log tập trung: Grafana → **Explore** → datasource **Loki**

```logql
{component="stream-consumer"} | json | level="ERROR"
{job="airflow-tasks", dag_id="40_sync_features_to_redis"}
{job="docker"} | json | event="shard_written"
{job="docker"} | json | feature_version="v20260805"
```

---

## 5. Cấu trúc thư mục

```
config/
  features/feature_spec.yml     ⭐ HỢP ĐỒNG feature (dbt + sync + API đều đọc file này)
  grafana/  prometheus/  loki/  promtail/  statsd/  postgres_exporter/
dbt/models/
  staging/                      CLEANED  (chuẩn hoá + dedup)
  marts/                        BUSINESS READY (feat_user_serving, training_dataset)
src/lzd_pipeline/
  common/                       config, logging JSON, metrics, clients, audit Postgres
  features/spec.py              đọc & validate feature spec
  features/online_store.py      ⭐ mọi thao tác Redis (version, shard, merge, GC)
  features/offline_store.py     mọi truy vấn DuckDB (shard, checksum, null rate)
  features/sync.py              ⭐ sync engine: prepare → shard → validate → activate → gc
  ingestion/                    event_producer, stream_consumer, seed_loader
  training/                     dataset.py (xong) + train.py (KHUNG)
  serving/                      app.py (xong) + model_loader.py (KHUNG)
airflow/dags/                   6 DAG + callbacks
sql/postgres/                   schema ops.* (audit, dq_result, inference_log)
tests/                          unit test chạy không cần Docker: pytest tests/ -v
docs/                           DATA_FLOW.md · RUNBOOK.md · OBSERVABILITY.md
```

---

## 6. Phần để trống cho teammate

Tất cả đánh dấu `TODO(model)`, đường ống xung quanh đã nối sẵn:

| File | Cần điền |
|---|---|
| `src/lzd_pipeline/training/train.py` | `build_model()`, `fit_model()`, `evaluate()` — S/T/X-learner hoặc DESCN theo [paper](docs/2207.09920v3.pdf) |
| `src/lzd_pipeline/serving/model_loader.py` | `MlflowUpliftModel.load()` và `.predict()` |
| `airflow/dags/dag_30_train_uplift_model.py` | `promote_model()` (đặt alias Production) |

Trước khi có model thật, API vẫn chạy được bằng `StubModel` (`model_version="stub-0"`)
→ đo được latency Redis, cache hit, QPS. Stub **không được dùng ra quyết định thật**.

`training/dataset.py` đã trả về đúng bộ feature mà lúc serve API sẽ đọc từ Redis
(cùng `feature_spec.yml`) → không lo training/serving skew.

---

## 7. Bốn tính chất của job sync (yêu cầu đề bài)

| Tính chất | Cài đặt ở đâu |
|---|---|
| **Consistency** | `validate_sync()` so mẫu Redis vs DuckDB + checksum + row count **trước khi** đổi `active_version`; DAG 50 kiểm tra lại định kỳ |
| **Freshness** | version đặt tên theo ngày logic; `lzd_feature_store_age_seconds` + alert > 26h |
| **Idempotency** | version deterministic (`v20260805`), ghi bằng `HSET`, đánh dấu shard đã xong trong Redis SET + Postgres |
| **Fault tolerance** | chia 32 shard độc lập, retry từng shard, shard `DONE` được bỏ qua khi rerun, `active_version` chỉ đổi ở bước cuối nên lỗi giữa chừng **không ảnh hưởng serving** |

Diễn tập: `99_ops_toolbox` → `action=chaos_partial_fail` → chạy lại DAG 40 →
xem log chỉ ghi lại shard hỏng. Chi tiết: [docs/RUNBOOK.md](docs/RUNBOOK.md)

---

## 8. Tình trạng production-readiness

### Đã tối ưu

| Hạng mục | Trước | Sau |
|---|---|---|
| Build context | **549 MB** (CSV bị gửi sang daemon mỗi lần build × 3 image) | **0.27 MB** — `.dockerignore` |
| `python-service` | 1 stage, giữ cả gcc + pip cache | multi-stage, chỉ copy venv, **non-root**, có HEALTHCHECK |
| `airflow` | cài `build-essential` + `libpq-dev` rồi giữ lại | bỏ hẳn (mọi package đều có wheel dựng sẵn) — nhẹ ~350 MB |
| `mlflow` | như trên | multi-stage + non-root |
| Rebuild sau khi sửa 1 dòng requirements | cài lại từ đầu | BuildKit cache mount → vài giây |
| Xung đột dependency | phát hiện lúc DAG chạy 1h30 sáng | `pip check` chạy **lúc build** |
| RAM | không giới hạn — Kafka JVM tự lấy 1/4 RAM máy | `mem_limit` + `KAFKA_HEAP_OPTS`/`JAVA_OPTS` cho 21/24 service |
| Tắt container | consumer bị `SIGKILL` sau 10s, cắt ngang lúc ghi parquet | `stop_grace_period: 90s` + `init: true` |
| Healthcheck | 3 service | 10 service |

### Năm lỗi thật đã sửa

1. **`/decide` mở kết nối Postgres mới mỗi request** để ghi `inference_log`
   (+5–20ms vào đường có SLA 100ms, trong khi ngân sách Redis chỉ 10ms).
   → [`serving/inference_logger.py`](src/lzd_pipeline/serving/inference_logger.py):
   queue + worker thread ghi batch, hàng đợi đầy thì vứt log chứ không làm chậm request.
2. **Kết quả `dbt test` không lên Grafana** — docstring nói có nhưng chưa implement.
   → task `publish_dbt_results` (chạy cả khi test fail) parse `run_results.json`
   → `ops.dq_result` + metric.
3. **`rt_events_1h` không phải cửa sổ 1 giờ.** TTL bị đẩy lùi sau mỗi lần ghi
   → user hoạt động liên tục thì counter cộng dồn cả ngày, trong khi dbt tính
   đúng 1 giờ → **training/serving skew ở nhánh realtime**.
   → cửa sổ trượt 12 ô × 5 phút, ghi/prune bằng một script Lua atomic;
   `feat_user_realtime_pit.sql` làm tròn ô y hệt. 5 test regression.
4. **`/decide` tốn 2 round-trip Redis** (`GET active_version` rồi mới `HGETALL`),
   và có khe hở: giữa 2 lệnh, version có thể bị activate + GC → cache miss oan.
   → Lua `read_for_serving()`: 1 RTT, atomic.
5. **`duckdb_conn()` mặc định mở chế độ GHI** → một query ad-hoc là khoá cả file
   và giết dbt đang chạy. → mặc định **read-only**, muốn ghi phải gọi
   `duckdb_writer()`; gỡ mount `lakehouse` khỏi `inference-api`.

### Còn thiếu nếu lên production thật

Alertmanager (alert chưa gửi đi đâu), tracing, HA, TLS, secret manager, backup Postgres.
Chi tiết + cách bổ sung: [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md#chưa-có-biết-trước-để-khỏi-bất-ngờ)

---

## 9. Test nhanh (không cần Docker)

```powershell
pip install -r requirements-dev.txt
$env:PYTHONPATH="src"
python -m pytest tests/ -v
```

24 test đã chạy xanh, phủ: expand `f0..f82` từ spec, key layout, merge
batch↔realtime↔default, version deterministic, ghi lại không nhân đôi
(idempotency), shard marker chặn ghi trùng, atomic swap + rollback,
dữ liệu version mới **không lộ ra** trước khi activate, TTL overlay, GC giữ
version active, validate event → DLQ.
