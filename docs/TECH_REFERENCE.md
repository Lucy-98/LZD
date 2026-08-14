# Tech Reference — LZD Uplift Feature Platform

> **Loại tài liệu:** code-level technical reference. Tài liệu này mô tả **những gì
> đang có trong code**, không mô tả thiết kế mong muốn.
>
> **Quan hệ với các doc khác:** `PIPELINE_ARCHITECTURE.md` / `RECONSTRUCTION_SPEC.md`
> nói *tại sao* và *phải như thế nào*. Tài liệu này nói *code nằm ở đâu, nhận gì,
> trả gì, và ràng buộc nào được enforce ở tầng nào*.
>
> Mọi phát biểu trong tài liệu này đọc được từ source. Nếu code đổi mà tài liệu
> không đổi thì tài liệu sai — không phải code sai.
>
> **Trạng thái xác minh (2026-08-14):** `python -m pytest tests/ -q` →
> `210 passed`. Xem §11.

---

## Mục lục

1. [Mental model 4 tầng](#1-mental-model-4-tầng)
2. [Bản đồ repository](#2-bản-đồ-repository)
3. [Runtime topology](#3-runtime-topology)
4. [Configuration reference](#4-configuration-reference)
5. [Python package reference](#5-python-package-reference)
6. [Reconstruction engine — internals](#6-reconstruction-engine--internals)
7. [dbt layer](#7-dbt-layer)
8. [Data contracts](#8-data-contracts)
9. [Orchestration — Airflow DAGs](#9-orchestration--airflow-dags)
10. [CLI & entrypoints](#10-cli--entrypoints)
11. [Test map & trạng thái xác minh](#11-test-map--trạng-thái-xác-minh)
12. [Known gaps](#12-known-gaps)

---

## 1. Mental model 4 tầng

```text
Layer 1  BUSINESS / OPERATIONAL     synthetic Lazada domain      docs/ERD.md
Layer 2  RAW / DOMAIN EVENT         Kafka v1 · raw/events_v2     ingestion/ · reconstruction/
Layer 3  FEATURE ENGINEERING        dbt marts · f0..f82          dbt/ · features/
Layer 4  MODEL / SERVING            uplift score · decision      training/ · serving/
```

Hai luồng dữ liệu **độc lập** chạy trên cùng stack:

```text
FORWARD (production path)
    CSV snapshot ──▶ MinIO raw ──▶ dbt marts ──▶ Redis ──▶ inference API
    Kafka v1     ──▶ MinIO raw ──▶ Redis rt overlay

RECONSTRUCTION (contract path, dry-run)
    36 selected features ──▶ solver ──▶ Track A witness events
                                    ──▶ CHÍNH SQL dbt ──▶ F'
                                    ──▶ Gate A..F ──▶ CustomerState(T0)
                                    ──▶ Track B future events
```

Điểm mấu chốt của toàn bộ thiết kế reconstruction: **forward pass không được
viết lại bằng Python**. `reconstruction/runner.py` render Jinja và thực thi
**chính file `.sql` của dbt** trong DuckDB. Nếu so đầu ra của một hàm với chính
nó thì Gate A không kiểm chứng gì cả (tautology, `RECONSTRUCTION_SPEC.md` §12).

---

## 2. Bản đồ repository

| Đường dẫn | Nội dung | Ghi chú |
|---|---|---|
| [airflow/dags/](../airflow/dags/) | 8 DAG + `lzd_utils/callbacks.py` | §9 |
| [config/features/](../config/features/) | `feature_spec.yml`, `fs_2026_08_v1.yaml`, `business_aliases.yml`, `feature_semantics_2026_08.yaml` | §4.2 |
| [config/reconstruction/runtime.yml](../config/reconstruction/runtime.yml) | runtime config của reconstruction | §4.3 |
| [config/grafana/](../config/grafana/), [config/prometheus/](../config/prometheus/), [config/loki/](../config/loki/), [config/promtail/](../config/promtail/), [config/statsd/](../config/statsd/) | observability provisioning | 5 dashboard |
| [dbt/](../dbt/) | 3 staging + 6 marts model, `profiles.yml` (DuckDB) | §7 |
| [docker/](../docker/) | Dockerfile cho airflow / python-service / mlflow | |
| [scripts/](../scripts/) | `stack.ps1` (Windows), `init_kafka.sh`, `init_minio.sh`, `load_test.py` | §10 |
| [sql/postgres/](../sql/postgres/) | `ops.*` audit schema, `biz.*` reconstruction schema | §8.5 |
| [src/lzd_pipeline/](../src/lzd_pipeline/) | ~6.5k dòng Python, 5 subpackage | §5 |
| [tests/](../tests/) | 210 test, 13 file | §11 |
| [data/](../data/) | `full_trainset.csv` (926,669 dòng), `full_testset.csv` | committed |
| `.tmp/` | output Track A batch, **git-ignored** | có thể rất lớn |

---

## 3. Runtime topology

`docker-compose.yml` khai báo 21 service, chọn bằng **compose profile**:

| Profile | Service |
|---|---|
| `core` | `postgres` · `redis` · `minio` · `minio-init` · `kafka` · `kafka-init` · `airflow-init` · `airflow-webserver` · `airflow-scheduler` |
| `obs` | `prometheus` · `pushgateway` · `statsd-exporter` · `redis-exporter` · `postgres-exporter` · `kafka-exporter` · `loki` · `promtail` · `grafana` · `redisinsight` · `kafka-ui` |
| `stream` | `event-producer` · `stream-consumer` |
| `serving` | `inference-api` |
| `ml` | `mlflow` |
| `all` | tất cả |

Reconstruction dry-run **không cần** profile nào — nó chạy hoàn toàn trên
DuckDB in-memory ở host (§10).

Cổng và UI: xem [README §7](../README.md).

**Ba image tự build:** `lzd/airflow:2.10.5`, `lzd/python-service:latest`,
`lzd/mlflow:2.16.2`. `python-service` là image dùng chung cho
`event-producer` / `stream-consumer` / `inference-api` — khác nhau ở command,
không khác nhau ở image.

---

## 4. Configuration reference

### 4.1 Environment → `Settings`

[`common/config.py`](../src/lzd_pipeline/common/config.py) là **nơi duy nhất**
đọc `os.environ`. Mọi module khác gọi `get_settings()` (cached, `lru_cache(1)`).
Không hardcode host/port ở chỗ nào khác.

| Env var | Default | Đi vào |
|---|---|---|
| `SERVICE_NAME` | `lzd-pipeline` | `Settings.service_name` |
| `LOG_LEVEL` | `INFO` | `Settings.log_level` |
| `DUCKDB_PATH` | `/opt/lakehouse/warehouse.duckdb` | `Settings.duckdb_path` |
| `LAKE_ROOT` | `s3://lakehouse` | `Settings.lake_root` |
| `PUSHGATEWAY_URL` | `http://pushgateway:9091` | `Settings.pushgateway_url` |
| `METRICS_PORT` | `9105` | `Settings.metrics_port` |
| `MLFLOW_TRACKING_URI` | `http://mlflow:5000` | `Settings.mlflow_tracking_uri` |
| `MLFLOW_EXPERIMENT` | `uplift-descn` | `Settings.mlflow_experiment` |
| `UPLIFT_DECISION_THRESHOLD` | `0.02` | `Settings.uplift_threshold` |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | `KafkaConfig` |
| `KAFKA_TOPIC_EVENTS` | `app.user.events.v1` | `KafkaConfig` |
| `KAFKA_TOPIC_DLQ` | `app.user.events.dlq.v1` | `KafkaConfig` |
| `KAFKA_CONSUMER_GROUP` | `feature-stream-consumer` | `KafkaConfig` |
| `REDIS_HOST` / `REDIS_PORT` / `REDIS_DB` | `redis` / `6379` / `0` | `RedisConfig` |
| `MINIO_ENDPOINT` | `http://minio:9000` | `MinioConfig` |
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | `minioadmin` / `minioadmin123` | `MinioConfig` |
| `MINIO_BUCKET_LAKE` / `MINIO_BUCKET_MODELS` | `lakehouse` / `models` | `MinioConfig` |
| `PG_HOST` / `PG_PORT` / `PG_USER` / `PG_PASSWORD` / `PG_PIPELINE_DB` | `postgres` / `5432` / `lzd` / `lzd_secret` / `pipeline` | `PostgresConfig` |
| `FEATURE_SPEC_PATH` | `/opt/project/config/features/feature_spec.yml` | `FeatureStoreConfig` |
| `FEATURE_SYNC_SHARDS` | `32` | `FeatureStoreConfig.shards` |
| `FEATURE_SYNC_BATCH_SIZE` | `1000` | pipeline size khi HSET |
| `FEATURE_VERSIONS_TO_KEEP` | `2` | GC version cũ |
| `FEATURE_STALE_TTL_SECONDS` | `86400` | TTL khi retire version |
| `REALTIME_OVERLAY_TTL_SECONDS` | `3600` | TTL key `rt:u:*` |
| `FEATURE_VALIDATION_SAMPLE` | `500` | số user đối chiếu Redis↔DuckDB |

Mọi dataclass config đều `frozen=True`; `_env_int` / `_env_float` nuốt giá trị
sai kiểu và trả về default thay vì crash lúc import.

### 4.2 Feature configuration — 4 file, 4 vai trò khác nhau

Đây là chỗ dễ nhầm nhất trong repo. Bốn file, **không** thay thế lẫn nhau:

| File | Ai đọc | Định nghĩa |
|---|---|---|
| `feature_spec.yml` | `features/spec.py` → sync job, inference API, dbt test | Redis key layout, dtype, default, batch vs realtime |
| `fs_2026_08_v1.yaml` | `reconstruction/feature_set.py` → solver | **36-column reconstruction scope**, tier, regime, tolerance, source attribute |
| `business_aliases.yml` | `features/business_aliases.py` → report/snapshot | tên nghiệp vụ synthetic cho `f*` (không phải Lazada fact) |
| `feature_semantics_2026_08.yaml` | *không module nào trong solver path* | câu hỏi ngỏ về semantic |

`feature_set.py` **cố ý không đọc** `feature_semantics_2026_08.yaml`
(`RECONSTRUCTION_SPEC.md` §13.2): semantic của `f9`/`f16` chưa chốt, và điều đó
không được phép chặn solver.

`load_feature_set()` chạy `_validate()` ngay lúc load và fail cứng nếu:
tier chồng lấn · regime không phủ hết 36 cột · `selected` của source attribute
không nằm trong T2 · cột `intermediate_only` lọt vào target · `len(columns) != 36`.

### 4.3 `config/reconstruction/runtime.yml`

```yaml
version: reconstruction_runtime_v1
semantic_status: UNIDENTIFIED     # trạng thái NHẬN THỨC
semantic_branch: H1               # KỊCH BẢN vận hành
reference_ts: "2026-08-01T23:59:59+00:00"
future_days: 2
generation_seed: 42
constraint_model_version: constraints_v1
encoding_version: encoding_2026_08_v1
objective_version: objective_v1
selection_policy_version: seeded_optimal_pool_v1
solver_version: prototype_v1
behaviour_policy_version: rule_v1
```

`semantic_status` và `semantic_branch` là **hai loại khác nhau** và
`SolverConfig.__post_init__` enforce quan hệ giữa chúng (I-6): `H1_SUPPORTED`
bắt buộc `branch == "H1"`; `UNIDENTIFIED` chấp nhận mọi branch. `branch == "H1"`
**không bao giờ** có nghĩa "f30 là active_days".

`reference_ts` cố ý là cuối ngày. Với `reference_ts` nửa đêm,
`engine._intra_day_ts` không đặt được event non-recency trong cùng ngày lịch
dưới biên PIT nghiêm ngặt và phải lùi sang ngày trước — đó là nhánh dành cho
fixture cũ, không phải cho production.

---

## 5. Python package reference

### 5.1 `common/` — hạ tầng dùng chung

| Module | Vai trò |
|---|---|
| `config.py` | §4.1 |
| `clients.py` | factory: `get_redis()`, `get_duckdb()`, `duckdb_conn()` (**read-only mặc định**), `duckdb_writer()`, `get_s3_client()`, `pg_cursor()`, `get_kafka_producer/consumer()` |
| `logging_setup.py` | JSON structured logging cho Promtail/Loki; `_PassthroughAdapter` giữ nguyên `extra` của từng lời gọi |
| `metrics.py` | 15 Prometheus metric + `push_batch_metrics()` (Pushgateway, cho job Airflow ngắn hạn) |
| `audit.py` | ghi sổ cái vào `ops.*`: `start_run/finish_run`, `open_sync`, `record_shard`, `get_pending_shards`, `set_sync_status`, `retire_versions`, `record_dq`, `log_inference` |

`duckdb_conn()` mặc định **read-only**, và ghi phải đi qua `duckdb_writer()`.
DuckDB là single-writer; `DuckDBWriteLockError` là lỗi được đặt tên riêng để
phân biệt với lỗi SQL. Airflow có pool `duckdb_writer` để serialize task ghi.

### 5.2 `ingestion/` — luồng stream v1

| Module | Vai trò |
|---|---|
| `schemas.py` | `AppEvent` dataclass, `EVENT_TYPES`, `EVENT_TO_COUNTER`, `validate_event()`, `LAKE_COLUMNS` |
| `event_producer.py` | sinh traffic web/mobile giả lập → Kafka; `_corrupt()` **cố ý** làm hỏng một tỷ lệ event để test DLQ và alert |
| `stream_consumer.py` | Kafka → parquet MinIO → Redis overlay |
| `seed_loader.py` | CSV → `s3://lakehouse/raw/user_snapshot/dt=.../*.parquet` |

**Thứ tự ghi của consumer là một contract**, không phải chi tiết cài đặt:

```text
_write_parquet(rows)      # 1. lake là source of truth, ghi TRƯỚC
_update_realtime(rows)    # 2. downstream state
consumer.commit()         # 3. commit offset SAU CÙNG
```

At-least-once: crash giữa bước 1 và 3 sinh bản ghi trùng — `stg_events_v2`
dedup theo `event_id` (`row_number() over (partition by event_id order by
observation_ts)`). Đổi thứ tự này sẽ mất dữ liệu, không phải trùng dữ liệu.

### 5.3 `features/` — feature store

| Module | Vai trò |
|---|---|
| `spec.py` | `Feature` / `FeatureSpec`; `batch_key()`, `realtime_key()`, `merge()`, `validate_columns()`; entry có thể là 1 feature hoặc 1 `range` (`f0..f82`) |
| `offline_store.py` | `OfflineFeatureStore` trên DuckDB: `iter_shard()` (chunked, không nạp hết RAM), `checksum()`, `null_rates()`, `freshness_hours()`, `treatment_ratio()` |
| `online_store.py` | `OnlineFeatureStore` — toàn bộ thao tác Redis (588 dòng, xem dưới) |
| `sync.py` | sync engine DuckDB → Redis (§5.3.2) |
| `business_aliases.py` | load `business_aliases.yml` |

#### 5.3.1 `OnlineFeatureStore`

Bọc mọi thao tác Redis. Ba nhóm API:

- **Version lifecycle** — `get_active_version()`, `activate_version()` (trả về
  version cũ để rollback), `rollback_to()`, `set/get_version_status()`,
  `list_versions()`, `expire_version()`, `delete_version()` (UNLINK, không block),
  `gc_old_versions(keep)`.
- **Ghi** — `write_rows()` (pipeline HSET), `write_shard(skip_if_done=True)`,
  `mark_shard_done()`, `is_shard_done()`.
- **Đọc/serving** — `read_for_serving()` (**1 round-trip, atomic, qua Lua**),
  `get_features_merged()`, `mget_features()`.

Realtime overlay dùng **cửa sổ trượt 1 giờ chia ô 5 phút**:
`RT_WINDOW_SECONDS` / `RT_BUCKET_SECONDS` / `RT_BUCKETS`. Field có dạng
`rt_events_1h|<bucket_epoch>`; `incr_realtime_counters()` cộng vào ô hiện tại,
`aggregate_realtime()` gấp các ô còn trong cửa sổ thành một giá trị.
Hai Lua script (`_LUA_READ_MERGED`, `_LUA_INCR_WINDOW`) làm việc này atomic;
`_run_script` degrade an toàn (`_NO_LUA`) nếu Redis không hỗ trợ script.

#### 5.3.2 Sync engine — máy trạng thái

```text
IN_PROGRESS ──▶ VALIDATING ──▶ ACTIVE ──▶ RETIRED
                     │
                     └──▶ FAILED   (active cũ giữ nguyên, serving không ảnh hưởng)
```

| Hàm | Làm gì |
|---|---|
| `make_version(logical_date)` | `v20260805` — **deterministic theo ngày logic**, nên rerun ghi đè đúng key cũ |
| `prepare_sync()` | check bảng tồn tại → `validate_columns()` vs spec → đếm row → checksum → `open_sync()` |
| `sync_shard(logical_date, shard_id)` | ghi 1 shard, bỏ qua nếu đã DONE |
| `sync_pending_shards()` | chỉ chạy lại shard chưa DONE |
| `validate_sync(sample_size)` | so mẫu Redis ↔ DuckDB **trước** khi activate |
| `activate_version()` | atomic swap `fs:meta:active_version` |
| `cleanup_old_versions(keep)` | retire + GC |
| `rollback(target_version)` | chỉ đổi con trỏ, không ghi lại dữ liệu |
| `simulate_partial_failure()` | công cụ dạy học: cố tình xoá dấu shard để diễn tập recovery |

Bốn tính chất được thiết kế có chủ đích: consistency (validate trước activate),
freshness (version theo ngày), idempotency (HSET + shard marker), fault tolerance
(shard độc lập, publish atomic ở bước cuối).

### 5.4 `training/` — khung, chưa có model thật

`dataset.py` đọc `marts.training_dataset`; `feature_columns(spec)` định nghĩa
**thứ tự cột dùng chung cho train và serve** — đây là điểm chống skew.
`split_xyt()` trả `(X, y, treatment)`.

`train.py` là **khung cho teammate**: `build_model`, `fit_model`, `evaluate`,
`predict_uplift`, `run_training`. Chưa có implementation model thật.

### 5.5 `serving/` — inference API

FastAPI, 9 endpoint:

| Method | Path | Ghi chú |
|---|---|---|
| GET | `/health` | liveness |
| GET | `/ready` | readiness (Redis + model) |
| GET | `/metrics` | Prometheus |
| GET | `/features/{user_id}` | debug feature lookup |
| GET | `/store/info` | active version, số key |
| POST | `/decide` | 1 user → uplift score + decision |
| POST | `/decide/batch` | nhiều user, **1 pipeline Redis** |
| POST | `/admin/reload-model` | nạp model mới không cần restart |

`model_loader.py` định nghĩa `UpliftModel` là **interface duy nhất** mà API phụ
thuộc, cùng hai implementation: `StubModel` (để hệ thống chạy được trước khi có
model) và `MlflowUpliftModel` (TODO). `is_stub()` được phơi ra để endpoint
`/ready` và Grafana biết đang chạy stub.

`inference_logger.py` đẩy log **ra khỏi đường request**: queue có giới hạn +
worker thread + batch insert vào `ops.inference_log`. `log()` không bao giờ
raise và không bao giờ block; 4 metric riêng theo dõi queued / written /
dropped / queue depth.

### 5.6 `reconstruction/` — 16 module

| Module | Vai trò | Ràng buộc enforce ở đây |
|---|---|---|
| `feature_set.py` | đọc `fs_2026_08_v1.yaml` | INVARIANT 1 (đúng 36 cột) |
| `target.py` | `ReconstructionTarget`, `SolverConfig`, `build_target()` | I-1..I-6, tamper hash |
| `canonical.py` | `float_repr`, `feature_payload_hash`, `target_hash`, `reproducibility_fingerprint` | §6 |
| `candidate.py` | `Slot`, `Candidate` — **cấu trúc**, không phải `list[Event]` | tất định khi song song hoá |
| `semantics.py` | `DecodedTarget`, `H1Branch`, `H2Branch`, `build_branch()` | một engine, hai strategy |
| `engine.py` | `solve()` = P1→P2→P3→P3b→P4 | INVARIANT 3 (seed chỉ chạm optimal pool) |
| `encoding.py` | `ValueEncoding`, `OneHotEncoding`, `EncodingRegistry` | §8.3 — decode phải có khoá thuộc tính |
| `runner.py` | render + chạy **chính SQL dbt**, `gate_a()` | chống tautology §12 |
| `handoff.py` | `to_customer_state()` | chỉ bàn giao khi Gate A/A-T3/B/C pass |
| `state.py` | `CustomerState`, `Provenance` | `FORBIDDEN_STATE_FIELDS`, P-3/P-4 |
| `live.py` | Track B generator | INVARIANT 4 — chỉ import `state` + stdlib |
| `capability.py` | quét AST đồ thị import | INVARIANT 4 (fitness function) |
| `persistence.py` | `Provenance` ↔ row, kiểm P-1/P-2/P-5 | parity với CHECK constraint SQL |
| `e2e.py` | orchestration Track A → T0 → Track B | `RuntimeConfig`, gate D/E/F |
| `track_a_batch.py` | materialize Track A từ CSV thật | H1 constructive, không exhaustive |
| `snapshot.py` | sinh artifact review vào `docs/` | SVG/HTML/CSV/manifest |

---

## 6. Reconstruction engine — internals

### 6.1 Năm pha, không đảo được

```text
P1  FEASIBILITY    hard constraints + EXACT feature equality
P2  OPTIMIZATION   argmin lexicographic  ──▶ OPTIMAL POOL
P3  SELECTION      seeded PRNG chọn MỘT phần tử TRONG pool
P3b MATERIALIZE    candidate_slot_id ──▶ occurrence ──▶ event_id
P4  ORDERING       canonical sort — CHỈ sắp thứ tự
```

Thứ tự này được enforce **bằng chữ ký hàm**, không bằng quy ước:
`select_from_pool(pool, ...)` nhận `pool` đã là argmin — nó **không có đường nào**
chạm vào tập feasible đầy đủ, nên không thể chọn nghiệm suboptimal
(`test_04_select_from_pool_khong_the_nhan_tap_feasible_day_du`).

P3 dẫn xuất PRNG từ `sha256(target_id ∥ seed ∥ track)`, nên mỗi user độc lập và
song song hoá không đổi kết quả.

P3b: `occurrence` tính trong phạm vi **từng `gen_reason`**, sắp theo
`(day_offset, sub_index)` — cố ý **không** dùng `event_ts` (tránh vòng định nghĩa
với P4) và **không** dùng thứ tự vòng lặp. `event_id = uuid5(EVENT_NS,
"{target_id}:{track}:{reason}:{occurrence}")`.

`Candidate` là **multiset các `Slot(gen_reason, day_offset, count)`**, không phải
`list[Event]`. Đây là gốc rễ của tính tất định: nếu candidate là list thì thứ tự
trong list là một sự thật ngẫu nhiên của implementation.

### 6.2 H1 vs H2

| | H1 | H2 |
|---|---|---|
| `f30` nghĩa là | số **ngày** active trong 30 ngày | counter của **lớp event riêng** `EVT_F30` |
| FREE_EVENT | được phép (`EVT_SESSION_STARTED`) | **cấm** (`unexplained_events != 0` ⇒ infeasible) |
| `active_days` | bị ghim bởi feature equality ⇒ **không** vào objective | tự do ⇒ **là** thành phần objective |
| objective | `(unexplained_events, sessions)` | `(active_days, sessions)` |

Phát hiện §4.2a: **cả hai thành phần của objective đều phụ thuộc nhánh**. Đó là
lý do dùng một `ReconstructionEngine` + `SemanticBranch` protocol thay vì
`solver_h1.py` + `solver_h2.py` (hai file sẽ drift khỏi nhau).

### 6.3 Ánh xạ `gen_reason` → `event_type`

```python
EVENT_TYPE_OF = {
    "f5": "EVT_F5",   "f11": "EVT_F11",  "f18": "EVT_F18",  "f30": "EVT_F30",
    "recency": "EVT_ORDER_PAID",     # CFS witness cho f1/f2
    "FREE": "EVT_SESSION_STARTED",   # không counter T1 nào đếm
}
```

Đây là **ranh giới có giá trị kiểm chứng**: `gen_reason` là nội bộ solver và bị
`stg_events_v2` DROP. Feature engine chỉ thấy `event_type`. Hai bên khai báo
**độc lập** cùng một giả định — solver nói "để đạt n5, phát n5 event `EVT_F5`",
engine nói "f5 đếm event `EVT_F5` trong cửa sổ W". Lệch nhau ⇒ Gate A đỏ.

`EVT_ORDER_PAID` hiển thị trong report dưới alias `CFS_RECENCY_MARKER`. Nó
**không** phải bằng chứng `f1`/`f2` là recency order-paid thật của Lazada.

### 6.4 Bảy gate

| Gate | Kiểm gì | Ở đâu |
|---|---|---|
| **A** | 18 cột T1+T2 khớp target theo regime | `runner.gate_a()` |
| **A-T3** | 18 cột T3 khớp — **báo cáo tỷ lệ RIÊNG** | `runner.gate_a()` |
| **B** | mọi event Track A có `event_ts < reference_ts` | `e2e.run_end_to_end` |
| **C** | candidate được chọn thật sự feasible | `branch.is_feasible()` |
| **D** | mọi event Track B là `SYNTHETIC` + lineage hợp lệ | `persistence.check_all()` |
| **E** | `label`/`is_treat` không chạm vào target | `target.validate_scope()` |
| **F** | số event trong `(0, 100_000)`, mọi event Track B `>= as_of_ts` | `e2e` |
| **G** | reproducibility fingerprint | `canonical.reproducibility_fingerprint()` |

Gate A và A-T3 **cố ý tách**: gộp chung thì tỷ lệ pass luôn ≥ 50% nhờ 18 cột
copy — con số vô nghĩa.

**Cấm dùng hash làm phép so feature equality** (§6.2): regime LN lệch bit cuối ở
~7% dòng dù reconstruction hoàn toàn đúng. So sánh theo regime:

| Regime | Cột | Cách so |
|---|---|---|
| `LOG10` | f18, f30 | exact (round 6 chữ số, round-trip khớp 100%) |
| `LN` | f5, f11 | **tương đối, tolerance 1e-15** — cấm nới |
| `REC` | f1, f2 | exact (số nguyên) |
| `CAT` | 12 cột T2 | exact |
| `PASS` | 18 cột T3 | exact |

### 6.5 Capability boundary (INVARIANT 4)

```text
Chữ ký hàm     =  API boundary
Đồ thị import  =  CAPABILITY boundary
```

`CustomerState.source_target_id` là lineage id hợp lệ, nhưng thành lỗ hổng nếu
Track B **giải được** nó: `source_target_id → target repo → 36 features`. Lúc đó
dù chữ ký hàm sạch, generator vẫn "nhìn trộm" được target và closed-loop test
kiểm chính nó.

`capability.py` quét AST, đi **bắc cầu** qua mọi import nội bộ, và fail ngay khi
`live.py` reachable tới `target` / `canonical` / `feature_set` / `handoff` /
`engine` / `semantics` / `candidate` — kể cả gián tiếp qua ba lớp module. Lỗi
in ra **đường đi cụ thể**. `state.py` cũng cố ý không import `target`/`canonical`.

Bổ sung ở tầng cấu trúc: `CustomerState` không có field nào trong
`FORBIDDEN_STATE_FIELDS = {values, target_hash, feature_payload_hash,
selected_feature_set_id}`, và `assert_state_has_no_target_channel()` kiểm điều
đó bằng `__dataclass_fields__` (TEST-10a).

### 6.6 Provenance (P-1..P-5)

| Luật | Nội dung | Enforce ở |
|---|---|---|
| P-1 | đồ thị `parent_run_id` không có chu trình | `assert_no_cycle()` + view SQL |
| P-2 | mọi bản ghi trong cùng cây có cùng `root_generation_id` | `assert_single_root()` |
| P-3 | `RECONSTRUCTED` bắt buộc có `parent_target_id` | `Provenance.__post_init__` **+** `CHECK ck_p3` |
| P-4 | `SYNTHETIC` bắt buộc có `ancestor_model_ids` **hoặc** `behaviour_policy_version` | `__post_init__` **+** `CHECK ck_p4` |
| P-5 | tập train production không được có tổ tiên synthetic, kể cả gián tiếp | `assert_production_train_eligible()` |

P-3/P-4 được mã hoá **hai lần** (Python + SQL CHECK) — có chủ đích.
`test_persistence.py` canh cho hai lớp không trôi khỏi nhau.

P-5 kiểm trên **đồ thị**, không trên từng dòng: một dòng `REAL` vẫn có thể có tổ
tiên `SYNTHETIC` qua nhiều đời.

### 6.7 Encoding contract

Bẫy §8.3 — `alphabet(f37)` là tập con hoàn toàn của `alphabet(f38)` (64/64 giá
trị), **nhưng** `card(tuple(f37,f38)) = 1133 > 241` ⇒ hai biến độc lập. Do đó
`EncodingRegistry` **không có API `decode(value)` không khoá**; mọi tra cứu phải
đi qua `attr`. Trong SQL: join theo `(attr_name, level_id)`, không join theo value.

`f79..f82` là **một** biến latent 515 mức với bốn encoding (song ánh, đúng ở cả
train lẫn test) — một `source_attribute`, không phải bốn.

Group-level (§5, G-1): `synthetic_segment_g2` có **10 mức** dù model chỉ đọc
`f43/f44/f45`. Dựng đủ 10 là bắt buộc, nếu không bất biến `sum(f43..f52) == 1`
sẽ vỡ và ba cột được chọn mất nghĩa "loại trừ lẫn nhau".

Hai property kiểm trên **toàn domain**, không sample:
`decode(encode(s)) == s` (`assert_bijective`) và `encode(decode(x)) == x`
(`assert_round_trip`). Giá trị lạ ⇒ `EncodingError`, **không fallback**.

### 6.8 Track A batch trên dữ liệu thật

`track_a_batch.py` khác `engine.solve()` ở chỗ nó **không** enumerate exhaustive.
`construct_h1_candidate()` dựng thẳng một nghiệm H1 hợp lệ (round-robin counter
lên các ngày active, vá FREE_EVENT vào ngày trống) rồi assert
`branch.is_feasible()`. Với 926,669 dòng, exhaustive enumeration là bất khả thi.

Chỉ hỗ trợ `semantic_branch=H1`; H2 raise. Row nào lỗi đi vào `quarantine.csv`
kèm lý do, vòng lặp chạy tiếp. `verify_limit` dòng đầu được chạy qua SQL dbt
thật và Gate A, kết quả nằm trong `manifest.json["gate_sample"]`.

Decode target:
`n5 = round(exp(f5))`, `n11 = round(exp(f11))`, `n18 = round(10^f18)`,
`n30 = round(10^f30)`, `d1 = round(f1)`, `d2 = round(f2)`, `window_days = 30`.

---

## 7. dbt layer

DuckDB adapter, profile trong [dbt/profiles.yml](../dbt/profiles.yml).

### Staging

| Model | Materialization | Vai trò |
|---|---|---|
| `stg_user_snapshot` | view | CSV snapshot đã typed |
| `stg_app_events` | view | event Kafka v1 |
| `stg_events_v2` | view | Track A witness — **DROP metadata solver** |

`stg_events_v2` chỉ cho qua `event_id · customer_id · target_id · event_type ·
event_ts · observation_ts · source_type · generation_run_id`, và DROP
`gen_reason · day_offset · sub_index · occurrence`. `day_offset` bị drop **dù nó
tiện** — feature engine phải tự tính lại từ `(event_ts, reference_ts)`, đúng như
production làm.

### Marts

| Model | Cột sinh ra | Ghi chú |
|---|---|---|
| `feat_cfs_counter` | f5, f11, f18, f30 | hai regime mã hoá khác nhau, xem dưới |
| `feat_cfs_recency` | f1, f2 | `date_diff('day', ...)`, biên PIT `<` nghiêm ngặt |
| `feat_cfs_categorical` | 12 cột T2 + 4 cột `_g*_onehot_sum` | join theo `(attr, level_id)` |
| `feat_passthrough` | 18 cột T3 | **copy có kiểm soát**, không phải feature engineering |
| `feat_user_selected_serving` | 36 cột + `user_id, dt, feature_ts` | nguồn sync Redis |
| `training_dataset` | feature + `label` + `is_treat` | chỉ cho training |
| `feat_user_behaviour`, `feat_user_realtime_pit`, `feat_user_serving` | | forward path baseline |

Ba chi tiết trong `feat_cfs_counter` là contract, không phải style:

```sql
-- LN: float64 ĐẦY ĐỦ, cấm round() — round sẽ phá round-trip 1e-15
case when n5 >= 1 then ln(n5) end  as f5

-- LOG10: round ĐÚNG 6 chữ số — quy ước lưu trữ đo được, round-trip khớp exact
case when n18 >= 1 then round(log10(n18), 6) end  as f18

-- KHÔNG coalesce n=0 thành 1. Miền đo được là [1,N]; engine thấy 0 nghĩa là
-- KHÔNG KHỚP THẬT ⇒ để NULL cho Gate A bắt.
```

Biên PIT dùng `<` **nghiêm ngặt** (khớp `feat_user_realtime_pit`) cộng điều kiện
availability `observation_ts <= reference_ts`. Dùng `<=` sẽ lệch một event ở
biên — lỗi rất khó tìm vì chỉ hiện ở một tỷ lệ nhỏ user.

Nhánh H1/H2 chọn bằng dbt var `f30_semantic_branch`, **không** bằng
`if reconstruction_mode` (TA-2 assert điều này).

### Cầu nối runner ↔ dbt

`runner.RELATIONS` ánh xạ `ref()`/`source()` sang bảng DuckDB in-memory:

```python
("ref", "stg_events_v2")                    -> "stg_events_v2"
("source", "raw", "events_v2")              -> "raw_events_v2"
("source", "biz", "reconstruction_boundary")-> "biz_reconstruction_boundary"
("source", "biz", "customer_attribute")     -> "biz_customer_attribute"
("source", "biz", "encoding_map")           -> "biz_encoding_map"
("source", "biz", "onehot_layout")          -> "biz_onehot_layout"
("source", "biz", "passthrough_source")     -> "biz_passthrough_source"
```

Jinja env dùng `StrictUndefined` — thiếu var là lỗi ngay, không render ra chuỗi rỗng.

---

## 8. Data contracts

### 8.1 Redis

| Key | Type | Nội dung |
|---|---|---|
| `fs:{version}:u:{user_id}` | HASH | 36 field `f*` + `_v`, `_ts`, `_feature_set_id` |
| `rt:u:{user_id}` | HASH | `rt_*_1h\|<bucket_epoch>` (ô 5 phút) + `rt_last_event_ts` |
| `fs:meta:active_version` | STRING | con trỏ version đang phục vụ |
| `fs:meta:{version}:status` | HASH | trạng thái sync |
| `fs:meta:{version}:shards` | SET | shard đã xong (idempotency) |
| `fs:meta:versions` | ZSET | version theo timestamp (dùng để GC) |

Active version không có TTL. Version cũ bị hạ TTL (`FEATURE_STALE_TTL_SECONDS`)
khi retire, rồi UNLINK.

### 8.2 Kafka

| Topic | Partitions | Retention | Key |
|---|---|---|---|
| `app.user.events.v1` | 3 | 2 ngày | `user_id` |
| `app.user.events.dlq.v1` | 1 | 7 ngày | — |

Key là `user_id` để đảm bảo per-user ordering. Track B v2 **chưa** publish lên
topic production.

### 8.3 MinIO

```text
lakehouse/raw/user_snapshot/dt=<date>/{train,test}.parquet
lakehouse/raw/app_events/dt=<date>/hour=<hh>/*.parquet
lakehouse/exports/
models/          # model artifact
mlflow/          # MLflow artifact root
```

### 8.4 DuckDB

`marts.feat_user_selected_serving` là **nguồn sync duy nhất** lên Redis. Nó chứa
`user_id, dt, feature_ts` + 36 cột, và **không** chứa `label`/`is_treat`.

### 8.5 Postgres

`ops.*` ([01_pipeline_schema.sql](../sql/postgres/01_pipeline_schema.sql)):
`pipeline_run` · `feature_sync_audit` · `feature_sync_shard` · `dq_result` ·
`inference_log`.

`biz.*` ([02_biz_reconstruction.sql](../sql/postgres/02_biz_reconstruction.sql)):
`generation_run` · `reconstruction_target` · `customer_attribute` ·
`encoding_map` · `onehot_layout` · `passthrough_source` · `provenance` ·
`reconstruction_result` · `reconstruction_event` · `customer_state` ·
`reject_target` · `reconstruction_diff`.

Các CHECK constraint đáng chú ý — chúng lặp lại luật của tầng Python **có chủ đích**:

```sql
ck_no_label      CHECK (NOT (payload ? 'label' OR payload ? 'is_treat'))
ck_scope_36      CHECK (biz.jsonb_object_keys_count(payload) = 36)
ck_status_branch CHECK (...)   -- I-6
ck_p3 / ck_p4    CHECK (...)   -- provenance
ck_quarantine_no_events, ck_last_event_before, ck_no_self_parent
```

> ⚠️ Volume Postgres cũ cần recreate để nhận schema `biz.*` (xem `ERD.md`).

### 8.6 Track A batch output (`.tmp/reconstruction_track_a/`)

Xem [README §13](../README.md). Ba file quan trọng nhất:
`raw_events_v2.csv` (schema dbt-facing), `targets_selected_features.csv`
(payload + `target_hash`), `manifest.json` (counts + `gate_sample`).

---

## 9. Orchestration — Airflow DAGs

| DAG | Schedule | Task chính |
|---|---|---|
| `00_bootstrap_lake` | manual | `create_schemas` → `seed_csv` → `verify_lake` → `next_steps` |
| `10_ingest_stream_to_lake` | `@hourly` | `check_new_files` · `measure_ingestion_lag` · `compact_hour` · `dq_streaming_freshness` |
| `20_build_features_dbt` | `0 1 * * *` | `dbt_debug` → `dbt_run` → `assert_spec_contract` → `dbt_test` → `publish_dbt_results` → `profile_features` |
| `30_train_uplift_model` | `0 3 * * 1` | `check_training_data` → `train` → `promote_model` → `notify_serving` — **khung** |
| `40_sync_features_to_redis` | `30 1 * * *` | `prepare` → `sync_shard.expand(...)` → `summarize_shards` → `validate` → `activate` → [`cleanup`, `smoke_test_serving`] |
| `50_data_quality` | `*/30 * * * *` | freshness · consistency · volume · streaming → `summarize` |
| `60_reconstruction_e2e` | manual | `run_contract` (dry-run) |
| `99_ops_toolbox` | manual | `run_action` — rollback, chaos, GC |

Chi tiết vận hành:

- **Pool `duckdb_writer`** serialize mọi task ghi DuckDB (single-writer).
- **Pool `redis_sync`** + `max_active_tis_per_dag=4` giới hạn song song khi sync.
- `sync_shard` là **dynamic task mapping** trên `range(SHARDS)`, `retries=3`.
- `validate` có `retries=0` — validate fail là tín hiệu dữ liệu sai, retry chỉ
  che lỗi.
- `on_sync_failed` dùng `trigger_rule=ONE_FAILED` và nhận upstream từ **mọi** bước.
- `publish_dbt_results` / `profile_features` dùng `ALL_DONE` để vẫn publish metric
  khi dbt test đỏ.
- Callback dùng chung: `lzd_utils/callbacks.py` → `ops.pipeline_run` + Pushgateway.

Thứ tự chạy tay cho batch path: `00` → `20` → `40` → `50`.

---

## 10. CLI & entrypoints

Module chạy được bằng `python -m` (cần `PYTHONPATH=src`):

| Lệnh | Làm gì |
|---|---|
| `python -m lzd_pipeline.reconstruction.e2e [--branch H1\|H2] [--future-days N] [--config P]` | E2E dry-run, in JSON summary, `exit 1` nếu có gate đỏ |
| `python -m lzd_pipeline.reconstruction.track_a_batch (--limit N \| --all) [--verify-limit 50] [--parquet]` | Track A từ CSV thật → `.tmp/` |
| `python -m lzd_pipeline.reconstruction.snapshot` | ghi artifact review vào `docs/reconstruction_snapshot/` |
| `python -m lzd_pipeline.features.spec` | in tóm tắt feature spec + ví dụ Redis key |
| `python -m lzd_pipeline.ingestion.event_producer` | producer (container `event-producer`) |
| `python -m lzd_pipeline.ingestion.stream_consumer` | consumer (container `stream-consumer`) |
| `python -m lzd_pipeline.training.train` | training (khung) |

`track_a_batch` **bắt buộc** chọn `--limit N` hoặc `--all`; không có default —
`--all` trên 926,669 dòng sinh hàng chục triệu event.

Wrapper: [`scripts/stack.ps1`](../scripts/stack.ps1) (Windows, ~20 lệnh:
`doctor · init · build · up-core · up · up-all · down · reset · ps · status ·
logs · health · redis · duckdb · reconstruction · snapshot · track-a · test`)
và [`Makefile`](../Makefile) (bash/WSL, tập lệnh tương đương).

---

## 11. Test map & trạng thái xác minh

**Chạy ngày 2026-08-14:** `python -m pytest tests/ -q` → **210 passed**.

| File | Test | Kiểm gì |
|---|---|---|
| `test_contract_slice1.py` | 30 | scope 36 cột, hash/tamper, fingerprint, encoding bijection, one-hot group |
| `test_engine.py` | 24 | optimal pool, seed determinism, mutant detection, occurrence, canonical order |
| `test_persistence.py` | 26 | P-1..P-5, parity Python ↔ SQL CHECK |
| `test_handoff_and_trackb.py` | 23 | handoff refusal, Gate B, Track B lineage |
| `test_forward_engine_contract.py` | 20 | TA-2/TA-3/TA-6 — SQL dbt không chạm solver internals |
| `test_sync_logic.py` | 16 | version, shard idempotency, validate |
| `test_capability_boundary.py` | 14 | INVARIANT 4 — AST import graph, import tương đối |
| `test_feature_spec.py` | 8 | spec loading, range expansion |
| `test_event_schema.py` | 6 | validate event |
| `test_end_to_end.py` | 5 | cả H1 và H2 chạy hết pipeline |
| `test_business_aliases.py` | 3 | alias loading |
| `test_track_a_batch.py` | 2 | batch materialization |
| `test_snapshot.py` | 1 | snapshot writer sinh đủ artifact + manifest |

Đặc điểm đáng chú ý của test suite:

- **Mutant testing** (`test_04_*`): cố tình dựng một engine chọn nghiệm
  suboptimal và assert test bắt được nó. Test kiểm chính năng lực phát hiện lỗi
  của mình.
- **Exhaustive oracle** (`test_01_pool_khop_exhaustive_argmin`): so optimal pool
  với enumeration độc lập — không tin vào chính solver đang test.
- **Architectural fitness function** (`test_capability_boundary.py`): kiểm đồ thị
  import, bao gồm cả import tương đối hai cấp và import trong `__init__.py`.
- **Parity test** (`test_persistence.py`): canh Python `__post_init__` và SQL
  CHECK không trôi khỏi nhau.

### Đã sửa: `output_dir` ngoài repo (2026-08-14)

`build_manifest()` từng giả định `output_dir` luôn nằm trong repo:

```text
ValueError: '<tmp_path>' is not in the subpath of '<repo root>'
  src/lzd_pipeline/reconstruction/snapshot.py in build_manifest
      "output_dir": str(output_dir.relative_to(ROOT)),
```

Đây là lỗi của **code**, không phải của test: `write_snapshot()` nhận
`output_dir` tuỳ ý (kể cả `--output-dir` trỏ ra ngoài repo) nhưng chỉ hoạt động
với đường dẫn trong repo. Nay `_display_path()` giữ đường dẫn tương đối khi
`output_dir` nằm trong repo và fallback sang đường dẫn tuyệt đối khi ở ngoài —
cùng cách `track_a_batch.py` vẫn xử lý field này. Manifest đã commit
(`docs/reconstruction_snapshot/manifest.json`) không đổi giá trị.

### Bẫy môi trường Windows đã xử lý cùng ngày

- `requirements-dev.txt` ghim `PyYAML==6.0.3` (khác `docker/` ghim `6.0.2`):
  6.0.2 không có wheel cho Python 3.14 nên pip build từ source, và bản build đó
  chết vì đường dẫn repo có dấu tiếng Việt dưới codepage cp932.
- `requirements-dev.txt` ghim thêm `redis==5.1.1` cho khớp `docker/`. Trước đó
  `redis` thả nổi lên 8.x, gửi `HELLO` khi connect, `fakeredis==2.25.1` không
  hiểu lệnh này và toàn bộ 15 test Redis đỏ với `ResponseError`.
- `stack.ps1 test` không còn trỏ `TEMP` vào `.tmp/`. pytest tạo basetemp bằng
  `mkdir(mode=0o700)`, và từ Python 3.13 Windows mới thật sự áp dụng mode đó →
  DACL chỉ còn SYSTEM + Administrators + OWNER RIGHTS. Nếu `pytest-of-<user>`
  đã bị một tài khoản khác tạo trước thì user hiện tại mất sạch quyền và mọi
  test dùng `tmp_path` chết với `PermissionError: [WinError 5]`.

---

## 12. Known gaps

Những thứ **chưa** có trong code, để không ai đọc doc rồi tưởng đã có:

| Hạng mục | Trạng thái |
|---|---|
| Model uplift thật | chưa — `StubModel` + khung `train.py` |
| MLflow model loading | `MlflowUpliftModel` là TODO |
| Materialize target từ toàn bộ train split | chưa |
| Writer production cho `raw/events_v2` vào MinIO | chưa — Track A chỉ ghi `.tmp/` |
| Kafka v2 schema/consumer/publisher cho Track B | chưa |
| Solver production (branch-and-bound / CP / MILP) | chưa — prototype dùng exhaustive; batch path dùng constructive H1 |
| Track A batch cho H2 | chưa — raise nếu `semantic_branch != "H1"` |
| Quarantine threshold thực nghiệm | chưa chốt |
| Semantic thật của `f30`, `f9`/`f16` | `UNIDENTIFIED` — có chủ đích |
| `test_snapshot.py` | đỏ, xem §11 |

Ranh giới nhận thức quan trọng nhất, lặp lại ở đây vì nó dễ bị đọc nhầm:

> Reconstruction **không** khôi phục semantic thật của feature Lazada. Nó xác
> định ràng buộc cấu trúc của feature quan sát được, gán semantic nghiệp vụ
> synthetic **một cách tường minh**, và dựng một hệ thống event mà pipeline
> feature engineering **xuôi chiều** của nó tái tạo đúng representation của
> feature training đã chọn. Khớp feature ⇒ validate *reconstruction*, không
> phải validate *semantic*.

---

## Liên kết

| Câu hỏi | Đọc |
|---|---|
| Cài đặt, chạy stack | [README.md](../README.md) |
| Kiến trúc & lý do | [PIPELINE_ARCHITECTURE.md](PIPELINE_ARCHITECTURE.md) |
| Hợp đồng reconstruction | [RECONSTRUCTION_CONTRACT.md](RECONSTRUCTION_CONTRACT.md) · [RECONSTRUCTION_SPEC.md](RECONSTRUCTION_SPEC.md) |
| Mô hình ràng buộc solver | [RECONSTRUCTION_CONSTRAINT_MODEL.md](RECONSTRUCTION_CONSTRAINT_MODEL.md) |
| Từng feature một | [FEATURE_DICTIONARY.md](FEATURE_DICTIONARY.md) · [FEATURE_LINEAGE.md](FEATURE_LINEAGE.md) |
| Alias nghiệp vụ | [BUSINESS_ALIAS_MAP.md](BUSINESS_ALIAS_MAP.md) |
| Vận hành / sự cố | [RUNBOOK.md](RUNBOOK.md) · [OBSERVABILITY.md](OBSERVABILITY.md) |
| Kế hoạch productionize | [MIGRATION_PLAN.md](MIGRATION_PLAN.md) |
| Artifact review đã commit | [reconstruction_snapshot/](reconstruction_snapshot/) |
