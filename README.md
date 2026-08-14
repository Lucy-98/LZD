# LZD Uplift Feature Platform

Repo nay dung de dung lai pipeline du lieu cho bai toan Lazada voucher uplift:
nap dataset goc, build feature bang dbt/DuckDB, sync 55 selected features len
Redis, va kiem chung reconstruction tu feature train ve event witness.

Scope hien tai cua nhanh nay la data pipeline va reconstruction. API serving va
model co trong repo de lam integration surface, nhung khong phai phan can sua
trong nhanh reconstruction.

## 1. Tong Quan Cho Nguoi Moi

### Muc tieu

- Co mot stack local de dong doi/mentor co the tai tao lai du lieu va quan sat
  luong `CSV -> lakehouse -> dbt -> Redis`.
- Giai thich ro 55 feature nao duoc sync len Redis thay vi sync ca `f0..f82`.
- Chung minh reconstruction: tu selected feature trong train, sinh nguoc Track A
  witness events, chay lai dbt SQL, va so lai feature expected/actual.

### Nguoi dung repo

- Data/ML engineer muon xem feature lineage va sync contract.
- Mentor/reviewer muon clone repo, cai dat, chay test, xem artifact
  reconstruction, va neu can thi chay lai pipeline.
- Developer khong can sua API/model trong nhanh nay.

### Luong chinh

```text
data/full_trainset.csv
data/full_testset.csv
        |
        v
Airflow DAG 00_bootstrap_lake
        |
        v
MinIO bucket lakehouse/raw/user_snapshot/*.parquet
        |
        v
Airflow DAG 20_build_features_dbt
        |
        v
DuckDB marts.feat_user_selected_serving
        |
        v
Airflow DAG 40_sync_features_to_redis
        |
        v
Redis fs:{version}:u:{user_id}
```

Realtime demo chay rieng:

```text
event-producer -> Kafka app.user.events.v1 -> stream-consumer
        -> MinIO lakehouse/raw/app_events
        -> Redis rt:u:{user_id}
```

Reconstruction chay rieng:

```text
selected train features -> Track A events -> dbt reconstruction SQL
        -> gates -> CustomerState(T0) -> Track B future events
```

Reconstruction dry-run khong ghi production vao MinIO, Kafka, Redis hay
Postgres.

## 2. Trang Thai Hien Tai

Dang hoan thanh:

- Local unit/contract tests: `280 passed` (2026-08-14). Xem
  [Tech reference](docs/TECH_REFERENCE.md) §11.
- Reconstruction snapshot da commit trong `docs/reconstruction_snapshot/`.
- Track A batch tu `data/full_trainset.csv` da chay pilot 1,000 va 10,000 row.
- Redis batch contract da chot cho 55 selected features.
- Airflow DAG cho batch/reconstruction dry-run da co.

Chua lam trong nhanh nay:

- Khong viet moi API.
- Khong tune/train model.
- Chua publish Track B production vao Kafka topic v2.
- Chua ghi reconstruction production `events_v2` vao MinIO trong DAG batch.
- Chua dung reconstruction artifact `.tmp/` lam source of truth production.

## 3. Repository Layout

```text
airflow/dags/                 Airflow DAGs
config/features/              feature spec, selected 55-feature set
config/features/business_aliases.yml  synthetic business aliases for selected f*
config/reconstruction/        reconstruction runtime config
dbt/                          DuckDB/dbt models and tests
docker/                       Dockerfiles for Airflow/Python/MLflow images
docs/                         architecture docs and committed review snapshot
docs/reconstruction_snapshot/ small committed reconstruction artifact
scripts/                      stack helpers and init scripts
src/lzd_pipeline/             Python pipeline code
tests/                        unit and contract tests
data/                         committed/expected source CSV data
.tmp/                         local generated runtime output, ignored by git
```

Important: `data/` is no longer ignored by git. If the mentor needs to recreate
the same run from a clean clone, commit these files:

```text
data/full_trainset.csv
data/full_testset.csv
```

`.dockerignore` still excludes `data/` from Docker build context. That is
intentional: Docker containers read `data/` at runtime through bind mounts; the
CSV files do not need to be baked into images.

## 4. Prerequisites

Required:

- Windows 10/11 with PowerShell 5+ or PowerShell 7+.
- Python 3.11+.
- Git.
- Docker Desktop with Compose v2 if running the full stack.

Recommended Docker Desktop resources:

- CPU: 4 cores.
- RAM: 8 GB minimum, 12 GB preferred.
- Disk: 15 GB free.

Recommended repo path:

```text
C:\dev\LZD
```

The repo can run from OneDrive, but Docker BuildKit and bind mounts are more
stable outside OneDrive because OneDrive can mark files as reparse points.

## 5. Quick Start Without Docker

Use this path first. It proves the code/reconstruction contract without needing
Docker Hub, MinIO, Kafka, Redis, or Airflow.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements-dev.txt
```

Run tests:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 test
```

Generate the committed-size reconstruction snapshot:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 snapshot
```

Generate Track A raw events from real train data, limited to 1,000 rows:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 track-a 1000
```

Output goes to:

```text
.tmp/reconstruction_track_a/
```

`.tmp/` is ignored because full Track A can generate very large files.

## 6. Quick Start With Docker

Create `.env`, check Docker Desktop, and build images:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 doctor
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 init
```

Start core services:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 up-core
```

Start core plus observability:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 up
```

Start every profile, including stream/serving/ML services:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 up-all
```

Check status and health:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 status
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 health
```

Stop:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 down
```

Reset Docker volumes:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 reset
```

## 7. Docker Services

Core services:

| Service | Purpose | URL |
|---|---|---|
| Postgres | Airflow/MLflow metadata and pipeline audit schema | localhost:5432 |
| Redis | online feature store | localhost:6379 |
| MinIO | local S3-compatible lake/model artifact store | http://localhost:9001 |
| Kafka | realtime event bus | localhost:29092 |
| Airflow | orchestration | http://localhost:8080 |

Observability services:

| Service | Purpose | URL |
|---|---|---|
| Grafana | dashboard/log exploration | http://localhost:3000 |
| Prometheus | metrics store | http://localhost:9090 |
| Loki | logs backend | http://localhost:3100 |
| Kafka UI | inspect topics/messages/lag | http://localhost:8082 |
| RedisInsight | inspect Redis keys | http://localhost:5540 |

Optional profiles:

| Service | Purpose |
|---|---|
| event-producer | sends synthetic realtime v1 app events to Kafka |
| stream-consumer | writes Kafka v1 events to MinIO and Redis overlay |
| inference-api | reads Redis features; not the scope of reconstruction work |
| mlflow | experiment/model registry surface; not the scope of reconstruction work |

Default UI logins:

| UI | Login |
|---|---|
| Airflow | `admin/admin` |
| Grafana | `admin/admin` |
| MinIO | `minioadmin/minioadmin123` |

## 8. MinIO And Kafka Config

MinIO creates 3 buckets:

```text
lakehouse  -> raw/staging/marts/export data
models     -> model artifacts surface
mlflow     -> MLflow artifact root
```

Inside `lakehouse`, init script creates:

```text
raw/user_snapshot
raw/app_events
exports
```

Kafka creates 2 topics in the current production/demo path:

```text
app.user.events.v1      partitions=3, retention=2 days
app.user.events.dlq.v1  partitions=1, retention=7 days
```

Kafka key is `user_id/customer_id` for per-user ordering. Track B v2 events are
currently dry-run only; they are not published to a production v2 topic yet.

## 9. Airflow Run Order

Open Airflow:

```text
http://localhost:8080
```

Run these manually in order for the batch feature path:

1. `00_bootstrap_lake`
2. `20_build_features_dbt`
3. `40_sync_features_to_redis`
4. `50_data_quality`

Manual reconstruction contract DAG:

```text
60_reconstruction_e2e
```

`60_reconstruction_e2e` only validates reconstruction in dry-run mode. It does
not write MinIO/Kafka/Redis production state.

## 10. dbt Data Flow

`00_bootstrap_lake` loads:

```text
data/full_trainset.csv -> lakehouse/raw/user_snapshot/dt=<run_date>/train.parquet
data/full_testset.csv  -> lakehouse/raw/user_snapshot/dt=<run_date>/test.parquet
```

`20_build_features_dbt` runs `dbt run` and `dbt test` over DuckDB.

Important dbt outputs:

```text
staging.stg_user_snapshot
staging.stg_events_v2
marts.feat_cfs_counter
marts.feat_cfs_recency
marts.feat_cfs_categorical
marts.feat_passthrough
marts.feat_user_selected_serving
marts.training_dataset
```

Redis sync reads only:

```text
marts.feat_user_selected_serving
```

That table contains:

```text
user_id
dt
feature_ts
55 selected features
```

It does not contain `label` or `is_treat`.

## 11. Redis Contract

Batch selected feature key:

```text
fs:{version}:u:{user_id}
```

Type: Redis HASH.

Fields:

```text
f1 f2 f5 f11 f18 f19 f30
f37 f38 f79 f80 f81 f82 f40 f43 f44 f45 f64 f68
f3 f4 f8 f9 f10 f12 f13 f16 f20 f21 f22 f23 f25 f26 f28 f29 f31 f35
_v
_ts
_feature_set_id
```

Metadata keys:

```text
fs:meta:active_version
fs:meta:{version}:status
fs:meta:{version}:shards
fs:meta:versions
```

Sync design:

- Version is deterministic by logical date, for example `v20260805`.
- Each shard writes idempotently with `HSET`.
- Shard completion is marked in `fs:meta:{version}:shards`.
- Validation compares Redis sample against DuckDB before activation.
- Activation is an atomic swap of `fs:meta:active_version`.
- Old versions are retired/GC'd after the active version is safe.

Realtime overlay key:

```text
rt:u:{user_id}
```

Type: Redis HASH.

Example fields:

```text
rt_events_1h|<bucket_epoch>
rt_page_view_1h|<bucket_epoch>
rt_add_to_cart_1h|<bucket_epoch>
rt_order_1h|<bucket_epoch>
rt_gmv_1h|<bucket_epoch>
rt_session_len_sec|<bucket_epoch>
rt_last_event_ts
```

The consumer persists raw event first, updates Redis overlay second, then commits
Kafka offset. This keeps the lake as source of truth.

## 12. Reconstruction

Reconstruction method:

```text
CFS / feature-consistent witness generation
```

This is not true recovery of historical Lazada events. It builds one valid set
of raw-like events that reproduces the selected feature vector under the dbt
feature logic.

Business alias layer:

```text
config/features/business_aliases.yml
docs/BUSINESS_ALIAS_MAP.md
```

This maps technical feature names such as `f30` to synthetic business names such
as `active_days_30d_log10`. These aliases make the Lazada voucher-uplift story
readable, but they are not confirmed Lazada/DESCN semantics. Redis and dbt still
use `f*` names.

Tracks:

| Track | Meaning | Output |
|---|---|---|
| Track A | historical witness events before `reference_ts` | `EVT_*` / `raw_events_v2` |
| Handoff | verified features become `CustomerState(T0)` | state object |
| Track B | future synthetic events after `reference_ts` | business-v2 future events |

Selected features:

- T1/event-level: `f1,f2,f5,f11,f18,f19,f30`
- T2/attribute-level: `f37,f38,f79,f80,f81,f82,f40,f43,f44,f45,f64,f68`
- T3/pass-through: `f3,f4,f8,f9,f10,f12,f13,f16,f20,f21,f22,f23,f25,f26,f28,f29,f31,f35`

Examples of synthetic aliases:

| Feature | Alias |
|---|---|
| `f5` | `product_browse_intensity_365d_ln` |
| `f11` | `cart_checkout_intent_365d_ln` |
| `f18` | `promo_touch_intensity_365d_log10` |
| `f19` | `promo_redemption_intensity_365d_log10` |
| `f30` | `active_days_30d_log10` |
| `f37` | `price_sensitivity_segment_64_encoded` |
| `f38` | `promo_affinity_segment_241_encoded` |
| `f79..f82` | `preferred_leaf_category_515_enc_*` |
| `f68` | `discount_hunter_flag` |

Track A event aliases in reports use `CFS_*` names. For example,
`EVT_ORDER_PAID` is displayed as `CFS_RECENCY_MARKER`; it should not be read as
proof that `f1/f2` are true Lazada order-paid recency features.

Run committed-size snapshot:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 snapshot
```

Review artifact:

```text
docs/reconstruction_snapshot/README.md
docs/reconstruction_snapshot/report.html
docs/reconstruction_snapshot/track_a_events.csv
docs/reconstruction_snapshot/track_b_events.csv
docs/reconstruction_snapshot/features_expected_actual.csv
docs/reconstruction_snapshot/feature_pass.svg
docs/reconstruction_snapshot/timeline.svg
docs/reconstruction_snapshot/summary.json
docs/reconstruction_snapshot/manifest.json
```

Run Track A from real train data:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 track-a 1000
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 track-a 10000
```

Full train:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 track-a all
```

Full train has 926,669 rows and can generate tens of millions of events. Keep
full output under `.tmp/`; commit only small review fixtures unless explicitly
needed.

## 13. Generated Track A Files

`track-a` writes to:

```text
.tmp/reconstruction_track_a/
```

Files:

| File | Meaning |
|---|---|
| `raw_events_v2.csv` | reconstructed raw events with dbt-facing schema |
| `raw_events_v2.parquet` | parquet copy if parquet dependencies are available |
| `track_a_events_audit.csv` | raw events plus solver audit columns |
| `biz_reconstruction_boundary.csv` | target/reference timestamp boundary |
| `biz_customer_attribute.csv` | decoded T2 source attributes |
| `biz_encoding_map.csv` | fitted categorical value encodings |
| `biz_onehot_layout.csv` | full one-hot layout |
| `biz_passthrough_source.csv` | frozen T3 source values |
| `targets_selected_features.csv` | expected 55-feature payload and hash |
| `quarantine.csv` | rows that cannot be reconstructed |
| `manifest.json` | counts, config, gate sample result |

## 14. Useful Commands

```powershell
# diagnose Docker registry/proxy/CA
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 doctor

# build images
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 build

# container status
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 ps

# logs for one service
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 logs airflow-scheduler

# redis-cli
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 redis

# DuckDB table list through Airflow container
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 duckdb

# local tests, no Docker
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 test
```

## 15. Troubleshooting

PowerShell blocks scripts:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 <command>
```

Docker daemon is not reachable:

- Start Docker Desktop.
- Wait until the engine is running.
- Retry `docker compose version`.

Docker pull fails with:

```text
x509: certificate signed by unknown authority
```

This is a Docker Desktop trust/proxy/CA problem, not a repo code problem.

Fix options:

- If no proxy is required, disable Docker Desktop proxy.
- If a company/MITM proxy is used, import the proxy root CA into Windows
  `Certificates (Local Computer) -> Trusted Root Certification Authorities`.
- Restart Docker Desktop.
- Verify with `docker pull redis:7.2-alpine`.

Docker build fails under OneDrive with `invalid file request`:

- Prefer cloning to `C:\dev\LZD`.
- Keep `.dockerignore` excluding runtime bind-mounted folders from build context.

Redis has no `fs:meta:active_version`:

- Run Airflow DAGs `00_bootstrap_lake`, `20_build_features_dbt`,
  `40_sync_features_to_redis` in order.

MinIO/Kafka/Redis do not change after reconstruction dry-run:

- Expected behavior.
- Reconstruction dry-run is a contract check only.
- Production writes for reconstructed `events_v2` are still a migration item.

## 16. Primary Docs

- [Tech reference](docs/TECH_REFERENCE.md) — code-level map: module, config,
  contract, DAG, test. Doc dau tien nen doc neu ban sap sua code.
- [Pipeline architecture](docs/PIPELINE_ARCHITECTURE.md)
- [Data flow](docs/DATA_FLOW.md)
- [Business alias map](docs/BUSINESS_ALIAS_MAP.md)
- [Reconstruction README](docs/RECONSTRUCTION_README.md)
- [Reconstruction contract](docs/RECONSTRUCTION_CONTRACT.md)
- [Reconstruction spec](docs/RECONSTRUCTION_SPEC.md)
- [Feature lineage](docs/FEATURE_LINEAGE.md)
- [Feature dictionary](docs/FEATURE_DICTIONARY.md)
- [Runbook](docs/RUNBOOK.md)
