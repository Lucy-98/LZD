# Reconstruction README

Huong dan nay chi cho nhanh data/reconstruction. API, model serving, policy va
downstream Redis representation khong nam trong scope.

## Business alias layer

36 cot selected van giu ten ky thuat `f*` trong dbt/Redis. De bai Lazada voucher
uplift doc co logic nghiep vu, repo them lop alias synthetic tai:

```text
config/features/business_aliases.yml
docs/BUSINESS_ALIAS_MAP.md
```

Vi du:

| Cot | Alias synthetic | Luu y |
|---|---|---|
| `f5` | `product_browse_intensity_365d_ln` | CFS counter, khong phai Lazada fact |
| `f11` | `cart_checkout_intent_365d_ln` | CFS counter, khong phai Lazada fact |
| `f18` | `promo_touch_intensity_365d_log10` | CFS counter, khong phai Lazada fact |
| `f30` | `active_days_30d_log10` | H1 active days / H2 marker scenario |
| `f37` | `price_sensitivity_segment_64_encoded` | T2 attribute alias |
| `f38` | `promo_affinity_segment_241_encoded` | T2 attribute alias |
| `f79..f82` | `preferred_leaf_category_515_enc_*` | mot latent category, bon encoding |

Track A event trong reconstruction la `CFS_WITNESS`. Neu thay `EVT_ORDER_PAID`
trong artifact, doc no la `CFS_RECENCY_MARKER`, khong phai bang chung rang
`f1/f2` that su la order-paid recency cua Lazada.

## Nhieu nhat nen chay cai gi?

Chay dry-run E2E. Dry-run nay kiem:

- build demo `ReconstructionTarget` dung 36 selected features;
- Track A solver sinh CFS witness events;
- chay chinh SQL dbt reconstruction trong DuckDB in-memory;
- Gate A/A-T3/B/C/D/E/F;
- handoff sang `CustomerState(T0)`;
- Track B sinh future business-v2 events tu `reference_ts` tro di;
- provenance va capability boundary.

Dry-run **khong ghi production** vao Kafka, MinIO, Redis hay Postgres.

## Chay nhanh tren host

```powershell
$env:PYTHONPATH="src"
if (-not (Test-Path ".tmp")) { New-Item -ItemType Directory -Force ".tmp" | Out-Null }
$env:TEMP=(Resolve-Path .tmp).Path
$env:TMP=$env:TEMP

python -m lzd_pipeline.reconstruction.e2e
python -m lzd_pipeline.reconstruction.e2e --branch H2
```

Expected output:

```json
{
  "semantic_status": "UNIDENTIFIED",
  "semantic_branch": "H1",
  "track_a_status": "SOLVED",
  "gates": {
    "A": true,
    "A-T3": true,
    "B": true,
    "C": true,
    "D": true,
    "E": true,
    "F": true
  },
  "all_passed": true
}
```

## Chay trong Docker

Core la du cho reconstruction dry-run:

```powershell
.\scripts\stack.ps1 init
.\scripts\stack.ps1 up-core
.\scripts\stack.ps1 health
.\scripts\stack.ps1 reconstruction
.\scripts\stack.ps1 reconstruction H2
```

Neu PowerShell chan script unsigned, dung:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 reconstruction H2
```

Lenh `reconstruction` uu tien chay trong container `airflow-scheduler` neu core
stack dang bat. Neu scheduler chua chay, script fallback sang Docker one-shot
container bang image local co san (`lzd/airflow:2.10.5`, `lzd/dbt-duckdb:dev`,
...):

```powershell
docker compose exec airflow-scheduler python -m lzd_pipeline.reconstruction.e2e
docker run --rm --entrypoint python -v "${PWD}:/opt/project" -w /opt/project `
  -e PYTHONPATH=/opt/project/src lzd/dbt-duckdb:dev `
  -m lzd_pipeline.reconstruction.e2e
```

## Chay bang Airflow

UI: http://localhost:8080, user/pass `admin/admin`.

Trigger DAG manual:

```powershell
docker compose exec airflow-scheduler airflow dags trigger 60_reconstruction_e2e
docker compose exec airflow-scheduler airflow dags list-runs -d 60_reconstruction_e2e
```

Hoac vao Airflow UI, unpause/trigger DAG `60_reconstruction_e2e`.

Task hop le in JSON summary va `all_passed=true`. Neu task fail, xem log task
`run_contract`.

## Snapshot commit vao repo

Reconstruction nguoc chi can chay mot lan cho mot contract/seed. Artifact nho
da duoc commit tai:

```text
docs/reconstruction_snapshot/
```

Nguoi khac pull repo co the mo:

```text
docs/reconstruction_snapshot/report.html
```

va doc cac file:

- `track_a_events.csv`: event Track A duoc sinh nguoc tu target feature.
- `track_b_events.csv`: event realtime/future synthetic tu `CustomerState(T0)`.
- `features_expected_actual.csv`: bang expected vs actual cho 36 selected features.
- `feature_pass.svg`: bieu do pass-count theo tier, render truc tiep tren GitHub.
- `timeline.svg`: timeline Track A/Track B, render truc tiep tren GitHub.
- `summary.json`: ket qua gates A/A-T3/B/C/D/E/F.
- `manifest.json`: config, version va checksum cua artifact.

Sinh lai snapshot khi contract thay doi:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 snapshot
```

Snapshot nay la fixture/review artifact. Production lakehouse/Kafka/Redis state
khong commit vao git.

## Track A tu full_trainset.csv

Neu can sinh raw reconstructed events tu du lieu train that ma khong can Docker,
dung:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 track-a 1000
```

Lenh nay doc `data/full_trainset.csv`, decode 36 selected features, fit T2
encoding map tren tap dang chay, sinh Track A raw events va verify sample qua
chinh dbt SQL runner.

Output runtime nam o:

```text
.tmp/reconstruction_track_a/
```

File quan trong:

- `raw_events_v2.csv`: raw events schema cho `stg_events_v2`.
- `track_a_events_audit.csv`: raw events kem cot audit solver.
- `biz_customer_attribute.csv`: decoded T2 attribute levels.
- `biz_encoding_map.csv`: categorical value encoding map.
- `biz_passthrough_source.csv`: T3 frozen/pass-through values.
- `manifest.json`: row counts, event counts, gate sample.

Chay full train:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 track-a all
```

Full train co 926,669 dong va co the sinh hang chuc trieu event, nen output nam
trong `.tmp/` va khong commit.

## Ket noi va quan sat

| Noi xem | URL / lenh | Xem gi |
|---|---|---|
| Airflow | http://localhost:8080 | DAG `60_reconstruction_e2e`, task log, status |
| Docker logs | `.\scripts\stack.ps1 logs airflow-scheduler` | import DAG, loi dependency, output CLI |
| Grafana | http://localhost:3000 (`admin/admin`) | dashboard Airflow/infra, Loki logs |
| Prometheus | http://localhost:9090 | Airflow scheduler/task metrics |
| MinIO | http://localhost:9001 (`minioadmin/minioadmin123`) | dry-run khong tao `raw/events_v2` production object |
| Kafka UI | http://localhost:8082 | dry-run khong publish topic v2 |
| RedisInsight | http://localhost:5540 | dry-run khong sua key `fs:*` hoac `rt:*` |

Log query huu ich trong Grafana Explore:

```logql
{job="airflow-tasks", dag_id="60_reconstruction_e2e"}
{component="airflow-scheduler"}
```

Prometheus query huu ich:

```promql
sum by (dag_id, state) (increase(airflow_task_instance_finished_total[30m]))
avg by (dag_id) (airflow_dagrun_duration_success_seconds)
```

## Config

Runtime config nam o:

```text
config/reconstruction/runtime.yml
```

Mac dinh:

- `semantic_status: UNIDENTIFIED`;
- `semantic_branch: H1`;
- `reference_ts: 2026-08-01T23:59:59+00:00`;
- `future_days: 2`;
- `generation_seed: 42`.

`semantic_status` la trang thai nhan thuc. `semantic_branch` chi la scenario van hanh.
Khong duoc viet trong bao cao rang `f30` da duoc chung minh semantic that.

## Test

```powershell
.\scripts\stack.ps1 test
```

Hoac chay truc tiep:

```powershell
$env:PYTHONPATH="src"
$env:TEMP=(Resolve-Path .tmp).Path
$env:TMP=$env:TEMP
python -m pytest tests -q -p no:cacheprovider
```

## Troubleshooting

Neu Docker bao `permission denied while trying to connect to docker_engine`, mo Docker
Desktop va chay terminal voi quyen co access Docker daemon.

Neu Docker pull bi loi `x509: certificate signed by unknown authority`, day la loi CA
cua Docker registry tren may local. Reconstruction dry-run van co the chay one-shot
neu co image local phu hop; `stack.ps1 reconstruction` se tu fallback.

Neu `airflow-scheduler` chua co, chay:

```powershell
.\scripts\stack.ps1 up-core
.\scripts\stack.ps1 ps
```

Neu Gate A fail, khong sua target va khong noi tolerance. Xem diff cot trong task log,
vi loi do thuong la SQL/dbt contract hoac event vocabulary bi lech.

Neu MinIO/Kafka/Redis khong thay thay doi sau dry-run, do la dung. Dry-run chi xac nhan
contract; production writer/publisher nam trong migration phase rieng.
