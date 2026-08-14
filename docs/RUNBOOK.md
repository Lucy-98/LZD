# Runbook

> **Scope:** Kafka, MinIO, dbt/DuckDB, reconstruction và data-quality operations.
> API cùng downstream state operations do downstream owner vận hành.

## Khởi động

```powershell
.\scripts\stack.ps1 up-all
.\scripts\stack.ps1 health
.\scripts\stack.ps1 status
```

Airflow: http://localhost:8080 (`admin/admin`).

Reconstruction dry-run chi can core stack. Neu observability image bi loi pull,
dung:

```powershell
.\scripts\stack.ps1 up-core
```

## Bootstrap không có dữ liệu

Triệu chứng: raw snapshot rỗng hoặc dbt source không tìm thấy parquet.

1. Trigger `00_bootstrap_lake`.
2. Kiểm log task `load_csv_to_lake`.
3. Xác nhận object xuất hiện dưới `raw/user_snapshot/` trong MinIO.
4. Trigger `20_build_features_dbt`.

Không sửa trực tiếp CSV hoặc parquet để làm test pass.

## Stream không có event

```powershell
.\scripts\stack.ps1 logs event-producer
.\scripts\stack.ps1 logs stream-consumer
docker compose exec kafka kafka-consumer-groups `
  --bootstrap-server kafka:29092 `
  --group lzd-feature-consumer `
  --describe
```

Kiểm theo thứ tự:

1. Producer có publish vào `app.user.events.v1`.
2. Consumer không đẩy event hợp lệ vào DLQ.
3. MinIO có object mới dưới `raw/app_events/`.
4. Offset chỉ tăng sau khi object MinIO đã ghi thành công.

Nếu consumer chết sau khi ghi MinIO nhưng trước commit, event sẽ được đọc lại. Đây
là at-least-once bình thường; dbt phải dedup theo `event_id`.

## dbt thất bại

```powershell
docker compose exec airflow-scheduler bash -lc `
  "cd /opt/project/dbt && dbt debug --no-version-check"
docker compose exec airflow-scheduler bash -lc `
  "cd /opt/project/dbt && dbt run --no-version-check"
docker compose exec airflow-scheduler bash -lc `
  "cd /opt/project/dbt && dbt test --no-version-check"
```

Kiểm MinIO credential, source path, timezone UTC và DuckDB writer lock. Không mở
DuckDB write từ notebook trong khi DAG dbt đang chạy.

## Feature sync Redis

DAG `40_sync_features_to_redis` đọc `marts.feat_user_selected_serving`, không đọc
full `feat_user_serving`. Redis batch key:

```text
fs:{version}:u:{user_id}
```

Hash này chỉ chứa 55 selected features của `fs_2026_08_v2` và metadata `_v`, `_ts`,
`_feature_set_id`. Con trỏ publish:

```text
fs:meta:active_version
```

Realtime overlay từ stream consumer dùng:

```text
rt:u:{user_id}
```

Nếu thấy Redis batch có `f0` hoặc đủ `f0..f82`, đó là contract cũ hoặc version cũ;
không dùng làm nguồn reconstruction.

## Reconstruction dry-run

```powershell
$env:PYTHONPATH="src"
python -m lzd_pipeline.reconstruction.e2e
python -m lzd_pipeline.reconstruction.e2e --branch H2

# Trong Docker
.\scripts\stack.ps1 reconstruction
.\scripts\stack.ps1 reconstruction H2
```

Kết quả hợp lệ phải có:

- `semantic_status = UNIDENTIFIED`;
- branch đúng với config/CLI;
- Track A `SOLVED`;
- Gate A, A-T3, B, C, D, E, F đều `true`;
- Track B có future events và generation lineage riêng.

Nếu Gate A fail, xem column diff. Không nới tolerance và không sửa target.

Chi tiết kết nối/quan sát: `docs/RECONSTRUCTION_README.md`.

## Reconstruction trên Postgres cũ

DDL nằm tại `sql/postgres/02_biz_reconstruction.sql`. Init script chỉ tự chạy khi
Postgres volume mới được tạo. Với volume cũ, chạy migration DDL có kiểm soát trước
khi materialize target hoặc provenance.

## Test host

```powershell
pip install -r requirements-dev.txt
$env:PYTHONPATH="src"
$env:TEMP=(Resolve-Path .tmp).Path
$env:TMP=$env:TEMP
python -m pytest tests -q -p no:cacheprovider
```

## Không làm

- Không commit Kafka offset trước khi ghi MinIO.
- Không dùng split `test` làm nguồn reconstruction.
- Không đưa `label` hoặc `is_treat` vào solver/Track B.
- Không sửa immutable target để Gate A pass.
- Không diễn giải witness là lịch sử thật.
- Không thiết kế API hoặc Redis representation trong runbook này.
