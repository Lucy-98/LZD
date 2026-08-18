# Data Flow

> **Scope:** ingest, lake, dbt feature engineering và reconstruction. API,
> policy contract và cách tổ chức key/value trong Redis không thuộc tài liệu này.

## 1. Batch

```text
data/full_trainset.csv + data/full_testset.csv
        |
        | DAG 00: seed_loader
        v
MinIO raw/user_snapshot/dt=.../*.parquet
        |
        | DAG 20: dbt
        v
stg_user_snapshot
        |
        +--> feat_user_behaviour
        +--> feat_user_realtime_pit
        +--> feat_user_serving              (baseline/full mart)
        +--> feat_user_selected_serving     (55-column Redis sync source)
        `--> training_dataset
```

`stg_user_snapshot` cast `f0..f82`, chuẩn hóa entity/time và dedup. Label cùng
`is_treat` chỉ được giữ trong training dataset; chúng không đi vào feature export
hoặc reconstruction solver.

Feature export hiện dùng `feat_user_selected_serving`: chỉ `user_id`, `dt`,
`feature_ts` và 55 cột trong `config/features/fs_2026_08_v2.yaml`. Redis batch
key là `fs:{version}:u:{user_id}`; mỗi hash có 55 selected features + metadata
`_v`, `_ts`, `_feature_set_id`.

## 2. Stream

```text
event_producer
    |
    v
Kafka app.user.events.v1
    |
    v
stream_consumer
    |
    +--> validate fail --> DLQ
    |
    +--> [1] MinIO raw/app_events/*.parquet
    +--> [2] Redis realtime overlay rt:u:{user_id}
    `--> [3] commit Kafka offset
```

Thứ tự `[1] -> [2] -> [3]` là invariant. Nếu ghi lake hoặc cập nhật downstream
state thất bại thì offset không được commit và batch sẽ được đọc lại.

Lake là source of truth. Replay có thể làm downstream realtime counter cộng dư,
nhưng `stg_app_events` dedup theo `event_id` trước khi tính feature offline.

## 3. Reconstruction Track A

```text
immutable 55-column target (train only)
        |
        | decode T1/T2/T3
        v
constraint solver H1 hoặc H2
        |
        v
RECONSTRUCTED event witness, event_ts < REFERENCE_TS
        |
        v
dbt SQL thật
  feat_cfs_counter
  feat_cfs_recency
  feat_cfs_categorical
  feat_passthrough
        |
        v
Gate A-F
        |
        v
CustomerState(T0)
```

`semantic_status=UNIDENTIFIED` là trạng thái nhận thức. `semantic_branch=H1|H2`
chỉ chọn scenario vận hành; nó không chứng minh semantic thật của `f30`.

## 4. Future Simulation Track B

```text
CustomerState(T0)
        |
        | rule-based behaviour model
        v
SYNTHETIC future events, event_ts >= REFERENCE_TS
        |
        v
Kafka app.user.events.v2 -> consumer -> MinIO raw/events_v2
```

Track B không nhận `ReconstructionTarget`, feature payload, label hay treatment.
Nó chỉ nhận state đã qua gate, counters, T2 level và provenance.

Track B event vocabulary là business v2. Không publish Track B vào consumer v1 hiện tại;
v1 chỉ nhận `app_open`, `page_view`, `search`, `add_to_cart`, `checkout`, `order`,
`voucher_view`, `voucher_claim`. Production Track B cần topic/schema/consumer v2 trước.

## 5. Storage Ownership

| Layer | Owner | Vai trò |
|---|---|---|
| CSV | source dataset | read-only reference |
| MinIO raw | ingestion | immutable event/snapshot history |
| DuckDB/dbt | transformation | staging, feature marts, training dataset |
| PostgreSQL `biz` | reconstruction control plane | target, run, provenance, diff, state |
| Downstream state sink | downstream owner | representation và access contract ngoài scope |

## 6. Chạy

Pipeline chính:

```bash
make up-all
make health
```

Trigger Airflow theo thứ tự:

1. `00_bootstrap_lake`
2. `20_build_features_dbt`
3. `40_sync_features_to_redis` nếu downstream sink được bật
4. `50_data_quality`

Reconstruction dry-run:

```bash
PYTHONPATH=src python3 -m lzd_pipeline.reconstruction.e2e
PYTHONPATH=src python3 -m lzd_pipeline.reconstruction.e2e --branch H2
# hoặc: ./scripts/stack.sh reconstruction H2
```
