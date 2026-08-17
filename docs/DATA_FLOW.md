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
        `--> feat_user_selected_serving     (55-column Redis sync source)
```

`stg_user_snapshot` cast `f0..f82`, chuẩn hóa entity/time và dedup. Label cùng
`is_treat` không đi vào feature export hoặc reconstruction solver. Runtime không
dựng training dataset vì model được chốt trong notebook và bake vào image.

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
  feat_cfs_reconstructed_selected (55 f)
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
        | bounded demo behaviour policy
        v
SYNTHETIC business events, event_ts >= REFERENCE_TS
        |
        | adapter + AppEvent v1 schema validation
        v
Kafka app.user.events.v1
        |
        v
stream_consumer
        +--> MinIO raw/app_events
        `--> Redis rt:u:{user_id}
                    |
                    v
          POST /campaign/decide
          model score/rank (55F) + rt timing policy
```

Track B không nhận `ReconstructionTarget`, feature payload, label hay treatment.
Nó chỉ nhận state đã qua gate, counters, T2 level và provenance.

Demo map `SESSION_STARTED → app_open`, `PRODUCT_VIEWED → page_view`,
`ITEM_ADDED_TO_CART → add_to_cart`, `PURCHASE_COMPLETED → order`. Chỉ adapter
output hợp lệ mới được publish; Track A `EVT_*` không đi vào topic app. Luồng
này chạy trong Compose project biệt lập và chỉ để demo, không biến event
synthetic thành lịch sử thật hay dữ liệu train.

Scenario v2 cố định theo user identity để có ba tình huống review được: cart
chưa order → SEND nếu thuộc Top-K; chỉ view → WAIT; đã order → SUPPRESS. `rt_*`
không sửa uplift score và response khai rõ `model_score_uses_realtime=false`.

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

```powershell
.\scripts\stack.ps1 up-all
.\scripts\stack.ps1 health
```

Trigger Airflow theo thứ tự:

1. `00_bootstrap_lake`
2. `20_build_features_dbt`
3. `40_sync_features_to_redis` nếu downstream sink được bật
4. `50_data_quality`

Reconstruction dry-run:

```powershell
$env:PYTHONPATH="src"
python -m lzd_pipeline.reconstruction.e2e
python -m lzd_pipeline.reconstruction.e2e --branch H2
```
