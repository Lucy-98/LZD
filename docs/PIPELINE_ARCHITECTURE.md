# Pipeline Architecture — Stack, Data Flow & Reconstruction Plan

> **Mục đích:** thống nhất **kiến trúc công nghệ** và **cách dữ liệu chảy**, trước khi
> viết bất kỳ solver nào.
>
> **Trạng thái:** Phần 1–6 mô tả hệ thống **đang chạy** (`[FACT]`, đọc từ code/compose).
> Phần 7–17 mô tả **Feature-consistent Event Reconstruction**: E2E dry-run đã có
> trong repo; materialize full dataset và publish production vẫn là roadmap.
>
> 🚫 Thuật ngữ "reverse-data" **đã bị bỏ** — xem §7.1. Reconstruction target là
> **55 cột**, không phải 83.
>
> Nhãn: `[FACT]` từ code/schema · `[MEASURED]` từ query · `[ASSUMPTION]` giả định ·
> `[UNKNOWN]` chưa đủ evidence · `[PLAN]` chưa tồn tại.

---

## Mục lục

1. [Technology stack](#1-technology-stack)
2. [Storage topology](#2-storage-topology)
3. [Hai dòng dữ liệu](#3-hai-dòng-dữ-liệu)
4. [Orchestration](#4-orchestration)
5. [Contracts giữa các thành phần](#5-contracts-giữa-các-thành-phần)
6. [Điểm mạnh & điểm yếu của kiến trúc hiện tại](#6-điểm-mạnh--điểm-yếu-của-kiến-trúc-hiện-tại)
7. [Terminology & epistemic status](#7-terminology--epistemic-status)
8. [Provenance model](#8-provenance-model--chống-feedback-contamination)
9. [Temporal canonicalization](#9-temporal-canonicalization)
10. [Immutable target layer](#10-immutable-target-layer)
11. [Nhánh reconstruction cắm vào đâu](#11-nhánh-reconstruction-cắm-vào-đâu)
12. [Dòng dữ liệu mục tiêu](#12-dòng-dữ-liệu-mục-tiêu-reconstruction--future-simulation)
13. [Semantic identification](#13-semantic-identification--thay-cho-chốt-nhánh)
14. [Gates A–F](#14-gates--sáu-cổng-không-phải-một)
15. [Roadmap R0–R12](#15-roadmap)
16. [Điều cấm](#16-điều-cấm-xuyên-suốt)
17. [Tóm tắt thay đổi](#17-tóm-tắt-thay-đổi-so-với-bản-trước)

---

## 1. Technology stack

`[FACT]` — đọc từ `docker-compose.yml`, `dbt/profiles.yml`, `requirements`.

| Vai trò | Công nghệ | Version | Profile | Ghi chú |
|---|---|---|---|---|
| **Message bus** | Confluent Kafka | `cp-kafka:7.6.1` | `core` | KRaft, không ZooKeeper |
| **Data lake** | MinIO (S3-compatible) | `RELEASE.2024-10-13` | `core` | bucket `lakehouse` |
| **Warehouse** | **DuckDB** qua `dbt-duckdb` | — | — | file `/opt/lakehouse/warehouse.duckdb` |
| **Transformation** | dbt | project `lzd_uplift` | — | staging (view) → marts (table) |
| **Downstream feature sink** | Redis | `7.2-alpine` | `core` | representation ngoài scope |
| **Metadata / ops** | PostgreSQL | `16-alpine` | `core` | DB `airflow` + DB `pipeline` |
| **Orchestration** | Apache Airflow | `2.10.5` | `core` | 8 DAG |
| **Model registry** | MLflow | `2.16.2` | `ml` | artifact store = MinIO |
| **Metrics** | Prometheus + Pushgateway + statsd-exporter | `2.54.1` | `obs` | + redis/postgres/kafka exporter |
| **Logs** | Loki + Promtail | `3.1.1` | `obs` | |
| **Dashboard** | Grafana | `11.2.2` | `obs` | 5 dashboard provisioned |
| **UI phụ** | Kafka-UI | — | `obs` | |

### 1.1 · Quyết định kiến trúc đáng chú ý

> **DuckDB đóng vai BigQuery, đọc parquet TRỰC TIẾP từ MinIO — không copy dữ liệu.**

`[FACT]` `dbt/profiles.yml` bật extension `httpfs` + `parquet`, trỏ `s3_endpoint` vào
MinIO. `_sources.yml` khai báo `external_location: s3://lakehouse/raw/**/*.parquet`.

⇒ Tầng `raw` **không nằm trong warehouse**. dbt đọc nó như external table, giống hệt
BigQuery đọc external table trên GCS. Đây là mô phỏng đúng kiến trúc lakehouse, không
phải shortcut.

**Hệ quả:** thêm dữ liệu vào lake = ghi parquet vào MinIO, **không** cần load job.
Điều này rất quan trọng cho nhánh reconstruction (§7).

### 1.2 · Profiles — bật từng phần

```
core     postgres · redis · minio · kafka · airflow
stream   event-producer · stream-consumer
ml       mlflow
obs      prometheus · grafana · loki · exporters
all      tất cả
```

---

## 2. Storage topology

### 2.1 · MinIO — data lake

```
s3://lakehouse/
├── raw/
│   ├── user_snapshot/dt=YYYY-MM-DD/{train,test}.parquet     ← từ CSV, 1 lần
│   └── app_events/dt=YYYY-MM-DD/hour=HH/part-*.parquet      ← stream-consumer ghi
└── (mlflow artifacts)
```

### 2.2 · DuckDB — warehouse

```
warehouse.duckdb
├── staging/          (VIEW)
│   ├── stg_user_snapshot     ← cast f0..f82 + dedup theo (user_id, dt)
│   └── stg_app_events        ← dedup theo event_id, tính gmv = price × quantity
├── marts/            (TABLE)
│   ├── feat_user_behaviour       ← hist_* từ event, cửa sổ 30 ngày
│   ├── feat_user_realtime_pit    ← rt_* point-in-time, ô 5 phút
│   ├── feat_user_serving         ← baseline/full mart: f0..f82 + hist_*
│   ├── feat_user_selected_serving← 55 selected cột  ← BẢNG ĐƯỢC SYNC
│   └── training_dataset          ← serving + rt_* + label + is_treat + split
└── dq_failures/      ← dbt test store_failures
```

### 2.3 · Downstream feature sink — ngoài scope

Redis hiện tồn tại trong runtime và nhận batch/realtime state. Batch state dùng
`fs:{version}:u:{user_id}` và chỉ chứa 55 selected features của
`fs_2026_08_v2`; `fs:meta:active_version` là con trỏ atomic. Realtime overlay dùng
`rt:u:{user_id}` với các field bucket 5 phút. API/policy và representation ngoài
hai key boundary này vẫn thuộc downstream owner.

### 2.4 · PostgreSQL — ops / audit

```
DB airflow      metadata của Airflow
DB pipeline
└── schema ops
    ├── pipeline_run          1 dòng / lần chạy stage
    ├── feature_sync_audit    1 dòng / feature version
    ├── feature_sync_shard    chi tiết shard → rerun được shard FAILED
    ├── dq_result             kết quả data quality
    └── v_latest_sync, v_dq_last_24h   (view cho Grafana)
```

---

## 3. Hai dòng dữ liệu

Phạm vi data pipeline gồm batch và stream. Lớp API/serving không thuộc tài liệu này.

### 3.1 · Flow A — BATCH (snapshot → feature table)

```
data/full_trainset.csv (926,669)          [FACT] 86 cột
data/full_testset.csv  (181,669)
        │
        │  seed_loader.load_csv_to_lake()          ← DAG 00, chạy tay 1 lần
        │  CHỈ: data_id → user_id, gắn split/dt/feature_ts
        ▼
s3://lakehouse/raw/user_snapshot/dt=…/{train,test}.parquet
        │
        │  stg_user_snapshot.sql                   ← DAG 20, 01:00 hằng ngày
        │  CHỈ: cast(f{i} as double) + dedup
        ▼
feat_user_selected_serving.sql
        55 selected features từ fs_2026_08_v2
        T1: f1 f2 f5 f11 f18 f30
        T2: f37 f38 f79 f80 f81 f82 f40 f43 f44 f45 f64 f68
        T3: f3 f4 f8 f9 f10 f12 f13 f16 f20 f21 f22 f23
            f25 f26 f28 f29 f31 f35
        │
        └──► Redis fs:{version}:u:{user_id}
```

### 3.2 · Flow B — STREAM (event → lake → realtime state)

```
event_producer.py                          ← profile `stream`
   _make_session() sinh event ngẫu nhiên theo SESSION_FLOW
   _pick_user()    U0000000..U0019999   ⚠ pool 20,000 (xem §6.2)
        │
        │  Kafka  topic app.user.events.v1   key = user_id
        ▼
stream_consumer.py
   poll → validate → [1] ghi parquet MinIO → [2] cập nhật downstream state
                         │                         │
                         ▼                         ▼
              s3://lakehouse/raw/app_events/   downstream sink
                 dt=…/hour=…                   (representation ngoài scope)
                         │
                         │
                         │ DAG 10 @hourly: đăng ký partition mới
                         │ DAG 20: stg_app_events (dedup event_id)
                         ▼
              feat_user_behaviour  +  feat_user_realtime_pit
                         │
                         └──────► marts.training_dataset

   ──✖──► app.user.events.dlq   (validate fail, theo reason)
```

**At-least-once:** commit offset **sau cùng** ⇒ chết giữa chừng thì đọc lại batch.
Lake dedup theo `event_id` ở `stg_app_events`. Downstream realtime state có thể cộng
dư khi consumer replay, nên chỉ là tín hiệu gần đúng; lake vẫn là source of truth.

---

## 4. Orchestration

`[FACT]` — 8 DAG:

| DAG | Schedule | Vai trò |
|---|---|---|
| `00_bootstrap_lake` | **manual** | Nạp CSV → lake. Chạy 1 lần |
| `10_ingest_stream_to_lake` | `@hourly` | Đăng ký partition event mới |
| `20_build_features_dbt` | `0 1 * * *` | dbt run + dbt test |
| `40_sync_features_to_redis` | `30 1 * * *` | Export marts sang downstream sink |
| `30_train_uplift_model` | `0 3 * * 1` | Train hằng tuần (thứ Hai) |
| `50_data_quality` | `*/30 * * * *` | DQ check → `ops.dq_result` |
| `60_reconstruction_e2e` | **manual** | Dry-run Track A → dbt SQL → Gate A-F → T0 → Track B |
| `99_ops_toolbox` | **manual** | Công cụ vận hành |

### 4.1 · ⚠ Phụ thuộc theo THỜI GIAN, không theo SENSOR

```
DAG 20 (01:00) ────30 phút────► DAG 40 (01:30)
```

`[FACT]` DAG 40 chạy lúc 01:30 với comment *"sau khi DAG 20 xong"* — nhưng đây là
**time-coupling**, không phải data-dependency thật. Nếu DAG 20 chạy quá 30 phút, DAG 40
sẽ sync **feature cũ** mà không báo lỗi.

**Đề xuất `[PLAN]`:** thay bằng `ExternalTaskSensor` hoặc Airflow Dataset. Rẻ, sửa được
ngay, không ảnh hưởng gì khác.

---

## 5. Contracts giữa các thành phần

### 5.1 · Feature contract

```
          config/features/feature_spec.yml
              version · entity · features · quality
                         │
              ┌──────────┴──────────┐
              ▼                     ▼
         dbt marts            export validation
         build columns        validate columns
```

`[FACT]` Đổi feature ⇒ sửa file này ⇒ bump `version` ⇒ các data component tự align.
`validate_columns()` **fail** nếu lệch.

> ⚠️ **Đây là DATA CONTRACT, không phải semantic source of truth.** Nó đủ để kiểm
> **cột có khớp nhau không**, **không** đủ để reconstruct. Ba thứ khác nhau:
>
> ```
> Column contract           ← feature_spec.yml làm được
> Transformation semantics  ← nằm trong dbt SQL
> Reconstruction semantics  ← `fs_2026_08_v2.yaml` + runtime branch config + dbt SQL
> ```

### 5.2 · Event schema — hợp đồng app ↔ data

`[FACT]` `schemas.py`: đổi field ⇒ **đổi version topic** (`v1 → v2`), không sửa tại chỗ.

### 5.3 · Cửa sổ realtime — hợp đồng aggregate

`[FACT]` Cả hai bên **cùng làm tròn về ô 5 phút**:

```
offline:  feat_user_realtime_pit.sql   floor(epoch(feature_ts)/300)*300
runtime:  online_store.rt_bucket_start()  ← implementation downstream hiện hữu
```

Comment trong SQL ghi rõ: nếu offline dùng `feature_ts - interval '1 hour'` thì hai bên
**lệch tới 5 phút dữ liệu**. Đây là chống-skew có chủ đích.

### 5.4 · Ranh giới label

`[FACT]` `feat_user_selected_serving.sql` **không** select `label`/`is_treat`;
`offline_store.iter_shard()` chỉ đọc `entity_key + batch_names`.
⇒ **Label không bao giờ rời training dataset sang downstream feature export.**
🚫 **Không được phá khi migrate.**

---

## 6. Điểm mạnh & điểm yếu của kiến trúc hiện tại

### 6.1 · Giữ nguyên — đã đúng chuẩn production

| Cơ chế | File |
|---|---|
| Chống skew cửa sổ realtime (ô 5 phút hai phía) | `feat_user_realtime_pit.sql` + `online_store.py` |
| Point-in-time join (`event_ts < feature_ts`) | `feat_user_realtime_pit.sql` |
| Feature spec làm hợp đồng cột | `spec.py` + `feature_spec.yml` |
| At-least-once + dedup theo `event_id` | `stream_consumer.py` + `stg_app_events.sql` |
| Idempotent sharded sync (`is_shard_done()`) | `sync.py` |
| Ranh giới label | `feat_user_selected_serving.sql`, `offline_store.py` |

### 6.2 · Cần sửa

| # | Vấn đề | Mức | Ở đâu |
|---|---|---|---|
| **A1** | `rt_session_len_sec`: offline tính thật, runtime state **không bao giờ ghi** ⇒ aggregate skew | 🔴 | `stream_consumer._update_realtime()` |
| **A2** | `hist_*` dùng `now()` lúc dbt build thay vì `feature_ts` ⇒ không point-in-time | 🟠 | `feat_user_behaviour.sql` |
| **A3** | `user_tenure_days` bị chặn ở 30 (lọc cửa sổ trước khi `min()`) | 🟠 | `feat_user_behaviour.sql` |
| **A4** | `voucher_used_30d` tên nói "used", công thức đếm **claim** trong legacy/full mart; không thuộc selected Redis sync v2 | 🟠 | `feat_user_behaviour.sql` |
| **A5** | Producer pool 20,000 user vs 1,108,338 đã seed ⇒ ~98% không bao giờ nhận event | 🟠 | `event_producer._pick_user()` |
| **A6** | DAG 20→40 coupling theo thời gian, không theo sensor | 🟡 | `dag_40` |
| **A7** | Model chưa tồn tại — `NotImplementedError` ×5, đang chạy `StubModel` | 🔴 | `train.py`, `model_loader.py` |
| **A9** | Không có Layer 1 (business entity) — event sinh từ `random()` | 🔴 | `event_producer.py` |

---

## 7. Terminology & epistemic status

> ⚠️ **Thuật ngữ "reverse-data" đã bị bỏ.** Nó ngụ ý khôi phục lịch sử thật — điều
> project **không** làm và **không thể** làm.

### 7.1 · Tên đúng

```
   Feature-consistent Event Reconstruction
   (constraint-based event reconstruction)
```

Solver **không** khôi phục sự kiện đã xảy ra. Nó tìm **một event history nhân chứng**
(*witness*):

```
   Events → Features   là MANY-TO-ONE
   ⇒ nghịch ảnh của F là một LỚP TƯƠNG ĐƯƠNG, không phải một điểm

   E₁ ┐
   E₂ ┤
   E₃ ┼──► F          solver chọn MỘT phần tử E* trong lớp đó
   …  ┤
   Eₙ ┘
```

### 7.2 · Bốn mức khẳng định — không được nhầm

| Mức | Nội dung | Chứng minh được? |
|---|---|---|
| **L1 · Feature equivalence** | `FeatureEngine(E*) == F_target` | ✅ **GATE A** |
| **L2 · Constraint validity** | `E*` thoả mọi ràng buộc nghiệp vụ + thời gian | ✅ **GATE B, C** |
| **L3 · Behavioral plausibility** | `P(E* \| customer context)` đủ cao | ⚠️ **GATE F** (yếu, chỉ sanity) |
| **L4 · Historical fidelity** | `E* == chuỗi event lịch sử thật` | ❌ **KHÔNG BAO GIỜ** — dataset không chứa event history gốc |

> **GATE A pass chỉ có nghĩa:** *"Tồn tại một event history hợp lệ sinh ra feature target."*
> 🚫 **Không** được diễn giải thành *"đã khôi phục lịch sử thật."*

### 7.3 · Ba loại "truth" phải tách bạch

```
   OBSERVED          f0..f82 trong CSV        ← bất biến, không sửa
   RECONSTRUCTED     E* do solver sinh        ← witness, không phải sự thật
   SYNTHETIC         hành vi tương lai Track B ← mô phỏng, không ràng buộc F
```

Trộn ba loại này là con đường ngắn nhất tới một **vòng lặp tự xác nhận chính nó**.
§8 là cơ chế chống điều đó.

---

## 8. Provenance model — chống feedback contamination

`[PLAN]` — `track=A/B` **không đủ**.

### 8.1 · Rủi ro cụ thể

```
   model → decision → Track B event → lake → training data → model
                                                                │
                                              ★ CONTAMINATION ──┘
```

Nếu event do chính model sinh ra quay lại tập train mà không phân biệt được, model học
lại chính đầu ra của nó.

### 8.2 · Trường provenance bắt buộc — trên MỌI event và MỌI feature row

```
provenance
├── source_type            REAL | RECONSTRUCTED | SYNTHETIC
├── generation_run_id      UUID của lần chạy sinh ra nó
├── parent_run_id          run trước đó trong chuỗi (null nếu gốc)
├── parent_entity_id       customer/target sinh ra nó
├── parent_feature_version version feature store lúc sinh
└── created_at
```

### 8.3 · Luật sử dụng

| Mục đích | `source_type` được phép |
|---|---|
| Train model production | **`REAL` only** |
| Đánh giá RCT (test set) | **`REAL` only**, không đụng |
| GATE A validation | `RECONSTRUCTED` |
| Kịch bản future simulation | `RECONSTRUCTED` (seed) + `SYNTHETIC` (live) |
| Train model **thí nghiệm** | được, nhưng **phải** ghi `parent_run_id` và không được đăng ký alias `Production` |

> **Assertion:** một dataset train không được chứa đồng thời `SYNTHETIC` và
> `parent_run_id` trỏ về model đang được train. Đây là vòng lặp — fail cứng.

---

## 9. Temporal canonicalization

`[FACT]` — **`feature_ts` hiện đang bị dùng cho quá nhiều nghĩa, và tệ hơn: nó là artifact.**

`seed_loader.py` gán `now() AS feature_ts` — tức là **thời điểm chạy loader**, không
liên quan gì tới thời điểm Lazada thực sự tính feature.

### 9.1 · Bốn mốc thời gian, phải tách tên

```
   event_ts          thời điểm sự kiện XẢY RA (client)
        │
        ▼
   observation_ts    thời điểm hệ thống NHÌN THẤY nó (= ingested_at)
        │
        ▼
   feature_ts        MỐC CHỐT của feature snapshot — biên point-in-time
        │
        ▼
   state_ts          thời điểm trạng thái T0 được bàn giao
```

### 9.2 · Assumption phải ghi rõ

```
[ASSUMPTION]  feature_ts == state_ts  trong kịch bản T0
```

Trong dry-run hiện tại hai cái này trùng nhau. Nhưng chúng **không đồng nhất về khái
niệm**: production có thể bàn giao state sau thời điểm feature snapshot.

### 9.3 · Sửa bắt buộc `[PLAN]`

```
🚫 feature_ts = now()          ← artifact, không tái lập được
✅ feature_ts = REFERENCE_TS   ← khai báo tường minh cho mỗi generation_run,
                                  lưu trong biz.generation_run
```

Không có cái này thì **rerun cho kết quả khác** và `event_ts < feature_ts` neo vào một
điểm tuỳ tiện.

---

## 10. Immutable target layer

`[PLAN]` — chèn một biên giữa CSV và solver.

```
data/full_trainset.csv          ← read-only tuyệt đối
        │
        ▼
biz.reconstruction_target       ← IMMUTABLE
   target_id
   lzd_user_id
   selected_feature_set_id      ← vd "fs_2026_08_v2"
   feature_version
   reference_ts                 ← §9.3, KHÔNG phải now()
   <55 SELECTED COLUMNS>        ← ★ CHỈ 55, KHÔNG phải f0..f82
   split                        ← chỉ 'train'
   target_hash                  ← canonical form: RECONSTRUCTION_SPEC.md §6.1
   created_at
```

> 🚫 **Target là 55 cột, không phải 83.** 28 cột còn lại **không** thuộc reconstruction
> contract, trừ khi là dependency bắt buộc để tính ra một trong 55 (hiện **không có**
> trường hợp nào — `RECONSTRUCTION_SPEC.md` §1.2).
>
> ⚠️ `target_hash` dùng để phát hiện **target bị sửa**, **KHÔNG** dùng làm phép so
> feature equality — regime `LN` lệch bit cuối ở ~7% dòng dù reconstruction đúng
> (`RECONSTRUCTION_SPEC.md` §6.2).

### 10.1 · Vì sao cần

| Lý do | |
|---|---|
| **Solver không bao giờ chạm CSV** | Một lớp chắn vật lý, không phải lời hứa |
| **Audit được** | So `target_hash` vs `reconstructed_hash` + `generation_run_id` |
| **Rerun ổn định** | `feature_ts` đóng băng trong target, không phải `now()` |
| **Phát hiện sửa lén** | `target_hash` đổi ⇒ có người đụng vào target ⇒ fail cứng |

### 10.2 · Invariant tối cao của project

```
        IMMUTABLE TARGET
               │
               ▼
        Reconstruction  →  Event Witness  →  Feature Engine
                                                   │
                                                   ▼
                                          Reconstructed F
                                                   │
                                                   ▼
                                    COMPARE TO IMMUTABLE TARGET
```

> 🚫 **Không component nào được phép sửa target để reconstruction pass.**

---

## 11. Nhánh reconstruction cắm vào đâu

`[PLAN]` — **prototype đã có**, nhưng phần này vẫn là roadmap cho production integration. Nguyên tắc: **additive, không phá gì đang chạy.**

### 11.1 · Thành phần mới

```
┌─ MỚI ────────────────────────────────────────────────────┐
│ PostgreSQL DB `pipeline`, schema `biz`                    │
│    customer · customer_opaque · customer_identity_map     │
│    encoding_map_515 / _64 / _241 · generation_run         │
│    reject_customer · reconstruction_diff                  │
└───────────────────────────────────────────────────────────┘

┌─ MỚI ────────────────────────────────────────────────────┐
│ MinIO:  s3://lakehouse/raw/events_v2/track={A,B}/dt=…/    │
└───────────────────────────────────────────────────────────┘

┌─ MỚI ────────────────────────────────────────────────────┐
│ dbt: stg_events_v2 · feat_cfs_counter · feat_cfs_recency  │
│      feat_cfs_categorical · feat_passthrough              │
└───────────────────────────────────────────────────────────┘
```

> ⚠️ **Xung đột tên cần chốt.** `biz.customer_opaque` trong tài liệu này nghĩa là
> **24 cột T3 pass-through**, **không** phải "opaque identity cho privacy". Đề xuất
> đổi tên cho hết mơ hồ:
>
> | Cũ | Mới | Nghĩa |
> |---|---|---|
> | `biz.customer` | `biz.synthetic_customer` | thực thể latent **synthetic**, **không** phải business entity thật |
> | `biz.customer_opaque` | `biz.customer_passthrough_vector` | 24 cột T3, không diễn giải |
> | — | `biz.reconstruction_target` | §10, immutable |

### 11.2 · Vì sao `events_v2` là path RIÊNG, không ghi chung `app_events`

| Lý do | Chi tiết |
|---|---|
| Provenance khác | Track A sinh từ solver ràng buộc; `app_events` sinh từ `random()` |
| Ranh giới thời gian khác | Track A **toàn bộ** trước `feature_ts`; Track B từ `feature_ts`/`reference_ts` trở đi |
| Không phá cái đang chạy | `app_events` + Flow B tiếp tục hoạt động y nguyên |
| Partition `track=` cho phép union có kiểm soát | `stg_events_v2` union A+B kèm cột `track` |

⇒ Tuân đúng nguyên tắc M1 (additive trước, destructive sau).

Partition đầy đủ (kèm provenance §8):

```
s3://lakehouse/raw/events_v2/source_type={REAL,RECONSTRUCTED,SYNTHETIC}/
                             run={generation_run_id}/dt=YYYY-MM-DD/
```

### 11.3 · Vì sao thêm dữ liệu vào lake là RẺ

`[FACT]` §1.1 — dbt đọc parquet trực tiếp qua `read_parquet()` trên external location.
⇒ Ghi parquet vào MinIO là **xong**, không cần load job, không cần đổi warehouse.

Đây là lý do kiến trúc hiện tại **chịu được** nhánh reconstruction mà gần như không phải
sửa gì ở tầng hạ tầng.

### 11.4 · Cái gì KHÔNG đổi

```
✅ Cơ chế ô 5 phút cho rt_*
✅ Ranh giới label
✅ Flow B (producer v1) chạy song song
✅ Toàn bộ observability stack
```

⚠️ **`feature_spec.yml` là CONTRACT, không phải SOURCE OF TRUTH đầy đủ.** Nó khai
báo `name/dtype/default/group` — **chưa đủ để reconstruct**. Phải mở rộng schema:

```yaml
hist_order_cnt_30d:
  dtype: int
  default: 0
  provenance:                    # ★ THIẾU trong spec hiện tại
    source_events:  [ORDER_PAID]
    aggregation:    COUNT
    window:         30d
    timestamp_semantics: "PIT tại feature_ts, không phải now()"
    reconstruction_tier: —       # T1/T2/T3, chỉ áp dụng cho f*
    lineage_ref:    feat_user_behaviour.sql
```

Không có khối `provenance`, spec không mô tả đủ để tái tạo — nó chỉ đủ để **kiểm tra
cột khớp nhau**.

### 11.5 · Track A / Track B — biên giới chia sẻ

> Bản trước viết *"không dùng chung code path"* — **quá thô**, sẽ đẻ ra duplication.

```
                    Event primitives (SHARED)
                             │
              ┌──────────────┴──────────────┐
              ▼                             ▼
         Track A                       Track B
      reconstruction                  simulation
```

| ✅ **Chia sẻ** | 🚫 **Không chia sẻ** |
|---|---|
| `EventSchema` / envelope v2 | event selection logic |
| Timestamp validation | constraint solver |
| Serialization + parquet writer | behaviour model |
| `event_id` generation (`uuid5`) | **quyền truy cập target** |
| Partitioning + provenance stamping | generation policy |
| Kafka producer | |

Biên giới thực thi bằng **chữ ký hàm**:

```
backfill_solver(target: ReconstructionTarget, ref_ts) -> Iterator[Event]
        # KHÔNG nhận behaviour_model

live_generator(state: CustomerState, behaviour_model, t_from, t_to) -> Iterator[Event]
        # KHÔNG nhận ReconstructionTarget
```

---

## 12. Dòng dữ liệu mục tiêu (reconstruction + future simulation)

```
   data/full_trainset.csv   [split = 'train' ONLY]
            │
            │  ① DECODE  (T1 counter đích · T2 level id · T3 pass-through)
            ▼
   ┌──────────────────────────────────────────────────┐
   │  Postgres  biz.customer / customer_opaque        │
   │            encoding_map_* / identity_map         │
   └────────────────────┬─────────────────────────────┘
                        │  ② SOLVER ràng buộc  (Track A)
                        │     event_ts < feature_ts
                        ▼
   ┌──────────────────────────────────────────────────┐
   │  MinIO  raw/events_v2/track=A/dt=…               │
   └────────────────────┬─────────────────────────────┘
                        │  ③ FEATURE ENGINE THẬT (dbt)
                        ▼
   feat_cfs_counter · feat_cfs_recency · feat_cfs_categorical
                        │
                        ▼
              reconstructed 31 cột (T1+T2)
                        │
              ┌─────────┴──────────┐
              ▼                    ▼
        ④ GATE A            feat_passthrough (24 cột T3)
     so với LZD row                │
              └─────────┬──────────┘
                        ▼
                 CustomerState(T0)
                        │
   ┌────────────────────┴─────────────────────────────┐
   │                                                  │
  │  ⑤ LIVE GENERATOR (Track B) — event_ts >= feature_ts
   │        │                                         │
   │        ▼                                         │
   │   Kafka  app.user.events.v2                      │
   │        │                                         │
   │        ▼                                         │
   │   consumer v2 ───► [1] raw/events_v2/track=B/    │
   │        └────────► [2] downstream state sink      │
   │                       (sau khi ghi raw)          │
   └──────────────────────────────────────────────────┘
```

### 12.1 · Ranh giới thời gian — bất biến toàn hệ

```
        quá khứ  ◄──────── feature_ts (= REFERENCE_TS) ────────►  tương lai
                                │
   Track A (reconstruction)     │     Track B (live)
   event_ts < feature_ts        │     event_ts >= feature_ts
   source_type=RECONSTRUCTED    │     source_type=SYNTHETIC
   ràng buộc: phải khớp F       │     tự do, có behaviour model
```

---

## 13. Semantic identification — thay cho "chốt nhánh"

> ⚠️ **Bản trước đặt R3 = "CHỐT nhánh `¬P1 ∨ ¬P2`". Sai.** Chọn một nhánh khi không có
> evidence là **ép semantic tuỳ ý**. R3 phải là **thí nghiệm phân định**, và được phép
> kết luận **không phân định được**.

### 13.1 · Hai hypothesis

```
H1 =  P1 ∧ ¬P2     f30 đếm distinct active days;  taxonomy KHÔNG đầy đủ (cần free event)
H2 = ¬P1 ∧  P2     f30 KHÔNG phải distinct days;  taxonomy đầy đủ
```

### 13.2 · ★ Phát hiện: H2 làm **tan biến** vấn đề capacity

Nếu `f30` đếm **lớp hành động riêng của nó** (bị cap ở 30), thì lớp đó là lớp thứ **sáu**,
và capacity phải gồm cả `n30`:

```
   capacity_H2 = n_rec + n5 + n11 + n18 + n30   ≥ n30   luôn đúng
```

⇒ Dưới H2, **bất đẳng thức capacity không hề bị vi phạm.** 47.56% **không phải** evidence
chống H2 — nó chỉ là vấn đề **dưới H1**.

> Hệ quả: H2 **không cần** thêm tiên đề nào. Theo Occam, H2 đơn giản hơn H1.

### 13.3 · Evidence ngược lại: trần 30 là **cấu trúc** hay **tuỳ tiện**?

`[MEASURED]` Phân bố đầy đủ của `n30`:

```
 n30    count     %
   1   53,279   5.75%
   5   83,656   9.03%   ← đỉnh
  12   42,983   4.64%  ┐
  13   43,768   4.72%  ├ bình nguyên
  14   43,788   4.73%  ┘
  15   25,203   2.72%   ← ★ SỤT 42% trong MỘT bậc
  …
  28    1,691   0.18%
  29    1,387   0.15%
  30    2,452   0.26%   ← ★ gai, 1.77× so với n=29
```

**Hai tín hiệu cấu trúc mới:**

| # | Quan sát | Đọc |
|---|---|---|
| **E1** | Gai tại `n30 = 30`, nhưng **nhỏ** (0.26% user, 1.77× bin kề) | Chữ ký **censoring**. Cả H1 và H2 đều dự đoán có gai ⇒ **không phân định**. Nhưng gai **nhỏ** nghĩa là rất ít user vượt trần ⇒ dưới H2, cái cap gần như **không ràng buộc gì** ⇒ đặt cap ở 30 là lựa chọn **kỳ quặc**. Dưới H1, trần 30 là **hệ quả tự động** của cửa sổ 30 ngày. ⇒ **evidence YẾU nghiêng về H1** |
| **E2** | Bình nguyên ở `n30 ∈ {12,13,14}` rồi **sụt 42%** tại 15 | **`[UNKNOWN]`** — không hypothesis nào giải thích được. Có thể là hỗn hợp hai quần thể, hoặc một cửa sổ phụ 14 ngày. **Phải điều tra ở R3** |

### 13.4 · Thiết kế R3 — Semantic Identification Experiment

**Chỉ dùng:** schema · SQL · lineage · feature implementation · dataset algebra.
🚫 **Không dùng:** model importance · SHAP · uplift · prediction score.

| # | Thí nghiệm | Phân định được gì |
|---|---|---|
| **X1** | Giải thích cliff 14→15 (E2) | Nếu tìm ra cửa sổ 14 ngày ⇒ có thể bác cả S-03 (cửa sổ 30) |
| **X2** | So phân bố `n30` với Binomial-mixture(30, p) vs truncated-count | Hình dạng nào khớp hơn |
| **X3** | Kiểm `n30` có bị chặn bởi **bất kỳ** tổ hợp counter nào khác không | Nếu có ⇒ hỗ trợ P2 |
| **X4** | Đối chiếu với `f19` (log10 counter, chưa vào selected set) | `f19` có cùng trần 30 không? Nếu có ⇒ trần 30 là quy ước hệ thống, không phải cửa sổ |
| **X5** | Lặp toàn bộ trên `full_testset.csv` (chỉ đọc) | Cấu trúc có bất biến giữa hai split không |

### 13.5 · Kết cục cho phép

```
H1_SUPPORTED            → chấp nhận PROPOSITION FE-1, solver cần FREE_EVENT class
H2_SUPPORTED            → bác S-03, tìm semantic khác cho f30, capacity không còn vấn đề
SEMANTICALLY_UNIDENTIFIED → ★ xem 13.6
```

### 13.6 · Nếu không phân định được — **không ép solver**

`SEMANTICALLY_UNIDENTIFIED` **không** phải bế tắc. Đường đi tiếp:

```
   Solver được THAM SỐ HOÁ theo nhánh, không hard-code semantic

   solver(target, semantic_branch=H1)  →  pilot 10k  →  GATE A + Model-Level Validation
   solver(target, semantic_branch=H2)  →  pilot 10k  →  GATE A + Model-Level Validation
                                                              │
                                                              ▼
                                              chọn nhánh theo KẾT QUẢ THỰC NGHIỆM
```

Đây là **phân định bằng hệ quả**, không phải bằng sự thật. Kết luận ghi là:

```
[ASSUMPTION]  chọn nhánh <H1|H2> vì Model-Level Validation tốt hơn
              KHÔNG phải vì đã chứng minh semantic đúng
```

---

## 14. Gates — sáu cổng, không phải một

| Gate | Kiểm | Phạm vi |
|---|---|---|
| **A · Feature equality** | `reconstructed == target` theo regime `LN`/`LOG10`/`REC`/`CAT` | 31 cột T1+T2 |
| **B · Temporal validity** | `∀e: event_ts < feature_ts`; Track A ∩ Track B = ∅ | mọi event |
| **C · Constraint validity** | H-1…H-12 của constraint model | mọi user |
| **D · Provenance** | mọi event có `source_type` + `generation_run_id` hợp lệ; không vòng lặp `parent_run_id` | mọi event |
| **E · No leakage** | `label`/`is_treat` vắng mặt ở mọi input của reconstruction | solver + biz.* |
| **F · Distribution sanity** | event/user · session duration · inter-event gap **không vô lý** | mức phân bố |

> **Gate F chỉ là sanity, không phải L3.** Nó bắt được "1 user có 14,245 event trong
> 1 giây", không chứng minh được behavioral plausibility.

---

## 15. Roadmap

```
R0  · Sửa A1–A4  (skew · PIT · contract naming)        ← độc lập, làm ngay
R1  · Model thật + đánh giá trên test RCT nguyên vẹn   ← gỡ A7
R2  · Feature semantic grouping (55 cột → ? tín hiệu, xem SPEC §2.7)
R3  · SEMANTIC IDENTIFICATION EXPERIMENT               ← §13, có thể ra UNIDENTIFIED
R4  · Immutable reconstruction target + REFERENCE_TS   ← §9.3, §10
R5  · Synthetic entity + encoding contract             ← §11.1, round-trip property
R6  · PILOT 10k (phân tầng theo n30; cả hai nhánh nếu UNIDENTIFIED)
R7  · Chốt acceptance / quarantine threshold           ← từ số liệu R6
R8  · Track-A solver
R9  · Gate A–F + provenance audit
R10 · Track-B behaviour model                          ← gỡ A9, cần Business State Layer
R11 · Future-event simulation
R12 · Production integration
```

### 15.1 · Vì sao R10 **không** đi ngay sau R9

> ⚠️ **Sửa phát biểu sai của bản trước.** Tôi đã viết *"Track B không phụ thuộc Track A —
> nó chạy được trên synthetic customer thuần"*. **Sai** — nó gộp **hai loại phụ thuộc
> khác nhau** làm một.

```
   DATA DEPENDENCY          ✅ CÓ
   Track B  ←  CustomerState(T0)  ←  Track A
   Track B bắt đầu từ CHÍNH user thật mà Track A vừa reconstruct.

   VALIDATION DEPENDENCY    ❌ KHÔNG
   Track A pass Gate A  ⇏  Track B hợp lệ.
   Hai bài toán khác nhau:
        Track A:  F      →  E*        →  F'      cần F' ≈ F
        Track B:  State  →  E_future  →  State'  →  Decision  →  E_next
```

Ghép R10 vào ngay sau R9 vì thấy *"A xong rồi"* là nhầm **validation dependency** với
**data dependency**. Track A xác nhận **trạng thái T0 đúng**; nó **không** nói gì về
behaviour model của Track B có hợp lý không.

### 15.1a · ★ Ranh giới quyền truy cập — Track B KHÔNG được thấy target

```
   Track A  CÓ QUYỀN biết:      ReconstructionTarget
   Track B  KHÔNG CÓ QUYỀN biết: ReconstructionTarget
```

Track B **chỉ** nhận `CustomerState(T0)` — trạng thái đã được Track A xác nhận.
Nó **không** reverse feature, **không** dùng canonical target để điều khiển việc sinh
event tương lai.

| | **Track A — Reconstruction** | **Track B — Live generator** |
|---|---|---|
| Input | 55-feature target | `CustomerState(T0)` + behaviour model |
| Output | **một** historical event witness `E*` | luồng event tương lai |
| Bài toán | `F → E* → F'`, cần `F' ≈ F` | `State → E_future → State'` |
| Thời gian | `event_ts < reference_ts` | `event_ts ≥ reference_ts` |
| Tần suất | **một lần** cho mỗi target / generation run | **liên tục** theo thời gian mô phỏng |
| Bản chất | deterministic reconstruction | behaviour simulation |
| **Không phải** | streaming · behaviour generator | reverse feature |

> 🚫 **Track A không phải streaming.** Nó chạy **một lần** để dựng trạng thái T0, xong
> là xong. Mọi mô tả coi nó như nguồn sinh event liên tục đều sai.

### 15.2 · Gate giữa các bước

| Gate | Điều kiện | Nếu fail |
|---|---|---|
| **R0** | Test parity offline↔online xanh cho 7 `rt_*` | Sửa trước khi đi tiếp |
| **R1** | **`do(ΔT1) → Δuplift`** có **dấu và độ lớn hợp lý** — không phải chỉ `importance > 0` | 🔴 Dừng — kịch bản sẽ vô nghĩa |
| **R2** | Importance đã group; `f79`–`f82` tính là **1** tín hiệu | Group lại |
| **R3** | Ra `H1_SUPPORTED` / `H2_SUPPORTED` / `SEMANTICALLY_UNIDENTIFIED` | UNIDENTIFIED ⇒ §13.6, **không** ép chọn |
| **R5** | `encode(decode(x)) == x` trên **toàn** domain hợp lệ | Encoding không reversible ⇒ dừng |
| **R6** | `unsolvable ≤ 5%` dưới nhánh đang thử | `UNSOLVABLE UNDER CURRENT SEMANTIC ASSUMPTIONS` |
| **R9** | Gate A–F pass | Sửa solver, **không** nới dung sai |

### 15.3 · Làm rõ R1 — sensitivity ≠ importance

```
❌ SAI:   feature_importance(T1) > 0
✅ ĐÚNG:  do(T1 = v₁) → uplift₁ ;  do(T1 = v₂) → uplift₂
          |uplift₂ − uplift₁| đủ lớn  VÀ  dấu hợp lý về nghiệp vụ
```

Importance nói feature được **dùng**; sensitivity nói output **dịch chuyển**. Việc đo
model downstream cần cái thứ hai, nhưng API/policy không thuộc scope này.

---

## 16. Điều cấm xuyên suốt

```
🚫 Sửa data/full_*.csv hoặc biz.reconstruction_target để validation pass
🚫 Dùng split='test' làm nguồn event  (RCT là tài sản đánh giá duy nhất, uplift +0.37pp)
🚫 label / is_treat đi vào solver
🚫 Viết feature engine riêng để ép GATE A pass
🚫 Nới dung sai LN từ 1e-15 lên cho "dễ pass"
🚫 Nâng FREE_EVENT từ hypothesis lên primitive khi taxonomy chưa xác nhận
🚫 ÉP chọn nhánh H1/H2 khi R3 ra SEMANTICALLY_UNIDENTIFIED
🚫 Train model production trên event source_type != REAL
🚫 Diễn giải GATE A pass thành "đã khôi phục lịch sử thật"  (L1 ≠ L4, §7.2)
🚫 feature_ts = now()  — phải là REFERENCE_TS khai báo tường minh
```

---

## 17. Tóm tắt thay đổi so với bản trước

| # | Thay đổi | Mục |
|---|---|---|
| 1 | Bỏ thuật ngữ "reverse-data" → **Feature-consistent Event Reconstruction** | §7.1 |
| 2 | Thêm **4 mức khẳng định** L1–L4; GATE A chỉ chứng minh L1+L2 | §7.2 |
| 3 | Thêm **provenance model** REAL/RECONSTRUCTED/SYNTHETIC chống feedback contamination | §8 |
| 4 | Tách **4 mốc thời gian**; phát hiện `feature_ts = now()` là **artifact** | §9 |
| 5 | Thêm **immutable target layer** + `target_hash` | §10 |
| 6 | Đổi tên `biz.customer` → `synthetic_customer` (không phải business entity thật) | §11.1 |
| 7 | `feature_spec.yml` là **contract**, không phải source of truth đủ — cần khối `provenance` | §11.4 |
| 8 | Track A/B **chia sẻ primitive**, không chia sẻ business logic | §11.5 |
| 9 | R3 đổi từ "chốt nhánh" → **semantic identification experiment**, cho phép `UNIDENTIFIED` | §13 |
| 10 | ★ Phát hiện **H2 làm tan biến vấn đề capacity** ⇒ 47.56% không phải evidence chống H2 | §13.2 |
| 11 | ★ Hai tín hiệu mới trong phân bố `f30`: gai nhỏ tại 30 (yếu → H1); **cliff 42% tại 14→15 chưa giải thích được** | §13.3 |
| 12 | GATE A → **6 cổng A–F** | §14 |
| 13 | Roadmap R0–R8 → **R0–R12**; R10 (Track B) tách khỏi R9 | §15 |
| 14 | R1 gate: **`do(ΔT1) → Δuplift`**, không phải `importance > 0` | §15.3 |

---

## Liên quan

`RECONSTRUCTION_CONSTRAINT_MODEL.md` (coupling, solver concept) ·
`RECONSTRUCTION_CONTRACT.md` (GATE A, tolerance) ·
`DATA_GENERATION.md` (tier, volume, seed) ·
`FEATURE_LINEAGE.md` · `FEATURE_DICTIONARY.md` · `ERD.md` ·
`MIGRATION_PLAN.md` · `AUDIT_KIEN_TRUC_VA_FEATURE.md`
