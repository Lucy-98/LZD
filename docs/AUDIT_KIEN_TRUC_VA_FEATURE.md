# Audit Kiến Trúc Và Feature

> **Scope:** dataset, ingest/lake/dbt và reconstruction. API, policy và downstream
> key/value design không phải đối tượng audit.

## 1. Kết luận

Repo có nền data pipeline dùng được cho reconstruction:

- Kafka consumer ghi MinIO trước rồi mới cập nhật downstream state và commit offset;
- MinIO parquet là raw source of truth;
- DuckDB/dbt đọc raw trực tiếp và dedup theo `event_id`;
- selected reconstruction scope là đúng 55 cột (`fs_2026_08_v2`);
- Track A/Track B E2E dry-run đã chạy bằng SQL dbt thật;
- semantic `f30` vẫn `UNIDENTIFIED`, branch H1/H2 là scenario config.

Phần chưa production-ready là full target/encoding materialization, solver scale,
writer `events_v2`, schema/consumer/publisher Kafka v2 và pilot threshold.

## 2. Luồng hiện tại

```text
Batch:  CSV -> MinIO snapshot -> dbt staging/marts -> downstream export

Stream: producer -> Kafka -> validate -> MinIO -> downstream state -> commit

Recon:  immutable target -> Track A -> dbt SQL -> Gate A-F -> T0 -> Track B
```

API và representation của downstream state không ảnh hưởng invariant của data path.

## 3. Điểm đúng cần giữ

| Cơ chế | Evidence |
|---|---|
| MinIO trước commit | `stream_consumer._flush()` |
| At-least-once + lake dedup | consumer + `stg_app_events.sql` |
| PIT strict `< feature_ts` | realtime/reconstruction dbt models |
| Label boundary | serving mart/export không chứa label/treatment |
| Immutable target | frozen dataclass + Postgres trigger |
| Solver branch strategy | một engine, H1/H2 strategy |
| Tautology prevention | dbt engine không thấy target payload/gen_reason |
| Capability boundary | Track B không reach target/solver modules |
| Provenance | REAL/RECONSTRUCTED/SYNTHETIC + run lineage |

## 4. Gaps

| Gap | Mức | Tác động |
|---|---|---|
| `feature_ts=now()` ở seed path cũ | cao | replay không ổn định |
| `rt_session_len_sec` lệch offline/runtime | cao | aggregate skew |
| `hist_*` dùng `now()` thay PIT | cao | leakage thời gian |
| producer chỉ chọn pool 20k user | vừa | coverage thấp |
| model training còn stub | cao | chưa đánh giá model-level |
| exhaustive solver | cao | không chạy full tail |
| production encoding maps chưa materialize | cao | T2 chưa chạy full data |
| Track B rule-based tối thiểu | vừa | chỉ đạt sanity, chưa chứng minh realism |

## 5. Feature Forensics

### 5.1 Scope

Dataset có `f0..f82`, nhưng reconstruction contract chỉ lấy 55 cột:

- T1: `f1 f2 f5 f11 f18 f30`;
- T2: 12 cột categorical/encoding;
- T3: 18 cột pass-through.

47 cột ngoài selected set không được solver tự ý kéo vào.

### 5.2 Transformation đã đo

| Cột | Regime | Kết quả |
|---|---|---|
| `f5`, `f11` | LN | `n=round(exp(f))`, relative tolerance `1e-15` |
| `f18`, `f30` | LOG10 | `n=round(10**f)`, encode 6 chữ số |
| `f1`, `f2` | REC | integer day distance, `f1 >= f2` |
| T2 | CAT | value encoding hoặc full one-hot group |
| T3 | PASS | truyền nguyên vẹn, không gọi là reconstruction |

### 5.3 Grouping

- `f79..f82`: một latent attribute 515 levels, bốn encoding;
- `f37` và `f38`: hai attribute độc lập dù dùng chung alphabet;
- one-hot groups phải dùng toàn bộ level domain, không chỉ selected columns;
- `f23/f25` và `f9/f16` chưa đủ evidence để gộp trong reconstruction scope.

### 5.4 Semantic f30

Hai hypothesis còn hợp lệ:

- H1: distinct active days trong cửa sổ;
- H2: counter của một event class riêng.

Evidence hiện tại chưa phân định được historical truth. Runtime mặc định H1 chỉ là
operational preference; mọi artifact phải lưu cả `semantic_status` và branch.

## 6. Reconstruction Status

Đã code/test:

- target hash/scope/leakage guard;
- encoding property tests;
- deterministic candidate selection/materialization;
- H1/H2 forward SQL;
- Gate A-F;
- provenance graph checks;
- Track A -> T0 -> Track B E2E;
- CLI và DAG manual.

Chưa code production:

- full train target loader/materializer: da co `lzd_pipeline.reconstruction.track_a_batch`;
- full encoding artifact build theo tap train dang chay: da co trong Track A batch;
- scalable solver;
- MinIO `events_v2` writer;
- Kafka Track B schema/consumer/publisher v2;
- pilot 10k report: da chay 10,000/10,000 solved, 0 quarantine, Gate sample pass;
- full production reporting.

## 7. Acceptance

Không được gọi reconstruction thành công nếu chỉ có code chạy. Tối thiểu cần:

1. Gate A/A-T3 theo đúng regime;
2. Gate B/C cho thời gian và hard constraints;
3. Gate D/E cho provenance và leakage;
4. Gate F cho sanity distribution;
5. deterministic replay cùng fingerprint;
6. quarantine thay vì sửa target.

## 8. Scope Boundary

Deliverable dừng ở event, feature tables, `CustomerState(T0)`, provenance và gate
report. API, model-serving contract, policy và state representation thuộc hệ
downstream khác; audit này không đưa ra yêu cầu thiết kế cho các phần đó.
