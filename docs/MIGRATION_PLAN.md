# Migration Plan — Reconstruction Productionization

> **Trạng thái:** E2E dry-run đã có. Kế hoạch này chỉ đưa Track A/Track B lên
> production data path. API, policy và downstream state representation ngoài scope.

## 1. Hiện trạng

Đã có:

- immutable 36-column `ReconstructionTarget` và tamper hash;
- solver dùng một engine với branch H1/H2;
- `semantic_status=UNIDENTIFIED` tách khỏi branch vận hành;
- event witness deterministic, provenance và capability boundary;
- forward runner thực thi chính SQL dbt;
- Gate A-F và handoff `CustomerState(T0)`;
- Track B rule-based sinh future events;
- CLI và DAG manual `60_reconstruction_e2e`.

Chưa có:

- materialize target từ toàn bộ train split;
- encoding maps đầy đủ từ dataset;
- writer production cho `raw/events_v2`;
- schema/consumer v2 và publish Track B lên Kafka v2;
- pilot/full-run reporting và quarantine threshold thực nghiệm.

## 2. Kiến trúc mục tiêu

```text
full_trainset.csv (read-only)
        |
        v
biz.reconstruction_target (immutable, 36 columns, REFERENCE_TS)
        |
        v
Track A solver -> RECONSTRUCTED events -> MinIO raw/events_v2
        |                                  |
        |                                  v
        |                             dbt feature SQL
        |                                  |
        `----------------------------> Gate A-F
                                           |
                                           v
                                  CustomerState(T0)
                                           |
                                           v
                                  Track B generator
                                           |
                                           v
                                  Kafka events v2
                                           |
                                           v
                                  consumer -> MinIO first
```

Downstream state export có thể tiếp tục tồn tại, nhưng representation/access contract
không phải deliverable của migration này.

## 3. Gaps cần xử lý

| Gap | Rủi ro | Cách đóng |
|---|---|---|
| `feature_ts=now()` trong seed path | rerun không tái lập | materialize `REFERENCE_TS` cố định |
| Encoding maps mới chỉ có prototype | T2 không chạy full data | fit + freeze + full-domain round-trip |
| Solver exhaustive | không scale với outlier | pilot trước, rồi CP/MILP hoặc bounded solver |
| `events_v2` chưa có production writer | E2E chỉ in-memory | shared parquet writer + provenance partition |
| Kafka/consumer v2 chưa có | Track B business-v2 event sẽ bị consumer v1 reject | topic + schema + consumer v2 trước publisher |
| H1/H2 chưa phân định | semantic overclaim | giữ `UNIDENTIFIED`, chạy hai scenario |
| Chưa có threshold quarantine | không biết pilot đạt | chốt từ pilot phân tầng |
| Track B rule-based tối thiểu | realism yếu | Gate F + distribution report, không gọi là lịch sử thật |

## 4. Nguyên tắc

1. Dataset train/test là read-only; test split không sinh event.
2. Additive trước: `events_v2` tách khỏi `app_events` v1.
3. Lake là source of truth; Kafka offset chỉ commit sau khi ghi MinIO thành công.
4. Feature engine là SQL dbt thật; không viết Python feature logic để ép Gate A.
5. Semantic branch là config, không phải truth.
6. Track B chỉ nhận `CustomerState`, không được lookup target.
7. Mọi event phải có provenance và generation lineage.
8. Không đưa `SYNTHETIC`/`RECONSTRUCTED` vào production training data.

## 5. Phases

### M0 — Freeze contracts

- Freeze `fs_2026_08_v1.yaml` và runtime config.
- Chốt `REFERENCE_TS` theo generation run.
- Áp DDL `sql/postgres/02_biz_reconstruction.sql`.
- Thêm migration cho Postgres volume cũ.

**Gate:** target hash ổn định khi rerun; schema không cho update/delete target.

### M1 — Materialize target và encoding

- Đọc train split, chỉ lấy đúng 36 cột.
- Fit ba value encoding và bốn one-hot layouts.
- Persist versioned maps.
- Chạy bijection/round-trip trên toàn observed domain.

**Gate:** `encode(decode(x)) == x` cho mọi giá trị hợp lệ; zero test rows được dùng
làm nguồn event.

### MX — Pilot Track A

- Chọn 10,000 target phân tầng theo `n30`, counter tail và T2 levels.
- Chạy H1 và H2 nếu semantic vẫn `UNIDENTIFIED`.
- Ghi event Track A vào partition riêng theo source/run/date.
- Chạy dbt models và Gate A-F.
- Persist diff/quarantine/objective summary.

**Gate:** Gate A/B/C/D/E pass; Gate F không có volume/timestamp phi lý;
quarantine không vượt threshold chốt từ pilot.

### M2 — Production writer

- Dùng chung event envelope, timestamp validation, UUID5 và parquet writer.
- Partition theo `source_type`, `generation_run_id`, `dt`.
- Writer idempotent theo event ID và run fingerprint.
- Rerun cùng fingerprint cho cùng event/reconstructed-feature hash.

**Gate:** replay không nhân đôi kết quả dbt; MinIO object và audit row truy vết được.

### M3 — Track B Kafka v2

- Nhận duy nhất `CustomerState(T0)` đã pass gate.
- Sinh event từ `REFERENCE_TS` trở đi.
- Publish topic v2 với provenance; **không** gửi vào `app.user.events.v1`.
- Consumer v2 ghi MinIO trước, cập nhật downstream state sau, commit cuối.

**Gate:** Track A và B không giao nhau về thời gian/event ID; Track B import graph
không reach target/canonical/solver modules.

### M4 — Full run

- Chạy theo shard, checkpoint từng shard.
- Quarantine thay vì sửa target.
- Xuất report theo branch/regime/column/reason code.
- So phân bố pilot với full run để bắt drift.

**Gate:** mọi artifact có run fingerprint; rerun deterministic; provenance graph sạch.

## 6. Acceptance Gates

| Gate | Điều kiện |
|---|---|
| A | T1/T2 reconstructed khớp target theo đúng regime |
| A-T3 | pass-through truyền nguyên vẹn, báo cáo riêng |
| B | Track A `< REFERENCE_TS`, Track B `>= REFERENCE_TS` |
| C | candidate thỏa hard constraints của branch |
| D | provenance đầy đủ, không cycle/contamination |
| E | label/treatment không vào solver hoặc Track B |
| F | volume, session, timestamp, gap không phi lý |

## 7. Ngoài Scope

- HTTP endpoint, request/response schema, authentication và latency budget;
- decision/policy engine;
- downstream state representation và lifecycle;
- model serving và UI;
- diễn giải witness thành lịch sử thật.

Ranh giới bàn giao của data/reconstruction là feature tables, event stream,
`CustomerState(T0)`, provenance và gate report.
