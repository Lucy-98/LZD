# WIP — Track A, Track B và ý nghĩa event

> **Ngày viết:** 2026-08-17 · **Trạng thái:** WIP triển khai trên nhánh
> `nhung-lala` (base commit `10ab540`).
>
> **Phạm vi bằng chứng:** tài liệu này chỉ mô tả những gì đã chạy thật và đo
> được. Mọi con số đến từ ba nguồn có thể chạy lại:
>
> | Nguồn | Lệnh | Kết quả tại thời điểm viết |
> |---|---|---|
> | Demo online 10 user | `make track-b-online-demo` | run `track-b-demo-20260817T103711Z` |
> | Reconstruction dry-run | `python -m lzd_pipeline.reconstruction.e2e` | Gate A–F `all_passed=true` |
> | Test suite | `.venv/bin/python -m pytest tests -q -p no:cacheprovider` | 336 passed |
>
> Phần nào là **thiết kế chưa code** đều được đánh dấu rõ. Không suy diễn.

---

## Mục lục

1. [Vì sao phải có hai track](#1-vì-sao-phải-có-hai-track)
2. [Ranh giới thời gian và CustomerState](#2-ranh-giới-thời-gian-và-customerstate)
3. [Track A — dựng ngược event từ feature](#3-track-a--dựng-ngược-event-từ-feature)
4. [Track B — sinh xuôi event tương lai](#4-track-b--sinh-xuôi-event-tương-lai)
5. [Ý nghĩa từng event](#5-ý-nghĩa-từng-event)
6. [Gate A–F kiểm cái gì](#6-gate-af-kiểm-cái-gì)
7. [Hiện tại đang làm được gì](#7-hiện-tại-đang-làm-được-gì)
8. [Chưa làm được — gap còn mở](#8-chưa-làm-được--gap-còn-mở)
9. [Chạy lại và tự kiểm chứng](#9-chạy-lại-và-tự-kiểm-chứng)

---

## 1. Vì sao phải có hai track

Bài toán gốc: dataset chỉ có **feature đã tính sẵn** (`f0..f82`, ẩn danh), **không
có raw event nào**. Nhưng một feature platform thật thì mọi thứ phải chảy từ
event lên — không có event thì dbt không có gì để build, Kafka không có gì để
stream, và không thể chứng minh feature online khớp feature offline.

Hai track giải hai nửa khác nhau của cùng vấn đề đó:

```
                          reference_ts (T0)
                                 │
   ─────── QUÁ KHỨ ──────────────┼────────── TƯƠNG LAI ──────────
                                 │
   TRACK A                       │                       TRACK B
   feature  ──►  event           │           state  ──►  event
   (dựng ngược, tất định)        │           (sinh xuôi, mô phỏng)
                                 │
   "55 feature này chỉ có thể    │    "user ở trạng thái này thì
    đến từ chuỗi event nào?"     │     tiếp theo sẽ làm gì?"
                                 │
   chạy MỘT LẦN cho một target   │    sinh lô bounded, có thể chạy lại
```

| | Track A | Track B |
|---|---|---|
| Hướng | Feature → event (inverse) | State → event (forward) |
| Tính chất | Tất định, cùng seed cho cùng kết quả | Mô phỏng hành vi, có seed |
| Biết gì | Toàn bộ 55 feature của target | **Chỉ** `CustomerState`, không thấy feature |
| Ghi vào đâu | `raw/events_v2` trên MinIO (batch) | Kafka `app.user.events.v1` (stream) |
| Dùng để | Dựng lịch sử, chứng minh contract | Chạy realtime, demo online |
| Nhãn `source_type` | `RECONSTRUCTED` | `SYNTHETIC` |

**Vì sao không gộp một track:** nếu Track B nhìn thấy được feature của target,
nó sẽ "lái" event tương lai sao cho khớp feature — và closed-loop test khi đó
chỉ đang tự kiểm chính nó. Ranh giới năng lực này là INVARIANT 4, được canh
bằng đồ thị import chứ không bằng code review (xem §4).

---

## 2. Ranh giới thời gian và CustomerState

Toàn bộ hai track chỉ nối với nhau qua **một** đối tượng: `CustomerState`.

```python
# src/lzd_pipeline/reconstruction/state.py
@dataclass(frozen=True)
class CustomerState:
    customer_id: str
    as_of_ts: datetime                 # = reference_ts, mốc T0
    source_target_id: str              # OPAQUE — chỉ để trace, cấm dùng để đọc target
    attributes: Mapping[str, int]      # 7 thuộc tính T2 đã decode -> level_id
    counters: Mapping[str, int]        # n5, n11, n18, n30... đã decode
    semantic_branch: SemanticBranchName | None
    semantic_status: SemanticStatus
    provenance: Provenance
    last_event_ts: datetime | None     # bắt buộc < as_of_ts
```

Bất biến thời gian được ép ngay trong `__post_init__`:

```
max(Track A event_ts)  <  reference_ts  ≤  min(Track B event_ts)
```

`source_target_id` là **lineage id mờ**: lưu để truy vết, cấm dùng để đọc nội
dung target. Ràng buộc thứ hai không thể ép bằng kiểu dữ liệu, nên nó được ép
bằng đồ thị import (`reconstruction/capability.py`, TEST-10b).

Điều kiện bàn giao: **chỉ khi Gate A + A-T3 + B + C pass**. T0 chưa được xác
nhận thì mọi event tương lai sinh ra từ nó đều vô nghĩa.

---

## 3. Track A — dựng ngược event từ feature

### 3.1 · Năm pha của solver

`src/lzd_pipeline/reconstruction/engine.py`:

```
P1  FEASIBILITY    hard constraint + EXACT feature equality
P2  OPTIMIZATION   argmin lexicographic          => OPTIMAL POOL
P3  SELECTION      seeded PRNG chọn MỘT phần tử TRONG pool
P3b MATERIALIZE    candidate_slot_id -> occurrence -> event_id (uuid5)
P4  ORDERING       canonical sort — CHỈ sắp thứ tự, không đổi nội dung
```

★ **INVARIANT 3**: seed chỉ tác động **sau** khi đã có optimal pool.

```
✅ P1 → P2 → optimal pool → P3 (seed) → P3b → P4
🚫 seed → random candidate → check objective
```

Điều này được ép bằng **chữ ký hàm**: `select_from_pool()` chỉ nhận `pool` đã
là argmin — nó không có đường nào chạm vào tập feasible đầy đủ. PRNG dẫn xuất
theo `(target_id, seed, track)` nên mỗi user độc lập, song song hoá không đổi
kết quả.

### 3.2 · Bảng chữ cái event của Track A

Track A **không** sinh event nghiệp vụ Lazada. Nó sinh **CFS witness** —
"nhân chứng" tối thiểu đủ để feature engine đếm lại ra đúng con số.

| `event_type` | Sinh ra để thoả feature | Alias trình bày | Nó **không** phải là gì |
|---|---|---|---|
| `EVT_F5` | `f5` — counter browse 365d (ln) | `ASSUMED_PRODUCT_VIEWED` | không có `product_id`; không phải product-view thật của Lazada |
| `EVT_F11` | `f11` — counter cart/checkout intent 365d (ln) | `ASSUMED_ITEM_ADDED_TO_CART` | không có `sku_id` |
| `EVT_F18` | `f18` — counter promo touch 365d (log10) | `ASSUMED_VOUCHER_VIEWED` | không có `issuance_id` |
| `EVT_F19` | `f19` — counter promo redemption 365d (log10) | `ASSUMED_VOUCHER_REDEEMED` | không có FK voucher/order |
| `EVT_F30` | `f30` — active days 30d (log10), **chỉ nhánh H2** | `ASSUMED_APP_ACTIVE_DAY` | không phải raw domain event |
| `EVT_ORDER_PAID` | `f1`/`f2` — recency | `ASSUMED_ORDER_PAID` | không có `order_id`; **không** chứng minh `f1/f2` thật sự là order-paid recency |
| `EVT_SESSION_STARTED` | slot `FREE` — không counter T1 nào đếm | `ASSUMED_SESSION_STARTED` | dùng để lấp active-day nhánh H1 |

Alias `ASSUMED_*` (`config/features/business_aliases.yml`) **chỉ để trình bày
báo cáo**. `semantic_status` mặc định là `UNIDENTIFIED` — không được viết trong
báo cáo rằng đã chứng minh `f30` mang nghĩa nghiệp vụ thật.

### 3.3 · Cơ chế chống tautology

Đây là chi tiết quan trọng nhất của Track A, nằm ở
[dbt/models/staging/stg_events_v2.sql](../dbt/models/staging/stg_events_v2.sql):

Solver gắn `gen_reason` lên từng event — *"event này sinh ra để phục vụ f5"*.
Nếu feature engine đếm theo `gen_reason`, nó đang đếm chính những event mà
solver đã tự khai là thuộc về `f5` ⇒ vòng tròn hoàn hảo, Gate A không kiểm gì cả.

Nên staging **drop** toàn bộ metadata của solver:

```
cho qua:  event_id · customer_id · event_type · event_ts · observation_ts
🚫 DROP:  gen_reason · day_offset · sub_index · occurrence · _gen_*
```

`day_offset` bị drop **có chủ ý** dù nó tiện: feature engine phải tự tính lại
từ `(event_ts, reference_ts)` — đúng như production làm. Hai bên khai báo **độc
lập** cùng một giả định:

```
solver:  "để đạt n5, phát n5 event loại EVT_F5"
engine:  "f5 đếm event loại EVT_F5 trong cửa sổ W"
```

Lệch nhau ⇒ Gate A đỏ. Đó là chỗ Gate A có giá trị.

### 3.4 · Số liệu thật (run `track-b-demo-20260817T103711Z`)

10 user đầu của `data/full_trainset.csv` → **115 event Track A**:

| `event_type` | Số event |
|---|---|
| `EVT_SESSION_STARTED` | 43 |
| `EVT_F5` | 22 |
| `EVT_F11` | 19 |
| `EVT_ORDER_PAID` | 11 |
| `EVT_F19` | 10 |
| `EVT_F18` | 10 |
| **Tổng** | **115** |

Đường đi thực tế của 115 event đó:

```
raw_events_v2.csv
  └─► MinIO  s3://lakehouse/raw/events_v2/dt=2026-08-01/events.parquet   (115 rows)
       └─► dbt tag:reconstruction  (run PASS, test PASS)
            └─► marts.feat_cfs_reconstructed_user_serving   (10 rows × 55 feature)
                 └─► Redis  fs:v20260801:u:{user_id}        (10 × 55 = 550 giá trị)
```

Control plane đi kèm (Postgres `biz.*`): `reconstruction_boundary` 10,
`customer_attribute` 80, `encoding_map` 19, `onehot_layout` 28,
`passthrough_source` 10.

Validate sync: 10 user sampled, 550 giá trị so sánh, **0 mismatch**, checksum
trước/sau ổn định (`35642d508bffcf4f`).

---

## 4. Track B — sinh xuôi event tương lai

### 4.1 · Ranh giới năng lực (INVARIANT 4)

[src/lzd_pipeline/reconstruction/live.py](../src/lzd_pipeline/reconstruction/live.py)
chỉ được import `state` + stdlib. **Cấm** (kể cả bắc cầu):

```
reconstruction.target        reconstruction.canonical
reconstruction.feature_set   reconstruction.handoff
reconstruction.engine        reconstruction.semantics
reconstruction.candidate
```

Lý do: nếu Track B có **bất kỳ đường nào** tới target repository, nó giải được
`source_target_id` và "nhìn trộm" feature ⇒ generator có thể lái event tương lai
để chiều theo feature ⇒ closed-loop test đo chính nó, không đo hệ thống.

Track A là **feature-space aware**. Track B chỉ **state/behaviour aware**.

### 4.2 · Bảng chữ cái event và adapter

Track B sinh vocabulary business-v2, rồi adapter đổi sang `AppEvent` v1 để đi
qua Kafka/consumer hiện có:

| Track B sinh ra | → `AppEvent` v1 | → counter realtime |
|---|---|---|
| `SESSION_STARTED` | `app_open` | (chỉ `rt_events_1h`) |
| `PRODUCT_VIEWED` | `page_view` | `rt_page_view_1h` |
| `ITEM_ADDED_TO_CART` | `add_to_cart` | `rt_add_to_cart_1h` |
| `PURCHASE_COMPLETED` | `order` | `rt_order_1h`, `rt_gmv_1h` |

Adapter nằm ở `TRACK_B_TO_APP` trong
[src/lzd_pipeline/demo/track_b_online.py](../src/lzd_pipeline/demo/track_b_online.py).
Mọi event đều đi qua `validate_event()` trước khi publish; sai là raise chứ
không âm thầm bỏ.

> ⚠️ Event Track A (`EVT_*`) **không bao giờ** được publish vào topic app
> online. `EVENT_TYPES` của v1 sẽ reject và đẩy vào DLQ.

### 4.3 · Kịch bản demo — ba trạng thái timing

`DemoOnlineBehaviour` chọn kịch bản theo **identity** của user (chữ số cuối
`customer_id`), **không** theo uplift score — nếu chọn theo score thì demo lại
tự chứng minh chính nó:

| Nhóm user (ordinal) | Chuỗi event | Trạng thái tạo ra |
|---|---|---|
| `1`, `6` | app_open, page_view ×3 | chưa có intent → `WAIT_FOR_INTENT` |
| `2`, `5`, `8` | app_open, page_view ×2, add_to_cart | có intent → `SEND_VOUCHER` |
| `0`, `3`, `4`, `7`, `9` | app_open, page_view ×2, add_to_cart, order | đã mua → `SUPPRESS_ALREADY_PURCHASED` |

Mỗi user 4 event nền, 5 user có thêm `order` ⇒ cohort 10 user luôn ra đúng
**45 event**, để audit giữa các lần chạy so sánh được.

### 4.4 · Số liệu thật (cùng run)

| `event_type` (v1) | Số event |
|---|---|
| `page_view` | 22 |
| `app_open` | 10 |
| `add_to_cart` | 8 |
| `order` | 5 |
| **Tổng** | **45** |

Đường đi:

```
45 AppEvent ──► Kafka app.user.events.v1  (key = user_id, DLQ = 0)
                  └─► stream-consumer
                       ├─► MinIO raw/app_events/*.parquet
                       │     └─► dbt stg_app_events  → 45 rows, dedup theo event_id
                       └─► Redis  rt:u:{user_id}     (TTL 3600s)
```

Ví dụ overlay realtime thật trên Redis:

| user | `rt_events_1h` | `rt_page_view_1h` | `rt_add_to_cart_1h` | `rt_order_1h` | `rt_gmv_1h` |
|---|---|---|---|---|---|
| `U0000000` | 5 | 2 | 1 | 1 | 199 000 |
| `U0000001` | 4 | 3 | 0 | 0 | 0 |
| `U0000002` | 4 | 2 | 1 | 0 | 0 |

> **Lưu ý về mốc thời gian trong demo:** demo neo Track B vào *thời gian thực*
> (`anchor − 9 phút` → `anchor − 5 giây`) chứ không phải vào `reference_ts`
> (2026-08-01), để 45 event nằm trong cửa sổ trượt 1h và `rt_*` nhìn thấy được.
> Bất biến `Track B ≥ as_of_ts` vẫn giữ. Bản dry-run `e2e` mới là bản chạy đúng
> từ `as_of_ts` với `future_days=2`.

---

## 5. Ý nghĩa từng event

### 5.1 · Đang chạy thật — `AppEvent` v1

Contract: [src/lzd_pipeline/ingestion/schemas.py](../src/lzd_pipeline/ingestion/schemas.py).
Đây là ranh giới giữa team app và team data — **đổi field = đổi version topic**,
không sửa tại chỗ.

| `event_type` | Ý nghĩa nghiệp vụ | Feature realtime nó nuôi | Track B có sinh? |
|---|---|---|---|
| `app_open` | mở app / bắt đầu phiên | `rt_events_1h` | ✅ |
| `page_view` | xem trang sản phẩm | `rt_page_view_1h` | ✅ |
| `search` | tìm kiếm | — | ❌ chưa dùng |
| `add_to_cart` | thêm giỏ — **tín hiệu intent chính** | `rt_add_to_cart_1h` | ✅ |
| `checkout` | bắt đầu thanh toán | — | ❌ chưa dùng |
| `order` | đặt đơn | `rt_order_1h`, `rt_gmv_1h` | ✅ |
| `voucher_view` | xem voucher | — | ❌ chưa dùng |
| `voucher_claim` | nhận voucher | — | ❌ chưa dùng |

Envelope v1: `event_id · user_id · event_type · event_ts · session_id ·
platform · item_id · category_id · price · quantity · schema_version`.
Bắt buộc: 4 field đầu. Sai ⇒ DLQ kèm `reason`.

### 5.2 · Thiết kế nhưng **chưa code** — business event v2

`docs/BUSINESS_EVENT_MODEL.md` đề xuất envelope v2 với 4 field mới
(`order_id`, `voucher_id`/`issuance_id`, `decision_id`, `amount`). Hai event
quan trọng nhất vẫn **chưa tồn tại trong repo**:

| Event thiếu | Vai trò nhân quả | Hệ quả của việc thiếu |
|---|---|---|
| `VOUCHER_ISSUED` | **treatment** — mốc phát voucher | không truy được một lần mua về treatment nào ⇒ không đo được uplift thật |
| `DECISION_MADE` | quyết định của model thành dữ liệu quan sát được trong stream | closed loop hở: không trả lời được "tại T=t model thấy gì, chấm bao nhiêu, sau đó user làm gì" chỉ bằng một stream |

Ngoài ra `order` v1 đang **gộp** hai sự kiện khác nhau: `ORDER_CREATED` (đặt
đơn) và `ORDER_PAID` (đã trả tiền). Với COD phổ biến ở SEA, khoảng cách giữa
hai mốc đó không hề nhỏ, và mốc label đúng phải là `ORDER_PAID`.

### 5.3 · Bảng đối chiếu ba vocabulary

| Track A (CFS witness) | AppEvent v1 (đang chạy) | Business v2 (thiết kế) |
|---|---|---|
| `EVT_SESSION_STARTED` | `app_open` | `SESSION_STARTED` |
| `EVT_F5` | `page_view` | `PRODUCT_VIEWED` |
| `EVT_F11` | `add_to_cart` | `ITEM_ADDED_TO_CART` |
| `EVT_F18` | `voucher_view` | `VOUCHER_VIEWED` |
| `EVT_F19` | `voucher_claim` | `VOUCHER_REDEEMED` |
| `EVT_ORDER_PAID` | `order` | `ORDER_CREATED` + `ORDER_PAID` |
| `EVT_F30` | — | — (marker active-day, nhánh H2) |
| — | — | `VOUCHER_ISSUED` ❌ thiếu |
| — | — | `DECISION_MADE` ❌ thiếu |

Ba cột này **không** đồng nhất. Cột trái là nhân chứng toán học, cột giữa là
schema đang chạy, cột phải là mô hình nghiệp vụ mong muốn.

---

## 6. Gate A–F kiểm cái gì

Định nghĩa thi hành ở
[src/lzd_pipeline/reconstruction/e2e.py:198-258](../src/lzd_pipeline/reconstruction/e2e.py#L198-L258):

| Gate | Kiểm | Fail nghĩa là |
|---|---|---|
| **A** | forward-recompute qua chính SQL dbt ra đúng feature T1/T2 của target | solver và feature engine lệch giả định |
| **A-T3** | feature T3 pass-through khớp | đường pass-through hỏng |
| **B** | mọi event Track A có `event_ts < reference_ts`, và tập không rỗng | rò rỉ thời gian |
| **C** | candidate được chọn vẫn feasible dưới nhánh semantic | optimizer trả nghiệm sai |
| **D** | mọi event Track B mang `source_type = SYNTHETIC` | nhầm lẫn dữ liệu thật/synthetic |
| **E** | target **không** chứa `label` hoặc `is_treat` | rò rỉ nhãn vào solver |
| **F** | sanity số lượng event (0 < n < 100 000) và mọi event Track B `≥ as_of_ts` | phân bố suy biến |

> Gate A và A-T3 **báo cáo tỉ lệ riêng**, không gộp — gộp lại thì tỉ lệ pass
> luôn ≥ 50% một cách vô nghĩa. Gate F **chỉ là sanity**, không được biến thành
> hard constraint của solver, và không được phản hồi ngược vào objective.

Kết quả dry-run tại thời điểm viết (`reference_ts=2026-08-01T23:59:59Z`,
`branch=H1`, `seed=42`, `future_days=2`):

```json
{
  "semantic_status": "UNIDENTIFIED",
  "semantic_branch": "H1",
  "track_a_status": "SOLVED",
  "track_a_events": 6,
  "track_b_events": 8,
  "gates": {"A": true, "A-T3": true, "B": true, "C": true,
            "D": true, "E": true, "F": true},
  "all_passed": true
}
```

---

## 7. Hiện tại đang làm được gì

Tất cả các dòng dưới đây đều đã chạy và đo được, không phải kế hoạch.

### 7.1 · Đường end-to-end hoàn chỉnh

```
data/full_trainset.csv  (10 user)
  │
  ├─ TRACK A ──► 115 EVT_* ──► MinIO raw/events_v2 ──► dbt ──► DuckDB mart 55F
  │                                                              │
  │                                                    Redis fs:v20260801:u:*
  │                                                              │
  ├─ handoff CustomerState(T0) ─────────────────────────────┐    │
  │                                                         ▼    ▼
  └─ TRACK B ──► 45 AppEvent ──► Kafka ──► consumer ──┬─► MinIO raw/app_events
                                                      │      └─► stg_app_events (45)
                                                      └─► Redis rt:u:*
                                                                 │
                                                    FastAPI /decide/batch, /campaign/decide
                                                                 │
                                       Prometheus + Grafana + Loki quan sát toàn tuyến
```

| Hạng mục | Trạng thái đo được |
|---|---|
| Track A solver | `SOLVED`, Gate A–F pass trên cả dry-run lẫn 10 user thật |
| dbt reconstruction | `run PASS`, `test PASS`, mart đúng 55 cột |
| Feature sync → Redis | version `v20260801`, 550 giá trị, 0 mismatch, checksum ổn định |
| Track B → Kafka | 45 event, DLQ = 0, ordering theo key `user_id` |
| Lake | 45/45 event khớp `event_id` trong parquet trên MinIO |
| Staging dbt | `staging.stg_app_events` 45 rows, dedup theo `event_id` |
| Redis realtime | overlay `rt:u:*` khớp giá trị kỳ vọng cho cả 10 user |
| Inference API | 10/10 kết quả, `cache_hit=true`, `features_supplied=55` |
| Model | `DRLearner-20260813`, nạp từ **trong image** (`source=docker_image`) |
| Monitoring | Prometheus 10/11 target UP (producer random cố ý tắt), Grafana 5 dashboard, Loki chỉ nhận log container LZD |
| Test | 336 passed |

### 7.2 · Campaign policy — model chọn USER, realtime chọn THỜI ĐIỂM

[src/lzd_pipeline/serving/campaign_policy.py](../src/lzd_pipeline/serving/campaign_policy.py)
tách rõ hai tầng. Thứ tự gate cho user đã nằm trong top-K:

```
1. rt_order_1h > 0        →  SUPPRESS_ALREADY_PURCHASED   (đã mua, đừng phát nữa)
2. rt_add_to_cart_1h > 0  →  SEND_VOUCHER                 (có intent, phát ngay)
3. còn lại                →  WAIT_FOR_INTENT              (chờ thêm tín hiệu)
```

Kết quả thật với `budget=3` trên cohort 10 user:

| Action | Số user | Ví dụ |
|---|---|---|
| `SEND_VOUCHER` | 1 | rank 1 · `U0000005` · uplift 0.5581 pp · có add_to_cart |
| `WAIT_FOR_INTENT` | 1 | rank 2 · `U0000001` · uplift 0.4739 pp · chưa có intent |
| `SUPPRESS_ALREADY_PURCHASED` | 1 | rank 3 · `U0000003` · uplift 0.4033 pp · đã order trong 1h |
| `SKIP_NON_POSITIVE_UPLIFT` | 1 | uplift ≤ 0 → không phát dù còn budget |
| `NOT_SELECTED_BUDGET` | 6 | ngoài top-3 |

`effective_cutoff = 0.004033`. Budget **không** tự backfill user hạng thấp khi
một user top-K bị suppress — để nhìn rõ hai tầng model-selection và
timing-policy tách nhau.

### 7.3 · Model contract 76 cột

`models/uplift_voucher/feature_contract.json`:

| Nguồn | Số cột | Ghi chú |
|---|---|---|
| Redis (DE tính và lưu) | 55 | đúng 55 feature của `feature_spec.yml` |
| Service tự tính | 7 | `fe_ratio_*`, `fe_inter_*`, `fe_nonzero_top10`, `fe_flag_sum` |
| Điền mặc định | 14 | `f7, f14, f36, f48..f51, f55, f56, f60, f61, f63, f66, f75` |
| **Tổng vào model** | **76** | thứ tự cột cố định theo `thu_tu_dua_vao_mo_hinh` |

---

## 8. Chưa làm được — gap còn mở

| # | Gap | Bằng chứng | Ảnh hưởng |
|---|---|---|---|
| G1 | **`rt_*` không vào model** | `realtime_applied=0`, score trước và sau Track B bằng nhau | DRLearner chỉ nhận 55 batch feature; realtime hiện chỉ gate *thời điểm*, không đổi *điểm số*. Ép score tăng để demo sẽ là phát biểu sai |
| G2 | **Thiếu `VOUCHER_ISSUED`** | `EVENT_TYPES` không có | không có event mang treatment ⇒ closed loop §12 chưa đóng được |
| G3 | **Thiếu `DECISION_MADE`** | như trên | quyết định của model không quan sát được từ stream |
| G4 | **Topic v2 chưa publish** | Track B phải adapter về v1 | mất `session_end`, `checkout`, `search`, nhóm `voucher_*`; `order` vẫn gộp created/paid |
| G5 | **`ops.inference_log` rỗng** | `lzd_inference_log_queued_total = 0` | chỉ `/decide` ghi log; `/decide/batch` và `/campaign/decide` thì không — mà demo dùng đúng hai cái sau. Panel "Inference log gần nhất" ở dashboard 04 trống |
| G6 | **Không điều khiển được thời gian** | — | kịch bản S2 "cooling down" (chờ 65 phút cho `rt_*` rơi về 0) chưa chạy tự động được |
| G7 | **Training không còn trong repo** | `dag_30_train_uplift_model.py`, `training_dataset.sql`, `eval_holdout.sql`, `docker/mlflow/` đã được loại khỏi nhánh triển khai | model hiện là artifact dựng sẵn bake vào image; không có đường train lại trong Airflow |
| G8 | **`semantic_status = UNIDENTIFIED`** | `config/reconstruction/runtime.yml` | không được viết trong báo cáo rằng `f30`/`f5`/`f11` đã được chứng minh nghĩa nghiệp vụ thật |
| G9 | **`event-producer` không chạy trong demo** | target Prometheus DOWN, alert `NoEventsIngested` pending | có chủ ý (demo dùng lô bounded 10 user), nhưng làm trang targets đỏ một dòng |

---

## 9. Chạy lại và tự kiểm chứng

```bash
# 1) Toàn bộ demo Track A -> Track B -> online (tự bật cả monitoring)
make track-b-online-demo
#    hoặc: sh scripts/run_track_b_online_demo.sh --users 10

# 2) In bảng inference/rank/action gọn sau khi luồng đã chạy
make show-inference-demo

# 3) Reconstruction dry-run, không ghi production
docker compose --env-file config/demo-stack.env run --rm --no-deps \
  airflow-scheduler python -m lzd_pipeline.reconstruction.e2e
docker compose --env-file config/demo-stack.env run --rm --no-deps \
  airflow-scheduler python -m lzd_pipeline.reconstruction.e2e --branch H2

# 4) Test
PYTHONPATH=src pytest tests -q
```

Report đầy đủ của mỗi lần chạy: `.tmp/track_b_online_demo/<run_id>/report.json`,
kèm `track_b_app_events.jsonl` và `track_b_lineage.json`.

Quan sát trong lúc chạy:

| Nơi xem | URL | Xem gì |
|---|---|---|
| Grafana | http://localhost:13000 (`admin/admin`) | 5 dashboard, folder *LZD Uplift Platform* |
| Prometheus | http://localhost:19090 | target, alert, `lzd_*` metric |
| Kafka UI | http://localhost:18082 | topic `app.user.events.v1`, DLQ |
| RedisInsight | http://localhost:15540 | key `fs:v20260801:u:*`, `rt:u:*` |
| MinIO | http://localhost:19001 | `raw/events_v2`, `raw/app_events` |
| FastAPI | http://localhost:18000/docs | `/decide`, `/decide/batch`, `/campaign/decide` |

Query hữu ích trong Grafana Explore:

```logql
{job="airflow-tasks", dag_id="60_reconstruction_e2e"}
{container="lzd-reconstruction-demo-stream-consumer-1"} | json | level="ERROR"
```

```promql
sum by (event_type) (rate(lzd_events_consumed_total[5m]))
histogram_quantile(0.99, sum by (le) (rate(lzd_inference_latency_seconds_bucket[5m])))
```

---

## Tài liệu liên quan

| Chủ đề | File |
|---|---|
| Spec đầy đủ của reconstruction | `docs/RECONSTRUCTION_SPEC.md` |
| Contract và bảng bất biến T-*/E-*/P-* | `docs/RECONSTRUCTION_CONTRACT.md` |
| Cách chạy reconstruction | `docs/RECONSTRUCTION_README.md` |
| Mô hình event nghiệp vụ v2 (thiết kế) | `docs/BUSINESS_EVENT_MODEL.md` |
| Alias `EVT_*` → `ASSUMED_*` | `docs/BUSINESS_ALIAS_MAP.md` |
| Từ điển 55 feature | `docs/FEATURE_DICTIONARY.md` |
| Vận hành, cổng demo, monitoring | `docs/RUNBOOK.md` |
| Báo cáo WIP 4 tuần (cũ hơn, mốc 36 feature) | `docs/WIP_4_WEEK_TECH_DOC.md` |
