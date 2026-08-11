# Migration Plan — từ kiến trúc hiện tại sang Synthetic Lazada Business System

> **Trạng thái: ĐỀ XUẤT, CHỜ XÁC NHẬN. Chưa sửa một dòng code nào.**
>
> Tài liệu này trả lời yêu cầu #6, #9, #11 của prompt: vẽ lại kiến trúc hiện tại,
> chỉ ra chỗ đi ngược kiến trúc mục tiêu, và trình bày lộ trình migration **trước khi**
> bắt đầu code.
>
> **Đọc trước:** `LAZADA_BUSINESS_DOMAIN.md` → `ERD.md` → `BUSINESS_EVENT_MODEL.md`
> → `FEATURE_DICTIONARY.md` → `FEATURE_LINEAGE.md` → (file này).

---

## Mục lục

1. [Kiến trúc hiện tại — vẽ lại](#1-kiến-trúc-hiện-tại--vẽ-lại)
2. [Kiến trúc mục tiêu](#2-kiến-trúc-mục-tiêu)
3. [Nơi code hiện tại đi ngược kiến trúc mục tiêu](#3-nơi-code-hiện-tại-đi-ngược-kiến-trúc-mục-tiêu)
4. [Nguyên tắc migration](#4-nguyên-tắc-migration)
5. [Lộ trình theo phase](#5-lộ-trình-theo-phase)
6. [Ánh xạ sang PHASE 0–16 của prompt](#6-ánh-xạ-sang-phase-016-của-prompt)
7. [Deliverable tài liệu còn lại](#7-deliverable-tài-liệu-còn-lại)
8. [Quyết định cần bạn xác nhận](#8-quyết-định-cần-bạn-xác-nhận)

---

## 1. Kiến trúc hiện tại — vẽ lại

Đọc từ code, không từ tài liệu.

```
  ┌─────────────────────────────────────────────────────────────────────┐
  │  LAYER 1 — BUSINESS / OPERATIONAL                                   │
  │                                                                     │
  │                      ✖  KHÔNG TỒN TẠI  ✖                            │
  │                                                                     │
  │  Không có CUSTOMER, SELLER, PRODUCT, SKU, CART, ORDER, PAYMENT,     │
  │  VOUCHER, CAMPAIGN. Không có database nghiệp vụ nào.                │
  └─────────────────────────────────────────────────────────────────────┘

  ┌─────────────────────────────────────────────────────────────────────┐
  │  LAYER 2 — RAW / EVENT   (một nửa giả)                              │
  │                                                                     │
  │  event_producer.EventProducer                                       │
  │    _pick_user()    → U0000000..U0019999  (pool 20,000)              │
  │    _make_session() → for (type,prob) in SESSION_FLOW:               │
  │                          if random() > prob: continue                │
  │                          price = lognormvariate(3.2, 0.8)           │
  │                          item_id = "item_"+randint(1,500000)        │
  │    _corrupt()      → 1% event hỏng có chủ ý (test DLQ)              │
  │            │                                                        │
  │            │ Kafka  app.user.events.v1   key=user_id               │
  │            ▼                                                        │
  │  stream_consumer.StreamConsumer                                     │
  │    validate → parquet(MinIO) → Redis rt:u:* → COMMIT (cuối cùng)   │
  │    ──✖──► app.user.events.dlq                                       │
  └─────────────────────────────────────────────────────────────────────┘
            │                                              │
            │ (1) lake                                     │ (2) online
            ▼                                              ▼
  ┌──────────────────────────────────┐        ┌────────────────────────┐
  │ data/full_trainset.csv 926,669   │        │ Redis rt:u:{uid}       │
  │ data/full_testset.csv  181,669   │        │ 6 counter, ô 5 phút    │
  │        │ seed_loader              │        │ TTL 3600s              │
  │        ▼                          │        └───────────┬────────────┘
  │ raw/user_snapshot/dt=…            │                    │
  │        │ stg_user_snapshot (cast) │                    │
  │        ▼                          │                    │
  │ ┌────────────────────────────────┐│                    │
  │ │ feat_user_serving.sql          ││                    │
  │ │   f0..f82   ← copy nguyên trạng││                    │
  │ │   4 hist_* ← feat_user_behaviour                     │
  │ └───────┬────────────────────────┘│                    │
  └─────────┼──────────────────────────┘                   │
            │                                              │
     ┌──────┴───────┐                                      │
     │ sync.py      │ training_dataset.sql                 │
     │ (versioned,  │ + feat_user_realtime_pit             │
     │  sharded,    │                                      │
     │  atomic swap)│                                      │
     ▼              ▼                                      │
  Redis          marts.training_dataset                    │
  fs:{ver}:u:*   94 feature + label + is_treat             │
  87 field            │                                    │
     │                ▼ DAG 30 → train.py                  │
     │           ✖ NotImplementedError ×3                  │
     │           ✖ MLflow Registry trống                   │
     │                                                     │
     └────────────────────┬────────────────────────────────┘
                          ▼
              FastAPI  POST /decide
                  read_for_serving()   ← 1 RTT Lua, atomic
                  aggregate_realtime() ← cộng ô 5 phút
                  spec.merge(batch ← realtime ← context)
                  model.predict_uplift()   ✖ StubModel (hàm băm)
                  score >= uplift_threshold ? SEND_VOUCHER : NO_VOUCHER
                          │
                          ▼
              ops.inference_log (Postgres)
                          │
                          ✖  ĐƯỜNG CỤT — không có event nào quay lại Kafka
```

### 1.1 · Đánh giá công bằng: cái gì đang tốt

Những cơ chế sau **đúng chuẩn production** và **không được phá** khi migrate:

| Cơ chế | File | Vì sao giữ |
|---|---|---|
| Versioned feature store + atomic swap | `online_store.py` | `fs:meta:active_version` trỏ version; ghi xong mới swap; rollback được |
| Chống skew cửa sổ realtime (12 ô × 5 phút hai phía) | `feat_user_realtime_pit.sql` + `online_store.py` | Thiết kế có chủ đích, comment giải thích rõ |
| Point-in-time join | `feat_user_realtime_pit.sql` | `event_ts < feature_ts` |
| Feature spec làm hợp đồng ba bên | `spec.py` + `feature_spec.yml` | dbt / sync / API đọc chung; `validate_columns()` fail nếu lệch |
| At-least-once + dedup | `stream_consumer.py` + `stg_app_events.sql` | Commit offset sau cùng; dedup `event_id` |
| Idempotent sharded sync | `sync.py` | `is_shard_done()` ⇒ rerun an toàn |
| **Ranh giới label** | `feat_user_serving.sql`, `offline_store.iter_shard()` | Label **không bao giờ** tới Redis |
| Warm-up + `/ready` tách khỏi `/health` | `app.py` | Đúng vận hành |
| Inference log bất đồng bộ | `inference_logger.py` | Không thêm 5–20ms vào đường serving |

> Kết luận: repo là **một data platform tốt thiếu tầng nghiệp vụ ở trên**, không phải
> một hệ thống sai cần viết lại.

---

## 2. Kiến trúc mục tiêu

```
  ┌─────────────────────────────────────────────────────────────────────┐
  │ LAYER 1 · SYNTHETIC BUSINESS SYSTEM        (Postgres schema `biz`)  │
  │  CUSTOMER SELLER STORE CATEGORY PRODUCT SKU                         │
  │  SESSION CART CART_ITEM                                             │
  │  ORDER ORDER_ITEM PAYMENT SHIPMENT RETURN_LINE                      │
  │  CAMPAIGN VOUCHER VOUCHER_ISSUANCE CLAIM REDEMPTION                 │
  │  DECISION EXPERIMENT EXPERIMENT_ARM                                 │
  │  CUSTOMER_IDENTITY_MAP  ◄── cầu nối tới user LZD                    │
  └────────────────────────────┬────────────────────────────────────────┘
                               │ business transaction (ERD §11)
                               ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │ LAYER 2 · DOMAIN EVENT                                              │
  │  app.user.events.v2  ·  biz.voucher.v1  ·  biz.decision.v1          │
  │  envelope có order_id / voucher_id / issuance_id / decision_id      │
  └──────────┬──────────────────────────────────┬───────────────────────┘
             │ lake                              │ stream
             ▼                                   ▼
  ┌────────────────────────┐         ┌──────────────────────────┐
  │ LAYER 3a · BATCH FE    │         │ LAYER 3b · STREAM FE     │
  │ dbt, point-in-time     │         │ ô 5 phút, parity với 3a  │
  └───────────┬────────────┘         └────────────┬─────────────┘
              └──────────────┬────────────────────┘
                             ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │ LAYER 3c · FEATURE STORE                                            │
  │   Track A  f0..f82   seed tĩnh, UNKNOWN, không cập nhật             │
  │   Track B  11 feature event-derived hiện có                         │
  │   Track C  feature mới từ Layer 1, lineage đầy đủ                   │
  └────────────────────────────┬────────────────────────────────────────┘
                               ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │ LAYER 4 · MODEL + POLICY                                            │
  │   uplift model → uplift_score → policy(threshold,budget,cooldown)   │
  │                              → DECISION                             │
  └────────────────────────────┬────────────────────────────────────────┘
                               │ SEND_VOUCHER
                               ▼
                    VOUCHER_ISSUANCE.insert
                               │
                               ▼
                  VOUCHER_ISSUED ──► Kafka ──┐
                                             │
       ┌─────────────────────────────────────┘
       ▼
   quay lại LAYER 2  ★ CLOSED LOOP
```

---

## 3. Nơi code hiện tại đi ngược kiến trúc mục tiêu

Xếp theo mức nghiêm trọng. Mỗi mục ghi rõ **vi phạm điều nào** trong §13 của prompt.

### 🔴 V1 · Layer 1 hoàn toàn vắng mặt

**Ở đâu:** [event_producer.py:73-99](../src/lzd_pipeline/ingestion/event_producer.py#L73-L99)

```python
for event_type, prob in SESSION_FLOW:
    if random.random() > prob: continue
    price = round(random.lognormvariate(3.2, 0.8), 2) if ...
    item_id = f"item_{random.randint(1, 500000)}"
    category_id = random.choice(CATEGORIES)
```

**Đi ngược cái gì:** mental model `Business Entity → Transaction → Event`. Ở đây
`random() → Event`. Không có customer, product, order, voucher nào tồn tại. Cùng một
`item_id` có giá khác nhau giữa hai event; `category_id` không liên quan tới `item_id`;
`add_to_cart` không dẫn tới `order` của cùng item.

**Hệ quả:** `hist_gmv_30d` và `rt_gmv_1h` không truy ngược được về catalog nào ⇒ lineage
"có công thức" nhưng "không có nghĩa nghiệp vụ".

---

### 🔴 V2 · Thiếu event mang treatment — closed loop hở

**Ở đâu:** [schemas.py:11-20](../src/lzd_pipeline/ingestion/schemas.py#L11-L20) —
`EVENT_TYPES` có `voucher_view`, `voucher_claim`, **không có `voucher_issued`**.

**Đi ngược cái gì:** §12 (online loop) và ASSUMPTION A-02. Treatment là *phát voucher*.
Không có event đó ⇒ không có bản ghi treatment trong stream ⇒ không đo được uplift
trong loop.

**Cộng thêm:** `/decide` kết thúc ở `ops.inference_log` (Postgres). Không event nào
quay lại Kafka. Vòng lặp `Decision → Customer Action → New Event` **không tồn tại**.

---

### 🔴 V3 · `order` gộp hai sự kiện nghiệp vụ khác nhau

**Ở đâu:** `EVENT_TYPES` có một `order` duy nhất; `stg_app_events` tính
`gmv = price × quantity` cho nó.

**Đi ngược cái gì:** ERD §8.1 (lifecycle) và ASSUMPTION A-01 (label = `PAID`).
`ORDER_CREATED` và `ORDER_PAID` là hai thời điểm khác nhau — đặc biệt quan trọng ở SEA
nơi COD phổ biến. Hiện `hist_order_cnt_30d` đếm cả đơn sẽ bị huỷ.

---

### 🟠 V4 · Không gian `user_id` lệch — user không tồn tại

**Ở đâu:** [event_producer.py:59](../src/lzd_pipeline/ingestion/event_producer.py#L59)
`PRODUCER_USER_POOL = 20000` vs 1,108,338 user đã seed.

**Đi ngược cái gì:** **điều cấm số 7** — "dùng random user ID không tồn tại nếu mục
tiêu là mô phỏng user thật". ~98% user đã seed không bao giờ nhận event; và không có
kiểm tra `user_id` có trong snapshot không.

**Đã có lời giải:** `CUSTOMER_IDENTITY_MAP` (ERD §3.2, ASSUMPTION A-04).

---

### 🟠 V5 · `rt_session_len_sec` — training/serving skew đang chạy production

**Ở đâu:** `feat_user_realtime_pit.sql` **có** công thức;
[stream_consumer.py:108-133](../src/lzd_pipeline/ingestion/stream_consumer.py#L108-L133)
`_update_realtime()` **không** ghi field này.

**Đi ngược cái gì:** **§10 training/serving parity**, đúng nguyên văn ví dụ trong prompt:
```
TRAIN:  rt_session_len_sec = calculated
ONLINE: rt_session_len_sec = 0/default
```

**Lưu ý khi sửa:** phải chốt **một** định nghĩa trước. Từ `SESSION.started_at` hay từ
`min(event_ts)` trong cửa sổ 1h? Hai công thức khác nhau với phiên > 1 giờ. Sửa vội
sẽ thay skew này bằng skew khác.

---

### 🟠 V6 · `hist_*` không point-in-time — **phát hiện mới, chưa có trong audit cũ**

**Ở đâu:** [feat_user_behaviour.sql:9](../dbt/models/marts/feat_user_behaviour.sql#L9)
`where event_ts >= now() - interval '30 days'` và dòng 32 `date_diff('day', first_seen_ts, now())`.

`seed_loader` gán `feature_ts = now()` lúc nạp CSV; dbt chạy **sau đó**. Nên
`feat_user_behaviour` gộp cả event xảy ra **sau** `feature_ts`.

**Đi ngược cái gì:** trong cùng một dòng `training_dataset`, `rt_*` tôn trọng biên PIT
còn `hist_*` thì không. Hiện chưa gây hại vì user seed chưa có event (gap G4) — nhưng
sẽ thành **leakage thật** ngay khi backfill event lịch sử ở Phase M3.

**Sửa:** join snapshot, dùng `feature_ts` làm mốc, đúng như `feat_user_realtime_pit` làm.

---

### 🟠 V7 · Hai feature sai contract

| Feature | Contract nói | Code làm |
|---|---|---|
| `user_tenure_days` | "số ngày từ lần đầu thấy user" | bị chặn ở **30** do lọc cửa sổ trước khi `min()` |
| `voucher_used_30d` | "số voucher **đã dùng**" | đếm `voucher_claim` — tức **claim**, không phải redeem |

**Đi ngược cái gì:** §15 (feature phải có lineage đúng) và §10 (một định nghĩa duy nhất).

---

### 🟡 V8 · `DECISION` không phải dữ liệu nghiệp vụ

**Ở đâu:** `ops.inference_log` — schema gần đúng nhưng nằm ở tầng ops/audit, **không
có FK tới voucher issuance**.

**Đi ngược cái gì:** N5 của ERD. Không join được `decision → issuance → order` ⇒ không
trả lời được *"voucher mình phát có tạo ra đơn không"*.

**Cách sửa không phá gì:** **giữ nguyên** `ops.inference_log` (Grafana đang dùng),
**thêm** `biz.decision`.

---

### 🟡 V9 · Policy engine hard-code trong endpoint

**Ở đâu:** [app.py:250-255](../src/lzd_pipeline/serving/app.py#L250-L255)
`decision = "SEND_VOUCHER" if score >= threshold else "NO_VOUCHER"`.

**Đi ngược cái gì:** §11 — không có budget, cooldown, guardrail, experiment arm.
Không có `reason_code`. Không tách module.

---

### 🟡 V10 · Không mô hình hoá được thí nghiệm TARGETED vs RCT

**Ở đâu:** không có gì tương ứng trong repo.

**Đi ngược cái gì:** §5 (so sánh treatment distribution với LZD). Dataset LZD có **hai**
chế độ gán treatment (train targeted 22.2%, test RCT 52.1%). Synthetic system chỉ có
một cách phát voucher ⇒ không tái tạo được cấu trúc thí nghiệm ⇒ mọi so sánh phân bố
đều khập khiễng.

**Đã có lời giải:** `EXPERIMENT.mode` (ERD §7.2).

---

### 🟢 V11 · Model chưa tồn tại (đã biết)

`train.build_model/fit_model/evaluate/predict_uplift` và `MlflowUpliftModel.load/predict`
đều `raise NotImplementedError`. Đang chạy `StubModel`.

**Không phải "đi ngược"** — chỉ là chưa làm. Nhưng nó **chặn** việc kiểm G2, mà G2 lại
quyết định toàn bộ scenario có ý nghĩa hay không.

---

### Bảng tổng hợp

| # | Vi phạm | Mức | Điều cấm / mục prompt |
|---|---|---|---|
| V1 | Layer 1 vắng mặt | 🔴 | mental model §2 |
| V2 | Thiếu `VOUCHER_ISSUED`, loop hở | 🔴 | §12 |
| V3 | `order` gộp CREATED+PAID | 🔴 | §8, A-01 |
| V4 | user_id không tồn tại | 🟠 | cấm #7 |
| V5 | `rt_session_len_sec` skew | 🟠 | §10 |
| V6 | `hist_*` không PIT | 🟠 | §10, §15 |
| V7 | 2 feature sai contract | 🟠 | §15 |
| V8 | DECISION không phải biz data | 🟡 | §12 |
| V9 | policy hard-code | 🟡 | §11 |
| V10 | không có TARGETED/RCT | 🟡 | §5 |
| V11 | model chưa có | 🟢 | §11 (chặn G2) |

---

## 4. Nguyên tắc migration

| # | Nguyên tắc | Cụ thể |
|---|---|---|
| **M1** | **Additive trước, destructive sau** | Thêm `biz` schema, thêm topic `v2`, thêm feature — không xoá gì ở bước đầu |
| **M2** | **Không xoá cột feature nào chưa qua ablation** | Điều cấm số 5. `f70` (hằng số) là ngoại lệ duy nhất, và vẫn phải bump version |
| **M3** | **Đổi event schema = bump topic version** | `app.user.events.v1` → `v2`, chạy song song. Đây là chính nguyên tắc `schemas.py` đã tự đặt ra |
| **M4** | **Đổi feature = bump `feature_spec.version`** | Điều cấm số 10. Không sửa tại chỗ |
| **M5** | **Test set bất khả xâm phạm** | Điều cấm số 9. Scenario chỉ dùng user `lzd_split='train'` |
| **M6** | **Mọi ghi Redis đi qua Kafka** | Điều cấm số 6. Kể cả đường quay ngược của decision |
| **M7** | **Mỗi phase có gate kiểm chứng được** | Không qua gate ⇒ không sang phase sau |
| **M8** | **Giữ nguyên phần hạ tầng đang tốt** | Danh sách §1.1 |

---

## 5. Lộ trình theo phase

### M0 · Sửa các lỗi độc lập *(làm được ngay, không phụ thuộc gì)*

| Việc | Vi phạm | Rủi ro |
|---|---|---|
| Chốt định nghĩa `rt_session_len_sec`, sửa `_update_realtime()` | V5 | Thấp |
| Viết test parity offline↔online cho 7 `rt_*` (gap G7) | — | Thấp |
| Sửa `feat_user_behaviour` dùng `feature_ts` thay `now()` | V6 | Thấp |
| Test parity truyền tải `f*`: `training_dataset` == Redis | — | Thấp |
| Cắm `quality.online_offline_tolerance` vào CI | — | Thấp |

**GATE M0:** test parity chạy xanh cho cả 7 `rt_*` và cho `f*`.

> Việc đầu tiên (V5) chính là **đề nghị số 1** trong audit cũ. Nó độc lập hoàn toàn
> với mọi kế hoạch mô phỏng, sửa rẻ, giá trị ngay.

---

### M1 · Trả lời G2 — **gate quyết định số phận cả project**

**Chưa xây gì thêm. Chỉ đo.**

1. Implement model tối thiểu (T-learner + LightGBM — đường ngắn nhất, V11)
2. Log `feature_list.json` để cố định thứ tự cột
3. **Đo độ nhạy:** giữ `f0..f82` cố định, quét 11 feature event-derived trong khoảng
   thực tế, đo `uplift_score` dịch bao nhiêu
4. Train song song `model_full` (94 feature) và `model_live` (11 feature) — lựa chọn
   (b) ở `FEATURE_LINEAGE.md` §2.4

**GATE M1 — bắt buộc:**

```
Nếu uplift_score gần như KHÔNG đổi khi 11 feature event-derived thay đổi
   ⇒ DỪNG. Toàn bộ scenario T0→T1→T2 sẽ chạy đẹp mà không đo được gì.
   ⇒ Xử lý trước (đưa rt_*/hist_* vào model với trọng số đủ lớn,
      hoặc chấp nhận đây là bàn thử HẠ TẦNG chứ không phải bàn thử QUYẾT ĐỊNH).
```

> Đây là rủi ro lớn nhất của cả project. Bảng feature importance bạn có **chỉ chứa
> `f*` và biến engineered từ `f*`** — không một dòng nào cho `rt_*`/`hist_*`. Phải
> biết điều này **trước**, không phải sau khi xây xong simulator.

**Đánh giá trên test set nguyên vẹn (AUUC/Qini) — không cắt, không đốt (M5).**

---

### M2 · Dựng Layer 1 — schema `biz` *(additive thuần)*

1. Tạo Postgres schema `biz` với các entity ở `ERD.md` §3–§7
2. Seed catalog tĩnh: SELLER / STORE / CATEGORY (3 cấp, ~500 lá) / PRODUCT / SKU
3. Tạo `CUSTOMER` + `CUSTOMER_IDENTITY_MAP`, bind với user `lzd_split='train'`
4. Tạo `CAMPAIGN` + `VOUCHER` + `EXPERIMENT`/`EXPERIMENT_ARM`

**Không đụng** vào bất cứ thứ gì đang chạy. Pipeline hiện tại tiếp tục hoạt động.

**GATE M2:** query được `biz` và mọi FK/ràng buộc ở `ERD.md` §9 hợp lệ;
`CUSTOMER_IDENTITY_MAP` không chứa user `test` nào.

---

### M3 · Business transaction engine + event v2 *(additive)*

1. Viết engine sinh **transaction** (không phải event): T1–T4 ở `ERD.md` §11
2. Engine ghi `biz` **và** phát event theo envelope `BUSINESS_EVENT_MODEL.md` §2
3. Topic `app.user.events.v2` chạy **song song** `v1` (M3)
4. `stream_consumer` đọc cả hai topic; consumer v2 map sang bảng raw riêng
5. Backfill lịch sử cho user được chọn để 4 `hist_*` khác 0 (gap **G4**)
6. Thêm `PRODUCER_MODE`: `random_v1` (giữ nguyên) | `business_v2` (mới)

**GATE M3:**
- Bất biến ở `BUSINESS_EVENT_MODEL.md` §4 giữ được (vd `ORDER_CREATED.amount ==
  ORDER.gross_amount`, `budget_spent ≤ budget_total`)
- Mọi `sku_id` trong event tồn tại trong `biz.sku`; `unit_price == SKU.list_price`
- Mọi `customer_id` có trong `CUSTOMER_IDENTITY_MAP`

---

### M4 · Feature Track C + bump spec *(có breaking change, có kiểm soát)*

1. `feature_spec.yml`: `version: 1 → 2`
2. Thêm feature Track C (`FEATURE_DICTIONARY.md` §5) — **mỗi feature phải có đủ 13
   trường lineage trước khi code**
3. Sửa V7: `user_tenure_days` lấy từ `CUSTOMER.registered_at`; tách
   `voucher_claimed_30d` / `voucher_redeemed_30d`
4. Cân nhắc loại `f70` (hằng số) — ngoại lệ M2
5. **Chưa** loại `f23`/`f25`, `f68`/`f71`/`f74`/`f77` — cần ablation trước (điều cấm số 5)
6. dbt build lại; sync version mới lên Redis; atomic swap

**GATE M4:** parity offline↔online xanh cho **mọi** feature Track C; rollback về
version 1 thử được và thành công.

---

### M5 · Closed loop *(phần khó nhất)*

1. Tách policy khỏi `/decide` thành module riêng: threshold + budget + cooldown +
   guardrail + `reason_code` (V9)
2. Thêm bảng `biz.decision`, **giữ nguyên** `ops.inference_log` (V8)
3. `/decide` publish `DECISION_MADE` (+ `VOUCHER_ISSUED` nếu SEND) vào Kafka —
   **không ghi thẳng Redis** (M6)
4. Business engine tiêu thụ `VOUCHER_ISSUED` → mô phỏng phản ứng user →
   `VOUCHER_VIEWED/CLAIMED/REDEEMED` → có thể dẫn tới `ORDER_PAID`
5. Feature `rt_voucher_active_cnt` cập nhật từ chính stream đó

**GATE M5:** chạy được kịch bản S3 (`BUSINESS_EVENT_MODEL.md` §6.3) và quan sát được
`reason_code = COOLDOWN` ở T4.

---

### M6 · Scenario driver + E2E validation

1. Điều khiển thời gian (gap **G6**) — tua nhanh để test cửa sổ trượt 30 ngày
2. Scenario driver: một user qua T0→T1→T2…, xuất bảng
   `(t, event mới, feature đổi, score, decision, reason_code)`
3. Chạy kịch bản S1 / S2 / S3
4. Chạy trên nhiều user, kiểm decision đổi **đúng hướng**
5. So phân bố synthetic vs LZD theo bảng `FEATURE_LINEAGE.md` §7.2

**GATE M6:** S1 cho `score₀ < score₃`; S2 cho `score_sau ≈ score₀`; treatment
distribution của chế độ TARGETED/RCT khớp bậc với LZD (22% / 52%).

---

### M7+ · Chỉ làm sau khi core chạy

Synthetic uplift ground truth `τ(x)` · ablation để gộp cột trùng · Optuna ·
adversarial validation · DESCN thay T-learner.

---

## 6. Ánh xạ sang PHASE 0–16 của prompt

| PHASE prompt | Trạng thái | Ở đâu |
|---|---|---|
| 0 · Audit repo + dataset | ✅ **Xong** | `AUDIT_KIEN_TRUC_VA_FEATURE.md` + §1 file này (+ I2 đã nâng lên CONFIRMED) |
| 1 · Research business domain | ✅ **Xong** | `LAZADA_BUSINESS_DOMAIN.md` |
| 2 · Define synthetic domain | ✅ **Xong** | `LAZADA_BUSINESS_DOMAIN.md` §10 (5 ASSUMPTION) |
| 3 · Design ERD | ✅ **Xong** | `ERD.md` |
| 4 · Define business transactions | ✅ **Xong** | `ERD.md` §11 |
| 5 · Define raw/domain events | ✅ **Xong** | `BUSINESS_EVENT_MODEL.md` |
| 6 · Map business data → features | ✅ **Xong** | `FEATURE_DICTIONARY.md` + `FEATURE_LINEAGE.md` |
| 7 · Build FE pipeline | ⬜ | **M3–M4** |
| 8 · Build Feature Store | 🟡 **Phần lớn đã có** | Cần bump version (M4) |
| 9 · Generate/validate training data | 🟡 **Đã có cho Track A/B** | Track C ở M4 |
| 10 · Baseline uplift model | ⬜ | **M1** ← đưa lên sớm, xem dưới |
| 11 · Validate model sensitivity | ⬜ | **M1 — GATE** |
| 12 · Kafka + streaming FE | ✅ **Đã có** | Cần topic v2 (M3) |
| 13 · Online inference | ✅ **Đã có** | Thiếu model thật (M1) |
| 14 · Policy / decision | 🟡 **Hard-code** | **M5** |
| 15 · Event-driven simulator | ⬜ | **M3 + M6** |
| 16 · E2E validation | ⬜ | **M6** |

### 6.1 · ⚠️ Một chỗ tôi đề nghị **đổi thứ tự** so với prompt

Prompt xếp model ở PHASE 10–11, **sau** khi xây pipeline. Tôi đề nghị đưa lên **M1,
ngay sau M0**.

**Lý do:** PHASE 11 ("validate model sensitivity to live features") là một **gate
tồn vong**, không phải một bước kiểm tra. Nếu model không nhạy với feature
event-derived thì PHASE 12–16 sẽ chạy trơn tru và **không đo được gì**. Xây xong
Layer 1 + event v2 + Track C rồi mới phát hiện điều đó là lãng phí lớn nhất có thể xảy ra.

Chi phí đưa lên sớm: thấp — hạ tầng train đã sẵn, chỉ cần T-learner + LightGBM.

**Đây là đề nghị, không phải quyết định.** Nếu bạn muốn giữ đúng thứ tự prompt thì
tôi làm theo — chỉ cần biết rằng rủi ro G2 sẽ được trả lời muộn hơn.

---

## 7. Deliverable tài liệu còn lại

Prompt §16 liệt kê 12 tài liệu. Trạng thái:

| Tài liệu | Trạng thái | Ghi chú |
|---|---|---|
| `LAZADA_BUSINESS_DOMAIN.md` | ✅ **Đã viết** | |
| `ERD.md` | ✅ **Đã viết** | |
| `BUSINESS_EVENT_MODEL.md` | ✅ **Đã viết** | |
| `FEATURE_DICTIONARY.md` | ✅ **Đã viết** | |
| `FEATURE_LINEAGE.md` | ✅ **Đã viết** | |
| `END_TO_END_ARCHITECTURE.md` | ✅ **Đã viết** (là file này, §1–§2) | Có thể tách ra nếu bạn muốn |
| `DATA_GENERATION.md` | ⬜ Viết ở **M3** | Cần chốt tham số phân bố trước — viết bây giờ sẽ là bịa |
| `FEATURE_ENGINEERING.md` | ⬜ Viết ở **M4** | Phụ thuộc Track C được duyệt |
| `FEATURE_STORE.md` | 🟡 Một phần đã có | `feature_spec.yml` + `DATA_FLOW.md`; viết đủ ở M4 |
| `MODEL_CONTRACT.md` | ⬜ Viết ở **M1** | Phụ thuộc lựa chọn (a)/(b)/(c) ở §8 |
| `ONLINE_OFFLINE_PARITY.md` | ⬜ Viết ở **M0** | Cùng lúc với test parity — doc và test sinh đôi |
| `DECISION_POLICY.md` | ⬜ Viết ở **M5** | Phụ thuộc CAMPAIGN budget |

> **Lý do hoãn 6 file:** viết chúng bây giờ đồng nghĩa với việc **bịa tham số và quyết
> định chưa được chốt** — đúng thứ nguyên tắc §17 cấm ("không silently invent").
> Mỗi file được lên lịch ở phase mà quyết định của nó đã có căn cứ.

---

## 8. Quyết định cần bạn xác nhận

Tôi **không** code gì cho tới khi có trả lời cho Q1–Q3.

### Q1 · Mô hình hai/ba track — chọn (a), (b) hay (c)?

`FEATURE_LINEAGE.md` §2.4. **Khuyến nghị (b)** — train song song `model_full` (A+B+C)
và `model_live` (B+C). Đây là cách duy nhất *đo* được rủi ro G2 thay vì phỏng đoán,
và kết quả quyết định luôn nên chọn (a) hay (c).

### Q2 · Có đồng ý đưa model lên M1 (trước khi xây Layer 1) không?

§6.1. **Khuyến nghị: có.** PHASE 11 là gate tồn vong; trả lời muộn tốn kém hơn nhiều.

### Q3 · Xác nhận 5 ASSUMPTION ở `LAZADA_BUSINESS_DOMAIN.md` §10?

| | Nội dung | Ảnh hưởng nếu sai |
|---|---|---|
| **A-01** | conversion = đơn đạt `PAID` | Đổi định nghĩa label ⇒ train lại toàn bộ |
| **A-02** | treatment = `VOUCHER_ISSUED` (phát), không phải claimed/redeemed | Đổi ⇒ ước lượng uplift vô nghĩa về mặt nhân quả |
| **A-03** | đơn vị quyết định = (customer, thời điểm), treatment nhị phân | Đổi ⇒ đổi entity key feature store ⇒ thay đổi lớn |
| **A-04** | ID space tách biệt + `CUSTOMER_IDENTITY_MAP` | Đổi ⇒ quay lại vấn đề G3 |
| **A-05** | không mô hình review/chat/livestream/ví/payout | Đổi ⇒ phình ERD + event + pipeline cùng lúc |

---

### Và ba việc tôi đề nghị làm **ngay sau khi bạn xác nhận** — tất cả nằm ở M0, đều rẻ:

1. **Sửa V5** (`rt_session_len_sec`) — training/serving skew có thật đang chạy, độc lập
   với mọi kế hoạch mô phỏng
2. **Viết test parity** offline↔online cho 7 `rt_*` — bài test này chính là thứ đã
   bắt được V5, và sẽ canh giữ mọi feature Track C sau này
3. **Sửa V6** (`hist_*` không point-in-time) — hiện chưa gây hại vì user seed chưa có
   event, nhưng sẽ thành leakage thật ngay khi backfill ở M3
