# Data Generation

> **Trạng thái: THIẾT KẾ + E2E prototype. Track A, handoff T0 và Track B rule-based đã chạy local; publish production ra MinIO/Kafka vẫn mở. API và Redis representation ngoài scope.**
>
> **Phạm vi:** cách sinh synthetic business data + raw event từ user có thật trong
> dataset LZD, phục vụ (A) tái tạo feature và (B) mô phỏng luồng online.
>
> **Nhãn bắt buộc dùng xuyên suốt:**
>
> | Nhãn | Nghĩa |
> |---|---|
> | `[FACT]` | Chứng minh từ schema / code trong repo |
> | `[MEASURED]` | Kết quả query/experiment trên dataset, có số cụ thể |
> | `[ASSUMPTION]` | Semantic được **giả định** để xây synthetic data |
> | `[UNKNOWN]` | Chưa đủ evidence |
>
> 🚫 Không đoạn nào được biến `[ASSUMPTION]` thành `[FACT]`.

---

## Mục lục

1. [Purpose](#1-purpose)
2. [Input dataset](#2-input-dataset)
3. [Feature tiers](#3-feature-tiers)
4. [Transformation regimes](#4-transformation-regimes)
5. [Semantic assumptions](#5-semantic-assumptions)
6. [Customer synthesis](#6-customer-synthesis)
7. [Event synthesis](#7-event-synthesis)
8. [Track A vs Track B](#8-track-a-vs-track-b)
9. [Raw event schema](#9-raw-event-schema)
10. [Temporal constraints](#10-temporal-constraints)
11. [Data lineage](#11-data-lineage)
12. [Volume](#12-volume)
13. [Validation & GATE A](#13-validation--gate-a)
14. [Failure handling](#14-failure-handling)
15. [Reproducibility & random seed](#15-reproducibility--random-seed)
16. [Measured vs assumed — bảng tổng](#16-measured-vs-assumed--bảng-tổng)

---

## 1. Purpose

Sinh ra một hệ thống nghiệp vụ synthetic + dòng event thô sao cho:

```
   (A)  chạy feature engineering XUÔI CHIỀU trên event đó
        tái tạo lại đúng feature row của LZD               → GATE A

   (B)  từ trạng thái đó sinh hành vi TƯƠNG LAI
        để feature dịch chuyển → model chấm lại → quyết định đổi
```

**Không** nhằm khôi phục semantic thật của Lazada. Xem `FEATURE_LINEAGE.md` §4.

---

## 2. Input dataset

`[FACT]` — nguồn duy nhất: `data/full_trainset.csv`, `data/full_testset.csv`.
Đường đi trong repo: CSV → `raw/user_snapshot` (`seed_loader.py`) →
`stg_user_snapshot.sql` (**chỉ** `cast(f{i} as double)` + dedup) →
`feat_user_selected_serving.sql` cho Redis selected feature sync.

`[MEASURED]`

| | Train | Test |
|---|---|---|
| Số dòng | 926,669 | 181,669 |
| % treated | 22.2% | 52.1% |
| Positive rate | 1.99% | 3.52% |
| CR treated / control | 5.66% / 0.94% | 3.70% / 3.33% |
| Chênh lệch thô | +4.72pp | **+0.37pp** |

`[FACT]` (paper §4.1) — train = observational có targeting bias; test = RCT.

> 🚫 **Test set không được dùng làm nguồn event stream.** Uplift thật chỉ +0.37pp;
> đủ 181,669 dòng ⇒ z ≈ 4.3, cắt còn 50k ⇒ z ≈ 2.3. Generator **chỉ** đọc
> `split = 'train'`.

**Scope reconstruction của tài liệu này:** `user_id`, `dt`, `feature_ts` và 36 feature
đã được selection chốt. Source CSV có thể chứa `label`/`is_treat`, nhưng hai cột đó
thuộc evaluation/training bên ngoài solver và **không được đi vào generation input**.
47 cột `f*` còn lại không tham gia generation.

---

## 3. Feature tiers

`[MEASURED]` — cập nhật sau khi đo `f5`, `f11`.

| Tier | Số cột | Cột |
|---|---|---|
| **T1 — event-level** | **6** | `f1`, `f2`, `f5`, `f11`, `f18`, `f30` |
| **T2 — attribute-level** | **12** | `f37`, `f38`, `f79`, `f80`, `f81`, `f82`, `f40`, `f43`, `f44`, `f45`, `f64`, `f68` |
| **T3 — pass-through** | **18** | `f3`,`f4`,`f8`,`f9`,`f10`,`f12`,`f13`,`f16`,`f20`,`f21`,`f22`,`f23`,`f25`,`f26`,`f28`,`f29`,`f31`,`f35` |
| **UNKNOWN** | **0** | — |

```
36 = 6 (T1) + 12 (T2) + 18 (T3)
```

### 3.1 · Redundancy trong selected set

`[MEASURED]` — 36 cột **không** bằng 36 tín hiệu độc lập:

| Nhóm | Bằng chứng | Tín hiệu thật |
|---|---|---|
| `f23`, `f25` | trùng 99.78% dòng, `corr = 0.999957` | **1** |
| `f79`,`f80`,`f81`,`f82` | `card(tuple) = card(mỗi cột) = 515` ⇒ song ánh | **1** |

```
36 cột  ≈  32 tín hiệu độc lập
```

> 🚫 **Không bỏ cột nào trong task này.** Cần ablation trước
> (thuộc model-evaluation/ablation workstream).

---

## 4. Transformation regimes

`[MEASURED]` — dataset dùng **hai quy ước log khác nhau**. Đây là phát hiện then chốt
cho generator và cho tolerance của GATE A.

### 4.1 · Regime `L10` — log cơ số 10, làm tròn 6 chữ số

| Cột | `10^f ∈ ℤ` | Miền `n` | Số mức | Ghi chú |
|---|---|---|---|---|
| `f30` | **100%** dòng | `[1, 30]` | 30 | trần đúng 30 |
| `f18` | **100%** dòng | `[1, 2260]` | 144 | **95.5%** dòng có `n = 1` |

`[MEASURED]` Round-trip `round(log10(round(10^f)), 6) = f` đúng **100.0000%** —
**bằng nhau chính xác**, không cần dung sai.

Lưu 6 chữ số thập phân ⇒ `10^1.477121 = 29.999982`. So sánh **giá trị đã giải mã**
phải `round()` trước; so sánh **giá trị feature** thì dùng đẳng thức chính xác.

### 4.2 · Regime `LN` — log tự nhiên, độ chính xác float64 đầy đủ

| Cột | `e^f ∈ ℤ` | Miền `n` | Số mức | `n = 1` |
|---|---|---|---|---|
| `f5` | **100%** dòng (tol. 1e-12) | `[1, 14245]` | 864 | 74.2% |
| `f11` | **100%** dòng (tol. 1e-12) | `[1, 2144]` | 669 | 70.2% |

`[MEASURED]` Round-trip `ln(round(e^f)) = f` **chỉ đúng 93.05% / 93.42%** khi so bằng
đẳng thức chính xác — do sai số bit cuối của float64. Với dung sai tương đối
**1e-15**: **100%**.

⇒ **Hai regime cần hai luật so sánh khác nhau.** Chi tiết ở `RECONSTRUCTION_CONTRACT.md` §9.

### 4.3 · Regime `REC` — recency

| Cột | Kiểu | Miền | Ràng buộc |
|---|---|---|---|
| `f1`, `f2` | nguyên | `[0, 365]`, 366 mức | `f1 ≥ f2` **đúng ở mọi dòng** (`min(f1−f2) = 0`) |

### 4.4 · Regime `CAT` — categorical

| Nhóm nguồn | Số mức | Sinh ra cột đã chọn |
|---|---|---|
| `G1` (`f40`–`f42`) | 3 | `f40` |
| `G2` (`f43`–`f52`) | 10 | `f43`, `f44`, `f45` |
| `G4` (`f63`–`f65`) | 3 | `f64` |
| `G6` (`f68`, `f78`) | 2 | `f68` |
| `synthetic_category_515` | 515 | `f79`, `f80`, `f81`, `f82` |
| `synthetic_attr_64` | 64 | `f37` |
| `synthetic_attr_241` | 241 | `f38` |

> ⚠️ **Là 7 thuộc tính categorical, không phải 6.** (4 nhóm one-hot + `cat_515` +
> `attr_64` + `attr_241`.) Con số "6" trong yêu cầu là đếm thiếu — ERD phải có đủ 7.

### 4.5 · Regime `PASS` — pass-through

18 cột T3. Không transformation. `[UNKNOWN]` semantic.

---

## 5. Semantic assumptions

Mọi mục dưới đây là `[ASSUMPTION]`. Cấu trúc là `[MEASURED]`; **ý nghĩa** là giả định.

### S-01/S-02 · `f1`/`f2` = first/last order recency

- **Ràng buộc đo được:** `f1 ≥ f2` ở 100% dòng.
- **Vì sao chọn semantic này:** quan hệ "lần đầu luôn xa hơn lần cuối" **tự thoả**
  ràng buộc — không phải ép. Tiêu chí chọn assumption: ưu tiên cái mà ràng buộc tự
  nhiên khớp ràng buộc quan sát được.
- **S-01:** `f1` = `days_since_first_order`. **Semantic thật:** `[UNKNOWN]`.
- **S-02:** `f2` = `days_since_last_order`. **Semantic thật:** `[UNKNOWN]`.

### S-03 · `f30` = `active_days_in_last_30d`

- **Ràng buộc đo được:** `n ∈ [1,30]`, trần đúng 30, 30 mức phân biệt.
- **Vì sao:** trần 30 là **hệ quả tự nhiên** của cửa sổ 30 ngày. Một counter "số đơn"
  không có lý do gì để dừng đúng ở 30.
- **Semantic thật:** `[UNKNOWN]`.

### S-04 · `f18` = counter của một hành động **hiếm**

- **Ràng buộc đo được:** `n ∈ [1,2260]`, **95.5% dòng có `n = 1`**.
- **Vì sao:** độ thưa cực cao ⇒ hành động hiếm. Nếu quy ước là `log10(1+count)` thì
  95.5% user **chưa từng** làm hành động này.
- **Chưa chốt** hành động cụ thể. `[UNKNOWN]`.

### S-05 · `f5`, `f11` = hai counter tần suất cao, **khác nhau**

- **Ràng buộc đo được:** `e^f ∈ ℤ`; `f5`: `n ≤ 14245`, 74.2% có `n=1`;
  `f11`: `n ≤ 2144`, 70.2% có `n=1`; hai cột khác nhau ở **381,058 dòng**,
  `card(tuple) = 16,479` ⇒ **hai biến độc lập**.
- **Chưa chốt** đếm cái gì. `[UNKNOWN]`.

> ⚠️ **`f5`/`f11` dùng cơ số `e`, còn `f18`/`f30` dùng cơ số 10.** Hai quy ước khác
> nhau trong cùng một dataset ⇒ nhiều khả năng do **hai pipeline/team khác nhau** sinh
> ra. Đây là `[MEASURED]` về hình thức, `[UNKNOWN]` về nguyên nhân — và là lý do
> generator phải tham số hoá cơ số log, không hard-code.

### S-06 · 7 thuộc tính categorical = phân khúc nghiệp vụ synthetic

Tên `synthetic_*` là **cố ý** — nhắc rằng đây là `[ASSUMPTION]`.
Semantic thật: `[UNKNOWN]`.

---

## 6. Customer synthesis

### 6.1 · Luồng

```
   reconstruction input (user_id + dt + feature_ts + 36 selected f)
                 │
       ┌─────────┼─────────┬──────────────┐
       ▼         ▼         ▼              ▼
   DECODE T1  DECODE T2  PASS T3      IDENTITY
       │         │         │              │
   counter    level id   18 cột      C… ↔ U…
   + dates                (đóng băng)
       │         │         │              │
       └─────────┴────┬────┴──────────────┘
                      ▼
              biz.customer  (1 dòng / user)
```

### 6.2 · Decode T1 → counter đích

| Cột | Công thức decode | Kiểu đích |
|---|---|---|
| `f30` | `n = round(10^f30)` | INT `[1,30]` |
| `f18` | `n = round(10^f18)` | INT `[1,2260]` |
| `f5` | `n = round(exp(f5))` | INT `[1,14245]` |
| `f11` | `n = round(exp(f11))` | INT `[1,2144]` |
| `f1` | `d = f1` (đã nguyên) | INT `[0,365]` |
| `f2` | `d = f2` | INT `[0,365]` |

### 6.3 · Decode T2 → level id

| Thuộc tính | Cách decode |
|---|---|
| `G1`, `G2`, `G4`, `G6` | vị trí bit `1` trong nhóm one-hot ⇒ level id trực tiếp |
| `synthetic_category_515` | tra `biz.encoding_map_515` bằng tuple `(f79,f80,f81,f82)` ⇒ level id |
| `synthetic_attr_64` | tra `biz.encoding_map_64` bằng `f37` |
| `synthetic_attr_241` | tra `biz.encoding_map_241` bằng `f38` |

**Bảng encoding map được *fit từ dataset*** (`SELECT DISTINCT` rồi đánh số), giống hệt
cách một encoder production được fit trên training data rồi lưu lại. `[FACT]` — 515
tuple phân biệt tồn tại; ánh xạ 1-1 nên decode không mất thông tin.

> 🚫 **Ràng buộc chống tautology:** level id **phải điều khiển hành vi mô phỏng**
> (xác suất mua, giá trị giỏ, phản ứng voucher) ở Track B. Nếu chỉ lưu rồi phát lại,
> đó là **copy trá hình**, không phải feature engineering.
> Xem `RECONSTRUCTION_CONTRACT.md` §12.

### 6.4 · Pass-through T3

18 cột copy nguyên trạng vào `biz.customer_opaque` (hoặc cột JSON). **Không** decode,
**không** gán semantic, **không** tham gia sinh event.

---

## 7. Event synthesis

> 🔄 **CẬP NHẬT — mục này đã được thay thế bởi `RECONSTRUCTION_CONSTRAINT_MODEL.md`.**
>
> Sinh event **không phải** 6 bài toán độc lập. Phải giải **một hệ ràng buộc** cho ra
> **một** event history duy nhất tái tạo đồng thời cả 6 target.
>
> `[MEASURED]` Hai kết quả làm thay đổi thiết kế — **phải phân biệt ba khái niệm**:
>
> | Khái niệm | Giá trị | Bản chất |
> |---|---|---|
> | **Structural conflict** (`f2 ↔ f30`) | **8 user — 0.001%** | `[MEASURED]`, **vô điều kiện** |
> | **Capacity infeasibility under assumptions** | **440,713 — 47.56%** | bất đẳng thức `[MEASURED]`, nhưng kết luận impossibility là **CÓ ĐIỀU KIỆN** |
> | **Actual unsolvable rate** | **`[UNKNOWN]`** | chưa biết |
>
> ⚠️ `PROPOSITION FE-1` (tồn tại hoạt động không quan sát được) là **mệnh đề có điều
> kiện, chưa phải fact**. `[FACT]` không dòng code nào trong repo tính `f5`/`f11`/`f18`/
> `f30`/`f1`/`f2` ⇒ event taxonomy của `f*` **không quan sát được**. Kết luận chứng minh
> được duy nhất: **ít nhất một trong S-03 hoặc P2 là sai**.
>
> Bảng dưới vẫn đúng về **event nào phục vụ cột nào**, nhưng thứ tự sinh và điều kiện
> khả thi phải theo constraint model.

### 7.1 · Chỉ 6 cột T1 cần event

| Cột | Event sinh ra | Số lượng / user | Ràng buộc thời gian |
|---|---|---|---|
| `f1` | `EVT_ORDER_PAID` (`ORDER_PAID` semantic alias) sớm nhất | 1 | tại `feature_ts − f1 ngày` |
| `f2` | `EVT_ORDER_PAID` (`ORDER_PAID` semantic alias) gần nhất | 1 | tại `feature_ts − f2 ngày` |
| `f30` | H1: ngày active từ mọi CFS event + `EVT_SESSION_STARTED` để phủ thiếu; H2: `EVT_F30` | `n` ngày **phân biệt** trong 30 ngày hoặc `n` event H2 | rải đều theo constraint |
| `f18` | `EVT_F18` | `n` | trong cửa sổ chưa chốt |
| `f5` | `EVT_F5` | `n` | cửa sổ chưa chốt |
| `f11` | `EVT_F11` | `n` | cửa sổ chưa chốt |

### 7.2 · Xử lý trường hợp biên

| Trường hợp | Xử lý |
|---|---|
| `f1 == f2` | Sinh **một** event duy nhất, không phải hai. "Lần đầu = lần cuối" ⇒ user chỉ có 1 đơn |
| `f1 == f2 == 0` | Event tại đúng `feature_ts − 0` ⇒ phải **strictly before** `feature_ts` (xem §10) |
| `f30` = 30 | 30 ngày phân biệt trong cửa sổ 30 ngày ⇒ **mọi ngày đều active**, không còn tự do |
| `n = 1` (đa số) | 1 event. Không được sinh 0 |
| `f2 > 0` nhưng `f30` yêu cầu active hôm nay | Hai ràng buộc có thể **xung đột** — xem §14 |

> ⚠️ **`f30` yêu cầu `n` ngày PHÂN BIỆT, không phải `n` event.** Nếu semantic S-03
> đúng thì sinh `n` event trong cùng một ngày sẽ cho `active_days = 1`, không phải `n`.
> Đây chính là cảnh báo "không được hiểu đơn giản là `10^f30` events".

### 7.3 · Cửa sổ của `f5`, `f11`, `f18` — **chưa chốt**

`[UNKNOWN]` — không có bằng chứng về cửa sổ thời gian. Ba lựa chọn, cần quyết:

| Lựa chọn | Hệ quả |
|---|---|
| Cùng 30 ngày như `f30` | Đơn giản; nhưng `f5` max 14,245 event / 30 ngày = 475/ngày — **không hợp lý cho một user** |
| 365 ngày | 14,245 / 365 ≈ 39/ngày — hợp lý hơn |
| Toàn bộ lifetime | Không có trần, luôn thoả |

**S-07 / Khuyến nghị:** 365 ngày, và ghi là `[ASSUMPTION]`. Lý do: `f1` cũng có miền `[0,365]`
⇒ dataset có ít nhất một cửa sổ 365 ngày. Không phải bằng chứng, chỉ là lựa chọn nhất quán.

---

## 8. Track A vs Track B

> **Đây là ranh giới quan trọng nhất của tài liệu này.**
> Trộn hai track làm validation biến thành tautology.

| | **Track A — Backfill / Reconstruction** | **Track B — Live generation** |
|---|---|---|
| Mục đích | Tái tạo đúng feature row gốc | Sinh hành vi tương lai |
| Bản chất | **Deterministic reconstruction problem** | **Behaviour simulation** |
| Ràng buộc | `event_ts < feature_ts`; **phải** khớp `f` | `event_ts >= feature_ts` (`feature_ts == reference_ts`); **không** ràng buộc `f` |
| Ngẫu nhiên | Chỉ ở chỗ không ảnh hưởng feature (vd giờ trong ngày) | Tự do, có mô hình hành vi |
| Đầu ra kiểm chứng | `reconstructed == original` → **GATE A** | feature dịch chuyển, decision đổi |
| Dùng label / is_treat? | ❌ **KHÔNG BAO GIỜ** | ❌ **KHÔNG BAO GIỜ** |

### 8.1 · Yêu cầu tách code path

```
❌ CẤM:  một generator có cờ `mode = backfill | live`
         dùng chung hàm sinh event
✅ ĐÚNG: hai module riêng, hai signature riêng

   backfill_generator(customer_state, feature_ts) -> list[Event]
        # KHÔNG nhận behaviour model
        # KHÔNG nhận label, is_treat

   live_generator(customer_state, behaviour_model, t_from, t_to) -> Iterator[Event]
        # KHÔNG nhận target feature values
```

`backfill_generator` **không được** nhìn thấy `behaviour_model`;
`live_generator` **không được** nhìn thấy giá trị `f` đích. Chữ ký hàm là nơi
thực thi ranh giới này, không phải comment.

---

## 9. Raw event schema

`[FACT]` — schema hiện tại (`schemas.AppEvent`) thiếu FK. Envelope đề xuất (v2) đã
mô tả ở `BUSINESS_EVENT_MODEL.md` §2. Trường bắt buộc thêm cho generation:

```jsonc
{
  "event_id":       "uuid",
  "event_type":     "EVT_ORDER_PAID",  // Track A CFS namespace; Track B business v2 dùng ORDER_PAID
  "event_ts":       1786531200.482,   // epoch giây, thời điểm XẢY RA
  "customer_id":    "C0000123",
  "session_id":     "…",
  "order_id":       null,             // FK
  "voucher_id":     null,
  "issuance_id":    null,
  "decision_id":    null,
  "sku_id":         null,
  "amount":         0.0,
  "schema_version": 2,

  // --- chỉ có ở Track A, phục vụ audit reconstruction ---
  "_gen_track":     "A",              // "A" | "B"  — BẮT BUỘC
  "_gen_reason":    "f30",            // cột nào yêu cầu event này
  "_gen_seed":      "…"
}
```

> Ba trường `_gen_*` **không** được đi vào feature engineering. Chúng chỉ tồn tại ở
> raw layer để truy vết. `stg_events` phải drop chúng — nếu không, chúng trở thành
> đường rò rỉ.

### 9.1 · Bảng raw / biz

```
raw.customer_snapshot        ← đã có (từ CSV)
raw.events                   ← MỚI, envelope v2

biz.customer                 ← thuộc tính đã decode (T1 counter đích + T2 level id)
biz.customer_opaque          ← 18 cột T3
biz.customer_identity_map    ← C… ↔ U…, chỉ split='train'
biz.encoding_map_515         ← fit từ dataset: (level_id, f79, f80, f81, f82)
biz.encoding_map_64          ← (level_id, f37)
biz.encoding_map_241         ← (level_id, f38)
```

### 9.2 · Model feature engineering — tách 4, không gộp

| Model | Nguồn | Sinh ra | Chịu GATE A? |
|---|---|---|---|
| `feat_cfs_counter` | `stg_events` | `f5`, `f11`, `f18`, `f30` | ✅ |
| `feat_cfs_recency` | `stg_events` | `f1`, `f2` | ✅ |
| `feat_cfs_categorical` | `biz.customer` + `encoding_map_*` | 12 cột T2 | ✅ |
| `feat_passthrough` | `raw.customer_snapshot` | 18 cột T3 | ❌ (không phải FE) |

Tách vì **contract validation khác nhau** (§13).

---

## 10. Temporal constraints

| # | Ràng buộc | Vì sao |
|---|---|---|
| **T-1** | Mọi event Track A: `event_ts < feature_ts` **nghiêm ngặt** | Trùng `feature_ts` là biên mập mờ; `feat_user_realtime_pit.sql` dùng `<`, phải khớp |
| **T-2** | Mọi event Track B: `event_ts >= feature_ts` (`feature_ts == reference_ts`) | Không được lẫn vào cửa sổ reconstruction vì Track A/PIT dùng `<` |
| **T-3** | Cửa sổ counter tính **lùi từ `feature_ts`**, không phải từ `now()` | Sửa lỗi D4 (`feat_user_behaviour` đang dùng `now()`) |
| **T-4** | `f30`: `n` **ngày phân biệt**, không phải `n` event | §7.2 |
| **T-5** | `f1 ≥ f2` ⇒ event của `f1` xảy ra **trước hoặc cùng lúc** event của `f2` | Ràng buộc đo được |
| **T-6** | Giờ-trong-ngày của event Track A là **tự do** | Không feature nào trong 36 cột phụ thuộc giờ ⇒ được phép ngẫu nhiên hoá |

---

## 11. Data lineage

```
   LZD row U
      │
      │ decode  [ASSUMPTION S-01..S-07]
      ▼
   biz.customer(C)                      biz.customer_opaque(C)
      │  counter đích + level id           │  18 cột T3
      │                                    │
      │ backfill_generator  [Track A]      │
      ▼                                    │
   raw.events   (event_ts < feature_ts)    │
      │                                    │
      │ stg_events (dedup, drop _gen_*)    │
      ▼                                    │
   feat_cfs_counter ──┐                    │
   feat_cfs_recency ──┤                    │
   feat_cfs_categorical┤                   │
                      │                    │
                      ▼                    ▼
                 reconstructed 18 cột   feat_passthrough 18 cột
                      │                    │
                      └────────┬───────────┘
                               ▼
                    feat_user_selected_serving (36 cột)
                               │
                    ┌──────────┴──────────┐
                    ▼                     ▼
              GATE A so với       CustomerState(T0)
              LZD row gốc                 │
                                          │ live_generator [Track B]
                                          ▼
                              raw/events_v2 track=B (event_ts >= feature_ts)
                                          ▼
                              Kafka v2 → consumer v2 → MinIO raw/events_v2
```

---

## 12. Volume

`[MEASURED]` — khối lượng event Track A cho **toàn bộ** 926,669 user train:

| Cột | Tổng `n` | Trung bình / user | p99 | max |
|---|---|---|---|---|
| `f30` | 7,622,020 | 8.23 | — | 30 |
| `f11` | 6,353,136 | 6.86 | 96 | 2,144 |
| `f5` | 6,054,207 | 6.53 | 93 | 14,245 |
| `f18` | 1,188,572 | 1.28 | 8 | 2,260 |
| **Tổng counter** | **21,217,935** | | | |
| `f1`, `f2` | ~1,853,338 | 2 | | |
| **TỔNG Track A** | **≈ 23.1 triệu event** | | | |

**Hệ quả thiết kế:**

- 23M event ≈ vài GB parquet — **khả thi** nhưng không nên chạy toàn bộ ngay.
- **Khuyến nghị:** scenario chạy trên **10,000 user** ⇒ ≈ **250,000 event**. Đủ để
  validate GATE A ở mức phân bố mà không tốn hạ tầng.
- Đuôi nặng: `f5` max 14,245 event cho **một** user. Generator phải xử lý được
  outlier này mà không nổ bộ nhớ ⇒ sinh theo stream, không build list trong RAM.

---

## 13. Validation & GATE A

Định nghĩa đầy đủ ở `RECONSTRUCTION_CONTRACT.md`. Tóm tắt ba mức:

```
1. ROW-LEVEL       mỗi user: reconstructed row == original row (T1 + T2)
2. COLUMN-LEVEL    mỗi cột: 100% dòng khớp theo luật của regime
3. DISTRIBUTION    phân bố reconstructed ≡ phân bố gốc (bắt lỗi hệ thống)
```

Luật so sánh theo regime `[MEASURED]`:

| Regime | Cột | Luật |
|---|---|---|
| `L10` | `f18`, `f30` | **đẳng thức chính xác** (round-trip đo được 100.0000%) |
| `LN` | `f5`, `f11` | **dung sai tương đối 1e-15** (đẳng thức chính xác chỉ 93.0% / 93.4%) |
| `REC` | `f1`, `f2` | **đẳng thức chính xác** (số nguyên) |
| `CAT` | 12 cột T2 | **đẳng thức chính xác** |
| `PASS` | 18 cột T3 | **không thuộc GATE A** — là copy, không phải reconstruction |

> ⚠️ **T3 không được tính vào tỉ lệ pass của GATE A.** Nếu tính, GATE A sẽ luôn hiển
> thị ≥50% pass chỉ nhờ copy — con số vô nghĩa.

---

## 14. Failure handling

| Tình huống | Xử lý bắt buộc |
|---|---|
| Ràng buộc xung đột (vd `f2 > 0` nhưng `f30` cần active hôm nay) | **Quarantine** user vào `biz.reject_customer` với `reason_code`. **KHÔNG** sửa giá trị `f` để cho vừa |
| Reconstruction lệch ở 1 cột | Ghi `biz.reconstruction_diff(user_id, column, expected, actual)`. **KHÔNG** silent-fix |
| Tỉ lệ quarantine > ngưỡng | GATE A **FAIL** toàn bộ. Xem lại assumption, không nới ngưỡng |
| Decode `f79..f82` không tra được trong encoding map | FAIL cứng — nghĩa là map fit sai hoặc dataset có giá trị mới |
| Counter `n` vượt trần đã đo | FAIL cứng — dữ liệu vào khác dữ liệu đã audit |

> 🚫 **Nghiêm cấm:** sửa `full_trainset.csv` để validation pass.
> Dataset gốc là REFERENCE, bất khả xâm phạm.

---

## 15. Reproducibility & random seed

| Yêu cầu | Cách làm |
|---|---|
| Chạy lại cho ra **đúng** kết quả | Seed **dẫn xuất từ `customer_id`**: `seed = hash(customer_id, salt, track)` |
| Không phụ thuộc thứ tự xử lý | Mỗi user sinh độc lập ⇒ song song hoá được, kết quả không đổi |
| Track A và Track B **không dùng chung** seed stream | `track` nằm trong hàm băm |
| Ghi lại salt | `biz.generation_run(run_id, salt, code_version, spec_version, started_at)` |
| Event id ổn định | `event_id = uuid5(namespace, f"{customer_id}:{track}:{reason}:{idx}")` ⇒ rerun không tạo trùng lặp giả |

`uuid5` deterministic là quan trọng: nếu dùng `uuid4`, rerun sẽ tạo event "mới" và
dedup theo `event_id` ở `stg_app_events` sẽ **không** bắt được ⇒ counter nhân đôi.

---

## 16. Measured vs assumed — bảng tổng

### `[FACT]` — từ code/schema

- `f0`–`f82` không có transformation nào trong repo; `stg_user_snapshot.sql` chỉ `cast`
- `f5`, `f11` không xuất hiện ở bất kỳ code/SQL/config nào ngoài vòng lặp generic
- Label không bao giờ rời training dataset sang feature export/solver
- Train = observational biased, test = RCT (paper §4.1)

### `[MEASURED]` — có số cụ thể

| # | Kết quả |
|---|---|
| 1 | `f5 = ln(n)`, `n ∈ [1,14245]`, 864 mức, **100%** dòng (tol. 1e-12), 74.2% có `n=1` |
| 2 | `f11 = ln(n)`, `n ∈ [1,2144]`, 669 mức, **100%** dòng, 70.2% có `n=1` |
| 3 | `f18`, `f30` = `log10(n)`, `10^f ∈ ℤ` **100%** dòng, lưu 6 chữ số |
| 4 | Round-trip `L10` khớp **chính xác 100.0000%**; `LN` chỉ 93.0%/93.4% chính xác, **100%** ở 1e-15 |
| 5 | `f1 ≥ f2` ở **100%** dòng |
| 6 | `f79`–`f82`: `card(tuple) = card(cột) = 515` ⇒ **1** biến |
| 7 | `f37`/`f38`: `card(tuple) = 1133 > 241` ⇒ **2** biến, chung alphabet (64/64) |
| 8 | `f23 ≈ f25` (99.78%); `f6 ≡ f15` và `f14 ≡ f39` (**trùng tuyệt đối**, 0 dòng lệch) |
| 9 | Khối lượng Track A ≈ **23.1 triệu event** cho 926,669 user |

### `[ASSUMPTION]` — semantic giả định

S-01 (`f1` = first order recency) · S-02 (`f2` = last order recency) ·
S-03 (`f30` = active days 30d) · S-04 (`f18` = hành động hiếm) ·
S-05 (`f5`/`f11` = 2 counter tần suất cao) · S-06 (7 thuộc tính categorical) ·
S-07 (cửa sổ 365 ngày cho `f5`/`f11`/`f18`)

### `[UNKNOWN]`

- Semantic thật của **mọi** cột trong 36 cột
- `f5`, `f11`, `f18` đếm **cái gì**; cửa sổ thời gian của chúng
- Vì sao dataset dùng **hai** cơ số log khác nhau
- 515 / 64 / 241 mức là thuộc tính gì
- Semantic của toàn bộ 18 cột T3

---

## Liên quan

`RECONSTRUCTION_CONTRACT.md` · `FEATURE_LINEAGE.md` · `FEATURE_DICTIONARY.md` ·
`ERD.md` · `BUSINESS_EVENT_MODEL.md` · `MIGRATION_PLAN.md`
