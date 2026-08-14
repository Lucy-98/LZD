# Reconstruction Contract

> **Loại tài liệu:** technical contract. Không phải narrative.
>
> **Trạng thái: CONTRACT + prototype. Các ràng buộc chính đã được code enforce; đây vẫn là tài liệu hợp đồng.**
>
> **Ràng buộc:** mọi điều khoản dưới đây là **kiểm chứng được bằng máy**. Điều khoản
> nào không viết được thành assertion thì không thuộc về tài liệu này.
>
> Nhãn: `[FACT]` `[MEASURED]` `[ASSUMPTION]` `[UNKNOWN]` — xem `DATA_GENERATION.md` §0.

---

## Mục lục

1. [Input contract](#1-input-contract)
2. [Output contract](#2-output-contract)
3. [Feature ownership](#3-feature-ownership)
4. [Tier ownership](#4-tier-ownership)
5. [Group-level reconstruction](#5-group-level-reconstruction)
6. [Temporal rules](#6-temporal-rules)
7. [Event constraints](#7-event-constraints)
8. [Categorical encoding rules](#8-categorical-encoding-rules)
9. [Equality & tolerance rules](#9-equality--tolerance-rules)
10. [Pass-through rules](#10-pass-through-rules)
11. [UNKNOWN rules](#11-unknown-rules)
12. [Tautology prevention](#12-tautology-prevention)
13. [Leakage rules](#13-leakage-rules)
14. [GATE A](#14-gate-a)
15. [Failure conditions](#15-failure-conditions)
16. [Acceptance criteria](#16-acceptance-criteria)

---

## 1. Input contract

### 1.1 · Nguồn hợp lệ

```
INPUT := raw.customer_snapshot
         WHERE split = 'train'
```

| Điều khoản | Quy định |
|---|---|
| **I-1** | Chỉ `split = 'train'`. Truy cập `split = 'test'` ⇒ **FAIL cứng** |
| **I-2** | Cột được đọc: 55 cột đã chọn + `user_id` + `feature_ts` + `dt` |
| **I-3** | `label`, `is_treat` **KHÔNG** được đọc bởi bất kỳ thành phần generation nào |
| **I-4** | Dataset gốc **read-only**. Mọi ghi vào `data/*.csv` ⇒ FAIL cứng |
| **I-5** | Mỗi `user_id` xuất hiện đúng **một** lần sau dedup theo `(user_id, dt)` |

### 1.2 · Enforcement của I-3

Ràng buộc I-3 phải được thực thi bằng **chữ ký hàm**, không bằng quy ước:

```python
# ĐÚNG — không thể truyền label vào
def decode_customer(row: SnapshotRow) -> CustomerState: ...
def backfill_generator(state: CustomerState, feature_ts: datetime) -> Iterator[Event]: ...

# SnapshotRow KHÔNG có field label / is_treat
```

Nếu `SnapshotRow` chứa `label` thì I-3 chỉ là lời hứa. Contract yêu cầu nó **bất khả thi**.

---

## 2. Output contract

```
OUTPUT := (biz.customer, biz.customer_opaque, raw.events, biz.encoding_map_*)
```

| Điều khoản | Quy định |
|---|---|
| **O-1** | `raw.events` chỉ chứa event có `_gen_track ∈ {A, B}` |
| **O-2** | Mọi event có `event_id` **deterministic** (`uuid5`), rerun không sinh id mới |
| **O-3** | `biz.customer` có đúng 1 dòng / `customer_id` |
| **O-4** | `biz.customer_identity_map` là song ánh `customer_id ↔ lzd_user_id` |
| **O-5** | `encoding_map_*` bất biến sau khi fit; đổi map ⇒ bump `map_version` |
| **O-6** | Trường `_gen_*` bị **drop** ở `stg_events`, không đi vào feature engineering |

---

## 3. Feature ownership

Mỗi cột trong 55 cột thuộc **đúng một** owner. Không cột nào có hai nguồn.

| Owner | Cột | Số |
|---|---|---|
| `feat_cfs_counter` | `f5`, `f11`, `f18`, `f30` | 4 |
| `feat_cfs_recency` | `f1`, `f2` | 2 |
| `feat_cfs_categorical` | `f37`, `f38`, `f79`, `f80`, `f81`, `f82`, `f40`, `f43`, `f44`, `f45`, `f64`, `f68` | 12 |
| `feat_passthrough` | `f3`,`f4`,`f8`,`f9`,`f10`,`f12`,`f13`,`f16`,`f20`,`f21`,`f22`,`f23`,`f25`,`f26`,`f28`,`f29`,`f31`,`f35` | 18 |

**Assertion:** `⋃ owners = 55 cột`, `⋂ owners = ∅`.

---

## 4. Tier ownership

`[MEASURED]` — bảng chốt sau khi đo `f5`, `f11`.

| Feature | Tier | Source | Dynamic? | Reconstruction path | Evidence |
|---|---|---|---|---|---|
| `f1` | T1 | events | ✅ | recency | measured (`f1 ≥ f2`, 100%) |
| `f2` | T1 | events | ✅ | recency | measured |
| `f5` | **T1** | events | ✅ | counter, base **e** | **measured** (`e^f5 ∈ ℤ`, 100% @1e-12) |
| `f11` | **T1** | events | ✅ | counter, base **e** | **measured** (`e^f11 ∈ ℤ`, 100% @1e-12) |
| `f18` | T1 | events | ✅ | counter, base **10** | measured (`10^f18 ∈ ℤ`, 100%) |
| `f30` | T1 | events | ✅ | counter, base **10** | measured (`10^f30 ∈ ℤ∩[1,30]`, 100%) |
| `f37` | T2 | customer | ❌ | categorical 64 | measured |
| `f38` | T2 | customer | ❌ | categorical 241 | measured (`card(tuple f37,f38)=1133`) |
| `f79` | T2 | customer | ❌ | categorical 515 | measured (`card(tuple)=515`) |
| `f80` | T2 | customer | ❌ | categorical 515 *(cùng nguồn `f79`)* | measured |
| `f81` | T2 | customer | ❌ | categorical 515 *(cùng nguồn)* | measured |
| `f82` | T2 | customer | ❌ | categorical 515 *(cùng nguồn)* | measured |
| `f40` | T2 | customer | ❌ | one-hot G1 (3 mức) | measured |
| `f43` | T2 | customer | ❌ | one-hot G2 (10 mức) | measured |
| `f44` | T2 | customer | ❌ | one-hot G2 *(cùng nguồn)* | measured |
| `f45` | T2 | customer | ❌ | one-hot G2 *(cùng nguồn)* | measured |
| `f64` | T2 | customer | ❌ | one-hot G4 (3 mức) | measured |
| `f68` | T2 | customer | ❌ | one-hot G6 (2 mức) | measured |
| `f3`,`f4`,`f8`,`f9`,`f10`,`f12`,`f13`,`f16`,`f20`,`f21`,`f22`,`f23`,`f25`,`f26`,`f28`,`f29`,`f31`,`f35` | T3 | snapshot | ❌ | **none** | `[UNKNOWN]` semantic |

```
T1 = 6    T2 = 12    T3 = 18    UNKNOWN = 0
```

---

## 5. Group-level reconstruction

> **Nguyên tắc:** reconstruction chạy ở mức **source group**, không phải mức cột đã chọn.

| Điều khoản | Quy định |
|---|---|
| **G-1** | Selection chỉ lấy một phần mức của một group ⇒ vẫn phải dựng **đủ mức** của group nguồn |
| **G-2** | Cột không được chọn của group **vẫn phải sinh ra** ở tầng trung gian, chỉ không xuất ra model |
| **G-3** | Một source attribute sinh nhiều cột ⇒ **một** thuộc tính duy nhất trong `biz.customer` |

### 5.1 · Bảng source group

| Source attribute | Số mức | Sinh ra (đã chọn) | Sinh ra (không chọn, vẫn phải dựng) |
|---|---|---|---|
| `synthetic_segment_g1` | 3 | `f40`, `f41`, `f42` | — (chọn đủ cả group) |
| `synthetic_segment_g2` | 10 | `f43`, `f44`, `f45`, `f46`, `f47`, `f52` | `f48`–`f51` |
| **`synthetic_segment_g3`** | **10** | `f53`, `f54`, `f57`, `f58`, `f59`, `f62` | `f55`, `f56`, `f60`, `f61` |
| `synthetic_segment_g4` | 3 | `f64`, `f65` | `f63` |
| `synthetic_segment_g6` | 2 | `f68` | `f78` |
| `synthetic_category_515` | 515 | `f79`, `f80`, `f81`, `f82` | — |
| `synthetic_attr_64` | 64 | `f37` | — |
| `synthetic_attr_241` | 241 | `f38` | — |

**Tổng: 8 thuộc tính categorical** sinh ra 24 cột T2 đã chọn.

> ⚠️ Con số **8**, không phải 7. (5 one-hot group + `cat_515` + `attr_64` + `attr_241`.)
> `g3` là group mới ở `fs_2026_08_v2`; `g5` `g7` `g8` tồn tại nhưng chưa được chọn.
> 🔴 `g4` vỡ bất biến one-hot trên test split — xem `SCOPE_EXPANSION_55F.md` §2.3.

### 5.2 · Assertion G-1

```
assert card(synthetic_segment_g2) == 10        # không phải 3
assert sum(f43..f52) == 1  for every row       # one-hot invariant giữ nguyên
```

Nếu chỉ dựng 3 mức, invariant one-hot của G2 vỡ và `f43`/`f44`/`f45` mất ý nghĩa
"loại trừ lẫn nhau".

### 5.3 · `f79`–`f82` — một nguồn, bốn cột

```
biz.customer.synthetic_category_515  (MỘT cột, level_id ∈ [1,515])
        │
        └──► encoding_map_515 ──► f79, f80, f81, f82
```

**Assertion:** `card(distinct (f79,f80,f81,f82)) == card(distinct level_id) == 515`.

🚫 **Cấm** tạo 4 thuộc tính riêng.

---

## 6. Temporal rules

| # | Điều khoản | Assertion |
|---|---|---|
| **T-1** | Track A: mọi event **strictly before** `feature_ts` | `max(event_ts) < feature_ts` |
| **T-2** | Track B: mọi event từ mốc T0 trở đi | `min(event_ts) >= feature_ts` |
| **T-3** | Cửa sổ counter tính lùi từ `feature_ts`, **không** từ `now()` | — |
| **T-4** | `f30`: đếm **ngày phân biệt**, không phải số event | `count(distinct date(event_ts)) == n` |
| **T-5** | Event của `f1` xảy ra ≤ event của `f2` | `ts(f1_event) <= ts(f2_event)` |
| **T-6** | Giờ-trong-ngày là tự do (không feature nào phụ thuộc) | — |
| **T-7** | Track A và Track B chia theo nửa khoảng | `max(A.event_ts) < feature_ts <= min(B.event_ts)` |

### 6.1 · Vì sao T-1 dùng `<` chứ không `<=`

`[FACT]` `feat_user_realtime_pit.sql` dùng `e.event_ts < s.feature_ts`. Generator phải
khớp **chính xác** biên đó. Dùng `<=` sẽ tạo lệch một event ở biên — loại lỗi rất khó
tìm vì chỉ xuất hiện ở một tỉ lệ nhỏ user.

### 6.2 · Vì sao T-2 dùng `>=` chứ không `>`

`feature_ts == reference_ts == CustomerState.as_of_ts` trong kịch bản T0. Track A dùng
nửa khoảng trái `event_ts < reference_ts`; Track B dùng nửa khoảng phải
`event_ts >= reference_ts`. Cặp bất biến này không tạo overlap, không tạo khoảng trống
ở biên, và khớp với `live_generator(state, state.as_of_ts, ...)`.

---

## 7. Event constraints

| # | Điều khoản |
|---|---|
| **E-1** | Số event sinh cho một counter = **đúng** giá trị `n` đã decode. Không ±1 |
| **E-2** | `n = 1` ⇒ sinh **1** event, không phải 0 |
| **E-3** | `f1 == f2` ⇒ sinh **một** event duy nhất, không phải hai trùng timestamp |
| **E-4** | `f30 == 30` ⇒ mọi ngày trong cửa sổ 30 ngày đều có ≥1 event; không còn bậc tự do |
| **E-5** | `event_id` deterministic: `uuid5(ns, f"{customer_id}:{track}:{reason}:{idx}")` |
| **E-6** | Mỗi event Track A ghi `_gen_reason` = cột yêu cầu nó |
| **E-7** | Một event **được phép** phục vụ nhiều cột (vd `EVT_ORDER_PAID` vừa cho `f2` vừa đếm vào `f30`) — nhưng phải ghi rõ, và assertion đếm phải tính đúng |

### 7.1 · E-7 là nguồn xung đột chính — **đã đo**

Nếu `EVT_ORDER_PAID` của `f2` rơi vào một ngày chưa active, nó **làm tăng** `active_days`
của `f30` lên 1. Generator phải giải hệ ràng buộc, không sinh từng cột độc lập.

**Thứ tự giải bắt buộc** *(chi tiết: `RECONSTRUCTION_CONSTRAINT_MODEL.md` §11)*:

```
P0. Precheck khả thi: n30 >= |D_forced|
P1. Đặt event ràng buộc CỨNG      (f1, f2 — timestamp cố định, không bậc tự do)
P2. Chọn tập ngày active
P3. Phủ ngày active               (ưu tiên event counter, sau đó FREE event)
P4. Xếp event counter còn lại vào ngày ĐÃ active  ⇒ không tạo ngày mới
P5. Chạy FEATURE ENGINE THẬT
P6. So sánh theo regime  →  PASS / REPAIR / QUARANTINE
```

`[MEASURED]` trên 926,669 dòng train:

| Điều kiện | Kết quả |
|---|---|
| Xung đột `f2 ↔ f30` (`n30 < \|D_forced\|`) | **8 user — 0.001%** ⇒ **không** phải bottleneck |
| **Capacity fail** (`n30 > n_rec + n5 + n11 + n18`) | **440,713 user — 47.56%** ⇒ **bottleneck thật** |
| `d1 = d2` | 902,731 — **97.42%** |
| `n5 = n11 = n18 = 1` | 539,019 — **58.17%** |

> 🔴 **47.56% user vi phạm capacity bound.** Cách đọc đúng: bất đẳng thức chỉ chứng minh
> impossibility **dưới hai tiền đề chưa chứng minh** — `P1` (`f30` = distinct active days,
> tức S-03) và `P2` (event taxonomy đầy đủ).
>
> ⇒ Kết luận chứng minh được **duy nhất**: `¬P1 ∨ ¬P2` — **ít nhất một trong S-03 hoặc
> P2 là SAI**. Dữ liệu không nói cái nào.
>
> ⇒ Contract này phụ thuộc **`PROPOSITION FE-1`** (`≡ ¬P2`), là **mệnh đề có điều kiện,
> KHÔNG phải axiom, KHÔNG phải fact**. `[FACT]` không dòng code nào trong repo tính
> `f5`/`f11`/`f18`/`f30`/`f1`/`f2` ⇒ event taxonomy của `f*` **không quan sát được**.
>
> ⇒ `FREE_EVENT` hiện là **hypothesis**, **chưa** phải reconstruction primitive. Chỉ được
> nâng lên primitive khi event taxonomy chứng minh được lớp event đó tồn tại
> (`RECONSTRUCTION_CONSTRAINT_MODEL.md` §7.6 FE-e).

---

## 8. Categorical encoding rules

| # | Điều khoản |
|---|---|
| **C-1** | `encoding_map_*` được **fit từ dataset** bằng `SELECT DISTINCT`, lưu bất biến |
| **C-2** | Fit **chỉ** trên `split = 'train'` |
| **C-3** | Decode phải là **toàn ánh**: mọi giá trị quan sát tra được ra `level_id` |
| **C-4** | Encode là **hàm thuần**: `level_id → giá trị`, không phụ thuộc user/thời gian |
| **C-5** | Giá trị xuất hiện lúc encode mà không có trong map ⇒ **FAIL cứng**, không fallback |
| **C-6** | One-hot: encode từ `level_id`, **không** copy bit từ dataset |

### 8.1 · Phân biệt C-1 và copy

`encoding_map_515` chứa giá trị float lấy từ dataset — đây **là** borrow, và phải nói thẳng:

```
[MEASURED]  515 tuple phân biệt tồn tại; ánh xạ 1-1
[FACT]      giá trị encode được LẤY TỪ dataset, không tự sinh
```

Điều này **hợp lệ** vì nó giống hệt một encoder production được fit trên training data
rồi lưu artifact. Điều làm nó **không** phải copy là §12: `level_id` phải điều khiển
hành vi. Encoder mượn *bảng tra*; hệ thống tự quyết định *user nào ở level nào*
và *level đó dẫn tới hành vi gì*.

`f40`–`f78` không mượn gì — one-hot của một level là 0/1, sinh từ đầu.

---

## 9. Equality & tolerance rules

`[MEASURED]` — luật khác nhau theo regime. Con số dưới đây đo trên 926,669 dòng.

| Regime | Cột | Luật so sánh | Bằng chứng |
|---|---|---|---|
| **`L10`** | `f18`, `f30` | **đẳng thức chính xác** `==` | `round(log10(round(10^f)),6) = f` đúng **100.0000%** |
| **`LN`** | `f5`, `f11` | **dung sai tương đối `1e-15`** | `ln(round(e^f)) = f` chỉ đúng **93.05% / 93.42%**; ở `1e-15`: **100%** |
| **`REC`** | `f1`, `f2` | **đẳng thức chính xác** (INT) | miền `[0,365]` nguyên |
| **`CAT`** | 24 cột T2 | **đẳng thức chính xác** | ánh xạ song ánh |
| **`PASS`** | 24 cột T3 | **không áp dụng** | không phải reconstruction |

### 9.1 · Vì sao `LN` không dùng được đẳng thức chính xác

`f5`/`f11` lưu ở **độ chính xác float64 đầy đủ** (16 chữ số). Phép `ln(round(exp(x)))`
đi qua hai hàm siêu việt ⇒ sai khác bit cuối ở ~7% số dòng. Đây là **giới hạn của số
dấu phẩy động**, không phải lỗi reconstruction.

`f18`/`f30` lưu ở **6 chữ số** ⇒ phép làm tròn hấp thụ toàn bộ sai số ⇒ khớp tuyệt đối.

### 9.2 · Cấm nới dung sai

```
🚫 Dung sai của regime LN CỐ ĐỊNH ở 1e-15.
   Nới lên 1e-6 để "cho pass" là che lỗi reconstruction thật.
   Nếu 1e-15 fail ⇒ generator sai, không phải dung sai sai.
```

### 9.3 · So sánh giá trị đã giải mã

Ngoài so sánh feature, contract yêu cầu so sánh **`n` đã decode**:

```
assert round(exp(f5_original))  == count(events where _gen_reason='f5')
assert round(pow(10,f30_orig))  == count(distinct date(event_ts) in window)
```

Đây là assertion **mạnh hơn** so sánh feature, vì nó bắt lỗi ngay ở tầng event thay vì
đợi tới tầng feature.

---

## 10. Pass-through rules

| # | Điều khoản |
|---|---|
| **P-1** | 24 cột T3 copy nguyên trạng từ `raw.customer_snapshot`, **không** biến đổi |
| **P-2** | T3 **không** tham gia sinh event |
| **P-3** | T3 **không** được tính vào tỉ lệ pass của GATE A |
| **P-4** | T3 **đóng băng** trong toàn bộ kịch bản Track B |
| **P-5** | Không được gán business semantic cho T3 |
| **P-6** | Tài liệu phải ghi rõ T3 là **baseline/fallback**, không phải feature engineering |
| **P-7** | T3 đi vào selected mart qua `feat_passthrough`; **không** đi qua `CustomerState` và Track B không được lookup target để lấy T3 |

### 10.1 · Vì sao P-3

Nếu tính T3 vào GATE A, tỉ lệ pass luôn ≥ `24/55 = 44%` chỉ nhờ copy — con số đó
**không mang thông tin**. GATE A chỉ tính trên 31 cột T1+T2.

---

## 11. UNKNOWN rules

| # | Điều khoản |
|---|---|
| **U-1** | Cột chưa đo ⇒ **không gán tier**. Ghi `UNKNOWN` + evidence còn thiếu |
| **U-2** | Cột `UNKNOWN` **không** vào reconstruction surface |
| **U-3** | Semantic `UNKNOWN` không được suy ra từ tên cột |
| **U-4** | Reconstruction khớp 100% **không** nâng semantic từ `[ASSUMPTION]` lên `[FACT]` |

Hiện tại: **0 cột UNKNOWN** trong 55 cột đã chọn (sau khi đo `f5`, `f11`, `f19`).

Ngoài 55 cột: `f14`, `f15`, `f39` chưa đo chi tiết (`f6` nay đã ở T3) — nhưng `[MEASURED]`
`f6 ≡ f15` và `f14 ≡ f39` **trùng tuyệt đối** (0 dòng lệch). Không thuộc scope này.

---

## 12. Tautology prevention

> **Điều khoản quan trọng nhất của contract.**

### 12.1 · Cấm

```
🚫 CẤM:
   original feature → encode → generator → feature engine → same original feature
   rồi tuyên bố "reconstruction thành công"
```

### 12.2 · Bắt buộc

```
✅ BẮT BUỘC có causal/data path:
   LZD snapshot → decoded semantic state → raw historical events
                → REAL feature engine → reconstructed feature
```

| # | Điều khoản | Cách kiểm |
|---|---|---|
| **TA-1** | Feature engine phải là **code path production-like**, không phải logic viết riêng để pass test | Cùng model dbt được dùng cho serving |
| **TA-2** | Không có nhánh `if reconstruction_mode` trong feature engine | Grep; code review |
| **TA-3** | Giá trị `f` gốc **không** được xuất hiện trong `raw.events` | `assert 'f5' not in event_payload_keys` |
| **TA-4** | `backfill_generator` **không** nhận behaviour model; `live_generator` **không** nhận giá trị `f` đích | Chữ ký hàm |
| **TA-5** | Thuộc tính T2 **phải điều khiển hành vi** ở Track B | Đo: đổi `level_id` ⇒ phân bố hành vi Track B phải đổi |
| **TA-6** | Trường `_gen_*` bị drop trước feature engineering | `stg_events` không select chúng |

### 12.3 · TA-5 là điều khoản chống "copy trá hình"

Nếu `synthetic_category_515` chỉ được lưu rồi encode ngược mà **không** ảnh hưởng
hành vi nào, thì nó là một biến trang trí và reconstruction là copy.

**Assertion cụ thể:**

```
Với hai nhóm user khác level_id, phân bố của (số order, giá trị giỏ, tỉ lệ claim voucher)
sinh ra ở Track B phải khác nhau có ý nghĩa thống kê.
Nếu không khác ⇒ TA-5 FAIL ⇒ reconstruction là tautology.
```

### 12.4 · Ranh giới thành thật

Contract này **thừa nhận**: GATE A ở tầng A là **gần với tautology** theo thiết kế —
ta cố tình dựng ngược transformation. Nó chứng minh *pipeline chạy đúng*, **không**
chứng minh *semantic đúng*.

Thứ thật sự validate là **Model-Level Validation** (`FEATURE_LINEAGE.md` §13.2),
nằm ngoài phạm vi contract này.

---

## 13. Leakage rules

| # | Điều khoản |
|---|---|
| **L-1** | `label`, `is_treat` không đi vào generator (I-3), không vào `biz.*`, không vào `raw.events` |
| **L-2** | Không event Track A nào có `event_ts >= feature_ts` (T-1) |
| **L-3** | Feature tính tại `feature_ts`, không tại `now()` (sửa lỗi D4) |
| **L-4** | Label không bao giờ tới downstream feature export — `[FACT]`, **không được phá** |
| **L-5** | `encoding_map_*` fit **chỉ** trên train (C-2) |
| **L-6** | Trường `_gen_*` không vào feature (TA-6) |

### 13.1 · L-4 — giữ nguyên thứ đang đúng

`[FACT]` `feat_user_selected_serving.sql` không select `label`; `offline_store.iter_shard()`
chỉ đọc `entity_key + batch_names` từ `feature_spec.yml` v2. Đây là chỗ nhiều team
làm sai. Migration **không được** làm hỏng nó.

---

## 14. GATE A

### 14.1 · Phạm vi

```
GATE A áp dụng cho:  T1 (7 cột)  +  T2 (24 cột)  =  31 cột
GATE A KHÔNG áp dụng cho:  T3 (24 cột)
```

### 14.2 · Ba mức kiểm

#### Mức 1 — Row-level

```
Với mỗi customer C:
    reconstructed_row(C)[31 cột]  ==  original_row(U)[31 cột]
    theo luật §9
```

**Metric:** `row_pass_rate = #(user pass toàn bộ 31 cột) / #user`

#### Mức 2 — Column-level

```
Với mỗi cột c trong 18:
    column_pass_rate(c) = #(dòng khớp) / #dòng
```

Tách theo cột để biết **cột nào** hỏng, không chỉ biết "có hỏng".

#### Mức 3 — Distribution-level

```
Với mỗi cột c:
    so sánh phân bố reconstructed vs original
        - min / max / mean
        - cardinality
        - tỉ lệ giá trị bằng 0
        - quantile (p50, p90, p99)
```

Mức 3 bắt lỗi **hệ thống** mà mức 1–2 có thể bỏ qua khi pass rate cao nhưng lệch
tập trung ở một vùng giá trị.

### 14.3 · Ngưỡng

| Metric | Ngưỡng | Ghi chú |
|---|---|---|
| `column_pass_rate` mỗi cột T1/T2 | **100%** | Reconstruction là bài toán deterministic — không có lý do để < 100% |
| `row_pass_rate` | **100%** trên tập không bị quarantine | |
| Tỉ lệ quarantine | ⬜ **CHƯA CHỐT** | Cần chạy thử để biết bao nhiêu user có ràng buộc xung đột (§7.1). Đặt ngưỡng trước khi đo là bịa |
| Distribution: cardinality | **khớp chính xác** | |
| Distribution: min/max | **khớp** theo luật §9 | |

> ⚠️ **Ngưỡng quarantine để trống có chủ ý.** Không có căn cứ để đề xuất con số cho
> tới khi chạy generator trên một mẫu. Điền số bây giờ là vi phạm nguyên tắc
> "không silently invent".

---

## 15. Failure conditions

FAIL **cứng** — dừng pipeline:

| # | Điều kiện |
|---|---|
| **F-1** | Truy cập `split = 'test'` |
| **F-2** | Ghi vào `data/full_*.csv` |
| **F-3** | `label` / `is_treat` xuất hiện trong `biz.*` hoặc `raw.events` |
| **F-4** | Event Track A có `event_ts >= feature_ts` |
| **F-5** | Giá trị không tra được trong `encoding_map_*` (C-5) |
| **F-6** | Counter `n` vượt trần đã đo (`f5 > 14245`, `f11 > 2144`, `f18 > 2260`, `f30 > 30`) |
| **F-7** | `column_pass_rate` của bất kỳ cột T1/T2 nào < 100% |
| **F-8** | Phát hiện nhánh `if reconstruction_mode` trong feature engine (TA-2) |
| **F-9** | TA-5 fail — level_id không ảnh hưởng hành vi Track B |

Xử lý **mềm** — ghi nhận, tiếp tục:

| # | Điều kiện | Xử lý |
|---|---|---|
| **S-1** | Ràng buộc xung đột cho một user | `biz.reject_customer(user_id, reason_code)` |
| **S-2** | Lệch ở một cột của một user | `biz.reconstruction_diff(user_id, column, expected, actual)` |

🚫 **Nghiêm cấm silent-fix.** Mọi lệch phải để lại dấu vết.

---

## 16. Acceptance criteria

Contract được coi là **thoả** khi **tất cả** điều kiện sau đúng:

```
[ ] A-1  31/31 cột T1+T2 đạt column_pass_rate = 100%
[ ] A-2  row_pass_rate = 100% trên tập không quarantine
[ ] A-3  Tỉ lệ quarantine ≤ ngưỡng đã chốt (ngưỡng chốt SAU khi chạy thử)
[ ] A-4  Distribution-level khớp cho cả 31 cột
[ ] A-5  Assertion §9.3 (so sánh n đã decode) pass
[ ] A-6  Assertion G-1 (one-hot invariant của group đầy đủ) pass
[ ] A-7  TA-1..TA-6 pass — không tautology
[ ] A-8  L-1..L-6 pass — không leakage
[ ] A-9  Rerun với cùng salt cho ra byte-identical output
[ ] A-10 Không điều kiện F-1..F-9 nào được kích hoạt
```

### 16.1 · Ngoài phạm vi contract này

| Việc | Ở đâu |
|---|---|
| Model-Level Validation (AUUC/Qini/importance) | `FEATURE_LINEAGE.md` §13.2 |
| Gain share theo tier | model evaluation workstream |
| Ablation `f25`, `f80`–`f82` | model evaluation workstream |
| Sensitivity của model với T1 | model evaluation workstream |
| Track B behaviour model | `DATA_GENERATION.md` §8 |

> **GATE A pass không có nghĩa hệ thống dùng được.** Nó chỉ có nghĩa reconstruction
> pipeline chạy đúng. Quyết định "dùng được hay không" thuộc về Model-Level Validation.

---

## Liên quan

`DATA_GENERATION.md` · `FEATURE_LINEAGE.md` §12–§13 · `FEATURE_DICTIONARY.md` ·
`ERD.md` §3.1a · `MIGRATION_PLAN.md` MX
