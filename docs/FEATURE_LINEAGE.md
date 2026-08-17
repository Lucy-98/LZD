# Feature Lineage

> **Vai trò:** chứng minh đường đi
> `Synthetic Business Entity → Raw Field/Event → Transformation → Selected Feature
> → Feature Store → Model Input` cho từng feature được chọn.
>
> **Trạng thái:** thiết kế + prototype. Track B mô tả code đang chạy; Track A1/A2 và Track C là phần đang hoàn thiện.
>
> **Nguyên tắc trung tâm của toàn bộ project:**
>
> > Chúng ta **không** tuyên bố khôi phục được semantic thật của feature Lazada.
> > Chúng ta xác định **ràng buộc cấu trúc** của feature LZD quan sát được, **chọn**
> > những feature thực sự quan trọng với uplift model đã train, **gán semantic
> > nghiệp vụ synthetic một cách tường minh**, và dựng một hệ thống vận hành/event
> > kiểu-Lazada mà pipeline feature engineering **xuôi chiều** của nó tái tạo lại
> > đúng representation của feature training đã chọn.
> >
> > **Khớp feature chính xác** ⇒ validate *reconstruction*.
> > **Đồng thuận ở mức model** ⇒ validate *hệ thống tái tạo có bảo toàn hành vi ML
> > hữu ích hay không*.

---

## Mục lục

1. [Mental model bắt buộc](#1-mental-model-bắt-buộc)
2. [Kiến trúc mục tiêu](#2-kiến-trúc-mục-tiêu)
3. [Constrained Forward Synthesis — khái niệm trung tâm](#3-constrained-forward-synthesis--khái-niệm-trung-tâm)
4. [Reconstruction ≠ Semantic Recovery](#4-reconstruction--semantic-recovery)
5. [Cardinality profile & reconstruction tier](#5-cardinality-profile--reconstruction-tier)
6. [Track A1 — Reconstructable](#6-track-a1--reconstructable)
7. [Track A2 — Pass-through](#7-track-a2--pass-through)
8. [Track B — event-derived hiện có](#8-track-b--event-derived-hiện-có)
9. [Track C — feature nghiệp vụ mới](#9-track-c--feature-nghiệp-vụ-mới)
10. [Feature Selection đặt ở đâu](#10-feature-selection-đặt-ở-đâu)
11. [Redundancy & feature-group selection](#11-redundancy--feature-group-selection)
12. [Tautology vs Validation](#12-tautology-vs-validation)
13. [Hai tầng validation](#13-hai-tầng-validation)
14. [Ranh giới point-in-time](#14-ranh-giới-point-in-time)
15. [Vai trò của dataset LZD](#15-vai-trò-của-dataset-lzd)

---

## 1. Mental model bắt buộc

```
    ERD                       ≠   Feature Store
    Business Data             ≠   ML Feature
    Raw Event                 ≠   Feature
    f0..f82                   ≠   Business Columns
    LZD Dataset               ≠   LZD Internal Database
    Synthetic Reconstruction  ≠   Historical Fact
    Reconstruction            ≠   Semantic Recovery
    Per-row match             ≠   Semantic proof
```

Ba dòng cuối là bổ sung của bản sửa này và là ba dòng quan trọng nhất.

---

## 2. Kiến trúc mục tiêu

```
                    LZD TRAINING DATASET
                            │
                            ▼
                 Feature Profiling / Audit
                            │
                            ▼
                 Train / Evaluate Model          ◄── BẮT BUỘC đứng trước selection
                            │
                            ▼
                    Feature Selection
                            │
                            ▼
                  Selected Feature Set
                            │
                            ▼
              Constrained Forward Synthesis
                            │
                ┌───────────┴───────────┐
                ▼                       ▼
          T1 Event-level        T2 Attribute-level
                │                       │
                └───────────┬───────────┘
                            ▼
              Synthetic Lazada-like
               Business Database              (ERD.md)
                            │
                            ▼
                     Raw / Events             (BUSINESS_EVENT_MODEL.md)
                            │
                            ▼
                 Feature Engineering
                            │
                            ▼
               Reconstructed Features
                            │
                            ▼
                     Feature Store
                            │
                            ▼
                         Model
                            │
                            ▼
                   Online Inference
                            │
                            ▼
                   Business Decision
                            │
                            ▼
                 New Business Event
                            │
                            ▼
                Feature Store Update
                            │
                            ▼
                     New Decision
```

> ⚠️ Kiến trúc **"opaque f0–f82 → Redis seed"** không còn là kiến trúc mục tiêu.
> Nó chỉ còn là **baseline / fallback** — dùng khi một feature không thoả điều kiện
> reconstruction (Track A2), hoặc khi cần một đường chạy nhanh để so sánh.

---

## 3. Constrained Forward Synthesis — khái niệm trung tâm

### 3.1 · Tên gọi

Quá trình này **không được gọi** là *"reverse engineering Lazada semantic"*.
Tên chính xác:

> **Constrained Forward Synthesis (CFS)**

### 3.2 · Sáu bước

```
   1. Observed Feature
            │  quan sát cấu trúc: cardinality, miền giá trị, tính nguyên,
            │  quan hệ giữa các cột, dạng biến đổi
            ▼
   2. Infer encoding / transformation constraints
            │  vd: 10^f30 ∈ ℤ ∩ [1,30]  ⇒ f30 = log10(counter)
            │  KHÔNG suy ra counter đó đếm cái gì
            ▼
   3. Choose synthetic business semantic          ◄── SYNTHETIC_ASSUMPTION
            │  vd: "counter này là số ngày active trong 30 ngày"
            ▼
   4. Generate synthetic raw/event source
            │  sinh đúng n = round(10^f30) ngày active bằng event `EVT_SESSION_STARTED`
            ▼
   5. Forward feature engineering
            │  count(*) → log10()
            ▼
   6. Reproduce observed feature
            │  reconstructed_f30 == original_f30      ✓
            ▼
      Reconstruction contract satisfied
```

### 3.3 · Điều gì làm CFS khả thi

> **Tính rời rạc cho phép đảo ngược *cách mã hoá*, không cần biết *semantic*.**

Với một cột rời rạc, ánh xạ `giá trị quan sát → level id / số nguyên / count` là
**1-1 và đảo ngược được**. Ta không cần biết cột đó *nghĩa là gì* để tái tạo nó
*chính xác từng dòng*. Ta chỉ cần:

1. giải mã giá trị về level/count,
2. gán nó cho một thuộc tính nghiệp vụ synthetic **(đây là assumption)**,
3. để thuộc tính đó **thật sự điều khiển hành vi mô phỏng**,
4. chạy feature engineering xuôi chiều.

**60/83 cột của dataset LZD là rời rạc** (xem §5) ⇒ CFS áp dụng được cho phần lớn.

### 3.4 · Điều kiện để CFS không trở thành trò lừa mình

CFS chỉ có giá trị khi thuộc tính được gán **thật sự tham gia vào mô hình hành vi**.
Nếu chỉ lưu giá trị rồi phát lại nguyên xi thì đó là **copy trá hình**, không phải
feature engineering — và điều đó bị cấm tường minh:

```
❌ CẤM:  LZD feature ──► copy trực tiếp ──► database ──► gọi là "feature engineering"

✅ ĐÚNG: LZD feature ──► decode ──► business attribute
                                        │
                                        ├──► điều khiển hành vi mô phỏng
                                        ▼
                                  business events
                                        │
                                        ▼
                              feature engineering
                                        │
                                        ▼
                            reconstructed feature
```

---

## 4. Reconstruction ≠ Semantic Recovery

| | Reconstruction | Semantic Recovery |
|---|---|---|
| Câu hỏi | *"Ta có dựng được một hệ thống sinh ra đúng giá trị này không?"* | *"Giá trị này thật sự nghĩa là gì trong Lazada?"* |
| Khả thi? | ✅ **Có** — cho 60/83 cột | ❌ **Không** — không có dictionary, paper không công bố |
| Bằng chứng cần | ràng buộc cấu trúc đo được | feature dictionary nội bộ của Lazada |
| Kết quả | hệ thống **nhất quán** | sự thật **lịch sử** |
| Nhãn confidence | `SYNTHETIC_ASSUMPTION` | vẫn là `UNKNOWN` |

**Cách viết đúng trong mọi tài liệu và code:**

```
❌ SAI:
   f30 IS order_count_30d

✅ ĐÚNG:
   f30
     structural constraint:      10^f30 ∈ ℤ ∩ [1,30]   (CONFIRMED, 100% rows)
     candidate synthetic semantic: order_count_30d
     confidence:                 SYNTHETIC_ASSUMPTION
     true semantic:              UNKNOWN
```

---

## 5. Cardinality profile & reconstruction tier

### 5.1 · Profile đo trên toàn bộ 926,669 dòng train

| Cardinality | Số cột | Cột | Ý nghĩa cấu trúc |
|---|---|---|---|
| **≤ 2** | **39** | `f40`–`f78` | khối one-hot |
| **3–30** | **3** | `f0`, `f7`, `f30` | số nguyên nhỏ / counter đã log |
| **31–400** | **8** | `f1`, `f2`, `f18`, `f19`, `f27`, `f34`, `f37`, `f38` | recency / counter / encoding |
| **401–1000** | **10** | `f5`, `f6`, `f11`, `f14`, `f15`, `f39`, `f79`–`f82` | categorical cardinality cao |
| **> 1000** | **23** | `f3`, `f4`, `f8`–`f10`, `f12`, `f13`, `f16`, `f17`, `f20`–`f24`, … | liên tục / cardinality rất cao |

```
Tổng:      83 cột
Rời rạc:   60 / 83   (72%)   → ứng viên CFS
Liên tục:  23 / 83   (28%)   → T3 / UNKNOWN
```

### 5.2 · Định nghĩa tier

| Tier | Định nghĩa | Cơ chế tái tạo |
|---|---|---|
| **T1 — EVENT-LEVEL** | Feature **nảy sinh từ business event** được sinh ra | sinh `n` event → aggregate → transform |
| **T2 — ATTRIBUTE-LEVEL** | Feature là **encoding của thuộc tính entity** | gán level cho thuộc tính → encode |
| **T3 — PASS-THROUGH** | Liên tục / cardinality cao, chưa đủ bằng chứng về semantic | mang theo có kiểm soát, **chỉ nếu selection cho thấy model cần** |
| **UNKNOWN** | Chưa đo đủ để phân loại | không gán tier |

> **Không được ép semantic cho T3 chỉ để tài liệu trông đẹp.**

### 5.3 · Phân loại hiện tại — chỉ ghi cái đã đo

| Cột | Bằng chứng cấu trúc (CONFIRMED) | Tier |
|---|---|---|
| `f30` | `10^f30 ∈ ℤ ∩ [1,30]`, **100%** dòng; lưu 6 chữ số thập phân | **T1** |
| `f18` | `10^f18 ∈ ℤ`, **100%** dòng; 144 mức; **95.5% dòng = 0.0**; alphabet = `log10(1), log10(2), log10(3)…`; max `3.354108` ⇒ `n_max = 2260` | **T1** |
| `f19` | `10^f19 ∈ ℤ`, **100%** dòng; 156 mức; **93.0% dòng = 0.0**; max `3.673205` ⇒ `n_max ≈ 4712` | **T1** |
| `f1`, `f2` | nguyên `[0,365]`, 366 mức, `f1 ≥ f2` **luôn đúng** | **T1** |
| `f27` | nguyên `[0,100]`, 32 mức | **T1 \| T2** — xem §5.4 |
| `f34` | nguyên `[0,100]`, 31 mức | **T1 \| T2** — xem §5.4 |
| `f0` | nguyên `{0,1,2,3,4,5}`, 6 mức | **T1 \| T2** — xem §5.4 |
| `f7` | nguyên `{0,1,2,3,4,5}`, 6 mức; khác `f0` ở **50.8%** dòng ⇒ **biến khác** | **T1 \| T2** — xem §5.4 |
| `f40`–`f78` | `row_sum = 11.0` ở **100%** dòng = **8** group one-hot + `f70` hằng số + 2 cột trùng khít; sau khử trùng ⇒ **8 biến categorical** | **T2** |
| `f79`–`f82` | cardinality từng cột = **515**; cardinality tuple `(f79,f80,f81,f82)` = **515** ⇒ **song ánh hoàn hảo**, **MỘT** biến 515 mức, 4 cách mã hoá | **T2** |
| `f37` | 64 mức, `[0, 0.715096]`; **không** có mẫu số hữu tỉ nhỏ (test `x·q ∈ ℤ` phẳng 17.8% = đúng số dòng bằng 0) ⇒ **không phải tỉ lệ đếm** | **T2** |
| `f38` | 241 mức, `[0, 0.843222]`; cùng đặc điểm | **T2** |
| `f5`, `f6`, `f11`, `f14`, `f15`, `f39` | cardinality 401–1000, **chưa đo chi tiết** | **UNKNOWN** |
| 23 cột `>1000` | liên tục / cardinality rất cao | **T3** |

### 5.4 · Khi tier **tự nó** là một assumption

`f0`, `f7`, `f27`, `f34` là số nguyên nhỏ trong miền chặn. Cùng một dữ liệu khớp
với **cả hai** cách đọc:

```
T1:  một counter bị cap        →  sinh n event, COUNT
T2:  một mức ordinal / tier     →  gán thuộc tính, encode
```

Cả hai đều cho **tái tạo chính xác từng dòng**. Việc chọn cái nào **không** được
suy ra từ dữ liệu — nó là `SYNTHETIC_ASSUMPTION` phải ghi rõ, không phải kết luận đo được.

### 5.5 · Hai cấu trúc trông giống nhau nhưng **phải mô hình hoá khác nhau**

Đây là điểm dễ sai nhất trong toàn bộ tài liệu.

| | `f79`–`f82` | `f37`, `f38` |
|---|---|---|
| Cardinality từng cột | 515, 515, 515, 515 | 64, 241 |
| Cardinality tuple | **515** | **1133** |
| Kết luận | `tuple = per-column` ⇒ **song ánh** ⇒ **MỘT** biến, 4 encoding | `tuple > max(per-column)` ⇒ **HAI** biến độc lập |
| Alphabet | — | alphabet của `f37` **⊂ hoàn toàn** alphabet của `f38` (64/64) ⇒ **dùng chung một bảng mã hoá** |
| Mô hình hoá | 1 thuộc tính `synthetic_category_515` → 4 biểu diễn | 2 thuộc tính riêng (64 mức và 241 mức) → cùng **một** hàm encode |

> ❌ **Không được** tạo bốn business attribute riêng cho `f79`–`f82`.
> ❌ **Không được** gộp `f37` và `f38` thành một biến chỉ vì chúng chung alphabet.

---

## 6. Track A1 — Reconstructable

Feature có đủ cấu trúc để xây CFS. Gồm T1 và T2 đã được audit chứng minh.

### 6.1 · Lineage T1 — ví dụ `f30`

```
LAYER 1   CUSTOMER  ──1:n──►  ACTIVITY DAY
             │
             │  SYNTHETIC_ASSUMPTION S-03:
             │  "counter đứng sau f30 là số ngày active trong 30 ngày"
             │  true semantic: UNKNOWN
             ▼
          sinh đúng  n = round(10^f30_observed)  ngày active
          trong cửa sổ 30 ngày trước feature_ts
                          │
LAYER 2                   ▼
                    EVT_SESSION_STARTED × n ngày
                          │
LAYER 3                   ▼
              active_days_last_30d = COUNT(DISTINCT date(event_ts))
                          │
                          ▼
                      log10(orders_last_30d)
                          │
                          ▼
                    reconstructed f30   ==   original f30    ✓
                          │
LAYER 3c                  ▼
                    FEATURE STORE
                          │
LAYER 4                   ▼
                    UPLIFT MODEL
```

Cùng cơ chế áp dụng cho `f18` (`n_max = 2260`) và `f19` (`n_max ≈ 4712`) —
**cùng họ biến đổi `log10(counter)`**, chỉ khác trần và độ thưa
(`f18`: 95.5% dòng bằng 0; `f19`: 93.0%).

### 6.2 · Lineage T1 — ví dụ `f1`, `f2`

```
LAYER 1   CUSTOMER.attr_date_A , CUSTOMER.attr_date_B
             │  ràng buộc CONFIRMED:  f1 ≥ f2  luôn đúng
             │  ⇒ attr_date_A xảy ra SỚM HƠN HOẶC BẰNG attr_date_B
             │  SYNTHETIC_ASSUMPTION: hai mốc nghiệp vụ có thứ tự
             ▼
LAYER 2   (mốc thời gian trên entity, không cần event riêng)
             ▼
LAYER 3   f1 = date_diff('day', attr_date_A, feature_ts)
          f2 = date_diff('day', attr_date_B, feature_ts)
             ▼
          reconstructed (f1,f2) == original (f1,f2)   ✓  và ràng buộc f1 ≥ f2 tự thoả
```

### 6.3 · Lineage T2 — one-hot `f40`–`f78`

```
LAYER 1   CUSTOMER.synthetic_segment_G1 .. G8      (8 biến categorical)
             │  bằng chứng: 11 nhóm loại trừ, row_sum = 11.0 ở 100% dòng
             │  sau khử trùng (f68≡f71≡f77; f74 lệch 2 dòng; f70 hằng số)
             │  ⇒ CHỈ CÒN 8 biến thật
             ▼
LAYER 2   (thuộc tính entity — điều khiển hành vi mô phỏng)
             ▼
LAYER 3   one-hot encoding
             ▼
          f40 … f78     ==   original    ✓
```

### 6.4 · Lineage T2 — `f79`–`f82` (**một** biến, bốn biểu diễn)

```
LAYER 1   CUSTOMER.synthetic_category_515          ◄── MỘT thuộc tính duy nhất
             │  bằng chứng: cardinality tuple = cardinality từng cột = 515
             │  ⇒ ánh xạ value ↔ level id là SONG ÁNH
             ▼
LAYER 3   level id
             │
             ├──► encoding_1  ──►  f79
             ├──► encoding_2  ──►  f80
             ├──► encoding_3  ──►  f81
             └──► encoding_4  ──►  f82
                                     │
                                     ▼
                        FEATURE STORE  (4 cột, 1 nguồn)
```

Bảng mã hoá được **suy ra từ chính dataset** (515 cặp `level ↔ value`), không phải bịa.

### 6.5 · Lineage T2 — `f37`, `f38` (**hai** biến, chung bảng mã hoá)

```
LAYER 1   CUSTOMER.synthetic_attr_64      (64 mức)
          CUSTOMER.synthetic_attr_241     (241 mức)
             │  bằng chứng: tuple cardinality 1133 > 241 ⇒ hai biến độc lập
             │             alphabet(f37) ⊂ alphabet(f38), 64/64 ⇒ chung hàm encode
             ▼
LAYER 3   shared_encoding(level)
             ├──►  f37
             └──►  f38
```

---

## 7. Track A2 — Pass-through

23 cột liên tục / cardinality > 1000: `f3`, `f4`, `f8`–`f10`, `f12`, `f13`, `f16`,
`f17`, `f20`–`f24`, …

```yaml
reconstruction_tier: T3
business_definition: UNKNOWN
confidence:          UNKNOWN
```

**Quy tắc xử lý — chỉ áp dụng SAU feature selection:**

```
if feature ∈ selected_set:
        → định nghĩa controlled pass-through strategy
          (mang theo như thuộc tính mờ của CUSTOMER, ghi rõ đây KHÔNG phải
           feature engineering, và đánh dấu là baseline/fallback)
else:
        → loại khỏi production feature pipeline
```

> **Không tự động gán business semantic cho 23 cột này.** Giữ `T3 / UNKNOWN` cho
> tới khi có bằng chứng cấu trúc mới.

Lưu ý: `f23` và `f25` nằm trong nhóm này và **trùng nhau 99.78%** (`corr = 0.999957`)
⇒ phải xử lý ở mức **feature group**, không phải hai nguồn thông tin độc lập (§11).

---

## 8. Track B — event-derived hiện có

11 feature đã có lineage đầy đủ trong repo. **Không đổi khái niệm** — chỉ nhắc lại
để bản đồ lineage trọn vẹn.

### 8.1 · Nhóm realtime `rt_*`

```
   Business transaction  (SẼ có sau M3 — hiện đang là random generator)
            │
            ▼
   Domain event ──► Kafka ──► stream_consumer (key = customer_id)
                         │
                         ├─(1)─► _write_parquet() ──► parquet lake
                         │              ──► stg_app_events (dedup event_id)
                         │              ──► feat_user_realtime_pit.sql
                         │                  cửa sổ = [bucket_start(feature_ts) − 11×300s , feature_ts)
                         │                          ═══ OFFLINE ═══
                         │
                         └─(2)─► _update_realtime() sau khi (1) thành công
                                    ──► downstream realtime state (representation ngoài scope)
                                    ──► online_store.aggregate_realtime()
                                               ═══ ONLINE ═══
```

**Parity:** hai nhánh **cùng làm tròn về ô 5 phút** — thiết kế có chủ đích, cần test
tự động canh giữ (gap G7, chưa có).

**Chỗ đứt:** `rt_session_len_sec` chỉ có nhánh (1). Online không bao giờ ghi ⇒
offline/online feature skew có thật (audit C16).

### 8.2 · Nhóm batch `hist_*`

Ba chỗ đứt lineage đã ghi ở `FEATURE_DICTIONARY.md` §6: D2 (`user_tenure_days` bị
chặn ở 30), D3 (`voucher_used_30d` đếm claim nhưng tên nói used), D4 (không point-in-time).
Các lỗi này thuộc legacy/full mart, không thuộc selected Redis sync 55 cột hiện tại.

---

## 9. Track C — feature nghiệp vụ mới

Feature **không** tồn tại trong LZD, do synthetic business system sinh ra để phục vụ
quyết định voucher. Lineage đầy đủ từ Layer 1. Chi tiết: `FEATURE_DICTIONARY.md` §5.

Ví dụ đóng closed loop — `voucher_redeemed_30d`:

```
LAYER 1   DECISION.insert ──► VOUCHER_ISSUANCE.insert
LAYER 2   VOUCHER_ISSUED ──► Kafka ──► VOUCHER_CLAIMED ──► VOUCHER_REDEEMED
LAYER 3   COUNT trên cửa sổ 30 ngày, point-in-time tại feature_ts
LAYER 4   MODEL ──► score ──► POLICY ──► DECISION
                │                            │
                └────────────────────────────┘   ★ VÒNG LẶP ĐÓNG
```

⚠️ **Cảnh báo nhân quả:** đây là *lịch sử treatment*. Nếu chính sách phát voucher
trong quá khứ đã targeted (như train set LZD), feature này mã hoá luôn *chính sách cũ*.
Cần ablation trước khi đưa vào production.

---

## 10. Feature Selection đặt ở đâu

### 10.1 · Luồng đúng

```
   LZD Dataset
        ▼
   Feature Profiling                    ← đã làm (§5)
        ▼
   Train / Evaluate Baseline Model      ← BẮT BUỘC
        ▼
   Feature Selection
        ▼
   Selected Features
        ▼
   Constrained Forward Synthesis
```

### 10.2 · Vì sao model phải đứng trước

❌ **Sai:** `Dataset → Feature Selection → Business Reconstruction` (không nói tới model).

Đây là bài toán **uplift / treatment effect**. Không thể quyết định feature chỉ bằng
tương quan với outcome. Phải cân nhắc **tám** tiêu chí:

| Tiêu chí | Vì sao cần |
|---|---|
| `predictive value` | dự báo outcome |
| `treatment interaction` | feature có tương tác với treatment không |
| `heterogeneous treatment effect` | uplift có khác nhau theo feature không |
| `uplift contribution` | đóng góp vào AUUC/Qini, **không phải** vào AUC |
| `redundancy` | xem §11 |
| `stability` | ổn định qua các seed / fold |
| `leakage` | có rò rỉ tương lai không |
| `online availability` | lúc serve có tính được không |

Một feature có `predictive value` cao nhưng `treatment interaction` bằng 0 sẽ giúp
dự đoán `P(mua)` mà **không** giúp xếp hạng uplift — tức là vô dụng cho bài toán này,
thậm chí có hại vì nó đẩy *sure thing* lên đầu.

### 10.3 · Hệ quả cho lộ trình

CFS **không thể bắt đầu** trước khi có model. Đây là lý do `MIGRATION_PLAN.md` đặt
model ở M1 và CFS ở PHASE X sau đó.

---

## 11. Redundancy & feature-group selection

### 11.1 · Trùng lặp đã đo được

| Nhóm | Bằng chứng | Số biến thật |
|---|---|---|
| `f23`, `f25` | trùng 99.78% dòng, `corr = 0.999957` | **1** |
| `f68`, `f71`, `f77` | `sum(abs(a−b)) = 0` — trùng tuyệt đối; `f74` lệch 2/926,669 dòng | **1 bit** |
| `f79`, `f80`, `f81`, `f82` | tuple cardinality = per-column cardinality = 515 | **1** biến 515 mức |
| `f70` | `min = max = 1` | **0** — zero information |
| `f40`–`f78` (39 cột) | 11 nhóm one-hot, sau khử trùng | **7** biến |

### 11.2 · Quy tắc

```
❌ SAI:   6 feature  =  6 nguồn thông tin độc lập
✅ ĐÚNG:  group các feature có cùng underlying information TRƯỚC khi selection
```

Feature selection phải chạy ở **hai** mức:

```
   feature level        → chọn/loại từng cột
   feature-group level  → tránh đếm MỘT tín hiệu nhiều lần
```

Hệ quả đã đo: **~21% "gain" ở đỉnh bảng importance thực chất thuộc về 2 biến,
không phải 6.** Mọi quyết định chọn feature dựa trên bảng importance chưa group
đều bị bóp méo.

### 11.3 · Ràng buộc

Không xoá/gộp cột chỉ vì correlation cao **mà chưa có baseline/ablation evidence**.
Grouping ở §11.1 là để **selection đọc đúng**, không phải lệnh xoá cột.
Ngoại lệ hiển nhiên duy nhất: `f70` (hằng số ⇒ zero information theo định nghĩa) —
và vẫn phải bump `feature_spec.version` chứ không sửa lén.

---

## 12. Tautology vs Validation

**Đây là section quan trọng nhất của tài liệu này.**

### 12.1 · Vấn đề

```
   f30
    ↓  n = round(10^f30)
   synthetic order_count
    ↓  log10(order_count)
   f30
```

rồi kết luận:

```
   generated f30 == original f30
```

Điều này **gần như là một tautology** nếu ta cố tình dựng ngược transformation.

### 12.2 · Nó chứng minh gì và không chứng minh gì

| | |
|---|---|
| ✅ **Chứng minh** | `transformation pipeline works` — đường ống decode → sinh → aggregate → encode chạy đúng, không mất mát, không lệch |
| ❌ **KHÔNG chứng minh** | `f30 thật sự là order_count trong Lazada` |

Semantic vẫn là:

```
SYNTHETIC_ASSUMPTION
```

### 12.3 · Vì sao vẫn phải làm

Dù là tautology về mặt logic, reconstruction validation vẫn bắt được lỗi thật:

- sai bảng mã hoá (encoding map lệch)
- sai cửa sổ thời gian
- sai thứ tự cột
- mất chính xác dấu phẩy động (vd `f30` lưu 6 chữ số ⇒ `10^f30 = 29.999982`, phải
  dùng dung sai tương đối chứ không phải so bằng tuyệt đối)
- point-in-time boundary bị vi phạm

Nó là **điều kiện cần**, không phải điều kiện đủ.

---

## 13. Hai tầng validation

### 13.1 · Tầng A — Reconstruction Validation

```
   generated_feature  ==  original_feature
```

**Mục đích:** kiểm tra pipeline có tái tạo đúng *representation* hay không.
**Không** chứng minh semantic thật. Xem §12.

**Phạm vi áp dụng:** Track A1 (T1 + T2), Track B, Track C.
**Dung sai:** `quality.online_offline_tolerance = 0.0001` (đã khai báo trong spec) —
**tương đối**, không tuyệt đối.

### 13.2 · Tầng B — Model-Level Validation ← **đây mới là validation quan trọng**

```
   Original LZD Dataset          Reconstructed Dataset
            ▼                              ▼
          Model                    Equivalent Model
            ▼                              ▼
   AUUC / Qini / uplift    ⟷    AUUC / Qini / uplift
```

**So sánh đầy đủ:**

| # | Chiều so sánh | Ý nghĩa nếu lệch |
|---|---|---|
| 1 | `uplift curve` | hình dạng xếp hạng khác ⇒ hệ tái tạo không giữ được cấu trúc |
| 2 | `Qini` | |
| 3 | `AUUC` | |
| 4 | `treatment heterogeneity` | mất tính không đồng nhất ⇒ mất chính thứ uplift model cần |
| 5 | `prediction distribution` | model nhìn thấy phân bố khác ⇒ ngưỡng policy vô nghĩa |
| 6 | `feature importance` | trọng số dịch chuyển ⇒ reconstruction đổi vai trò feature |
| 7 | `decision / policy distribution` | tỉ lệ `SEND_VOUCHER` khác ⇒ hệ quả kinh tế khác |

**Mục tiêu:**

> Xác định hệ feature tái tạo có **bảo toàn cấu trúc ML hữu ích** của bài toán
> training hay không.

### 13.3 · Quan hệ giữa hai tầng

```
   Tầng A đạt, Tầng B đạt      →  reconstruction dùng được
   Tầng A đạt, Tầng B KHÔNG    →  tái tạo đúng con số nhưng mất cấu trúc
                                   (vd: thuộc tính được gán nhưng KHÔNG điều khiển
                                    hành vi ⇒ tương quan giữa các feature bị phá)
   Tầng A KHÔNG                →  pipeline sai, chưa cần xét tầng B
```

Trường hợp giữa là **cái bẫy nguy hiểm nhất** và là lý do §3.4 tồn tại.

---

## 14. Ranh giới point-in-time

```
              quá khứ  ◄────────── feature_ts ──────────►  tương lai
                                        │
   ✅ dùng để tính feature               │   ❌ TUYỆT ĐỐI KHÔNG
   event_ts < feature_ts                 │   event_ts >= feature_ts
                                        │
                                        │   ✅ label lấy ở đây,
                                        │      trong cửa sổ attribution
```

| Thành phần | Tôn trọng biên PIT? | Bằng chứng |
|---|---|---|
| `feat_user_realtime_pit.sql` | ✅ | `e.event_ts < s.feature_ts` |
| `online_store.aggregate_realtime()` | ✅ | cutoff theo `now()` lúc serve |
| `feat_user_behaviour.sql` | ❌ | dùng `now()` lúc dbt build — **D4** |
| `feat_user_serving.sql` | ⚠️ | baseline/full mart join behaviour `using (user_id)`, không theo `dt` |
| `feat_user_selected_serving.sql` | ✅ | selected 55-column sync source; không chứa label/is_treat |
| `offline_store.iter_shard()` | ✅ | chỉ đọc `entity_key + batch_names` ⇒ **label không rời staging snapshot** |

Dòng cuối: **ranh giới label đang đúng** — nhiều team làm sai chỗ này. Đừng phá khi migrate.

**Với CFS:** event sinh ra ở bước 4 phải nằm **trước** `feature_ts`. Nếu sinh event
ở thời điểm sau rồi tính ngược, reconstruction sẽ "đạt" trong khi vi phạm PIT —
một dạng tautology nguy hiểm hơn §12.

---

## 15. Vai trò của dataset LZD

### 15.1 · Dataset gốc là bất khả xâm phạm

```
   LZD Dataset  =  REFERENCE / TRAINING DATASET
```

**Không modify source dataset để ép validation pass.**

Dữ liệu sinh ra phải đi đúng chiều:

```
   ✅ Business DB → Raw/Event → Feature Engineering → Generated Features
                                                            │
                                                            ▼
                                                   compare với reference

   ❌ LZD feature → copy trực tiếp → database → gọi là "feature engineering"
```

### 15.2 · Seed user & closed loop

Synthetic system **được phép** khởi đầu từ user/dòng có thật trong LZD dataset.
Mục tiêu trước mắt:

```
   existing users → business events → feature updates → inference → decisions
```

Bài toán **sinh user hoàn toàn mới** là một bài toán generative modeling riêng
(cần fit joint distribution trên 83 chiều) và **chưa cần** cho closed-loop simulator.
Ghi nhận là việc tương lai, không phải thiếu sót.

> **Lợi ích phụ quan trọng:** vì mỗi synthetic customer được dựng bằng cách decode
> **một dòng thật**, phân bố đồng thời giữa các thuộc tính chính là empirical joint
> của dataset ⇒ cấu trúc tương quan giữa các feature được bảo toàn **miễn phí**.
> Đây là điều mà một generator độc lập từng thuộc tính không bao giờ đạt được.

### 15.3 · Ràng buộc bảo toàn test set

```
   full_testset.csv  =  RCT  =  tài sản đánh giá không thiên lệch DUY NHẤT
```

Uplift thật chỉ **+0.37pp** trên nền 3.3%. Đủ 181,669 dòng ⇒ SE ≈ 0.087pp, z ≈ 4.3.
Cắt xuống 50k ⇒ SE ≈ 0.16pp, z ≈ 2.3 — **mất độ tin cậy**.

⇒ Scenario chỉ dùng user `lzd_split = 'train'` (926,669 user, dư thừa).

### 15.4 · Ràng buộc thống kê để đối chiếu

| Chiều | Train (observational) | Test (RCT) |
|---|---|---|
| % treated | **22.2%** | **52.1%** |
| positive rate | **1.99%** | **3.52%** |
| CR treated | 5.66% | 3.70% |
| CR control | 0.94% | 3.33% |
| chênh lệch thô | **+4.72pp** | **+0.37pp** |

Bốn dòng cuối là ràng buộc **mạnh nhất và cụ thể nhất** dataset áp lên synthetic
system — và là lý do `EXPERIMENT.mode ∈ {TARGETED, RCT}` tồn tại trong `ERD.md` §7.2.

---

## Liên quan

`LAZADA_BUSINESS_DOMAIN.md` · `ERD.md` · `BUSINESS_EVENT_MODEL.md` ·
`FEATURE_DICTIONARY.md` · `MIGRATION_PLAN.md` · `AUDIT_KIEN_TRUC_VA_FEATURE.md`
