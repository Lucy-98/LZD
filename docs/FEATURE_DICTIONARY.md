# Feature Dictionary

> Business alias note: selected `f*` columns now have synthetic business aliases
> in `config/features/business_aliases.yml` and `docs/BUSINESS_ALIAS_MAP.md`.
> These names make the voucher-uplift story readable, but they are not confirmed
> Lazada/DESCN semantics. Redis/dbt names remain `f*`.

> **Vai trò:** Layer 3 (Feature Engineering / Feature Store) — hợp đồng của từng feature.
>
> **Trạng thái:** Track A/B mô tả dữ liệu & code hiện có; Track C là đề xuất.
>
> **Nguyên tắc §15:** mỗi feature có đủ **14 trường** lineage (13 trường gốc +
> `reconstruction_tier`). Feature chưa biết semantic ⇒ `business_definition: UNKNOWN`,
> `confidence: UNKNOWN`. **Không bịa.**
>
> **Nguyên tắc bổ sung của bản sửa này:**
> ```
> ❌ SAI:  f30 IS order_count_30d
> ✅ ĐÚNG: f30
>            structural constraint:        10^f30 ∈ ℤ ∩ [1,30]  (CONFIRMED)
>            candidate synthetic semantic: order_count_30d
>            confidence:                   SYNTHETIC_ASSUMPTION
>            true semantic:                UNKNOWN
> ```

---

## Mục lục

1. [Thang confidence & schema bản ghi](#1-thang-confidence--schema-bản-ghi)
2. [Cardinality profile — 83 cột](#2-cardinality-profile--83-cột)
3. [Reconstruction tier](#3-reconstruction-tier)
4. [Track A1 — reconstructable](#4-track-a1--reconstructable)
5. [Track A2 — pass-through (T3)](#5-track-a2--pass-through-t3)
6. [Feature group & redundancy](#6-feature-group--redundancy)
7. [Track B — event-derived hiện có](#7-track-b--event-derived-hiện-có)
8. [Track C — feature nghiệp vụ mới](#8-track-c--feature-nghiệp-vụ-mới)
9. [Lỗi contract phát hiện được](#9-lỗi-contract-phát-hiện-được)
10. [Feature bị loại và lý do](#10-feature-bị-loại-và-lý-do)

---

## 1. Thang confidence & schema bản ghi

### 1.1 · Confidence

| Mức | Định nghĩa | Được phép làm gì |
|---|---|---|
| **CONFIRMED** | Có công thức trong repo **hoặc** đo trực tiếp trên dữ liệu | Dùng tự do, viết test |
| **SYNTHETIC_ASSUMPTION** | Semantic do project **đặt ra** để phục vụ CFS. Ràng buộc cấu trúc là CONFIRMED, nhưng *nghĩa* là giả định | Dùng trong synthetic system, **luôn** kèm nhãn. Không bao giờ trình bày như fact |
| **INFERRED** | Suy luận hợp lý, chưa chứng minh | Ghi vào doc, không hard-code vào tên biến |
| **SYNTHETIC** | Feature do project thiết kế, không đối chiếu với LZD | Dùng tự do trong synthetic system |
| **UNKNOWN** | Không có bằng chứng | Chỉ pass-through có kiểm soát. Không diễn giải |

> ⚠️ **`SYNTHETIC_ASSUMPTION` ≠ `CONFIRMED`.** Việc reconstruction khớp 100% **không**
> nâng semantic lên CONFIRMED — xem `FEATURE_LINEAGE.md` §12 (Tautology vs Validation).

### 1.2 · Schema bản ghi (14 trường)

```yaml
feature_name:                 …
feature_group:                …    # ★ MỚI — nhóm cùng underlying information
business_definition:          …
candidate_synthetic_semantic: …    # ★ MỚI — chỉ dùng cho Track A1
source_entity:                …
source_field:                 …
source_event:                 …
transformation:               …
window:                       …
aggregation:                  …
encoding:                     …
batch_or_realtime:            …
offline_definition:           …
online_definition:            …
reconstruction_tier:          T1 | T2 | T3 | UNKNOWN    # ★ MỚI
confidence:                   …
```

---

## 2. Cardinality profile — 83 cột

Đo trên **toàn bộ** 926,669 dòng `full_trainset.csv`, không lấy mẫu.

| Cardinality | Số cột | Cột |
|---|---|---|
| **≤ 2** | **39** | `f40`–`f78` |
| **3–30** | **3** | `f0`, `f7`, `f30` |
| **31–400** | **8** | `f1`, `f2`, `f18`, `f19`, `f27`, `f34`, `f37`, `f38` |
| **401–1000** | **10** | `f5`, `f6`, `f11`, `f14`, `f15`, `f39`, `f79`, `f80`, `f81`, `f82` |
| **> 1000** | **23** | `f3`, `f4`, `f8`, `f9`, `f10`, `f12`, `f13`, `f16`, `f17`, `f20`, `f21`, `f22`, `f23`, `f24`, … |

```
Tổng:      83 cột
Rời rạc:   60 / 83   (72%)   →  ứng viên Constrained Forward Synthesis
Liên tục:  23 / 83   (28%)   →  T3 / UNKNOWN
```

> Đây là thông tin nền của toàn bộ chiến lược reconstruction. **Không được làm mất
> khi rewrite tài liệu.**

---

## 3. Reconstruction tier

| Tier | Định nghĩa | Cơ chế |
|---|---|---|
| **T1 — EVENT-LEVEL** | Feature nảy sinh từ business event được sinh ra | sinh `n` event → aggregate → transform |
| **T2 — ATTRIBUTE-LEVEL** | Feature là encoding của thuộc tính entity | gán level → encode |
| **T3 — PASS-THROUGH** | Liên tục / cardinality cao, chưa đủ bằng chứng semantic | mang theo có kiểm soát, **chỉ nếu** selection cho thấy model cần |
| **UNKNOWN** | Chưa đo đủ để phân loại | **không gán tier** |

### 3.1 · Tổng quan phân bổ hiện tại

| Tier | Số cột | Cột |
|---|---|---|
| **T1** | **7** | `f1`, `f2`, `f5`, `f11`, `f18`, `f19`, `f30` |
| **T1 \| T2** (tier tự nó là assumption) | **4** | `f0`, `f7`, `f27`, `f34` |
| **T2** | **45** | `f40`–`f78` (39), `f79`–`f82` (4), `f37`, `f38` |
| **UNKNOWN** | **4** | `f6`, `f14`, `f15`, `f39` — ngoài selected set, xem §4.9 |
| **T3** | **23** | 23 cột `>1000` |

### 3.2 · Tier của 55 cột **đã chọn** (`fs_2026_08_v2`)

| Tier | Số | Cột |
|---|---|---|
| **T1** | **7** | `f1`, `f2`, `f5`, `f11`, `f18`, **`f19`**, `f30` |
| **T2** | **24** | `f37`, `f38`, `f79`, `f80`, `f81`, `f82` · `f40`, `f41`, `f42` · `f43`, `f44`, `f45`, `f46`, `f47`, `f52` · `f53`, `f54`, `f57`, `f58`, `f59`, `f62` · `f64`, `f65` · `f68` |
| **T3** | **24** | `f0`,`f3`,`f4`,`f6`,`f8`,`f9`,`f10`,`f12`,`f13`,`f16`,`f17`,`f20`,`f21`,`f22`,`f23`,`f24`,`f25`,`f26`,`f27`,`f28`,`f29`,`f31`,`f34`,`f35` |
| **UNKNOWN** | **0** | — |

> `f0`, `f27`, `f34` được §3.1 xếp `T1|T2` — tức **chính tier cũng là assumption**.
> Ở v2 chúng vào **T3** một cách có ý: pass-through là lựa chọn **không cam kết
> semantic gì cả**. Nâng lên T1/T2 đòi hỏi bằng chứng hiện chưa có.
>
> Lịch sử: v1 (`fs_2026_08_v1`) là 6 · 12 · 18 = 36 cột. v2 là **superset chặt**.
> Xem `SCOPE_EXPANSION_55F.md`.

Chi tiết reconstruction: `DATA_GENERATION.md` · `RECONSTRUCTION_CONTRACT.md` §4.

---

## 4. Track A1 — reconstructable

### 4.1 · `f30` — log10 counter, trần 30

```yaml
feature_name:                 f30
feature_group:                G_counter_30
business_definition:          UNKNOWN
candidate_synthetic_semantic: order_count_30d          # ★ chỉ là ứng viên
structural_constraint:        10^f30 ∈ ℤ ∩ [1,30], 100% dòng (926,669/926,669)
                              lưu 6 chữ số thập phân ⇒ max(10^f30) = 29.999982
                              ⇒ so sánh phải dùng DUNG SAI TƯƠNG ĐỐI (1e-4), không tuyệt đối
transformation:               log10(counter)
aggregation:                  COUNT → log10
encoding:                     none
reconstruction_tier:          T1
confidence:                   CONFIRMED (cấu trúc) / SYNTHETIC_ASSUMPTION (semantic)
true_semantic:                UNKNOWN
```

### 4.2 · `f18`, `f19` — cùng họ `log10(counter)`, trần khác

| | `f18` | `f19` |
|---|---|---|
| Cardinality | 144 | 156 |
| `10^x ∈ ℤ` | **100%** dòng | **100%** dòng |
| Tỉ lệ dòng `= 0.0` | **95.5%** | **93.0%** |
| max | 3.354108 ⇒ `n_max = 2260` | 3.673205 ⇒ `n_max ≈ 4712` |
| Alphabet quan sát | `log10(1)=0`, `log10(2)=0.30103`, `log10(3)=0.477121`, `log10(4)=0.60206`, `log10(5)=0.69897`, … | cùng dạng |
| `reconstruction_tier` | **T1** | **T1** |

```yaml
structural_constraint:        10^f{18,19} ∈ ℤ, 100% dòng
candidate_synthetic_semantic: counter thưa (95.5% / 93.0% dòng = giá trị nhỏ nhất)
                              trần rất khác f30 ⇒ ĐẾM MỘT THỨ KHÁC
confidence:                   CONFIRMED (cấu trúc) / SYNTHETIC_ASSUMPTION (semantic)
```

> **Ba cột `f18`, `f19`, `f30` cùng một họ biến đổi nhưng ba trần khác nhau
> (2260 / 4712 / 30).** Chúng đếm ba thứ khác nhau. Không được gán cùng một semantic.

### 4.3 · `f1`, `f2` — cặp có thứ tự

```yaml
feature_group:                G_recency_pair
structural_constraint:        nguyên [0,365], 366 mức mỗi cột
                              f1 ≥ f2 ĐÚNG Ở MỌI DÒNG  (min(f1−f2) = 0)
candidate_synthetic_semantic: hai mốc ngày có thứ tự trên CUSTOMER
transformation:               date_diff('day', attr_date, feature_ts)
reconstruction_tier:          T1
confidence:                   CONFIRMED (cấu trúc) / SYNTHETIC_ASSUMPTION (semantic)
```

Ràng buộc `f1 ≥ f2` **tự thoả** khi reconstruct nếu `attr_date_A ≤ attr_date_B`.

### 4.4 · `f0`, `f7`, `f27`, `f34` — tier tự nó là assumption

| Cột | Cardinality | Miền | Ghi chú |
|---|---|---|---|
| `f0` | 6 | `{0,1,2,3,4,5}` nguyên | phân bố: 271043 / 123771 / 159113 / 170725 / 91804 / 110213 |
| `f7` | 6 | `{0,1,2,3,4,5}` nguyên | khác `f0` ở **470,346 / 926,669 dòng (50.8%)** ⇒ **biến khác hẳn** |
| `f27` | 32 | nguyên `[0,100]` | |
| `f34` | 31 | nguyên `[0,100]` | |

```yaml
reconstruction_tier:          T1 | T2      # ★ dữ liệu KHỚP CẢ HAI cách đọc
```

```
T1:  counter bị cap        →  sinh n event, COUNT
T2:  mức ordinal / tier    →  gán thuộc tính, encode
```

Cả hai đều cho tái tạo chính xác từng dòng. **Việc chọn cái nào không suy ra được
từ dữ liệu** — nó là `SYNTHETIC_ASSUMPTION`, phải ghi rõ khi chốt.

### 4.5 · `f40`–`f78` — khối one-hot, **8 biến thật**

```yaml
feature_group:                G_onehot_1 .. G_onehot_8
structural_constraint:        row_sum(f40..f78) = 11.0 ở 100% dòng (926,669/926,669)
                                 = 8 group one-hot (mỗi group đóng góp 1)
                                 + f70 hằng số 1
                                 + 2 cột trùng khít với mức nóng của g6
                              row_sum(f40..f78 trừ f70) = 10.0
                              f68 ≡ f71 ≡ f77   (sum(abs(a−b)) = 0)
                              f78 ≡ f69 ≡ f72   (sum(abs(a−b)) = 0)   ← ★ MỚI ĐO
                              f74 lệch f68 ở 2/926,669 dòng
                              f73 lệch f69 ở 2/926,669 dòng
                              f70 hằng số = 1   ⇒ zero information
                              ⇒ sau khử trùng: 39 cột mã hoá 8 BIẾN CATEGORICAL
candidate_synthetic_semantic: CUSTOMER.synthetic_segment_G1..G8
encoding:                     one-hot
reconstruction_tier:          T2
confidence:                   CONFIRMED (cấu trúc) / SYNTHETIC_ASSUMPTION (semantic)
```

`[MEASURED]` **Tám** group thoả `sum == 1` tuyệt đối trên toàn bộ train:

| Group | Cột | levels | test split | Trong `fs_2026_08_v2` |
|---|---|---|---|---|
| `g1` | `f40` `f41` `f42` | 3 | ✅ | chọn **3/3** |
| `g2` | `f43`–`f52` | 10 | ✅ | chọn 6/10 |
| **`g3`** | **`f53`–`f62`** | **10** | ✅ | **MỚI** — chọn 6/10 |
| `g4` | `f63` `f64` `f65` | 3 | 🔴 **VỠ** | chọn 2/3 |
| `g5` | `f66` `f67` | 2 | ✅ | không chọn |
| `g6` | `f68` `f78` | 2 | ✅ | chọn 1/2 |
| `g7` | `f73` `f74` | 2 | ✅ | không chọn |
| `g8` | `f75` `f76` | 2 | ✅ | không chọn |

> ⚠️ **Sửa lỗi bản trước.** `row_sum = 11.0` là **đúng**, nhưng bản trước đọc nó
> thành *"11 nhóm loại trừ lẫn nhau"* và kết luận *"7 biến"* — cả hai đều sai.
> 11 = **8** group + `f70` + 2 cột trùng khít, không phải 11 group.
> Bản trước cũng bỏ sót quan hệ `f78 ≡ f69 ≡ f72`.

> 🔴 **`g4` vỡ bất biến one-hot trên test split:** `sum(f63,f64,f65) == 0` ở
> **3/181,669** dòng test, 0 dòng train ⇒ attribute thật có mức baseline toàn-0
> không xuất hiện trong train. `split_allowed: train` nên chưa nổ.
> 🚫 Không mở scope sang test trước khi xử lý mức thứ tư.

### 4.6 · `f79`–`f82` — **MỘT** biến categorical 515 mức, bốn encoding

```yaml
feature_name:                 f79, f80, f81, f82
feature_group:                G_category_515                    # ★ MỘT nhóm duy nhất
structural_constraint:        cardinality(f79) = cardinality(f80)
                            = cardinality(f81) = cardinality(f82) = 515
                              cardinality(tuple f79,f80,f81,f82) = 515      ← BẰNG NHAU
                              ⇒ ánh xạ value ↔ level id là SONG ÁNH HOÀN HẢO
                              ⇒ bốn cột là BỐN CÁCH MÃ HOÁ CỦA MỘT BIẾN
                              (bằng chứng cũ: phân bố tần suất trùng khít
                               663,495 / 28,969 / 3,947 giống hệt cả bốn cột)
source_entity:                CUSTOMER
source_field:                 synthetic_category_515            # MỘT thuộc tính
encoding:                     4 encoding khác nhau của cùng level id
                              (bảng mã hoá suy ra từ chính dataset: 515 cặp level↔value)
reconstruction_tier:          T2
confidence:                   CONFIRMED (cấu trúc) / SYNTHETIC_ASSUMPTION (semantic)
true_semantic:                UNKNOWN
```

> ❌ **Không được coi `f79`–`f82` là bốn business variable độc lập.**
> Trong ERD chúng ứng với **một** thuộc tính duy nhất.

### 4.7 · `f37`, `f38` — **HAI** biến, chung một bảng mã hoá

```yaml
feature_group:                G_encoded_64  /  G_encoded_241     # ★ HAI nhóm khác nhau
structural_constraint:        cardinality(f37) = 64,  miền [0, 0.715096]
                              cardinality(f38) = 241, miền [0, 0.843222]
                              cardinality(tuple f37,f38) = 1133  >  max(64,241)
                                    ⇒ KHÔNG song ánh ⇒ HAI BIẾN ĐỘC LẬP
                              alphabet(f37) ⊂ alphabet(f38) hoàn toàn (64/64 giá trị)
                                    ⇒ DÙNG CHUNG MỘT HÀM ENCODE
                              test x·q ∈ ℤ phẳng 17.8% (f37) / 6.2% (f38) với mọi
                              q ∈ {2,3,4,5,6,8,10,12,20,50,100,1000}
                                    = đúng tỉ lệ dòng bằng 0
                                    ⇒ KHÔNG phải tỉ lệ của các counter nhỏ
                              f37 = f38 ở 109,690 dòng; f37 ≤ f38 ở 685,289/926,669
reconstruction_tier:          T2
confidence:                   CONFIRMED (cấu trúc) / SYNTHETIC_ASSUMPTION (semantic)
```

> ⚠️ **Đây là cấu trúc trông giống `f79`–`f82` nhưng phải mô hình hoá KHÁC.**
>
> | | `f79`–`f82` | `f37`, `f38` |
> |---|---|---|
> | tuple cardinality | **= per-column** (515) | **> per-column** (1133 > 241) |
> | Kết luận | **1** biến, 4 encoding | **2** biến, 1 bảng mã hoá dùng chung |
> | ERD | 1 thuộc tính | 2 thuộc tính |

### 4.8 · `f5`, `f11` — **T1**, counter log cơ số **e** *(đã đo)*

```yaml
feature_group:                G_counter_ln_a / G_counter_ln_b     # HAI biến độc lập
structural_constraint:        e^f ∈ ℤ ở 100% dòng (dung sai tương đối 1e-12)
                              f5 :  n ∈ [1, 14245], 864 mức, 74.2% dòng có n = 1
                              f11:  n ∈ [1,  2144], 669 mức, 70.2% dòng có n = 1
                              lưu ở ĐỘ CHÍNH XÁC float64 ĐẦY ĐỦ (16 chữ số)
                              f5 ≠ f11 ở 381,058 dòng; card(tuple) = 16,479
                                    ⇒ HAI biến độc lập, không song ánh
transformation:               ln(counter)
reconstruction_tier:          T1
confidence:                   CONFIRMED (cấu trúc) / SYNTHETIC_ASSUMPTION (semantic)
true_semantic:                UNKNOWN — đếm cái gì, cửa sổ bao lâu
```

> ⚠️ **Dataset dùng HAI quy ước log khác nhau:**
>
> | Regime | Cột | Cơ số | Lưu trữ | Round-trip khớp chính xác |
> |---|---|---|---|---|
> | `L10` | `f18`, `f19`, `f30` | 10 | 6 chữ số | **100.0000%** |
> | `LN` | `f5`, `f11` | **e** | float64 đầy đủ | **93.05% / 93.42%** — cần dung sai `1e-15` |
>
> ⇒ Hai regime cần **hai luật so sánh khác nhau** ở GATE A.
> Xem `RECONSTRUCTION_CONTRACT.md` §9.

### 4.9 · `f6`, `f14`, `f15`, `f39` — ngoài selected set, có trùng lặp

`[MEASURED]` — không thuộc 55 cột đã chọn, ghi lại để không đo lại:

| Cặp | Kết quả | Ghi chú |
|---|---|---|
| `f6` ≡ `f15` | **trùng tuyệt đối** — `sum(abs(f6−f15)) = 0`, 0 dòng lệch | `10^x ∈ ℤ` 100% ⇒ log10 counter |
| `f14` ≡ `f39` | **trùng tuyệt đối** — 0 dòng lệch | `[0, 0.999327]`, **không** phải log counter |

⇒ Khối "401–1000" gồm 10 cột chỉ mang **5 biến độc lập**:
`f5` · `f11` · `f6≡f15` · `f14≡f39` · `f79`–`f82`.

---

## 5. Track A2 — pass-through (T3)

23 cột: `f3`, `f4`, `f8`, `f9`, `f10`, `f12`, `f13`, `f16`, `f17`, `f20`, `f21`,
`f22`, `f23`, `f24`, …

```yaml
business_definition:          UNKNOWN
candidate_synthetic_semantic: (không có — KHÔNG được ép)
reconstruction_tier:          T3
confidence:                   UNKNOWN
```

**Quy tắc xử lý — chỉ áp dụng SAU feature selection:**

```
if feature ∈ selected_set:
        → controlled pass-through strategy
          (thuộc tính mờ của CUSTOMER; ghi rõ đây KHÔNG phải feature engineering;
           đánh dấu baseline/fallback)
else:
        → loại khỏi production feature pipeline
```

> **Không tự động gán business semantic cho 23 cột này chỉ để tài liệu trông đẹp.**

---

## 6. Feature group & redundancy

### 6.1 · Bảng nhóm

| `feature_group` | Cột | Số biến thật | Bằng chứng |
|---|---|---|---|
| `G_dup_continuous` | `f23`, `f25` | **1** | trùng 99.78% dòng, `corr = 0.999957` |
| `G_onehot_1..7` | `f40`–`f78` (39 cột) | **7** | 11 nhóm loại trừ; `f68≡f71≡f77`; `f74` lệch 2 dòng; `f70` hằng số |
| `G_category_515` | `f79`–`f82` | **1** | tuple cardinality = per-column = 515 |
| `G_encoded_64` | `f37` | **1** | |
| `G_encoded_241` | `f38` | **1** | tuple(f37,f38) = 1133 ⇒ tách khỏi `f37` |
| `G_zero_info` | `f70` | **0** | `min = max = 1` |

### 6.2 · Quy tắc selection

```
❌ SAI:   6 feature  =  6 nguồn thông tin độc lập
✅ ĐÚNG:  group TRƯỚC, rồi mới selection
```

Selection chạy ở **hai** mức: `feature level` và `feature-group level`.

**Hệ quả đã đo:** ~21% "gain" ở đỉnh bảng importance thực chất thuộc về **2 biến,
không phải 6**. Mọi quyết định dựa trên bảng importance chưa group đều bị bóp méo.

### 6.3 · Ràng buộc

Không xoá/gộp cột chỉ vì correlation cao **mà chưa có baseline/ablation evidence**.
Grouping ở §6.1 là để **selection đọc đúng**, không phải lệnh xoá.
Ngoại lệ duy nhất: `f70` — và vẫn phải bump `feature_spec.version`.

---

## 7. Track B — event-derived hiện có

11 feature event-derived tồn tại trong baseline/full mart hoặc training path cũ.
Chúng **không** thuộc Redis selected feature sync, vốn chỉ có 55 cột
`fs_2026_08_v2`. `reconstruction_tier` không áp dụng cho nhóm này.

### 7.1 · `user_tenure_days` ⚠️ **LỖI CONTRACT**

```yaml
business_definition:  "Số ngày kể từ lần đầu nhìn thấy user"      # legacy/full mart
transformation:       date_diff('day', min(event_ts), now())
window:               30 ngày                                     # ← VẤN ĐỀ
confidence:           CONFIRMED (công thức) / ⚠️ SAI so với business_definition
```

CTE `events` lọc `event_ts >= now() − 30 days` **trước** khi `min(event_ts)` ⇒
feature **bị chặn trên ở 30**. Nó là *"số ngày kể từ event đầu tiên trong 30 ngày
gần nhất"*, không phải tuổi tài khoản.

**Sửa:** lấy từ `CUSTOMER.registered_at` (ERD §3.1) — tuổi tài khoản là thuộc tính
entity, không phải hàm của event window.

### 7.2 · `hist_order_cnt_30d`

```yaml
source_event:         order      → ĐỀ XUẤT ĐỔI: ORDER_PAID
transformation:       count(*) filter (event_type = 'order')
window:               30 ngày (var history_days)
batch_or_realtime:    BATCH
confidence:           CONFIRMED
```

Hiện `order` không phân biệt đơn đã trả tiền hay chưa ⇒ đang đếm cả đơn sẽ bị huỷ.

### 7.3 · `hist_gmv_30d`

```yaml
transformation:       sum(price × quantity) filter (event_type='order')
confidence:           CONFIRMED (công thức) / ⚠️ dữ liệu không nhất quán
```

`price` do `event_producer` sinh ngẫu nhiên **cho từng event** ⇒ cùng `item_id` có
giá khác nhau ở hai event ⇒ GMV không truy ngược về catalog nào. Sau khi có SKU
(ERD §3.4) thì `unit_price` lấy từ `SKU.list_price`.

### 7.4 · `voucher_used_30d` ⚠️ **TÊN SAI SO VỚI CÔNG THỨC**

```yaml
business_definition:  "Số voucher đã dùng 30 ngày"
transformation:       count(*) filter (event_type = 'voucher_claim')   # ← đếm CLAIM
confidence:           CONFIRMED (công thức) / ⚠️ SAI so với business_definition
```

**CLAIM ≠ REDEEM.** Hai feature khác nhau, nên tồn tại song song:

| Feature | Nguồn đúng |
|---|---|
| `voucher_claimed_30d` | `VOUCHER_CLAIMED` ← thứ đang được tính |
| `voucher_redeemed_30d` | `VOUCHER_REDEEMED` ← thứ tên đang hứa |

Nếu đưa lại feature này vào selected feature contract thì phải đặt tên đúng và
bump `feature_spec.version`; hiện nó không thuộc 55 cột Redis sync.

### 7.5 · Nhóm `rt_*` (7 cột)

Cửa sổ 1h chia **12 ô 5 phút** ở cả hai phía — thiết kế chống skew tốt, giữ nguyên.

| Feature | source_event | aggregation | offline | online | confidence |
|---|---|---|---|---|---|
| `rt_events_1h` | mọi event | `COUNT(*)` | ✅ | ✅ | **CONFIRMED** |
| `rt_page_view_1h` | `page_view` | `COUNT filter` | ✅ | ✅ | **CONFIRMED** |
| `rt_add_to_cart_1h` | `add_to_cart` | `COUNT filter` | ✅ | ✅ | **CONFIRMED** |
| `rt_order_1h` | `order` | `COUNT filter` | ✅ | ✅ | **CONFIRMED** |
| `rt_gmv_1h` | `order` | `SUM(price×qty)` | ✅ | ✅ | **CONFIRMED** |
| `rt_session_len_sec` | mọi event | `date_diff('second', min, max)` | ✅ | ❌ **KHÔNG GHI** | ⚠️ **SKEW** |
| `rt_last_event_ts` | mọi event | `MAX(event_ts)` | ✅ | ✅ | **CONFIRMED** |

**`rt_session_len_sec`:** offline tính thật, online luôn nhận default `0.0`.
Đây là offline/online feature skew có thật (audit C16).

> **Khi sửa:** chốt **một** định nghĩa trước — từ `SESSION.started_at` hay từ
> `min(event_ts)` trong cửa sổ 1h? Hai công thức khác nhau với phiên > 1 giờ.
> Sửa vội sẽ thay skew này bằng skew khác.

### 7.6 · Vấn đề point-in-time của nhóm batch

| | Cửa sổ tính tại | PIT? |
|---|---|---|
| `rt_*` (`feat_user_realtime_pit.sql`) | `feature_ts`, làm tròn ô 5 phút | ✅ |
| `hist_*`, `user_tenure_days` (`feat_user_behaviour.sql`) | **`now()` lúc dbt build** | ❌ |

`seed_loader` gán `feature_ts = now()` lúc nạp CSV; dbt chạy **sau đó** ⇒
`feat_user_behaviour` gộp cả event xảy ra **sau** `feature_ts`.

Hiện chưa gây hại vì user seed chưa có event (gap G4) — nhưng sẽ thành **leakage thật**
ngay khi backfill event lịch sử.

---

## 8. Track C — feature nghiệp vụ mới

Feature **không** có trong LZD, do synthetic business system sinh ra.
`reconstruction_tier` không áp dụng. Confidence: `SYNTHETIC`.

> **Chưa feature nghiệp vụ mới nào được thêm vào selected feature contract.**
> Thêm feature ⇒ bump `feature_spec.version` và cập nhật mart sync tương ứng.

### 8.1 · Profile (từ `CUSTOMER`, batch)

| Feature | source | transformation | encoding |
|---|---|---|---|
| `cust_tenure_days_true` | `CUSTOMER.registered_at` | `date_diff('day', registered_at, feature_ts)` | none |
| `cust_member_tier` | `CUSTOMER.member_tier` | — | one-hot (4) |
| `cust_city_tier` | `CUSTOMER.city_tier` | — | ordinal |
| `cust_country` | `CUSTOMER.country` | — | one-hot (6) |

### 8.2 · RFM (từ `ORDER`, batch)

| Feature | source_event | aggregation | window |
|---|---|---|---|
| `recency_days_since_last_order` | `ORDER_PAID` | `date_diff('day', max(event_ts), feature_ts)` | 365d |
| `frequency_orders_90d` | `ORDER_PAID` | `COUNT` | 90d |
| `monetary_gmv_90d` | `ORDER_PAID` | `SUM(amount)` | 90d |
| `avg_order_value_90d` | `ORDER_PAID` | `SUM/COUNT` | 90d |
| `log_orders_90d` | ↑ | `log10(1 + frequency_orders_90d)` | 90d |

> `log_orders_90d` dùng log10 vì đó là dạng biến đổi **đã quan sát được** trong dataset
> (`f18`, `f19`, `f30`). ✅ Lấy cảm hứng từ cấu trúc LZD là hợp lệ.
> ❌ Nó **không** tuyên bố tương ứng với bất kỳ `f*` nào.

### 8.3 · Cart / intent

| Feature | source | window | mode |
|---|---|---|---|
| `cart_value_current` | `CART_ITEM × SKU.list_price` | hiện tại | REALTIME |
| `cart_item_cnt_current` | `CART_ITEM` | hiện tại | REALTIME |
| `cart_age_hours` | `min(CART_ITEM.added_at)` | hiện tại | REALTIME |
| `cart_abandoned_cnt_30d` | giỏ có item nhưng không `ORDER_CREATED` | 30d | BATCH |

> Nhóm có sức dự báo uplift cao nhất về nghiệp vụ — giỏ giá trị cao để lâu chưa mua
> chính là *persuadable* điển hình. Không nhóm feature nào hiện có nắm được tín hiệu này.
>
> ⚠️ Đây là feature **trạng thái**, không cộng dồn được qua ô 5 phút ⇒ cơ chế parity
> hiện tại không áp dụng trực tiếp. Cần thiết kế riêng — công việc thật, không miễn phí.

### 8.4 · Voucher

| Feature | source_event | window | mode |
|---|---|---|---|
| `voucher_issued_30d` | `VOUCHER_ISSUED` | 30d | BATCH |
| `voucher_claimed_30d` | `VOUCHER_CLAIMED` | 30d | BATCH |
| `voucher_redeemed_30d` | `VOUCHER_REDEEMED` | 30d | BATCH |
| `voucher_claim_rate_30d` | ↑ | 30d | BATCH |
| `voucher_redeem_rate_30d` | ↑ | 30d | BATCH |
| `rt_voucher_active_cnt` | `ISSUED` − `REDEEMED`/`EXPIRED` | hiện tại | **REALTIME** |
| `days_since_last_voucher` | `VOUCHER_ISSUED` | 365d | BATCH |

> ⚠️ **Cảnh báo nhân quả:** đây là *lịch sử treatment*. Nếu chính sách phát voucher
> quá khứ đã targeted (như train set LZD), feature này mã hoá luôn *chính sách cũ*,
> và model có thể học "ai từng được phát thì phát tiếp" thay vì học uplift thật.
> **Cần ablation trước khi đưa vào production.**

### 8.5 · Realtime mở rộng

| Feature | source_event | window |
|---|---|---|
| `rt_checkout_started_1h` | `CHECKOUT_STARTED` | 1h |
| `rt_search_1h` | `SEARCH_PERFORMED` | 1h |
| `rt_cart_remove_1h` | `ITEM_REMOVED_FROM_CART` | 1h |
| `rt_payment_failed_1h` | `PAYMENT_FAILED` | 1h |
| `rt_distinct_category_1h` | `PRODUCT_VIEWED` | 1h |

Tất cả tuân cơ chế 12 ô × 5 phút ⇒ parity miễn phí. Ngoại lệ: `rt_distinct_category_1h`
dùng `COUNT(DISTINCT)` **không cộng dồn được** qua các ô — cần HyperLogLog hoặc chấp
nhận xấp xỉ. **Cân nhắc bỏ nếu không muốn thêm phức tạp.**

---

## 9. Lỗi contract phát hiện được

| # | Feature | Vấn đề | Mức | Sửa ở đâu |
|---|---|---|---|---|
| **D1** | `rt_session_len_sec` | Offline tính thật, online luôn 0 ⇒ **skew** | 🔴 | `stream_consumer._update_realtime()` |
| **D2** | `user_tenure_days` | Bị chặn ở 30; không phải tuổi tài khoản | 🟠 | Legacy/full mart; không thuộc selected Redis sync |
| **D3** | `voucher_used_30d` | Tên nói "used", công thức đếm **claim** | 🟠 | Legacy/full mart; chỉ đưa lại khi đặt tên đúng |
| **D4** | `hist_*` không PIT | Dùng `now()` lúc dbt build thay vì `feature_ts` | 🟠 | Legacy/full mart; không thuộc selected Redis sync |
| **D5** | `hist_gmv_30d`, `rt_gmv_1h` | `price` ngẫu nhiên mỗi event | 🟡 | Cần SKU (ERD §3.4) |
| **D6** | `f70` | Hằng số 1 — zero information | 🟡 | Loại khi bump spec |
| **D7** | `f23`/`f25`, `f68`/`f71`/`f74`/`f77`, `f79`–`f82` | Trùng lặp ⇒ importance đếm nhiều lần | 🟡 | **Group trước selection** (§6), ablation trước khi xoá |

> **Không cột selected nào bị xoá trong tài liệu này.** `f70` không thuộc 55 cột
> selected; nếu một ngày đưa vào contract thì vẫn phải bump `feature_spec.version`.

---

## 10. Feature bị loại và lý do

| Ứng viên | Lý do |
|---|---|
| Feature từ `REVIEW`/`RATING` | Entity đã bị loại (A-05) |
| `shipment_delay_days` | Xảy ra **sau** điểm quyết định ⇒ không dùng được lúc inference |
| `return_rate_90d` | RETURN hiện chỉ để đóng lifecycle (ERD §5.4) |
| Embedding sản phẩm / text search | Vượt scope; thêm hạ tầng model thứ hai |
| Feature mức `(user × category)` | Đổi entity key feature store ⇒ thay đổi lớn (A-03) |

---

## Liên quan

`FEATURE_LINEAGE.md` (CFS, tautology vs validation) · `ERD.md` ·
`MIGRATION_PLAN.md` MX · `AUDIT_KIEN_TRUC_VA_FEATURE.md`
