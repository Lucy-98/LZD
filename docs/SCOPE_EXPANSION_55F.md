# Scope Expansion — 36 → 55 cột (`fs_2026_08_v1` → `fs_2026_08_v2`)

> **Trạng thái: ĐÃ IMPLEMENT + verify trên dữ liệu thật.**
> Gate A / Gate A-T3 pass trên 200 dòng train thật qua **chính SQL dbt**;
> suite 245 test xanh; solver mới đạt argmin trên 200,000/200,000 target thật.
>
> Nhãn: `[FACT]` `[MEASURED]` `[ASSUMPTION]` `[UNKNOWN]`

---

## 0. Tóm tắt

| | v1 | v2 |
|---|---|---|
| Feature set artifact | `fs_2026_08_v1.yaml` | **`fs_2026_08_v2.yaml`** |
| T1 event-level | 6 | **7** (+`f19`) |
| T2 attribute-level | 12 | **24** |
| T3 pass-through | 18 | **24** |
| **Tổng scope** | 36 | **55** |
| **Gate A surface** (T1+T2) | 18 | **31** (+72%) |
| Gate A-T3 surface | 18 | **24** |
| `intermediate_only` | 12 | **10** |
| Source attribute | 7 | **8** (+`synthetic_segment_g3`) |
| Capacity bound vi phạm | 47.56% | **39.54%** |
| Event không giải thích được | 14.1% | **10.9%** |

`[MEASURED]` **v2 là superset chặt của v1** — không cột nào bị bỏ, chỉ cộng 19.
Hệ quả: mọi gate đang xanh ở v1 không phải re-validate.

---

## 1. 19 cột được thêm

| Tier | Cột | Cơ chế |
|---|---|---|
| **T1** (+1) | `f19` | counter LOG10 thứ 7 — sinh `EVT_F19` |
| **T2** (+12) | `f41` `f42` | hoàn tất group `g1` (3/3) |
| | `f46` `f47` `f52` | mở rộng `g2` (3/10 → 6/10) |
| | `f53` `f54` `f57` `f58` `f59` `f62` | **`g3` — group MỚI HOÀN TOÀN** (6/10) |
| | `f65` | mở rộng `g4` (1/3 → 2/3) |
| **T3** (+6) | `f0` `f6` `f17` `f24` `f27` `f34` | pass-through, không cam kết semantic |

### 1.1 · ⚠️ Cảnh báo: tồn tại một tập 19 cột **giả** cũng cho ra đúng 55

`[MEASURED]` Trong 47 cột ngoài v1, có **đúng 19 cột** không tốn công solver nào:
12 cột anh em one-hot đã dựng sẵn, 6 cột trùng khít 100% với cột đã chọn
(`f32` `f33` = `f31`; `f71` `f77` = `f68`; `f69` `f72` = `f78`), và `f70` ≡ 1.0.

```
36 + 19 (bản sao)  =  55       ← 🚫 KHÔNG dùng
36 + 19 (tập trên) =  55       ← ✅ tập đã implement
```

Trùng hợp con số này rất dễ dẫn tới chọn nhầm. Tập giả nâng tỉ lệ pass Gate A
mà **không thêm một bit thông tin nào** — đúng loại con số vô nghĩa mà §12 cấm
khi nó cấm gộp Gate A với Gate A-T3.

`[MEASURED]` Tập 55 đã chọn **không có cặp trùng khít nào** (kiểm 1,485 cặp trên
926,669 dòng).

---

## 2. `[MEASURED]` Bằng chứng cho từng nhóm bổ sung

### 2.1 · `f19` — counter LOG10 thứ 7

| Chỉ số | `f18` (đã có) | **`f19`** | `f30` (đã có) |
|---|---|---|---|
| round-trip `round(log10(round(10^f)),6) == f` | 100.0000% | **100.0000%** | 100.0000% |
| trên test split | — | **181,669/181,669** | — |
| số mức | 144 | **156** | 30 |
| `max(n)` | 2,260 | **4,712** | 30 |

⇒ Cùng cơ chế với `f18`. Regime `LOG10`, so sánh **chính xác**, không dung sai.

**`f19` làm NHẸ bài toán, không làm nặng:**

```
capacity bound vi phạm       47.56%  →  39.54%      (n30 > n_rec+n5+n11+n18[+n19])
implied unexplained events    2.39M  →   1.94M
tỉ lệ event không giải thích  14.1%  →   10.9%
```

Theo §18.3 của `RECONSTRUCTION_CONSTRAINT_MODEL.md`, tỉ lệ thấp hơn nghĩa là
reconstruction **mang nhiều thông tin hơn**. Giá phải trả: +1,384,621 event
(+9.5% khối lượng Track A).

### 2.2 · `g3` — source attribute chưa từng được khai báo

`[FACT]` `ERD.md:157` ghi *"`synthetic_segment_g1` … `g7` — **7** biến"*.
`[MEASURED]` Thực tế có **8** group one-hot thật:

| Group | Cột | levels | `sum == 1` train | `sum == 1` test | Trạng thái v2 |
|---|---|---|---|---|---|
| `g1` | `f40` `f41` `f42` | 3 | ✅ | ✅ | chọn **3/3** |
| `g2` | `f43`–`f52` | 10 | ✅ | ✅ | chọn 6/10 |
| **`g3`** | **`f53`–`f62`** | **10** | ✅ | ✅ | **MỚI**, chọn 6/10 |
| `g4` | `f63` `f64` `f65` | 3 | ✅ | ⚠️ **VỠ** | chọn 2/3 |
| `g5` | `f66` `f67` | 2 | ✅ | ✅ | không dùng |
| `g6` | `f68` `f78` | 2 | ✅ | ✅ | chọn 1/2 |
| `g7` | `f73` `f74` | 2 | ✅ | ✅ | không dùng |
| `g8` | `f75` `f76` | 2 | ✅ | ✅ | không dùng |

Ngoài ra: `f70` ≡ 1.0 (hằng số), và 4 cột trùng khít `f69` `f71` `f72` `f77`.
ERD chỉ ghi `f68≡f71≡f77` và `f70`; nó **bỏ sót** `f69≡f72≡f78` và đếm sai số group.

### 2.3 · 🔴 `g4` vỡ bất biến one-hot trên test split

`[MEASURED]`

```
train:  sum(f63,f64,f65) == 1   ở  926,669 / 926,669   (100.0000%)
test:   sum(f63,f64,f65) == 0   ở        3 / 181,669   (0.0017%)
```

Attribute thật có **mức baseline toàn-0** không xuất hiện trong train.

**Hệ quả:**

1. `RECONSTRUCTION_SPEC.md` §8.2(b) khẳng định *"Domain đóng: map fit trên train
   phủ trọn test… Luật C-5 sẽ **không** bị kích hoạt oan"* — **sai với `g4`**.
2. `encoding.OneHotEncoding.decode` và `track_a_batch.decode_attributes` sẽ
   **fail cứng** `hot_count=0` trên 3 dòng đó.
3. `split_allowed: train` nên chưa nổ ra.

```
🚫 KHÔNG mở scope sang test split trước khi xử lý mức thứ tư của g4.
```

Đây là bằng chứng độc lập rằng **domain closure không phải tính chất phổ quát**
của dataset này — nó đã được kiểm cho `f37`/`f38`/`f79`–`f82`, không phải cho mọi group.

### 2.4 · Trùng lặp chưa được ghi nhận ở đâu

`[MEASURED]` 100% trên **cả hai** split:

```
f31 ≡ f32 ≡ f33          f6 ≡ f15          f14 ≡ f39
f68 ≡ f71 ≡ f77          f78 ≡ f69 ≡ f72   f70 ≡ 1.0
```

Câu hỏi ngỏ `semantic_signal_count` (§2.6) nên tính thêm các cặp này bằng đúng
functional-dependency test mà §2.3 yêu cầu, thay vì correlation.

### 2.5 · Pipeline chọn feature đã dedup — và dedup đúng

`[MEASURED]` Suy từ trường `thu_tu_dua_vao_mo_hinh` của bảng importance: model
input có 69 cột (index 0..68), tức 14 cột đã bị loại khỏi 83. Các khoảng trống
index rơi đúng vào `f15`, `f32`, `f33`, `f39` — **chính 4 cột đo được là bản sao
100%**. `[ASSUMPTION]` phần còn lại (`f66`/`f67`, `f69`–`f78`) bị loại theo cùng
nguyên tắc khử trùng.

---

## 3. `f0` `f27` `f34` — vì sao đặt ở T3

`ERD.md` xếp `f0` `f7` `f27` `f34` vào tier **`T1|T2`** — tức là **chính tier
cũng là assumption**.

| Cột | card | miền | Đặt ở |
|---|---|---|---|
| `f0` | 6 | `{0..5}` nguyên | T3 |
| `f27` | 32 | `[0,100]` nguyên | T3 |
| `f34` | 31 | `[0,100]` nguyên | T3 |

Pass-through là lựa chọn **không cam kết semantic gì cả**. Nâng lên T1 (counter)
hay T2 (attribute encoding) đòi hỏi bằng chứng hiện chưa có. `[UNKNOWN]`.

---

## 4. Solver — thay greedy bằng constructive argmin

### 4.1 · Vấn đề của bản cũ

`[FACT]` `engine.solve` (P1→P2→P3→P3b→P4, đúng §4.4a) **chỉ chạy ở demo và
unit test** với `window_days ≤ 6`. Đường dữ liệu thật gọi
`track_a_batch.construct_h1_candidate` — một greedy round-robin **bỏ qua P1, P2,
P3** rồi tự dựng `SolveOutcome(status="SOLVED", pool_size=1, objective_summary=…)`.

`[MEASURED]` Kích thước không gian mà exhaustive enumeration phải duyệt, mỗi target:

| quantile | 36-scope | 55-scope |
|---|---|---|
| median | 10^9.5 | 10^10.4 |
| p90 | 10^18.6 | 10^20.1 |
| max | 10^126.8 | 10^166.0 |
| tỉ lệ ≤ 10^6 | 20.0% | 19.3% |

⇒ Exhaustive **không bao giờ** chạy được ở quy mô thật. Nhưng bài toán **không
cần search**.

### 4.2 · Dạng đóng của argmin

`src/lzd_pipeline/reconstruction/constructive.py` chứng minh (docstring module):

```
F      = {d1,d2} ∩ [0,W)          ngày recency TRONG cửa sổ
n_out  = |{d1,d2}| − |F|          ngày recency NGOÀI cửa sổ
C      = n5 + n11 + n18 [+ n19]
k      = n30

unexplained_min = max(0, k − |F| − C)
T_in            = C + |F| + unexplained_min
sessions_min    = n_out + max(k, ceil(T_in / SESSION_CAPACITY))
```

Cả hai cận đều **đạt được** bằng một phép dựng `O(n30)`, và `solve_h1` **assert**
kết quả bằng đúng tuple này — lệch thì nổ ngay tại chỗ, không âm thầm vào manifest.

### 4.3 · Seed vẫn tuân thủ §4.4a-1

Cả hai thành phần objective phụ thuộc **duy nhất** vào `(k, |F|, n_out, C)` —
không phụ thuộc chọn **những ngày nào**. ⇒ mọi cách chọn `k − |F|` ngày đều cho
**cùng** giá trị objective, tức đều nằm trong argmin ⇒ seed đang chọn một phần tử
**trong** pool, đúng như §4.4a-1 yêu cầu.

### 4.4 · `[MEASURED]` Kết quả trên 200,000 target thật

| | greedy cũ | constructive mới |
|---|---|---|
| **Ngoài argmin** | **10,765 (5.38%)** | **0 (0.00%)** |
| Session thừa | 42,906 (TB 3.99/target lệch) | 0 |

**Phân bố ngày active** — greedy cũ luôn lấy ngày `0,1,2,…` nên mọi user có một
khối liền kề ở mép gần nhất của cửa sổ (bệnh lý §4.4 nói seed sinh ra để tránh,
Gate F để bắt):

```
greedy cũ    : d-0 ≈ 100%,  d-29 ≈ 0%
constructive : d-0 … d-29 mỗi ngày 3.30%–3.37%,  max/min = 1.019
```

### 4.5 · `pool_size` báo `0`, không phải `1`

Solver constructive **không liệt kê pool** nên không biết kích thước thật.
Báo `1` là nói *"nghiệm là duy nhất"* — một khẳng định chưa chứng minh.
Báo `0` = *"không liệt kê"*.

---

## 5. Kiểm chứng

### 5.1 · Cross-validation với exhaustive enumeration

`tests/reconstruction/test_constructive.py` đối chiếu nghiệm constructive với
`engine.optimal_pool(engine.feasible_candidates(...))` trên 7 fixture nhỏ —
**nguồn sự thật độc lập**, không phải so đầu ra của một hàm với chính nó.

```
test_nghiem_dung_ra_nam_trong_optimal_pool     7/7 pass
test_can_duoi_chung_minh_bang_dung_argmin_that 7/7 pass
test_moi_seed_deu_cho_nghiem_toi_uu            7/7 pass  (12 seed mỗi fixture)
```

### 5.2 · Gate A trên dữ liệu thật, qua chính SQL dbt

```
$ python -m lzd_pipeline.reconstruction.track_a_batch --limit 5000 --verify-limit 200

  selected_feature_set_id : fs_2026_08_v2
  selected_feature_count  : 55
  processed_rows          : 5000
  solved_rows             : 5000
  quarantined_rows        : 0
  raw_events              : 95937
  gate_a_passed           : 200/200
  gate_a_t3_passed        : 200/200
```

### 5.3 · Suite

```
245 passed        (trước: 210 — thêm 30 test constructive + 5 test contract v2)
```

---

## 6. Version bump — §11

Đổi output mà không bump = hai kết quả khác nhau mang **cùng** fingerprint
⇒ Gate G mất hiệu lực. Các trường đã bump trong `config/reconstruction/runtime.yml`:

| Trường | v1 → v2 | Lý do |
|---|---|---|
| `constraint_model_version` | `constraints_v1` → `constraints_v2` | thêm ràng buộc counter `f19` |
| `encoding_version` | `encoding_2026_08_v1` → `_v2` | thêm one-hot layout của `g3` |
| `selection_policy_version` | `seeded_optimal_pool_v1` → `seeded_day_choice_v1` | cùng seed → witness **khác** |
| `solver_version` | `prototype_v1` → `constructive_argmin_v1` | greedy → constructive |
| `objective_version` | `objective_v1` **giữ nguyên** | định nghĩa objective §4.2b không đổi |

---

## 7. INVARIANT 1 chuyển từ code sang artifact

```
trước:  if len(fs.columns) != 36:  raise      # hard-code trong feature_set.py
nay:    expected_column_count: 55             # trong CHÍNH fs_2026_08_v2.yaml
```

Con số nằm trong code làm artifact không tự mô tả được scope của nó, và khi cần
mở scope thì cách dễ nhất để vượt assert là **xoá assert**. Nằm trong yaml thì
đổi scope **bắt buộc** hiện ra ở git diff của hợp đồng, kèm một `id` mới.

Thêm một bất biến mới: **mọi cột của group phải được khai báo** — hoặc `selected`
hoặc `intermediate_only`. Quên một mức ở cả hai chỗ sẽ làm SQL dựng thiếu mức và
bất biến one-hot vỡ âm thầm.

---

## 8. Việc chưa làm

| # | Hạng mục | Ghi chú |
|---|---|---|
| **1** | **Mức thứ tư của `g4`** | §2.3. Chặn việc mở scope sang test split |
| **2** | Gate B–G ở đường batch thật | Hiện `track_a_batch` chỉ chạy Gate A + A-T3. Gate B–G chỉ có trong `e2e.run_end_to_end` (demo) |
| **3** | Gate F đúng nghĩa §12.3 | Hiện chỉ kiểm `0 < len(events) < 100_000`, không phải sanity phân bố |
| **4** | `encoding_version` vs map fit theo `--limit` | `fit_value_maps` fit trên chính subset của run; hai run khác `--limit` gán `level_id` khác nhau dưới cùng `encoding_version` |
| **5** | Cập nhật 14 tài liệu còn ghi scope 36 | `RECONSTRUCTION_SPEC.md` §1.1, `RECONSTRUCTION_CONTRACT.md`, `FEATURE_DICTIONARY.md`, `ERD.md` (số group), `README.md`, … |
| **6** | `H2` branch trên dữ liệu thật | `track_a_batch` vẫn chỉ hỗ trợ `semantic_branch=H1` |

> 🚫 Không hạng mục nào ở trên được coi là đã xong. Chúng là nợ kỹ thuật đã biết,
> ghi ra để không bị nhầm thành đã giải quyết.

---

## Liên quan

`RECONSTRUCTION_SPEC.md` · `RECONSTRUCTION_CONSTRAINT_MODEL.md` ·
`RECONSTRUCTION_CONTRACT.md` · `FEATURE_DICTIONARY.md` · `ERD.md`
