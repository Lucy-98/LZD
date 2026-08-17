# Reconstruction Spec — Solver Contract, Objective, Hash, Provenance

> **Trạng thái: SPEC + implementation. Contract là source of truth.**
>
> Tài liệu này formalize những thứ §22 yêu cầu và trước đây còn mơ hồ:
> solver I/O contract · solver objective + tie-break · canonical target hash ·
> provenance lineage · semantic identification result.
>
> ⚠️ Mục có dấu **`⬜ CẦN QUYẾT`** là chỗ chưa đủ căn cứ — **không được implement**
> cho tới khi chốt.
>
> Nhãn: `[FACT]` `[MEASURED]` `[ASSUMPTION]` `[UNKNOWN]` `[PLAN]`

---

> ## ⚠️ SCOPE HIỆN HÀNH LÀ **55 CỘT** (`fs_2026_08_v2`)
>
> Tài liệu này được viết cho scope **36 cột** (`fs_2026_08_v1`) và đã được đồng bộ
> lên **55 cột**. Mọi lập luận về contract, objective, hash, provenance **giữ
> nguyên hiệu lực** — vì `v2` là **superset chặt** của `v1`, không cột nào bị bỏ.
>
> | | v1 | **v2 (hiện hành)** |
> |---|---|---|
> | T1 · T2 · T3 | 6 · 12 · 18 = 36 | **7 · 24 · 24 = 55** |
> | Gate A surface (T1+T2) | 18 | **31** |
> | Source attribute | 7 | **8** |
>
> Bằng chứng đo đạc, danh sách 19 cột được thêm, việc thay solver greedy bằng
> constructive argmin, và nợ kỹ thuật còn lại: **`SCOPE_EXPANSION_55F.md`**.
>
> 🚫 Chỗ nào tài liệu này còn nói "36" mà **không** kèm nhãn `(v1)` thì đó là
> lỗi đồng bộ — sửa, đừng làm theo.

---

## 0. Phát biểu chuẩn — Track A vs Track B

> **Track A và Track B là hai pipeline độc lập về mục đích.**
>
> **Track A** là **one-time Feature-consistent Event Reconstruction**: lấy 55-feature
> target, tìm **một** historical event witness trước `reference_ts`, replay qua feature
> engine **thật** và kiểm tra reconstructed features khớp target.
>
> Sau khi trạng thái **T0 được xác nhận**, **Track B** mới sử dụng state đó làm
> **initial state** để sinh future synthetic events qua behaviour model, publish
> vào Kafka để test streaming → lake/MinIO → downstream state boundary.
>
> **Track B tuyệt đối không nhận `ReconstructionTarget`, không reverse feature, và
> không được dùng canonical target để điều khiển future event generation.**

| | Track A | Track B |
|---|---|---|
| Input | 55-feature target | `CustomerState(T0)` + behaviour model |
| Output | **một** event history `E*` | luồng event tương lai |
| Thời gian | `event_ts < reference_ts` | `event_ts ≥ reference_ts` |
| Tần suất | **một lần** / target / generation run | **liên tục** theo thời gian mô phỏng |
| Bản chất | deterministic reconstruction | behaviour simulation |
| **KHÔNG phải** | streaming · behaviour generator | reverse feature |

Chi tiết ranh giới bàn giao: §3.4.

---

## 1. Reconstruction scope — chốt cứng

```
83  =  FULL FEATURE SPACE          (f0..f82, dataset gốc)
55  =  RECONSTRUCTION TARGET SPACE (selected columns, fs_2026_08_v2)
??  =  SEMANTIC SIGNAL SPACE       ⬜ CẦN ĐO LẠI — xem §2
28  =  NGOÀI SCOPE                 (83 − 55)
```

### 1.1 · Danh sách 55 cột — nguồn duy nhất

> 🚫 Bảng này **không phải** source of truth. Source of truth duy nhất là
> `config/features/fs_2026_08_v2.yaml`. Bảng dưới là bản sao để đọc; nếu lệch
> nhau thì **artifact đúng**, tài liệu sai (đây chính là lỗi B5 mà §13.3 chống).

| Tier | n | Cột |
|---|---|---|
| **T1** | 7 | `f1` `f2` `f5` `f11` `f18` **`f19`** `f30` |
| **T2** | 24 | `f37` `f38` `f79` `f80` `f81` `f82` · `f40` `f41` `f42` · `f43` `f44` `f45` `f46` `f47` `f52` · `f53` `f54` `f57` `f58` `f59` `f62` · `f64` `f65` · `f68` |
| **T3** | 24 | `f0` `f3` `f4` `f6` `f8` `f9` `f10` `f12` `f13` `f16` `f17` `f20` `f21` `f22` `f23` `f24` `f25` `f26` `f27` `f28` `f29` `f31` `f34` `f35` |

`[MEASURED]` **v2 là superset chặt của v1** — cả 36 cột cũ đều còn, thêm đúng 19.
Chi tiết bằng chứng: `SCOPE_EXPANSION_55F.md` §1–§2.

### 1.2 · Ngoại lệ duy nhất cho phép vượt 55

```
Một cột ngoài 55 CHỈ được đưa vào contract nếu nó là DEPENDENCY BẮT BUỘC
để feature engine tính ra một trong 55 cột mục tiêu.
```

`[MEASURED]` Hiện tại **không có** ngoại lệ nào: 24 cột T2 cần **source group đầy đủ**
(§5), nhưng các cột anh em (`f48`–`f51`, `f55`, `f56`, `f60`, `f61`, `f63`, `f78`) là
**đầu ra trung gian**, không phải input. Chúng **không** vào target contract.

`[FACT]` Bất biến này nay được **thực thi bằng code**: `feature_set._validate` bắt
mọi cột của một source attribute phải nằm ở `selected` **hoặc** `intermediate_only`.
Quên một mức ở cả hai chỗ sẽ làm SQL dựng thiếu mức và bất biến one-hot vỡ âm thầm.

🚫 Mọi mở rộng scope khác đều vi phạm §21.

---

## 2. ⬜ CẦN QUYẾT — con số "32 semantic signals" chưa hợp lệ

> ⚠️ **Cả mục §2 này được viết trên nền scope 36 cột (v1).** Với scope 55 cột,
> con số nền đổi và có thêm bằng chứng mới — xem §2.7. Lập luận phương pháp
> (correlation là công cụ sai, phải dùng functional-dependency test) **không đổi**.

### 2.1 · Phương pháp cũ sai

Con số 32 được suy ra bằng: `36 (v1) − 1 (f25≡f23) − 3 (f80,f81,f82)`.
**Chưa test hết 630 cặp.** Đã test lại.

### 2.2 · `[MEASURED]` Kết quả test toàn bộ 630 cặp, `|corr| > 0.95`

| Cặp | \|corr\| | Đọc |
|---|---|---|
| `f23` `f25` | **0.999957** | đã biết — 1 tín hiệu |
| `f79` `f82` | 0.993692 | trong nhóm 515 mức |
| **`f9` `f16`** | **0.992705** | ★ **MỚI — chưa từng xác định** |
| `f1` `f2` | 0.980135 | **KHÔNG** phải trùng lặp (xem 2.4) |

### 2.3 · ★ Correlation là công cụ SAI để phát hiện encoding group

`[MEASURED]` Tương quan **bên trong** nhóm `f79`–`f82` — nhóm đã **chứng minh** là
**một** biến 515 mức (`card(tuple) = card(mỗi cột) = 515`, song ánh):

```
   79–80 = −0.4135      79–81 = +0.1882      79–82 = −0.9937
   80–81 = +0.7180      80–82 = +0.4717      81–82 = −0.1187
```

> **Bốn cột được xác định bởi CÙNG một biến latent, nhưng tương quan cặp trải từ
> −0.99 đến +0.72; ba trong sáu cặp dưới 0.5.**
>
> Một bộ lọc redundancy dựa trên correlation sẽ chỉ bắt được **1/6 cặp**, và sẽ giữ
> `f80`, `f81` như "tín hiệu độc lập" trong khi chúng **bị quyết định hoàn toàn** bởi
> cùng một biến.

**Hệ quả bắt buộc cho R2:**

```
❌ CẤM:  grouping bằng correlation / |corr| threshold
✅ ĐÚNG: functional-dependency test
         card(tuple(A,B)) == card(A) == card(B)   ⇒ song ánh, MỘT tín hiệu
         card(tuple(A,B)) == card(A)              ⇒ B là hàm của A
```

Đây chính là phép test đã tìm ra nhóm 515; correlation thì không.

### 2.4 · `f1`/`f2` — corr cao nhưng **hai** tín hiệu

`[MEASURED]` `corr = 0.98` vì `f1 = f2` ở 97.42% dòng. Nhưng 2.58% còn lại lệch nhau
trung bình ~123 ngày. `card(tuple) ≫ card(f1)` ⇒ **không** phải hàm của nhau.
⇒ Giữ **2** tín hiệu.

Đây là bằng chứng thứ hai rằng correlation không quyết định được.

### 2.5 · `f9`/`f16` — chưa giải quyết

`[MEASURED]`

```
f9 = f16 ở 684,207 / 926,669 dòng  (73.84%)
card(f9)  = 25,659      card(f16) = 44,314      card(tuple) = 158,669
range f9  = [0, 8.1696]  range f16 = [0, 8.1696]   ← MAX TRÙNG KHÍT
avg(f9 − f16) = +0.1034   sd = 0.2223
```

`card(tuple) ≫ card(f9)` ⇒ **không** phải hàm của nhau ⇒ **hai** tín hiệu theo test §2.3.
Nhưng `max` trùng khít tới 4 chữ số và bằng nhau 73.84% ⇒ nhiều khả năng **cùng đại
lượng nền, khác cửa sổ/độ mịn**. `[UNKNOWN]`.

> ⬜ **CẦN QUYẾT:** `f9`/`f16` là 1 hay 2 semantic signal? Cả hai đều T3 nên **không
> ảnh hưởng solver**, nhưng ảnh hưởng `selected_feature_set` và cách đọc importance ở R2.

### 2.6 · Kết luận (nền v1, 36 cột)

```
semantic_signal_count = ⬜ CHƯA CHỐT
    36 − 3 (f80,f81,f82 thuộc nhóm f79) = 33
    − 1 nếu f23/f25 gộp              = 32
    − 1 nếu f9/f16 gộp               = 31
```

🚫 **Không được ghi "32" như một con số đã chốt** cho tới khi R2 chạy full
functional-dependency test trên toàn bộ 630 cặp (không phải correlation).

### 2.7 · `[MEASURED]` Nền v2 (55 cột) — và bằng chứng mới

Số cột **không** bằng số tín hiệu. Ở scope 55, khoảng cách còn rộng hơn:

```
55 cột
 − 3   f80,f81,f82 cùng biến latent 515 với f79
 − 1   g1 được chọn ĐỦ 3 cột ⇒ sum(f40,f41,f42)==1 ⇒ 3 cột chỉ mang 2 bậc tự do
 − ?   f23/f25 (trùng 99.78%),  f9/f16 (bằng nhau 73.84%)   — vẫn ⬜ CHƯA CHỐT
```

**Ba trùng lặp 100% mới đo được** (cả train lẫn test), chưa từng ghi ở tài liệu nào:

```
f31 ≡ f32 ≡ f33          f6 ≡ f15          f14 ≡ f39
```

Trong 55 cột chỉ có `f31` và `f6` được chọn ⇒ **không** ảnh hưởng scope hiện tại,
nhưng nếu ai đó mở scope bằng cách thêm `f32`/`f33`/`f15`/`f39` thì đó là thêm
**0 tín hiệu**. Xem `SCOPE_EXPANSION_55F.md` §1.1 — có đúng một tập 19 cột bản sao
cũng cho ra con số 55.

`[MEASURED]` Trong chính 55 cột đã chọn: **không có cặp trùng khít nào**
(kiểm 1,485 cặp trên 926,669 dòng).

---

## 3. Solver I/O contract

### 3.1 · Input

```python
@dataclass(frozen=True)
class ReconstructionTarget:
    target_id:                str
    selected_feature_set_id:  str        # vd "fs_2026_08_v2"
    feature_version:          str
    reference_ts:             datetime   # KHÔNG BAO GIỜ now()
    split:                    Literal["train"]      # chỉ train
    values:                   Mapping[str, float]   # ĐÚNG 55 khoá
    target_hash:              str

@dataclass(frozen=True)
class SolverConfig:
    # semantic_branch = KỊCH BẢN VẬN HÀNH đang chạy, KHÔNG phải semantic truth
    semantic_branch:            Literal["H1", "H2"] | None
    # semantic_status = TRẠNG THÁI NHẬN THỨC (epistemic)
    semantic_status:            Literal["H1_SUPPORTED","H2_SUPPORTED","UNIDENTIFIED"]
    constraint_model_version:   str
    encoding_version:           str
    objective_version:          str   # ★ objective LÀ semantics — phải version
    selection_policy_version:   str   # ★ seed KHÔNG mô tả đủ policy
    solver_version:             str
    generation_seed:            int
    max_repair_rounds:          int
```

> ★ **Hai trường mới, đều bắt buộc:**
>
> | Trường | Vì sao thiếu nó là lỗi |
> |---|---|
> | `objective_version` | Objective (§4.1) là **semantics**, không phải chi tiết implementation. Đổi objective mà fingerprint không đổi ⇒ hai kết quả khác nhau mang **cùng** fingerprint ⇒ Gate G mất hiệu lực |
> | `selection_policy_version` | Solver là controlled stochastic (§4.4). `seed = 123` dưới `policy_v1` và `policy_v2` cho **nghiệm khác nhau**. Seed một mình không xác định được nghiệm |

**Bất biến bắt buộc trên kiểu:**

| # | Ràng buộc | Thực thi |
|---|---|---|
| I-1 | `ReconstructionTarget` **không có** field `label`/`is_treat` | kiểu dữ liệu, không phải quy ước |
| I-2 | `values` có **đúng 55 khoá** khớp `selected_feature_set_id` | validate lúc dựng |
| I-3 | `split == "train"` | validate lúc dựng |
| I-4 | `frozen=True` — target **bất biến** | ngôn ngữ |
| I-5 | Solver **không** nhận `behaviour_model` | chữ ký hàm |
| I-6 | Ràng buộc `status` ↔ `branch` — bảng dưới | validate lúc dựng |

**I-6 · Bất biến `semantic_status` ↔ `semantic_branch`:**

```
semantic_status == UNIDENTIFIED    →  semantic_branch ∈ {H1, H2, None}
semantic_status == H1_SUPPORTED    →  semantic_branch == H1     (bắt buộc)
semantic_status == H2_SUPPORTED    →  semantic_branch == H2     (bắt buộc)
```

> Hai trường **khác loại**: `status` là *nhận thức*, `branch` là *kịch bản đang chạy*.
> `branch == "H1"` **không bao giờ** có nghĩa `f30` là `active_days` — chỉ khi
> `status == H1_SUPPORTED` mới được nói evidence nghiêng về H1 (§10.1).

### 3.2 · Output

```python
@dataclass(frozen=True)
class ObjectiveSummary:
    unexplained_events:  int      # số FREE_EVENT (0 dưới nhánh H2)
    active_days:         int
    sessions:            int
    total_events:        int

@dataclass(frozen=True)
class ReconstructionResult:
    target_id:          str
    status:             Literal["SOLVED","REPAIRED","QUARANTINED"]
    events:             list[Event]          # rỗng nếu QUARANTINED
    reason_code:        str | None
    repair_rounds:      int
    objective_summary:  ObjectiveSummary | None   # ★ BẮT BUỘC khi SOLVED/REPAIRED
    provenance:         Provenance
    fingerprint:        str                  # §11
```

> ★ **`objective_summary` là bắt buộc.** Không có nó, câu hỏi *"vì sao witness này
> được chọn?"* không trả lời được, và không kiểm tra được solver có thật sự tối ưu
> đúng hay chỉ trả về nghiệm khả thi đầu tiên. `status = "SOLVED"` một mình **không**
> chứng minh objective đã chạy.

### 3.3 · Chữ ký

```python
def backfill_solver(
    target: ReconstructionTarget,
    config: SolverConfig,
) -> ReconstructionResult: ...
    # KHÔNG nhận behaviour_model
    # KHÔNG đọc CSV, KHÔNG đọc Redis, KHÔNG đọc model

def live_generator(
    state: CustomerState,
    behaviour_model: BehaviourModel,
    t_from: datetime,
    t_to: datetime,
) -> Iterator[Event]: ...
    # KHÔNG nhận ReconstructionTarget
```

### 3.4 · ★ `CustomerState` — thứ DUY NHẤT đi qua ranh giới Track A → Track B

> Ranh giới quyền truy cập:
>
> ```
> Track A  CÓ QUYỀN biết:       ReconstructionTarget
> Track B  KHÔNG CÓ QUYỀN biết: ReconstructionTarget
> ```

```python
@dataclass(frozen=True)
class CustomerState:
    """Trạng thái T0 đã được Track A xác nhận. Đầu vào DUY NHẤT của Track B.

    🚫 KHÔNG có field nào cho phép suy ngược ra target:
       không `values`, không `target_hash`, không `feature_payload_hash`,
       không `selected_feature_set_id`.
    """
    customer_id:        str
    as_of_ts:           datetime          # = reference_ts của Track A
    source_target_id:   str               # CHỈ để truy vết provenance
    attributes:         Mapping[str, int] # 7 source attribute → level_id
    counters:           Mapping[str, int] # counter đã giải mã (n5, n11, n18, n30…)
    last_event_ts:      datetime | None
    semantic_branch:    Literal["H1","H2"] | None
    semantic_status:    Literal["H1_SUPPORTED","H2_SUPPORTED","UNIDENTIFIED"]
    provenance:         Provenance
```

#### 3.4-1 · ★ `source_target_id` — **opaque lineage identifier**, KHÔNG phải lookup key

Trường này **được giữ** vì nó cần cho audit:

```
future event  →  state_id  →  reconstruction target  →  generation run
```

Nhưng nó là **lỗ hổng capability** nếu Track B được phép giải nó:

```
🚫 source_target_id  →  target repository  →  reconstruction_target  →  55 features
```

Khi đó **dù chữ ký hàm sạch**, Track B vẫn "nhìn trộm" được target.

```
✅ ĐƯỢC:  lưu ID để trace
🚫 CẤM:   dùng ID để ĐỌC nội dung target
```

> **Chữ ký hàm chỉ là interface boundary. Cái cần là CAPABILITY boundary** — xem INVARIANT 4
> (§14.1) và TEST-10 (§15).

#### 3.4a · Bàn giao — chỉ sau khi Gate A pass

```
   Track A  →  ReconstructionResult(status = SOLVED | REPAIRED)
                        │
                        │  Gate A + A-T3 + B + C PASS
                        ▼
              CustomerState(T0)   ← ĐÃ XÁC NHẬN
                        │
                        ▼
   Track B  →  live_generator(state, behaviour_model, t_from, t_to)
```

```
🚫 CẤM bàn giao khi status == QUARANTINED
🚫 CẤM bàn giao khi Gate A chưa pass — T0 chưa xác nhận thì mọi event
   tương lai sinh ra đều bám vào một trạng thái sai
```

#### 3.4b · Hai loại phụ thuộc — không được gộp

```
   DATA DEPENDENCY        ✅  Track B ← CustomerState(T0) ← Track A
   VALIDATION DEPENDENCY  ❌  Track A pass Gate A ⇏ Track B hợp lệ
```

Track A xác nhận **trạng thái T0 đúng**. Nó **không** nói gì về behaviour model của
Track B có hợp lý hay không — đó là bài toán khác, cần validation khác.

---

## 4. Solver objective + deterministic tie-break

> `[FACT]` Trước tài liệu này, objective **chưa được định nghĩa** — chỉ có thuật toán
> khả thi (P0–P7). Vì `Events → Features` là many-to-one, feasibility **không** xác
> định nghiệm duy nhất. §18 yêu cầu chốt trước khi code.

### 4.1 · Bốn pha — feasibility TÁCH KHỎI objective

> **Sửa lỗi:** bản trước xếp `EXACT FEATURE EQUALITY` làm mục 2 của objective. **Sai** —
> nó là **điều kiện khả thi cứng**, không phải thành phần mục tiêu. Gọi nó là objective
> mâu thuẫn với chính khẳng định *"feature equality không phải mục tiêu mềm"*.

```
┌─ PHA 1 · FEASIBILITY ────────────────────────────────────────┐
│   hard constraints H-1…H-12                                  │
│   EXACT feature equality (55 cột, regime §7)                 │
│   → không thoả ⇒ QUARANTINE. KHÔNG đánh đổi được.            │
└──────────────────────────┬───────────────────────────────────┘
                           ▼
┌─ PHA 2 · OPTIMIZATION (lexicographic) ───────────────────────┐
│   1. MIN unexplained events      (vô hiệu dưới nhánh H2)     │
│   2. MIN temporal complexity     (ngày active thừa, session) │
│   → cho ra CANDIDATE POOL = argmin                           │
└──────────────────────────┬───────────────────────────────────┘
                           ▼
┌─ PHA 3 · CANDIDATE SELECTION ────────────────────────────────┐
│   controlled stochastic, seeded PRNG  (§4.4, §4.4a)          │
│   → chọn MỘT phần tử trong candidate pool                    │
└──────────────────────────┬───────────────────────────────────┘
                           ▼
┌─ PHA 4 · OUTPUT ─────────────────────────────────────────────┐
│   canonical event ordering  (§4.3)                           │
│   → CHỈ sắp thứ tự, KHÔNG chọn nghiệm                        │
└──────────────────────────────────────────────────────────────┘
```

Pha 2 dùng **thứ tự từ điển**, không dùng tổng có trọng số — trọng số cho phép đánh đổi
giữa hai mục tiêu, ở đây không muốn thế.

### 4.2 · Định nghĩa CHÍNH XÁC của objective — cả hai mục đều phụ thuộc nhánh

> ⚠️ **Sửa lỗi trong chính bản trước.** Tôi đã viết *"dưới H2 mục 1 vô hiệu"* và ngụ ý
> mục 2 thì không đổi. **Sai** — mục 2 cũng phụ thuộc nhánh, vì lý do dưới đây.

#### 4.2a · `active_days` BỊ GHIM dưới H1 ⇒ không thể là thành phần objective

Dưới H1, `f30 = active_days_in_last_30d`. Feature equality (Pha 1) ép:

```
   active_days(E)  ==  n30   CHÍNH XÁC
```

⇒ `active_days` **không còn bậc tự do nào** ⇒ `active_days(E) − required` **luôn = 0**
⇒ đưa nó vào objective là **vô nghĩa** (metric hằng số).

Dưới H2, `f30` đếm thứ khác ⇒ `active_days` **tự do** ⇒ **là** thành phần objective hợp lệ.

#### 4.2b · Định nghĩa tuple — bất biến, không mơ hồ thứ tự

```
objective(E, branch) =                                    # lexicographic tuple
    branch == "H1":  ( unexplained_events(E),
                       sessions(E) )
    branch == "H2":  ( active_days(E),
                       sessions(E) )
```

| Thành phần | H1 | H2 | Vì sao |
|---|---|---|---|
| `unexplained_events` | ✅ tự do | ❌ **không tồn tại** | H2 không có `FREE_EVENT` |
| `active_days` | ❌ **bị ghim** bởi feature equality | ✅ tự do | §4.2a |
| `sessions` | ✅ tự do | ✅ tự do | không feature nào trong 55 cột ghim số session |

> **Thứ tự trong tuple là một phần của contract.** Không được đảo. Hai implementer
> cùng nói *"min temporal complexity"* nhưng một người tối ưu `(active_days, sessions)`
> còn người kia `(sessions, active_days)` sẽ sinh ra **witness khác nhau**.

#### 4.2c · Tuyệt đối / thặng dư — chốt: dùng **tuyệt đối**

```
✅ sessions(E)                          — giá trị tuyệt đối
🚫 sessions(E) − required_sessions      — KHÔNG dùng
```

Lý do: `required_sessions` **không được định nghĩa bởi bất kỳ ràng buộc nào** (không
feature nào trong 55 cột ghim số session). Một "thặng dư so với 0" chính là giá trị
tuyệt đối ⇒ thêm khái niệm `required_*` chỉ tạo thêm chỗ để hiểu sai.

Với `unexplained_events` cũng vậy: cận dưới lý thuyết là
`[MEASURED] ≈ 2.39M / 23.1M ≈ 10%` toàn cục, nhưng cận dưới **cho từng target** phụ
thuộc nghiệm ⇒ so tuyệt đối là đúng, objective tự khắc ép về cận dưới.

#### 4.2d · Vì sao cần `MIN unexplained_events` (H1)

Solver được phép sinh `FREE_EVENT` để thoả `n30`. Không có mục tiêu này, solver có thể
sinh **tuỳ ý** nhiều free event và vẫn pass GATE A.

> `SolverConfig.semantic_branch` phải được truyền vào objective, **không hard-code**.
> Objective là hàm hai biến `(E, branch)`, không phải hàm một biến.

### 4.3 · Canonical event sequence key — **một** định nghĩa duy nhất

> Bản trước có 3 bước (a/b/c) trong đó b và c **dư thừa** — chúng đã bị bao hàm bởi a.
> Nhiều cách hiểu tie-break ⇒ nhiều implementation khác nhau. Gộp lại:

```
event_sequence_key(e) = (e.event_ts, e.event_type, e.event_id)

canonical_sequence(E) = sorted(E, key=event_sequence_key)
```

**Vai trò:** chỉ **sắp thứ tự output** (Pha 4). **Không** chọn nghiệm — xem §4.4.

### 4.3a · ⚠ `event_id` phải dựa trên CANONICAL LOGICAL IDENTITY, không phải thứ tự duyệt

Bản trước viết `uuid5(ns, f"{target_id}:{track}:{reason}:{idx}")` với `idx` không định
nghĩa. Nếu `idx` là **chỉ số lặp của search** thì chạy song song / đổi thứ tự duyệt sẽ
cho `event_id` khác ⇒ phá bất biến §4.5.

> ⚠️ **Bản trước vẫn còn dependency ẩn:** *"hoà ⇒ theo thứ tự dựng"*. "Thứ tự dựng"
> **vẫn** phụ thuộc implementation nếu candidate builder chạy song song hoặc đổi
> traversal. Phải đóng bằng cách khác.

#### 4.3a-1 · Candidate là CẤU TRÚC, không phải danh sách event

Gốc rễ: nếu candidate được biểu diễn bằng `list[Event]` thì thứ tự trong list là một
sự thật ngẫu nhiên của implementation. Bỏ hẳn cách biểu diễn đó:

```
Candidate = multiset of  (gen_reason, day_offset, count)
```

Event **không** được lưu trong candidate — chúng được **materialize** bằng một hàm
thuần, tất định:

```
materialize(candidate, seed, reference_ts) -> list[Event]

    với mỗi (gen_reason r, day_offset d, count c) trong candidate:
        với sub_index i = 0 … c−1:
            candidate_slot_id = (r, d, i)          ← CANONICAL, không traversal
            event_ts          = intra_day_ts(seed, r, d, i)   ← hàm thuần
```

`candidate_slot_id` được xác định **hoàn toàn bởi cấu trúc candidate**, trước và độc
lập với mọi vòng lặp.

#### 4.3a-2 · `occurrence` dẫn xuất từ `candidate_slot_id`

```
occurrence_key(e) = (e.gen_reason, e.day_offset, e.sub_index)   # = candidate_slot_id

occurrence = thứ hạng của occurrence_key trong TẬP event cùng gen_reason,
             sắp theo (day_offset, sub_index) tăng dần

event_id   = uuid5(namespace,
                   f"{target_id}:{track}:{gen_reason}:{occurrence}")
```

| Ràng buộc | Được đảm bảo bởi |
|---|---|
| `occurrence` **không** phụ thuộc thứ tự loop | dẫn xuất từ `candidate_slot_id` (§4.3a-1) |
| `occurrence` **không** phụ thuộc số worker / partition | như trên |
| `occurrence` **không** phụ thuộc `event_ts` | dùng `(day_offset, sub_index)`, không dùng ts |
| **Không** dùng `event_id` để tính `occurrence` | tránh vòng lặp định nghĩa với §4.3 |
| `occurrence` tính **SAU** Pha 3 | candidate đã chốt |

Chuỗi phụ thuộc **một chiều**, không vòng:

```
candidate structure → candidate_slot_id → occurrence → event_id → canonical ordering
```

🚫 **Cấm** mọi biến thể tạo vòng:

```
🚫 event_id → occurrence → event_id
🚫 worker traversal order → occurrence
🚫 event_ts → occurrence → event_ts    (intra_day_ts đã phụ thuộc sub_index)
```

### 4.4 · ⬜ ĐÃ QUYẾT — solver là **controlled stochastic**, không phải deterministic thuần

Hai lựa chọn:

| | Input | Vấn đề |
|---|---|---|
| **Deterministic thuần** | `(target, config)` — bỏ seed | ❌ xem dưới |
| **Controlled stochastic** | `(target, config, seed)` | ✅ **chọn cái này** |

**Vì sao deterministic thuần KHÔNG dùng được:**

Solver có điểm chọn **không** bị ràng buộc quyết định — cụ thể: chọn
`n30 − |D_forced|` ngày active trong 29 slot còn trống, và đặt event `n5`/`n11`/`n18`
vào ngày nào trong số ngày đã active. Objective §4.1 mục 4 (`min temporal complexity`)
thu hẹp nhưng **không** xác định duy nhất — rất nhiều lựa chọn cho cùng một số đếm.

Nếu dùng §4.3 làm **cơ chế chọn nghiệm**, lexicographic-min trên `event_ts` sẽ luôn
đẩy ngày active về **sớm nhất có thể** ⇒ **mọi user** có hoạt động dồn vào cùng vài
ngày đầu cửa sổ ⇒ phân bố temporal suy biến ⇒ **Gate F FAIL**.

⇒ `generation_seed` là **load-bearing**, không phải trang trí.

**Hệ quả — §4.3 bị thu hẹp phạm vi:**

```
❌ SAI:  canonical_sequence dùng để CHỌN nghiệm trong không gian nghiệm
✅ ĐÚNG: với seed cố định, lựa chọn ĐÃ được quyết định;
         canonical_sequence chỉ đảm bảo THỨ TỰ ĐẦU RA ổn định
         (stable output ordering, không phải solution selection)
```

### 4.4a · Pipeline đầy đủ — MỘT thứ tự duy nhất, không đảo được

> Đây là hợp nhất của §4.1 (bốn pha) và bước gán `event_id` (§4.3a). Implementer làm
> theo diagram này.

```
   ┌─ P1 ─────────────────────────────────────────────┐
   │  generate feasible candidates                    │
   │  enforce hard constraints H-1…H-12               │
   │  enforce EXACT feature equality (55 cột, §7)     │
   └───────────────────────┬──────────────────────────┘
                           ▼
   ┌─ P2 ─────────────────────────────────────────────┐
   │  optimize objective (lexicographic)              │
   │     1. min unexplained events                    │
   │     2. min temporal complexity                   │
   │  ⇒ OPTIMAL POOL = argmin        (§4.4b)          │
   └───────────────────────┬──────────────────────────┘
                           ▼
   ┌─ P3 ─────────────────────────────────────────────┐
   │  generation_seed → deterministic PRNG            │
   │     seeded theo hash(target_id, seed, track)     │
   │  ⇒ chọn MỘT nghiệm TRONG optimal pool            │
   └───────────────────────┬──────────────────────────┘
                           ▼
   ┌─ P3b ────────────────────────────────────────────┐
   │  logical_slot assignment  (§4.3a)                │
   │     occurrence = rank trong từng gen_reason      │
   │  → event_id = uuid5(target_id, track,            │
   │                     gen_reason, occurrence)      │
   └───────────────────────┬──────────────────────────┘
                           ▼
   ┌─ P4 ─────────────────────────────────────────────┐
   │  canonical event ordering  (§4.3)                │
   │     sort by (event_ts, event_type, event_id)     │
   │  → CHỈ sắp thứ tự, KHÔNG chọn nghiệm             │
   └───────────────────────┬──────────────────────────┘
                           ▼
                        output
```

```
   objective         quyết định CHẤT LƯỢNG nghiệm
   seed              chọn MỘT nghiệm trong số các nghiệm ĐÃ NGANG NHAU
   sequence key      chỉ sắp THỨ TỰ OUTPUT
```

### 4.4a-1 · ★ INVARIANT QUAN TRỌNG NHẤT CỦA PROTOTYPE

```
   Seed CHỈ được phép tác động SAU khi đã có optimal pool.
```

```
✅ ĐÚNG:  P1 → P2 → optimal pool → P3 (seed) → P3b → P4

🚫 CẤM:   seed → random candidate → check objective
          → seed đang quyết định CẢ CHẤT LƯỢNG nghiệm,
            objective trở thành trang trí, solver = random feasible search
```

Đây là invariant **dễ vi phạm nhất** khi implement, vì "seed rồi kiểm tra" là cách viết
tự nhiên hơn với một constraint search. Prototype phải có test riêng cho nó
(§13.0, hạng mục `candidate-pool construction`).

**Semantics:**

```
   cùng target + cùng config + cùng seed   →  CÙNG event witness
   khác seed                               →  witness KHÁC nhưng VẪN HỢP LỆ
```

Dòng thứ hai là **đúng theo thiết kế**, không phải lỗi: `F` ứng với một **lớp tương
đương** nghiệm (§7.1 `PIPELINE_ARCHITECTURE.md`), seed chọn một phần tử trong lớp đó.

> ⚠️ `DETERMINISTIC ≠ DESIRABLE`. Deterministic thuần cho ra nghiệm canonical nhưng
> **bệnh lý** về phân bố. Controlled stochastic **là một phần của specification**,
> không phải workaround cho solver yếu.

### 4.4b · ★ "EQUIVALENT" — định nghĩa chính xác của candidate pool

```
CandidatePool(target, config) =
    { E : E thoả PHA 1 (hard constraints ∧ exact feature equality)
        ∧ Objective(E) == lexicographic_min over all feasible E' }
```

Tương đương nghĩa là **bằng nhau trên cả ba**:

```
   ✅ cùng hard constraints thoả
   ✅ cùng exact feature equality
   ✅ cùng GIÁ TRỊ OBJECTIVE (Pha 2, mục 1 và 2)
```

🚫 **KHÔNG** phải chỉ *"cùng feature target"*.

```
🚫 CẤM:  dùng seed để chọn TRƯỚC khi tối ưu xong
         → objective mất hết ý nghĩa, solver trở thành random feasible search
✅ ĐÚNG: tối ưu trước → thu được argmin pool → seed chọn trong pool
```

Nếu `|CandidatePool| == 1` thì seed **không** ảnh hưởng gì — đó là trường hợp bình
thường, không phải lỗi.

### 4.4c · SPEC SEMANTICS ≠ IMPLEMENTATION STRATEGY

> `CandidatePool = argmin over all feasible E` là **định nghĩa toán học**.
> Nó **không** bắt implementation phải materialize toàn bộ pool trong bộ nhớ.

```
SPEC SEMANTICS
    CandidatePool = TOÀN BỘ nghiệm đạt argmin

IMPLEMENTATION OBLIGATION
    solver phải CHỨNG MINH nghiệm đã chọn ∈ argmin
    — không bắt buộc liệt kê hết pool
```

| Mức | Chiến lược cho phép | Trạng thái |
|---|---|---|
| **Toy solver** | **exhaustive enumeration** trên không gian nhỏ ⇒ kiểm được objective + selection đúng theo nghĩa đen | ✅ `engine.feasible_candidates` — **chỉ dùng trong test** |
| **Solver thật** | branch-and-bound · constraint programming · MILP · **dạng đóng có chứng minh cận** | ✅ `constructive.solve_h1` |

> Exhaustive enumeration **phải** tồn tại — đó là cách duy nhất để test `TEST-01`
> (P2 tìm đúng optimal pool) mà không phải tin vào chính solver đang test. Nhưng
> nó **không** phải đường chạy sản xuất.

#### 4.4c-1 · `[MEASURED]` Vì sao exhaustive KHÔNG thể là đường chạy thật

Kích thước không gian mà `feasible_candidates` phải duyệt, đo trên 926,669 target thật:

| quantile | scope 36 | scope 55 |
|---|---|---|
| median | 10^9.5 | 10^10.4 |
| p90 | 10^18.6 | 10^20.1 |
| max | 10^126.8 | 10^166.0 |
| tỉ lệ ≤ 10^6 | 20.0% | 19.3% |

⇒ Exhaustive **đã** bất khả thi ngay ở scope 36. Chỉ dùng được với `window_days ≤ 6`.

#### 4.4c-2 · Bài toán H1 có **dạng đóng** — không cần search

```
F      = {d1,d2} ∩ [0,W)          n_out  = |{d1,d2}| − |F|
C      = n5 + n11 + n18 [+ n19]   k      = n30

unexplained_min = max(0, k − |F| − C)
T_in            = C + |F| + unexplained_min
sessions_min    = n_out + max(k, ceil(T_in / SESSION_CAPACITY))
```

Cả hai cận đều **đạt được** bằng một phép dựng `O(n30)`. Chứng minh đầy đủ nằm ở
docstring của `src/lzd_pipeline/reconstruction/constructive.py`; `solve_h1` **assert**
kết quả bằng đúng tuple này nên lệch argmin sẽ nổ ngay tại chỗ.

`[MEASURED]` Trên 200,000 target thật: **0/200,000** nghiệm nằm ngoài argmin.
Bản greedy round-robin trước đó: **10,765/200,000 (5.38%)** nằm ngoài argmin,
trung bình dư 3.99 session mỗi target bị lệch.

#### 4.4c-3 · Seed vẫn tuân thủ §4.4a-1

Cả hai thành phần objective phụ thuộc **duy nhất** vào `(k, |F|, n_out, C)` — không
phụ thuộc **chọn ngày nào**. ⇒ mọi cách chọn `k − |F|` ngày đều cho **cùng** giá trị
objective ⇒ đều nằm trong argmin ⇒ seed chọn một phần tử **trong** pool.

`[MEASURED]` Hệ quả trên phân bố temporal (Gate F sanity), 200,000 target:

```
greedy cũ    : luôn lấy ngày 0,1,2,…  ⇒  d-0 ≈ 100%,  d-29 ≈ 0%
constructive : d-0 … d-29 mỗi ngày 3.30%–3.37%,  max/min = 1.019
```

### 4.5 · Bất biến tất định

```
   cùng (target, config, generation_seed)  ⇒  cùng events  ⇒  cùng fingerprint
```

Seed dẫn xuất theo `hash(target_id, generation_seed, track)` ⇒ mỗi user độc lập ⇒
song song hoá **không** đổi kết quả, thứ tự xử lý **không** ảnh hưởng.

---

## 5. Group-level reconstruction — 8 source attributes

| Source attribute | Mức | Cột đã chọn | Cột anh em (dựng, không xuất) |
|---|---|---|---|
| `synthetic_segment_g1` | 3 | `f40` `f41` `f42` | — (**chọn đủ cả group**) |
| `synthetic_segment_g2` | 10 | `f43` `f44` `f45` `f46` `f47` `f52` | `f48`–`f51` |
| **`synthetic_segment_g3`** | **10** | `f53` `f54` `f57` `f58` `f59` `f62` | `f55` `f56` `f60` `f61` |
| `synthetic_segment_g4` | 3 | `f64` `f65` | `f63` |
| `synthetic_segment_g6` | 2 | `f68` | `f78` |
| `synthetic_category_515` | 515 | `f79` `f80` `f81` `f82` | — |
| `synthetic_attr_64` | 64 | `f37` | — |
| `synthetic_attr_241` | 241 | `f38` | — |

**Assertion bắt buộc:** `sum(f43..f52) == 1` cho mọi dòng — one-hot invariant của
group **đầy đủ**, không phải của 6 cột đã chọn. Tương tự cho `g1`, `g3`, `g4`, `g6`.

### 5.1 · `[MEASURED]` Có **8** group one-hot, không phải 7

`[FACT]` `ERD.md:157` ghi *"`g1` … `g7` — **7** biến"*. Đo lại trên toàn bộ
926,669 dòng train: có **8** group thoả `sum == 1` tuyệt đối, cộng một cột hằng số
và bốn cột trùng khít.

| Group | Cột | Trong scope v2? |
|---|---|---|
| `g1` `g2` `g3` `g4` `g6` | xem bảng trên | ✅ |
| `g5` = `f66` `f67` · `g7` = `f73` `f74` · `g8` = `f75` `f76` | — | ❌ không được chọn |
| `f70` ≡ 1.0 (hằng số) · `f69` `f71` `f72` `f77` (trùng khít) | — | ❌ |

`g3` **chưa từng được khai báo** ở bất kỳ tài liệu nào trước v2. ERD cũng bỏ sót
quan hệ `f69 ≡ f72 ≡ f78`.

### 5.2 · 🔴 `g4` vỡ bất biến one-hot trên test split

`[MEASURED]`

```
train:  sum(f63,f64,f65) == 1   ở  926,669 / 926,669   (100.0000%)
test:   sum(f63,f64,f65) == 0   ở        3 / 181,669   (0.0017%)
```

⇒ Attribute thật có **mức baseline toàn-0** không xuất hiện trong train.
⇒ Khẳng định "domain đóng" ở §8.2(b) **không đúng với `g4`**.
⇒ `OneHotEncoding.decode` sẽ fail cứng `hot_count=0` trên 3 dòng đó.

`split_allowed: train` nên chưa nổ ra.

```
🚫 CẤM mở scope sang test split trước khi xử lý mức thứ tư của g4.
```

---

## 6. Canonical target hash

> `[FACT]` Bản trước chỉ viết *"sha256 của vector feature đã chuẩn hoá"* — **"chuẩn hoá"
> không được định nghĩa**. Với float64 điều này là mơ hồ nghiêm trọng.

### 6.1 · Canonical form — payload là primitive dùng chung

```
float_repr(x) = format(x, ".17g")      # round-trip đúng cho IEEE-754 double
                                        # NaN → "nan", ±Inf → "inf"/"-inf"

# ── PRIMITIVE: chỉ payload, không identity ──────────────────────────────
canonical_feature_payload(target) =
    join("\x1e", [ f"{col}={float_repr(values[col])}"
                   for col in sorted(selected_columns) ])

feature_payload_hash = sha256(canonical_feature_payload).hexdigest()

# ── RECORD: identity BỌC NGOÀI payload ──────────────────────────────────
canonical_target_record(target) =
    join("\x1f", [ selected_feature_set_id,
                   feature_version,
                   reference_ts.isoformat(),
                   canonical_feature_payload(target) ])

target_hash = sha256(canonical_target_record).hexdigest()
```

**Bất biến lồng nhau:**

```
target_hash          = H( set_id, feature_version, reference_ts,
                          canonical_feature_payload )
feature_payload_hash = H( canonical_feature_payload )
```

⇒ `canonical_feature_payload` được định nghĩa **một lần**, dùng cho **cả hai** hash.
Không có hai cách chuẩn hoá payload ⇒ không thể lệch nhau.

Quy tắc: cột **sắp theo tên**, không theo thứ tự file · dấu phân cách không xuất hiện
trong dữ liệu · `.17g` đảm bảo round-trip float64.

### 6.1a · Hai hash, hai mục đích khác nhau

> `target_hash` hash **cả identity lẫn payload** ⇒ nó **không** thuần là "hash của
> feature vector". Gọi đúng tên:

```
target_hash          = tamper-evident hash của CANONICAL TARGET RECORD
                       (= identity: set_id + feature_version + reference_ts
                        + payload: 55 giá trị)

feature_payload_hash = sha256 CHỈ trên phần payload
                       join("\x1e", [f"{col}={float_repr(v)}" for col in sorted(cols)])
```

| Hash | Đổi khi | Dùng để |
|---|---|---|
| `target_hash` | payload **hoặc** identity đổi | phát hiện sửa target |
| `feature_payload_hash` | **chỉ** payload đổi | so hai target khác `reference_ts` nhưng cùng feature; dedup; audit |

`feature_payload_hash` không bắt buộc cho solver, nhưng làm audit sạch hơn nhiều —
nó tách "target đã được tái phát hành" khỏi "giá trị feature đã đổi".

### 6.2 · ★ Hash **KHÔNG** dùng làm phép so feature equality

`[MEASURED]` Regime `LN` (`f5`, `f11`): round-trip `ln(round(exp(f)))` chỉ khớp **chính
xác 93.05% / 93.42%**; khớp 100% ở dung sai tương đối `1e-15`.

⇒ `reconstructed_hash` sẽ **KHÁC** `target_hash` ở ~7% user **dù reconstruction hoàn
toàn đúng**.

| Hash | Dùng để | **Không** dùng để |
|---|---|---|
| `target_hash` | Phát hiện **target bị sửa** (tamper-evidence) | so feature equality |
| `reconstructed_hash` | Truy vết / audit / dedup kết quả | quyết định GATE A |

```
🚫 CẤM:  GATE A pass ⟺ reconstructed_hash == target_hash
✅ ĐÚNG: GATE A theo luật so sánh TỪNG REGIME (§7)
```

### 6.3 · Tamper-evidence

```
Mỗi lần đọc target:  assert sha256(canonical_string(target)) == target.target_hash
Sai  ⇒  FAIL CỨNG  (có người sửa target)
```

---

## 7. Comparison regimes — nhắc lại, là điều kiện của GATE A

| Regime | Cột | Decode | Encode | Compare | Bằng chứng |
|---|---|---|---|---|---|
| `LOG10` | `f18` **`f19`** `f30` | `round(10^f)` | `round(log10(n), 6)` | `==` | round-trip **100.0000%** |
| `LN` | `f5` `f11` | `round(exp(f))` | `ln(n)` float64 | `rel < 1e-15` | `==` chỉ 93.05%/93.42% |
| `REC` | `f1` `f2` | `f` (nguyên) | `date_diff` | `==` | miền `[0,365]` nguyên |
| `CAT` | 24 cột T2 | tra `encoding_map` | tra ngược | `==` | song ánh |
| `PASS` | 24 cột T3 | — | copy | `==` | không phải reconstruction |

`[MEASURED]` `f19` vào regime `LOG10` với bằng chứng cùng loại `f18`/`f30`:
round-trip đúng **926,669/926,669** dòng train **và 181,669/181,669** dòng test,
156 mức, `max(n) = 4712`.

🚫 **Cấm** nới `1e-15` của regime `LN`. Fail ở `1e-15` ⇒ **solver sai**, không phải
dung sai sai.

---

## 8. Encoding contract — bijection phải là invariant, không phải sample test

### 8.1 · Property bắt buộc, kiểm trên **toàn** domain

```
∀ x ∈ observed_domain :  encode(decode(x)) == x
∀ s ∈ level_domain    :  decode(encode(s)) == s
```

### 8.2 · `[MEASURED]` Trạng thái — **DATASET property**, chưa phải implementation property

> ⚠️ **Sửa cách dùng từ.** Bản trước ghi *"đã CHỨNG MINH"* — **sai**. Thứ đã kiểm là:
>
> ```
> ✅ DATASET STRUCTURE SUPPORTS A BIJECTION      ← đã có bằng chứng
> ✅ PROTOTYPE ENCODING IMPLEMENTATION IS BIJECTIVE    ← full-domain property test đã có
> ⬜ PRODUCTION MAPS MATERIALIZED FROM FULL DATASET
> ```
>
> Không thể chứng minh tính chất của một implementation chưa được viết. Hai trạng
> thái này phải tách — đúng theo chính nguyên tắc L1≠L4 của project.

Ba phép kiểm chạy trên **toàn bộ** train (926,669) + test (181,669):

**(a) Float-key integrity** — `card(DISTINCT value)` vs `card(DISTINCT CAST(value AS VARCHAR))`:

| Cột | distinct float | distinct text | `−0.0` | Kết luận |
|---|---|---|---|---|
| `f37` | 64 | **64** | 0 | ✅ khoá float an toàn |
| `f38` | 241 | **241** | 0 | ✅ |
| `f79`–`f82` | 515 | **515** | 0 | ✅ |

⇒ Không có va chạm float→text, không có `−0.0` ⇒ **map keyed by value là hàm hợp lệ**.

**(b) Domain closure** — giá trị có trong test nhưng không có trong train:

| Cột | train | test | **unseen in test** |
|---|---|---|---|
| `f37` | 64 | 64 | **0** ✅ |
| `f38` | 241 | 241 | **0** ✅ |
| `f79`–`f82` | 515 | 504 | **0** ✅ |

⇒ Domain **đóng**: map fit trên train phủ trọn test. Luật C-5 (giá trị lạ ⇒ FAIL cứng)
sẽ **không** bị kích hoạt oan.

**(c) Bijection bất biến qua split:**

```
card(tuple f79..f82)   train = 515 = card(mỗi cột)     ✅ song ánh
                        test  = 504 = card(mỗi cột)     ✅ song ánh
```

⇒ Tính song ánh **không phải artifact của train** — nó giữ nguyên trên split độc lập.
Đây cũng là câu trả lời một phần cho **X5** (`PIPELINE_ARCHITECTURE.md` §13.4).

| Encoding | **DATASET property** | **IMPLEMENTATION property** |
|---|---|---|
| `encoding_map_515` | ✅ verified (song ánh, 2 split độc lập) | ✅ prototype + property test; ⬜ full production map |
| `encoding_map_64` | ✅ verified (float-key sạch ⇒ value↔level 1-1) | ✅ prototype + property test; ⬜ full production map |
| `encoding_map_241` | ✅ verified | ✅ prototype + property test; ⬜ full production map |

⇒ **Không còn evidence cho thấy dataset structure cản trở việc xây dựng bijection.**
⇒ **Vẫn còn verification gate** — implementation phải được test (§8.4).

> Câu trên cố ý **không** viết *"không còn rủi ro toán học"*. Ta đã kiểm dataset, chưa
> kiểm mọi assumption của production materialization chưa tồn tại. Đây đúng là distinction
> L1/L4 mà project canh giữ.

### 8.3 · ⚠ Bẫy: `f37` và `f38` **dùng chung alphabet**

`[MEASURED]` `alphabet(f37) ⊂ alphabet(f38)` hoàn toàn (64/64 giá trị), nhưng
`card(tuple(f37,f38)) = 1133 > 241` ⇒ **hai biến độc lập**.

```
🚫 CẤM:  một hàm encode() dùng chung cho cả f37 và f38
         → giá trị v thuộc cả hai alphabet ⇒ decode(v) MẬP MỜ
✅ ĐÚNG: encode/decode PHẢI có khoá thuộc tính:
         decode(attr="synthetic_attr_64",  value=v)
         decode(attr="synthetic_attr_241", value=v)
```

### 8.4 · Test bắt buộc (đã implement ở `test_contract_slice1.py`)

```
test_encoding_bijection_full_domain(attr):
    for x in SELECT DISTINCT <col> FROM reconstruction_target:
        assert encode(attr, decode(attr, x)) == x
    assert card(level_domain(attr)) == card(observed_domain(attr))
```

---

## 9. Provenance lineage

### 9.1 · Schema đầy đủ

```
provenance
├── source_type              REAL | RECONSTRUCTED | SYNTHETIC
├── generation_run_id        UUID của run sinh ra bản ghi
├── parent_run_id            run trước trong chuỗi (null nếu gốc)
├── root_generation_id       ★ run gốc của cả chuỗi — chống contamination xuyên đời
├── parent_target_id         ★ target sinh ra nó (Track A)
├── parent_entity_id         customer sinh ra nó
├── parent_feature_version   version feature store lúc sinh
├── ancestor_model_ids       ★ list ML model đã tham gia sinh ra chuỗi này
├── behaviour_policy_version ★ version của policy sinh (khi KHÔNG dùng ML model)
└── created_at
```

Bốn trường ★ là bổ sung so với bản trước.

> **Vì sao cần `behaviour_policy_version`:** Track B **không nhất thiết** dùng ML model.
> Một rule-based simulator là behaviour model hợp lệ và sẽ có `ancestor_model_ids` **rỗng
> một cách chính đáng**. Yêu cầu thật không phải "phải có model id" mà là:
>
> ```
> source_type = SYNTHETIC  ⇒  CƠ CHẾ SINH phải truy vết được
>     ML-generated    →  ancestor_model_ids ≠ ∅
>     rule-generated  →  behaviour_policy_version ≠ NULL
> ```

### 9.2 · Bất biến

| # | Assertion |
|---|---|
| P-1 | Đồ thị `parent_run_id` **không có chu trình** |
| P-2 | `root_generation_id` bằng nhau cho mọi bản ghi trong cùng cây |
| P-3 | `source_type = RECONSTRUCTED` ⇒ `parent_target_id IS NOT NULL` |
| P-4 | `source_type = SYNTHETIC` ⇒ **cơ chế sinh truy vết được**: `ancestor_model_ids ≠ ∅` **hoặc** `behaviour_policy_version IS NOT NULL` |
| P-5 | Dataset train production: kiểm trên **ĐỒ THỊ**, không phải trên dòng — xem §9.3 |

### 9.3 · Luật chống contamination — kiểm trên GRAPH, không phải trên ROW

> Kiểm mức dòng (`source_type = REAL`) **không đủ**: một dòng `REAL` vẫn có thể có
> tổ tiên `SYNTHETIC` qua nhiều đời.

```
ProductionTrainSet hợp lệ  ⟺
    ∀ row r :
        r.source_type == REAL
      ∧ ∄ ancestor a của r (theo parent_run_id / root_generation_id)
            với a.source_type != REAL
```

Tận dụng `root_generation_id`: mọi bản ghi trong cùng cây có **cùng** `root_generation_id`
(P-2) ⇒ kiểm ở gốc là đủ, không phải duyệt toàn bộ chuỗi:

```
   root_generation_id ∈ REAL_LINEAGE_SET   ⇒  hợp lệ
   ngược lại                                ⇒  loại khỏi tập train production
```

```
🚫 Model production KHÔNG được train trên dữ liệu có tổ tiên synthetic —
   kể cả GIÁN TIẾP qua nhiều đời.
```

Chỉ có `track=A/B` **không** phát hiện được contamination đời thứ ba trở đi.

---

## 10. Semantic identification result — kiểu dữ liệu

```python
@dataclass(frozen=True)
class SemanticIdentification:
    feature:        str                      # "f30"
    status:         Literal["H1_SUPPORTED","H2_SUPPORTED","UNIDENTIFIED"]
    branch_in_use:  Literal["H1","H2"] | None
    rationale:      str
    evidence_refs:  list[str]                # X1..X5
    decided_at:     datetime
```

### 10.1 · Luật diễn giải — **bắt buộc**

| `status` | Được phép nói | 🚫 Cấm nói |
|---|---|---|
| `UNIDENTIFIED` + `branch_in_use="H1"` | *"H1 là kịch bản vận hành tốt hơn"* | *"f30 nghĩa là active_days"* |
| `H1_SUPPORTED` | *"evidence nghiêng về H1"* | *"đã chứng minh semantic"* |

```
🚫 Model-Level Validation của H1 tốt hơn H2
   ⇒ CHỈ kết luận:  "H1 is a better operational scenario"
   ⇒ KHÔNG kết luận: "f30 definitely means H1"
```

### 10.2 · Trường bắt buộc mang theo

Mọi `ReconstructionResult`, mọi feature row sinh ra từ nó, và mọi artifact model
train trên nó **phải** mang `semantic_status` + `semantic_branch`.
Không mang ⇒ sau này không phân biệt được kịch bản với sự thật.

---

## 11. Reproducibility fingerprint

```
reproducibility_fingerprint = sha256( join("\x1f", [
    target_hash,
    selected_feature_set_id,
    feature_spec_version,
    constraint_model_version,
    encoding_version,
    objective_version,            # ★ objective là semantics
    selection_policy_version,     # ★ seed không mô tả đủ policy
    solver_version,
    semantic_branch,
    reference_ts.isoformat(),
    str(generation_seed),
]) )
```

> Thiếu `objective_version` ⇒ đổi objective mà fingerprint **không đổi** ⇒ hai kết quả
> khác nhau mang cùng fingerprint ⇒ **Gate G mất hiệu lực**.
> Thiếu `selection_policy_version` ⇒ cùng seed dưới hai policy cho nghiệm khác nhau
> nhưng fingerprint trùng ⇒ **cùng lỗi**.

**Gate G:** cùng `reproducibility_fingerprint` ⇒ **cùng** `event_hash`, **cùng**
`reconstructed_feature_hash`, **cùng** kết quả gate.

Lưu trong `biz.generation_run`.

---

## 12. Gates — bảy cổng

| Gate | Kiểm | Phạm vi |
|---|---|---|
| **A** · Feature equality | reconstructed == target theo regime §7 | **31** cột T1+T2 |
| **A-T3** · Passthrough integrity | copy đúng, không sai lệch | **24** cột T3 |
| **B** · Temporal validity | §12.1 — **disjoint theo THỜI GIAN**, không phải theo user | mọi event |
| **C** · Constraint validity | hard constraints H-1…H-12 | mọi user |
| **D** · Provenance | §9.2 P-1…P-5, không chu trình | mọi bản ghi |
| **E** · No leakage | `label`/`is_treat`/test data vắng mặt | solver + biz.* |
| **F** · Distribution sanity | §12.3 — **SANITY ONLY**, kết quả `PASS \| WARN \| FAIL` | phân bố |
| **G** · Reproducibility | §11 | mỗi run |

> **Gate A và Gate A-T3 báo cáo tỉ lệ RIÊNG.** Gộp chúng làm tỉ lệ pass luôn ≥ 50%
> nhờ copy — con số vô nghĩa. Tổng target vẫn là **55 cột**.
>
> **Gate F chỉ là sanity, không phải L3 (behavioral plausibility).**

### 12.1 · Gate B — "disjoint" nghĩa là gì

> Bản trước ghi `Track A ∩ Track B = ∅` — **mơ hồ**. Cùng một customer **đương nhiên**
> có cả event Track A lẫn Track B; giao theo `customer_id` là chuyện bình thường.
> Thứ **không được** giao nhau là **miền thời gian**.

**Contract chính — dạng quantifier, đúng cả khi tập rỗng:**

```
Với MỌI customer u:

    ∀ a ∈ TrackA(u) :  a.event_ts       <  reference_ts(u)
    ∀ a ∈ TrackA(u) :  a.observation_ts ≤  reference_ts(u)     ← availability
    ∀ b ∈ TrackB(u) :  b.event_ts       ≥  reference_ts(u)
```

> ⚠️ **Chỉ hai dòng trên là contract.** Chúng đúng **vacuously** khi tập rỗng — điều
> này quan trọng vì prototype chắc chắn có customer chỉ có một phía
> (`TrackA(u) = ∅` hoặc `TrackB(u) = ∅`).

**Hệ quả dẫn xuất — CHỈ áp dụng khi cả hai tập khác rỗng:**

```
   TrackA(u) ≠ ∅  ∧  TrackB(u) ≠ ∅
   ⇒  max(TrackA(u).event_ts)  <  reference_ts(u)  ≤  min(TrackB(u).event_ts)
```

```
🚫 CẤM: implement Gate B bằng max/min mà không kiểm tập rỗng trước
        → max() trên tập rỗng ném exception hoặc trả sentinel ⇒ false FAIL
```

| Được phép | Không được phép |
|---|---|
| `TrackA(u) ≠ ∅` **và** `TrackB(u) ≠ ∅` | một event Track A có `event_ts ≥ reference_ts` |
| Cùng `customer_id` ở cả hai track | một event Track B có `event_ts < reference_ts` |
| Session của A kết thúc, B **mở session mới** | một **session** bắc qua `reference_ts` |

**Availability semantics** (dòng `observation_ts`): một event xảy ra trước
`reference_ts` nhưng hệ thống chỉ nhìn thấy **sau** đó thì **không** có mặt lúc tính
feature. `[FACT]` `stg_app_events` đã có `ingested_at` — đây là cột dùng cho
`observation_ts`.

### 12.2 · Session boundary — invariant, không phải kiểm `session_id`

> Kiểm *"cùng `session_id` ⇒ fail"* là **sai** và quá thô: nó không nói được session
> nào hợp lệ. Phát biểu đúng là một invariant trên **toàn bộ** event của session:

```
∀ session s:
    ( ∀ e ∈ s :  e.event_ts <  reference_ts )
  ∨ ( ∀ e ∈ s :  e.event_ts >= reference_ts )
```

**Không session nào được cắt qua boundary.**

| Tình huống | Hợp lệ? |
|---|---|
| Session A kết thúc lúc `reference_ts − 1s`, session B mới mở lúc `reference_ts + 5s` | ✅ |
| Session A kết thúc **đúng** `reference_ts`, B mở session **mới** | ✅ |
| Một session có event ở cả hai phía `reference_ts` | ❌ **FAIL** |

Cách này sạch hơn kiểm `session_id` vì nó phát biểu **tính chất cần giữ**, không phải
một heuristic phát hiện vi phạm.

### 12.3 · Gate F — SANITY ONLY, ba trạng thái

```
   Gate F  ∈  { PASS, WARN, FAIL }
```

🚫 **Gate F KHÔNG được biến thành hard solver constraint** khi chưa có evidence về
ngưỡng. Nó **quan sát**, không **ràng buộc**.

| Metric | Đo gì | Ngưỡng |
|---|---|---|
| `events_per_user` | mean / p50 / p99 / max | ⬜ chưa chốt |
| `session_duration` | phân bố | ⬜ chưa chốt |
| `inter_event_gap` | phân bố | ⬜ chưa chốt |
| **boundary artifact** | xem dưới | ⬜ chưa chốt |

**Boundary artifact — ứng viên cần theo dõi (mới ghi nhận, chưa thành ngưỡng):**

```
· gai tại n30 = 30                  (censoring — có thể là thật, xem PIPELINE §13.3)
· event dồn cụm gần reference_ts    (dấu hiệu solver thiên lệch về biên)
· session length sụp về 0           (session một-event hàng loạt)
· ngày active dồn về đầu cửa sổ     (triệu chứng lexicographic-min, §4.4)
```

Dòng cuối chính là thứ §4.4 dùng để bác deterministic thuần ⇒ Gate F là **cơ chế phát
hiện** cho vấn đề đó, nên nó phải chạy ngay từ prototype dù chưa có ngưỡng.

`WARN` không chặn pipeline; `FAIL` chỉ được kích hoạt khi ngưỡng đã chốt bằng evidence
từ pilot.

### 12.4 · ★ Gate F KHÔNG được phản hồi ngược vào objective

```
🚫 CẤM:  Gate F WARN  →  solver TỰ ĐỘNG tăng penalty temporal / sửa objective
```

Nếu làm vậy, Gate F từ **observer** biến thành **hidden objective** — và objective thật
sự đang chạy sẽ khác objective đã version hoá ⇒ `objective_version` nói dối ⇒ Gate G
mất hiệu lực.

**Quy trình bắt buộc — có con người ở giữa:**

```
   prototype chạy
        ▼
   observe Gate F
        ▼
   phân tích (người)
        ▼
   nếu cần đổi → BUMP objective_version, sửa spec §4.2b
        ▼
   rerun
```

🚫 **Không** có đường tắt `F fail → solver tự sửa objective`.

---

## 13. Điều kiện được bắt đầu implement solver

> Chia **hai** danh sách. Bản trước gộp chung, làm hai vấn đề **metadata** chặn nhầm
> solver core.

### 13.0 · ★ Ba mức, không phải hai — tránh vòng lặp chết

> **Vòng lặp phải tránh:** *phải implement solver → mới biết objective có vấn đề →
> nhưng không được implement → không test được objective.*

| Mức | Được làm gì | Điều kiện |
|---|---|---|
| **Proof-of-contract + E2E prototype** | unit/property/constraint test + dbt SQL runner + Track B dry-run | ✅ **ĐÃ XONG** — 174 reconstruction test, TEST-01…10 pass |
| **Pilot** | chạy 10k target, đo Gate A–G | cần **toàn bộ** §13.1 |
| **Production** | full dataset, ghi lake trước downstream state, publish Track B | cần §13.1 + kết quả pilot |

#### 13.0a · ★ Pilot phải giữ Track A là ONE-SHOT

```
✅ ĐÚNG
   10k TRAIN target
        ▼
   Track A  — one-time reconstruction, MỖI target đúng MỘT lần
        ▼
   Gate A–G
        ▼
   T0 CustomerState
        ▼
   Track B  — streaming simulation, chạy liên tục

🚫 SAI
   10k target  →  vừa reverse vừa live-generate liên tục
```

Track A là **one-shot initialization/reconstruction**. Nó **không** phải nguồn sinh
event liên tục (§0). Chạy nó lặp lại trong vòng streaming là hiểu sai bản chất bài toán
— và sẽ làm `reference_ts` trôi, phá bất biến temporal của Gate B.

**Giới hạn tuyệt đối của prototype:**

```
🚫 không ghi production data          🚫 không chạy full dataset
🚫 không ghi Kafka/MinIO/downstream state   ✅ Track B chạy dry-run
🚫 không ghi đè artifact trong models/uplift_voucher
```

### 13.1 · GATE cho PILOT & PRODUCTION

```
[x] §8.2 bijection property CHỨNG MINH cho cả 3 encoding   ← đã đo, 2 split
[x] §4.1 objective (thứ tự từ điển) định nghĩa xong
[x] §4.3 canonical event_sequence_key — MỘT định nghĩa
[x] §4.4 quyết định controlled stochastic (seed load-bearing)
[x] §4.4a tách seed=chọn-nghiệm khỏi ordering=thứ-tự-output
[x] §6.1 canonical payload primitive + §6.1a hai hash lồng nhau
[x] §12.1–12.2 Gate B: disjoint theo THỜI GIAN + session boundary invariant
[x] §3.1 objective_version + selection_policy_version vào SolverConfig
[x] §11 fingerprint gồm cả hai version mới
[x] selected_feature_set_id artifact          → config/features/fs_2026_08_v2.yaml
[x] §8.4 bijection test viết thành CODE       → assert_bijective / assert_round_trip
[x] §6.3 tamper-evidence check implement      → ReconstructionTarget.verify()
[x] §10 semantic identification RECORD tồn tại → feature_semantics_2026_08.yaml
        status=UNIDENTIFIED · branch_in_use=H1 · evidence_refs · X1–X5 NOT_RUN
[x] §9 provenance PERSIST vào biz.*  → sql/postgres/02_biz_reconstruction.sql
                                      + persistence.py (P-1/P-2/P-5 trên đồ thị)
```

> ✅ **Toàn bộ §13.1 đã đóng.** Pilot 10k không còn bị chặn bởi contract gate.
> Việc phân định H1/H2 (R3, X1–X5) là **epistemic path độc lập** — theo §13.6 nó có
> thể vĩnh viễn `UNIDENTIFIED` mà pipeline vẫn hợp lệ.

> ⚠️ **Hai ô cuối khác bản chất — đừng gộp:**
>
> | | Prototype | Pilot |
> |---|---|---|
> | **provenance** | ✅ contract in-memory (`Provenance`, P-3/P-4 có test) | ⬜ persist vào Postgres `biz.*` |
> | **semantic_status** | ✅ record tồn tại, `UNIDENTIFIED` | ⬜ chạy X1–X5 để **phân định** |
>
> `UNIDENTIFIED` **là giá trị hợp lệ** (§13.1a). Ô §10 đóng khi **record tồn tại**,
> không phải khi H1/H2 được phân định. Việc phân định là công việc **riêng**, và
> theo §13.6 nó có thể vĩnh viễn không xong mà hệ thống vẫn chạy được.

**E2E prototype đã implement** (`src/lzd_pipeline/reconstruction/`):

| Module | Đóng gate nào |
|---|---|
| `feature_set.py` + `fs_2026_08_v2.yaml` | 55-column scope thành artifact máy đọc được; INVARIANT 1 (`expected_column_count`) |
| `canonical.py` | §6.1 payload primitive · §6.1a hai hash lồng nhau · §11 fingerprint |
| `target.py` | I-1…I-4, I-6 · §6.3 tamper-evidence |
| `encoding.py` | §8.1 bijection full-domain · §8.3 khoá thuộc tính |

Test đã pass: **TEST-05** (mọi version vào fingerprint) · **TEST-09** (55-column scope)
· **TEST-08 phần hash** (`semantic_status` không có trong chữ ký `fingerprint()`).
Engine, forward SQL runner, Gate A-F, handoff T0 và Track B rule-based đều đã có test.
Phần còn lại là materialize map/full target và publish production ra MinIO/Kafka.

### 13.1a · `semantic_status` — schema tách khỏi giá trị

```
Schema tồn tại  ⇒  KHÔNG chặn solver
    status = "UNIDENTIFIED",  branch_in_use = "H1" | "H2" | null
    tạo được NGAY, R3 chỉ update record

Giá trị đã chốt  ⇒  chỉ chặn việc ĐÓNG semantic branch
```

Solver **đã** được thiết kế để chấp nhận `UNIDENTIFIED` (§3.1, §10.1) ⇒ về mặt kỹ
thuật nó không bị chặn. Cái bị chặn là **kết luận** về `f30`.

### 13.2 · FEATURE-SEMANTIC AUDIT — **không** chặn solver core

Solver nhận 55 cột qua `selected_feature_set_id` và **không** đọc
`semantic_signal_count`. Hai mục dưới ảnh hưởng **R2 metadata / cách đọc importance**,
không ảnh hưởng việc sinh nghiệm.

```
[ ] §2 semantic_signal_count chốt bằng functional-dependency test (KHÔNG correlation)
[ ] §2.5 f9/f16 — 1 hay 2 signal   (cả hai đều T3 ⇒ solver không chạm)
```

> ⚠️ **Một ngoại lệ đã được xử lý:** grouping *có* ràng buộc solver ở đúng một chỗ —
> `f79`–`f82` là **một** biến latent nên solver phải dựng **một** thuộc tính, không
> phải bốn. Điều đó đã nằm trong §5 (bảng 7 source attribute) và **độc lập** với con
> số `semantic_signal_count`.

### 13.3 · HAI artifact riêng — để `f9`/`f16` không chặn solver

```yaml
# ── fs_2026_08_v2.yaml ── SOLVER ĐỌC CÁI NÀY ────────────────────
id: fs_2026_08_v2
expected_column_count: 55        # ★ INVARIANT 1 do ARTIFACT chốt, không phải code
columns:
  T1: [f1, f2, f5, f11, f18, f19, f30]
  T2: [f37, f38, f79, f80, f81, f82, f40, f41, f42,
       f43, f44, f45, f46, f47, f52, f53, f54, f57, f58, f59, f62,
       f64, f65, f68]
  T3: [f0, f3, f4, f6, f8, f9, f10, f12, f13, f16, f17, f20, f21, f22,
       f23, f24, f25, f26, f27, f28, f29, f31, f34, f35]
source_attributes:
  synthetic_category_515: {levels: 515, outputs: [f79, f80, f81, f82]}
  synthetic_attr_64:      {levels:  64, outputs: [f37]}
  synthetic_attr_241:     {levels: 241, outputs: [f38]}
  synthetic_segment_g1:   {levels:   3, outputs: [f40, f41, f42]}
  synthetic_segment_g2:   {levels:  10, outputs: [f43, "...", f52]}
  synthetic_segment_g3:   {levels:  10, outputs: [f53, "...", f62]}
  synthetic_segment_g4:   {levels:   3, outputs: [f63, f64, f65]}
  synthetic_segment_g6:   {levels:   2, outputs: [f68, f78]}
```

> ★ **`expected_column_count` nằm trong artifact, không phải trong code.**
> Trước v2, INVARIANT 1 là `if len(fs.columns) != 36: raise` ngay trong
> `feature_set.py`. Con số nằm trong code làm artifact không tự mô tả được scope,
> và khi cần mở scope thì cách dễ nhất để vượt assert là **xoá assert**. Nằm trong
> yaml thì đổi scope **bắt buộc** hiện ra ở git diff của hợp đồng, kèm một `id` mới.

```yaml
# ── feature_semantics_2026_08.yaml ── SOLVER KHÔNG ĐỌC ──────────
semantic_signal_count: UNRESOLVED
open_questions:
  - {pair: [f9, f16],  status: UNRESOLVED}
  - {pair: [f23, f25], status: LIKELY_ONE_SIGNAL}
```

> **Tách hai artifact là điều kiện để §13.2 thật sự không chặn §13.1.** Nếu nhét
> `semantic_signal_count` vào `fs_2026_08_v2`, thì `f9`/`f16` chưa giải quyết sẽ chặn
> luôn artifact mà solver cần — đúng thứ ta vừa quyết định là **không** nên xảy ra.

✅ §13.1 đã đóng ở mức contract/prototype. Pilot vẫn cần chạy và báo cáo gate trên mẫu
thật; §13.2 chưa xong **không** chặn solver, nhưng chặn kết luận về feature importance
ở R2.

---

## 14. Solver implementation status

```
ĐÃ ĐÓNG BĂNG (frozen)
─────────────────────
✅ 55-feature scope                    ✅ canonical target hash
✅ T1/T2/T3 membership                 ✅ feature payload hash
✅ f79–f82 source-attribute contract   ✅ provenance model
✅ reconstruction target contract      ✅ semantic branch/status model
✅ solver I/O                          ✅ temporal boundary + session invariant
✅ objective semantics                 ✅ Gate A–G
✅ controlled stochastic justified     ✅ objective/selection policy versioning
✅ output ordering semantics
✅ selected_feature_set_id artifact     ✅ encoding full-domain tests
✅ provenance schema migration          ✅ tamper-evidence implementation

CÒN THIẾU (production gates, không phải solver blocker)
──────────────────────────────────────────────────────
✅ host-side full train target loader/materialization (`track_a_batch`)
⬜ scalable solver path cho tail lớn
⬜ MinIO events_v2 writer
⬜ Kafka v2 Track B publisher/consumer contract
✅ pilot 10k Gate A/A-T3 sample report (50/50 pass); Gate G production report còn lại
⬜ semantic identification result (R3) — epistemic, không chặn `UNIDENTIFIED`

KHÔNG CÒN LÀ SOLVER BLOCKER
───────────────────────────
   B1  semantic_signal_count   →  FEATURE-SEMANTIC AUDIT (§13.2)
   B2  f9/f16                  →  FEATURE-SEMANTIC AUDIT (§13.2)
```

### 14.1 · Ba invariant cuối cùng cho AI cowork

```
INVARIANT 1
    55 columns = hard reconstruction scope.
    KHÔNG tự mở lên 83.

INVARIANT 2
    UNIDENTIFIED ≠ UNKNOWN implementation.
    Solver ĐƯỢC chạy scenario H1/H2 — nhưng semantic CHƯA được chứng minh.
    semantic_branch = kịch bản.  semantic_status = nhận thức.  KHÔNG lẫn.

INVARIANT 3
    Seed CHỈ tác động SAU khi đã có optimal pool (§4.4a-1).
    Objective quyết định CHẤT LƯỢNG · seed chọn TRONG SỐ ĐÃ NGANG NHAU ·
    canonical ordering CHỈ sắp thứ tự witness đã chọn.
    🚫 seed → random candidate → check objective

INVARIANT 4                                              ★ CAPABILITY BOUNDARY
    Track B MUST NOT possess any capability that can resolve
    source_target_id → reconstruction target contents.

    Chữ ký hàm sạch là CHƯA ĐỦ. Track B không được có ĐƯỜNG NÀO —
    kể cả bắc cầu qua nhiều module — tới target repository / loader.
    Kiểm bằng đồ thị import (TEST-10b), không bằng code review.
```

### 14.2 · Mức cho phép hiện tại

```
✅ Proof-of-contract prototype     — CHO PHÉP NGAY  (§13.0, trong giới hạn)
✅ Pilot 10k                        — ĐÃ CHẠY host-side Track A batch
⬜ Production                       — CHẶN
```

---

## 15. Sáu test nghiệm thu prototype

> Prototype được coi là **faithful với spec** — chứ không chỉ *"code chạy được"* —
> khi cả sáu test dưới pass.

### TEST-01 · P2 tìm đúng optimal **POOL** — không chỉ nghiệm

```
expected_pool = exhaustive_argmin(all_feasible)      # tính độc lập với solver
actual_pool   = solver.candidate_pool()

assert canonicalize(expected_pool) == canonicalize(actual_pool)
```

> **So POOL, không so nghiệm đã chọn.** Đây là điều tách được hai lỗi khác nhau:
>
> ```
> pool sai        ⇒ P2 (optimizer) hỏng
> pool đúng nhưng
> selected ∉ pool ⇒ P3 (selector) hỏng
> ```
>
> Nếu chỉ so nghiệm cuối, hai lỗi này trộn vào nhau và không debug được.

TEST-03 sau đó mới kiểm `selected ∈ expected_pool`.

### TEST-02 · Replay determinism qua số worker

```
cùng (target, config, seed), chạy 2 lần với worker_count ∈ {1, 8}
assert  witness₁ == witness₂           (event_id, event_ts, thứ tự — giống hệt)
assert  fingerprint₁ == fingerprint₂
```
Bắt lỗi: `occurrence` phụ thuộc traversal (§4.3a-2).

### TEST-03 · Đổi seed — chất lượng KHÔNG đổi

> ⚠️ **Sửa lỗi bản trước.** Tôi đã viết `assert witness_a != witness_b`. **Sai** —
> nó fail khi `|CandidatePool| == 1`, trường hợp mà §4.4b nói rõ là **bình thường**.
> Ngay cả với pool lớn, hai seed khác nhau **vẫn có thể** chọn trúng cùng candidate.

```
seed_a ≠ seed_b

# ── INVARIANT (bắt buộc) ────────────────────────────
assert objective(witness_a) == objective(witness_b)     ← BẰNG NHAU
assert witness_a ∈ expected_pool  và  witness_b ∈ expected_pool
assert GateA(witness_a) và GateA(witness_b)   đều PASS
assert GateB(...)  GateC(...)                 đều PASS

# ── OBSERVATION (KHÔNG phải invariant) ──────────────
witness_a != witness_b        ← chỉ ghi nhận, KHÔNG assert
```

Bắt lỗi: seed đang ảnh hưởng **chất lượng** nghiệm, không chỉ lựa chọn.

### TEST-04 · ★ Mutant test cho INVARIANT 3 — quan trọng nhất

```
Cố tình implement biến thể:  seed → random FEASIBLE candidate → check objective
assert  TEST SUITE PHẢI FAIL
```

**Fixture phải được thiết kế để mutant CHẮC CHẮN lộ:**

```
feasible set  =  ~100 candidate ở argmin  (objective BẰNG NHAU, phân bố temporal RẤT khác)
              +  nhiều candidate feasible nhưng SUBOPTIMAL

   candidate A → active days [1, 2, 3]      ┐
   candidate B → active days [7, 13, 21]    ├ cùng objective → đều ∈ pool
   candidate C → active days [5, 12, 28]    ┘
   candidate X → objective XẤU HƠN          ← mutant có thể chọn trúng
```

> ⚠️ **Fixture bình thường KHÔNG đủ.** Nếu feasible set gần như trùng optimal pool thì
> mutant `seed → random feasible` vẫn trả nghiệm tối ưu ở **hầu hết** seed và
> TEST-03 sẽ pass ⇒ mutant sống sót. Fixture phải có **đủ nhiều** candidate suboptimal
> để mutant chọn trúng chúng với xác suất cao.
>
> Nếu mutant **pass**, nghĩa là **bộ test hỏng**, không phải implementation đúng.

### TEST-05 · Version nào cũng phải đổi fingerprint

```
với mỗi v ∈ {objective_version, selection_policy_version,
             constraint_model_version, encoding_version, solver_version}:
    đổi v  →  assert fingerprint đổi
```
Bắt lỗi: field bị quên khi ghép fingerprint (§11).

### TEST-06 · `logical_slot` / `event_id` độc lập traversal

```
Candidate có NHIỀU event cùng gen_reason (vd n5 = 20, cùng day_offset)
Chạy với thứ tự materialize đảo ngược + worker_count khác nhau
assert  tập event_id giống hệt
assert  ánh xạ candidate_slot_id → event_id giống hệt
```
Bắt lỗi: `occurrence` rơi lại vào thứ tự dựng.

### TEST-07 · ★ H1/H2 objective differentiation — bắt hard-code nhánh

Một fixture, hai nhánh, **kết quả phải NGƯỢC nhau**:

```
Candidate A:  unexplained = 2,  active_days = 10,  sessions = 2
Candidate B:  unexplained = 5,  active_days =  3,  sessions = 1

H1: objective = (unexplained, sessions)
    A = (2, 2)   B = (5, 1)   ⇒  A < B  ⇒  chọn A

H2: objective = (active_days, sessions)
    A = (10, 2)  B = (3, 1)   ⇒  B < A  ⇒  chọn B

assert  solve(target, branch="H1").selected == A
assert  solve(target, branch="H2").selected == B
```

> Đây là test chứng minh phát hiện §4.2 **không chỉ nằm trên giấy**: cùng một candidate
> space **phải** được đánh giá khác nhau dưới hai nhánh.
>
> Nếu cả hai nhánh chọn **cùng** candidate ⇒ objective bị **hard-code**, không đọc
> `config.semantic_branch` ⇒ vi phạm §4.2d.

### TEST-08 · `semantic_status` KHÔNG được đổi solver semantics

```
cùng target · cùng branch="H1" · cùng seed · cùng mọi version

chạy với  semantic_status = "UNIDENTIFIED"
chạy với  semantic_status = "H1_SUPPORTED"

assert  witness₁ == witness₂        ← GIỐNG HỆT
```

> Bảo vệ trực tiếp **INVARIANT 2**: `status` là **metadata nhận thức**, `branch` là
> **cấu hình vận hành**. Nếu output đổi theo `status`, nghĩa là code đang suy diễn
> semantic từ trạng thái nhận thức — đúng thứ §10.1 cấm.
>
> ⚠️ Ngoại lệ hợp lệ **duy nhất**: `fingerprint` **được phép** khác nếu `semantic_status`
> nằm trong fingerprint. Hiện §11 **không** đưa `semantic_status` vào ⇒ fingerprint
> cũng phải giống. Nếu sau này thêm, test này phải sửa cùng lúc.

### TEST-09 · 55-column scope được thực thi

```
Dựng target cố tình chứa ĐỦ f0..f82 (83 giá trị)
nhưng selected_feature_set_id chỉ khai báo 55 cột

assert  ReconstructionTarget từ chối dựng (I-2)   HOẶC  bỏ qua 47 cột ngoài scope
assert  solver KHÔNG BAO GIỜ sinh ràng buộc cho cột ngoài 55
assert  GateA/GateA-T3 chỉ đánh giá 31 + 24 cột
```

> Bảo vệ trực tiếp **INVARIANT 1**. Bắt lỗi: ai đó "tiện tay" mở scope lên 83 vì
> target vô tình mang đủ dữ liệu.

### TEST-10 · ★ Track B KHÔNG thể thấy `ReconstructionTarget` — **hai tầng**

#### 10a · Interface boundary — chữ ký hàm

```
sig = inspect.signature(live_generator)
assert "target" not in sig.parameters
assert ReconstructionTarget not in {p.annotation for p in sig.parameters.values()}

fields = CustomerState.__dataclass_fields__
assert "values" / "target_hash" / "feature_payload_hash"
       / "selected_feature_set_id"   KHÔNG có trong fields

với result.status == "QUARANTINED":  assert to_customer_state(result) raises
```

#### 10b · ★ CAPABILITY boundary — đồ thị import, **bắc cầu**

> Tầng 10a **không đủ**. `source_target_id` vẫn giải được nếu Track B import được
> repository. Phải chặn ở mức **khả năng**, không phải mức **giao diện**.

```
Quét AST của package Track B, đi BẮC CẦU qua mọi import nội bộ:

assert  reconstruction.target      KHÔNG reachable
assert  reconstruction.canonical   KHÔNG reachable
assert  mọi target repository/loader  KHÔNG reachable
```

```
Track B chỉ được biết CustomerState
→ KHÔNG có repository/tool nào giúp nó quay ngược về Target
```

> **Đây là architectural fitness function, không phải unit test.** Nó fail ngay khi ai
> đó thêm một dòng `import` — kể cả import gián tiếp qua ba lớp module.
>
> Nếu Track B thấy được target, generator có thể "lái" event tương lai để chiều theo
> feature: `target → feature → future events → future feature`. Lúc đó ta tưởng
> streaming system đang tái tạo hành vi, trong khi generator **đã biết đáp án** —
> tautology nguy hiểm hơn §12.4 vì nó nằm ở tầng streaming, nơi không ai nghĩ tới việc kiểm.

---

### 15.1 · Điều kiện nghiệm thu

```
[x] TEST-01   optimal POOL đúng (so pool, không so nghiệm)
[x] TEST-02   replay determinism qua worker count (1 vs 8)
[x] TEST-03   seed KHÔNG đổi chất lượng  (KHÔNG assert witness khác nhau)
[x] TEST-04   mutant seed-before-optimization PHẢI fail            ★ INVARIANT 3
[x] TEST-05   mọi version đều vào fingerprint
[x] TEST-06   event_id độc lập traversal
[x] TEST-07   H1/H2 chọn NGƯỢC nhau trên cùng fixture              ★ §4.2
[x] TEST-08   semantic_status KHÔNG đổi output                     ★ INVARIANT 2
[x] TEST-09   55-column scope được thực thi                        ★ INVARIANT 1
[x] TEST-10   Track B không thấy ReconstructionTarget              ★ §3.4 / INVARIANT 4
              10a interface boundary  ✅ chữ ký live_generator + CustomerState
              10b CAPABILITY boundary ✅ áp lên live.py, forbidden 7 module
```

**Cả 10 test nghiệm thu đã pass** — 174 test xanh trong
`tests/reconstruction/`; toàn repo 204 test xanh tại lần đồng bộ này. Prototype
được coi là **faithful với spec**, không chỉ *"code chạy được"*.

**Slice 2 đã implement** (`candidate.py` · `semantics.py` · `engine.py`):

| Module | Nội dung |
|---|---|
| `candidate.py` | `Candidate = multiset(Slot)` §4.3a-1; metric dẫn xuất tất định |
| `semantics.py` | `SemanticBranch` protocol + `H1Branch`/`H2Branch` — **một** engine, hai strategy |
| `engine.py` | P1 `feasible_candidates` → P2 `optimal_pool` → P3 `select_from_pool` → P3b `materialize` → P4 `canonical_sequence` |

**Slice 3 đã implement** — ranh giới bàn giao:

| Module | Phía | Nội dung |
|---|---|---|
| `handoff.py` | **Track A** | `GateReport` (không default) · `to_customer_state()` · `HandoffRefused` khi `QUARANTINED` hoặc gate fail |
| `live.py` | **Track B** | `live_generator()` · `RuleBasedBehaviour` · `TemporalViolation`. Import **chỉ** `state` + stdlib |

Capability boundary được canh bằng `assert_cannot_reach(live.py, forbidden=7 module)`,
và có test **ngược lại** xác nhận `handoff.py` **được phép** reach Track A — nếu không,
checker có thể đang báo động giả mà không ai biết.

Chín test này **không** thay thế Gate A–G. Chúng kiểm **solver có trung thành với spec
không**; Gate A–G kiểm **kết quả có đúng không**. Hai việc khác nhau.

### 15.2 · Fixture matrix — Layer 1

```
n30 = 1                      ·  n30 = 30
d1 = d2                      ·  d1 ≠ d2
capacity-valid               ·  capacity-fail
|pool| = 1 (nghiệm duy nhất) ·  |pool| ≫ 1 (nhiều nghiệm ngang nhau)
zero FREE_EVENT              ·  many FREE_EVENT
TrackA rỗng                  ·  TrackB rỗng          ·  cả hai khác rỗng
session đúng tại boundary    ·  session bắc qua boundary (phải FAIL)
nhiều seed khác nhau
```

### 15.3 · Hai tầng test — fixture KHÔNG thay thế real target

```
Layer 1 · Contract fixtures          deterministic / adversarial, kiểm từng invariant
    ▼
TEST-01 … TEST-09
    ▼
Layer 2 · Real target smoke test     20–50 dòng train THẬT
    ▼
Gate A / B / C / F smoke
```

> Fixture cho biết **expected behavior chính xác**; dữ liệu thật cho biết
> **integration có chạy không**. Với dữ liệu thật, một test fail không phân biệt được
> `solver bug` / `constraint bug` / `semantic assumption sai` / `data peculiarity` —
> đó là lý do fixture phải đi trước.
>
> **Chưa cần 10k** ở giai đoạn này.

### 15.4 · Kiến trúc — MỘT engine, hai branch strategy

```
🚫 CẤM:  solver_h1.py  +  solver_h2.py     → duplication, hai nhánh sẽ drift

✅ ĐÚNG:
                  ReconstructionEngine          ← P1..P4, fingerprint, provenance, gates
                          │
                   semantic_branch
                      /        \
                 H1Branch    H2Branch           ← CHỈ khác: decode · constraints ·
                                                   feature_predicates · objective ·
                                                   candidate_generator
```

```python
class SemanticBranch(Protocol):
    name: str
    def decode_target(self, target) -> DecodedState: ...
    def build_constraints(self, state) -> Constraints: ...
    def feature_predicates(self, state) -> Predicates: ...
    def objective(self, candidate) -> tuple: ...        # §4.2b
    def candidate_generator(self, state) -> Iterable[Candidate]: ...
```

Engine chung giữ: P1 feasibility · P2 optimizer · P3 selector · P3b materializer ·
P4 canonical ordering · fingerprint · provenance · gates.

⇒ Test được điều quan trọng nhất: **`semantic_branch` là configuration, không phải
hai codebase.**

### 15.5 · Prototype KHÔNG cần hạ tầng

```
❌ Kafka   ❌ downstream state   ❌ Airflow   ❌ MinIO   ❌ Postgres persistence
```

Prototype chạy in-memory + fixture. Hạ tầng chỉ vào từ pilot.

---

## Liên quan

`PIPELINE_ARCHITECTURE.md` · `RECONSTRUCTION_CONSTRAINT_MODEL.md` ·
`RECONSTRUCTION_CONTRACT.md` · `DATA_GENERATION.md` · `FEATURE_DICTIONARY.md`
