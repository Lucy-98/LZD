# Reconstruction Constraint Model

> **Trạng thái: THIẾT KẾ + E2E prototype/test. Track B rule-based đã chạy dry-run;
> generator production chưa chốt. Training pipeline vẫn không bị đụng.**
>
> **Câu hỏi tài liệu này trả lời:**
>
> > Làm thế nào sinh **MỘT** event history duy nhất `E` sao cho
> > `FeatureEngine(E)` tái tạo **đồng thời** toàn bộ target state `T` của 6 feature T1,
> > thay vì sinh từng feature độc lập?
>
> Nhãn: `[FACT]` `[MEASURED]` `[ASSUMPTION]` `[UNKNOWN]`.

---

## Mục lục

1. [Target state](#1-target-state)
2. [Event state](#2-event-state)
3. [Feature dependency graph](#3-feature-dependency-graph)
4. [Event → feature mapping](#4-event--feature-mapping)
5. [Cross-feature coupling](#5-cross-feature-coupling)
6. [★ Kết quả đo: hai điều kiện khả thi](#6--kết-quả-đo-hai-điều-kiện-khả-thi)
7. [★ Free-event axiom](#7--free-event-axiom)
8. [Hard constraints](#8-hard-constraints)
9. [Medium constraints](#9-medium-constraints)
10. [Soft constraints](#10-soft-constraints)
11. [Reconstruction algorithm concept](#11-reconstruction-algorithm-concept)
12. [Repair strategy](#12-repair-strategy)
13. [Quarantine strategy](#13-quarantine-strategy)
14. [GATE A](#14-gate-a)
15. [LN & LOG10 comparison regimes](#15-ln--log10-comparison-regimes)
16. [Leakage & tautology prevention](#16-leakage--tautology-prevention)
17. [Unresolved assumptions](#17-unresolved-assumptions)
18. [Pilot experiment design](#18-pilot-experiment-design)

---

## 1. Target state

```
T(u) = {
    f1_target,   f2_target,      # REC regime, nguyên [0,365]
    f5_target,   f11_target,     # LN regime,  float64
    f18_target,  f30_target      # L10 regime, 6 chữ số
}
```

Giá trị **đã giải mã** (thứ solver thực sự làm việc với):

| Ký hiệu | Decode | Miền `[MEASURED]` |
|---|---|---|
| `d1` | `f1` | `[0, 365]` nguyên |
| `d2` | `f2` | `[0, 365]` nguyên, `d1 ≥ d2` ở **100%** dòng |
| `n5` | `round(exp(f5))` | `[1, 14245]`, 864 mức |
| `n11` | `round(exp(f11))` | `[1, 2144]`, 669 mức |
| `n18` | `round(10^f18)` | `[1, 2260]`, 144 mức |
| `n30` | `round(10^f30)` | `[1, 30]`, 30 mức |

> `T` **không** chứa `label`, `is_treat`, T2, T3. Solver không nhìn thấy chúng.

---

## 2. Event state

```
E(u) = [e₁, e₂, …, eₘ]        sắp theo event_ts tăng dần

eᵢ = (event_id, event_type, event_ts, customer_id, …)
```

Ràng buộc toàn cục: `∀ eᵢ ∈ E : eᵢ.event_ts < feature_ts` (Track A).

**Bài toán:**

```
   tìm E  sao cho  FeatureEngine(E) ≡ T   theo luật so sánh §15
```

`FeatureEngine` là **code path production** (`feat_cfs_counter`, `feat_cfs_recency`).
🚫 Không được viết feature engine riêng để ép GATE A pass.

---

## 3. Feature dependency graph

```
                        ┌──────────────────┐
                        │   E (event set)  │
                        └────────┬─────────┘
                                 │
        ┌───────────┬────────────┼────────────┬───────────┐
        ▼           ▼            ▼            ▼           ▼
   ┌─────────┐ ┌─────────┐  ┌────────┐  ┌────────┐  ┌────────┐
   │   d1    │ │   d2    │  │   n5   │  │  n11   │  │  n18   │
   │ MIN(ts) │ │ MAX(ts) │  │ COUNT  │  │ COUNT  │  │ COUNT  │
   └────┬────┘ └────┬────┘  └───┬────┘  └───┬────┘  └───┬────┘
        │ ép        │ ép        │ tuỳ chọn  │ tuỳ chọn  │ tuỳ chọn
        │           │           │           │           │
        └───────────┴───────────┴─────┬─────┴───────────┘
                                      ▼
                            ┌───────────────────┐
                            │       n30         │  ◄── HUB
                            │ COUNT(DISTINCT    │
                            │   date(event_ts)) │
                            │ trong cửa sổ 30d  │
                            └───────────────────┘
```

**Cấu trúc:** `n30` là **nút hub kiêm sink**. Mọi event đều *có thể* đóng góp vào nó;
nó không ép ngược event loại nào cụ thể. `d1`, `d2` là hai nút có timestamp **cố định
cứng** ⇒ chúng **ép** ngày active nếu rơi trong cửa sổ 30 ngày.

---

## 4. Event → feature mapping

`[FACT]` — `schemas.EVENT_TYPES` hiện có: `app_open`, `page_view`, `search`,
`add_to_cart`, `checkout`, `order`, `voucher_view`, `voucher_claim`.

`[ASSUMPTION]` — ánh xạ sang T1. **Không có evidence nào trong repo** nối event type
với `f*`.

| Event type | Feature bị tác động | Nhãn |
|---|---|---|
| `EVT_ORDER_PAID` | `d1` (MIN ts), `d2` (MAX ts); có thể làm tăng active day nếu trong cửa sổ H1 | `[ASSUMPTION]` S-01, S-02 |
| `EVT_F5` | `n5`; có thể làm tăng active day nếu trong cửa sổ H1 | operational CFS witness; business semantic **`[UNKNOWN]`** |
| `EVT_F11` | `n11`; có thể làm tăng active day nếu trong cửa sổ H1 | operational CFS witness; business semantic **`[UNKNOWN]`** |
| `EVT_F18` | `n18`; có thể làm tăng active day nếu trong cửa sổ H1 | operational CFS witness; business semantic **`[UNKNOWN]`** |
| `EVT_SESSION_STARTED` | free witness: chỉ phủ active day còn thiếu dưới H1 | `[CONDITIONAL]` FE-1 |
| `EVT_F30` | `n30` counter dưới H2 | `[ASSUMPTION]` H2 |

> 🚫 `EVT_*` là namespace nội bộ của CFS Track A, không phải bằng chứng rằng LZD gốc có
> đúng các event business đó. Gán `EVT_F5` cho một hành động cụ thể như `add_to_cart`
> hiện **không có căn cứ**.

### 4.1 · Điều kiện bắt buộc lên taxonomy

> **NẾU giữ S-03, thì phải tồn tại ít nhất một event type mà KHÔNG feature T1 nào đếm.**

Nếu mọi event đều bị một counter đếm, không thể thêm ngày active mà không phá counter
khác. §6–§7 cho thấy điều kiện này là **bắt buộc dưới S-03** — nó là hệ quả **có điều
kiện**, không phải sự thật đã chứng minh về dataset.

`EVT_SESSION_STARTED` được dùng làm witness operational cho vai trò đó: nó không đếm
vào `n5`/`n11`/`n18`/recency, chỉ giúp phủ ngày active còn thiếu dưới H1. Business
semantic tương ứng vẫn là **hypothesis** cho tới khi event taxonomy xác nhận — xem
§7.6 FE-e.

---

## 5. Cross-feature coupling

| Feature | Event dependency | Tác động feature khác? | Coupling | Chiều |
|---|---|---|---|---|
| `d1` | `EVT_ORDER_PAID` sớm nhất | `n30` nếu `d1 < 30`; `d2` nếu `d1 = d2` | **MEDIUM** | ép, một chiều → `n30` |
| `d2` | `EVT_ORDER_PAID` muộn nhất | `n30` nếu `d2 < 30` | **MEDIUM** | ép, một chiều → `n30` |
| `n5` | `EVT_F5` × n5 | `n30` **nếu đặt trong cửa sổ 30d** | **LOW** | tuỳ chọn |
| `n11` | `EVT_F11` × n11 | `n30` nếu đặt trong cửa sổ | **LOW** | tuỳ chọn |
| `n18` | `EVT_F18` × n18 | `n30` nếu đặt trong cửa sổ | **LOW** | tuỳ chọn |
| `n30` | mọi event, **ngày phân biệt** | — | **HIGH (hub)** | nhận, không phát |

### 5.1 · Vì sao `n5`/`n11`/`n18` chỉ LOW

Ba counter này đếm **số event**, không phải số ngày phân biệt `[ASSUMPTION]`.
⇒ được phép **xếp chồng** nhiều event vào cùng một ngày.
⇒ có thể đặt **toàn bộ** ra ngoài cửa sổ 30 ngày, hoặc gộp hết vào một ngày đã active.
⇒ chúng **không bao giờ ép** thêm ngày active.

### 5.2 · Vì sao `d1`/`d2` là MEDIUM chứ không HIGH

Timestamp của chúng **cố định cứng** (`feature_ts − d`), không có bậc tự do.
Nếu `d < 30`, ngày đó **bắt buộc** active. Nhưng chúng chỉ ép **tối đa 2** ngày.

---

## 6. ★ Kết quả đo: hai điều kiện khả thi

Đây là đóng góp chính của tài liệu này — **đo trực tiếp trên dataset, không cần generator.**

### 6.1 · Điều kiện 1 — xung đột `d1`/`d2` ↔ `n30`

Suy ra bằng đại số:

```
D_forced = {d1, d2} ∩ [0, 29]                    (tập ngày bị ép active)
CONFLICT ⟺ n30 < |D_forced|
         ⟺ d1 < 30 ∧ d2 < 30 ∧ d1 ≠ d2 ∧ n30 = 1
```

`[MEASURED]` trên 926,669 dòng train:

| Điều kiện | Số user | Tỉ lệ |
|---|---|---|
| `d1 < 30` | 118,416 | 12.78% |
| `d2 < 30` | 121,862 | 13.15% |
| `d1 < 30 ∧ d2 < 30 ∧ d1 ≠ d2` | 1,038 | 0.11% |
| `n30 = 1` | 53,279 | 5.75% |
| **CONFLICT (cả bốn)** | **8** | **0.001%** |

> ✅ **Coupling `f2 ↔ f30` mà yêu cầu lo ngại là bottleneck lớn nhất — thực tế chỉ ràng
> buộc 8 user trên 926,669.** Không phải bottleneck.
>
> Lý do: `[MEASURED]` **`d1 = d2` ở 97.42% dòng** (902,731 user) ⇒ `|D_forced| ≤ 1`
> gần như luôn luôn ⇒ điều kiện xung đột hiếm khi thoả.

### 6.2 · Điều kiện 2 — capacity: **đây mới là bottleneck thật**

`n30` yêu cầu `n30` **ngày phân biệt** có event. Mỗi ngày cần ≥1 event.
Số event "có kế toán" (được một counter T1 đếm):

```
capacity(u) = n_rec + n5 + n11 + n18        với n_rec = 1 nếu d1 = d2, ngược lại 2
INFEASIBLE (không có free event) ⟺ n30 > capacity(u)
```

`[MEASURED]`:

| Chỉ số | Giá trị |
|---|---|
| **CAPACITY FAIL** | **440,713 user — 47.56%** |
| `n30` trung bình | 8.23 |
| `capacity` trung bình | 15.70 |
| Thiếu hụt TB (khi fail) | **5.43 ngày** |
| Thiếu hụt max | 26 ngày |
| User có `n5 = n11 = n18 = 1` | 539,019 — **58.17%** |

> 🔴 **47.56% user vi phạm capacity bound.**
>
> Nguyên nhân trực tiếp: 58.17% user có `n5 = n11 = n18 = 1` ⇒ capacity chỉ 4, trong
> khi `n30` trung bình là 8.23.
>
> ⚠️ **Cách đọc đúng:** đây **không** phải "47.56% user không tái tạo được". Bất đẳng
> thức chỉ chứng minh impossibility **dưới hai tiền đề chưa chứng minh** (P1, P2).
> Xem §7.1 và §7.8.

Đây là kết quả **bác bỏ** cách tiếp cận "sinh đủ số event cho từng feature" — đúng như
nguyên tắc cuối của yêu cầu.

---

## 7. ★ PROPOSITION FE-1 — conditional, chưa phải fact

> ⚠️ **Bản trước gọi đây là `AXIOM FE-1`. Sai.** 47.56% là hệ quả của một
> **impossibility proof CÓ ĐIỀU KIỆN**, không phải quan sát trực tiếp về dữ liệu.
> Mục này sửa lại thành mệnh đề có điều kiện và trình bày điều tra event taxonomy.

### 7.1 · Phát biểu đúng

```
PROPOSITION FE-1   [CONDITIONAL]
--------------------------------------------------------------------------
NẾU  P1: f30 thực sự đếm số NGÀY PHÂN BIỆT có hoạt động (= S-03)
VÀ   P2: mọi event có thể tạo ra một ngày active đều thuộc các lớp event
         đã được mô hình hoá bởi f1/f2/f5/f11/f18
VÀ   P3: một event chiếm đúng một ngày
THÌ  n30 > n_rec + n5 + n11 + n18  ⇒  KHÔNG THỂ reconstruct.
--------------------------------------------------------------------------
```

`P3` đúng hiển nhiên. Do đó, với 440,713 user quan sát được bất đẳng thức trên:

```
   n30 > capacity  ∧  P1 ∧ P2 ∧ P3   ⇒  MÂU THUẪN
   ⇒   ¬P1  ∨  ¬P2
```

> **Đây là kết luận duy nhất chứng minh được:**
> **ít nhất một trong hai — S-03 (`P1`) hoặc P2 — là SAI**, cho 47.56% user.
>
> `FE-1 ≡ ¬P2`. Vậy:
>
> ```
> FE-1 được suy ra  ⟺  ta giữ S-03
> S-03 bị bác        ⟸  ta khăng khăng giữ P2
> ```
>
> Hai cái **không thể cùng đúng**. Dữ liệu không nói cái nào sai.

### 7.2 · `capacity_upper_bound` — điều kiện hiệu lực

```
n_rec = 1  nếu f1 = f2
        2  ngược lại

capacity_upper_bound = n_rec + n5 + n11 + n18
```

Bất đẳng thức `n30 > capacity_upper_bound` chỉ chứng minh **impossibility** khi:

| # | Điều kiện | Trạng thái |
|---|---|---|
| **B-1** | Mỗi counter event đóng góp **tối đa một** ngày active phân biệt | ✅ đúng (một event ⇒ một ngày) |
| **B-2** | Event taxonomy **đầy đủ** — không lớp event nào ngoài 5 lớp đã mô hình hoá | ❓ **CHƯA CHỨNG MINH** = P2 |
| **B-3** | `f30` aggregation là `COUNT(DISTINCT day)` | ❓ **CHƯA CHỨNG MINH** = P1 |

> Nếu một lớp event tạo ra nhiều trạng thái active-day qua một aggregation khác
> (vd `f30` không phải distinct-day mà là một counter khác bị cap ở 30), **bound phải
> viết lại**. Bound hiện tại **chỉ** hợp lệ dưới B-1 ∧ B-2 ∧ B-3.

### 7.3 · Điều tra event taxonomy — kết quả

**Câu hỏi §2:** *tất cả event/activity types có thể làm tăng `f30` là những loại nào?*

```
KẾT QUẢ:  KHÔNG XÁC ĐỊNH ĐƯỢC TỪ REPOSITORY
```

`[FACT]` — grep toàn repo (`*.py`, `*.sql`, `*.yml`) cho `f30|f18|f5|f11|f1|f2|
active_day|distinct_day|log10|ln(|exp(`:

| Hit | Nội dung | Có phải evidence? |
|---|---|---|
| `tests/test_sync_logic.py:47`, `tests/test_feature_spec.py:72` | dùng `f0`,`f1`,`f2` làm tên giả trong fixture | ❌ Không |
| `src/lzd_pipeline/serving/feature_contract.py` | đọc `f30` như đầu vào model hoặc dùng median khi thiếu | ❌ **Không** — không cho biết semantics hay aggregation gốc |
| **Không hit nào khác** | | |

`[FACT]` **Không có một dòng code nào trong repo tính `f1`,`f2`,`f5`,`f11`,`f18`,`f30`.**
Lineage của chúng nằm trong hệ thống nội bộ Lazada, **ngoài phạm vi quan sát**.

⇒ **H1 không thể kiểm chứng bằng repo evidence.** Câu hỏi "event type nào làm tăng
`f30`" là **không trả lời được**, không phải "chưa trả lời".

### 7.4 · Taxonomy THẬT của repo — dùng làm loại suy cấu trúc

`[FACT]` — đây là taxonomy repo **thực sự** có, chi phối Track B (`hist_*`, `rt_*`),
**không** chi phối `f*`. Đọc từ `schemas.EVENT_TO_COUNTER`,
`stream_consumer._update_realtime()`, `feat_user_behaviour.sql`, `feat_user_realtime_pit.sql`.

| Event type | `rt_events_1h` | `rt_page_view` | `rt_add_to_cart` | `rt_order` | `hist_order` | `voucher_used` | `session_cnt_30d` |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| `app_open` | ✓ | | | | | | ✓ |
| `page_view` | ✓ | ✓ | | | | | ✓ |
| `search` | ✓ | | | | | | ✓ |
| `add_to_cart` | ✓ | | ✓ | | | | ✓ |
| `checkout` | ✓ | | | | | | ✓ |
| `order` | ✓ | | | ✓ | ✓ | | ✓ |
| `voucher_view` | ✓ | | | | | | ✓ |
| `voucher_claim` | ✓ | | | | | ✓ | ✓ |

**Quan sát cấu trúc `[FACT]`:**

1. **Bốn** event type (`app_open`, `search`, `checkout`, `voucher_view`) tăng **duy nhất**
   các aggregate rộng, **không** tăng counter hẹp nào. Đây chính là mẫu
   *"event tự do tương đối với một tập con counter"* — và nó tồn tại sẵn trong thiết
   kế production của repo.
2. `session_cnt_30d = count(distinct session_id)` là một **DISTINCT counter** — loại suy
   cấu trúc gần nhất với `f30` (`count(distinct day)`). Nó được **mọi** event type nuôi,
   trong khi `hist_order_cnt_30d` và `voucher_used_30d` chỉ được nuôi bởi một loại.
3. ⇒ Trong chính hệ feature của repo,
   `session_cnt_30d > hist_order_cnt_30d + voucher_used_30d` là **chuyện thường**,
   không phải dị thường cần một tiên đề đặc biệt.

> ⚠️ Đây là **loại suy thiết kế**, không phải bằng chứng về Lazada. Nó cho thấy cấu
> hình khiến bất đẳng thức capacity thất bại là **phổ biến**, không kỳ lạ.
> Nó **không** chứng minh `f30` hoạt động như vậy.

### 7.5 · Dataset algebra — hỗ trợ ¬P2 độc lập với P1

`[MEASURED]`

| Chỉ số | Giá trị | Đọc |
|---|---|---|
| `corr(n30, n5+n11+n18)` | **0.161** | rất yếu — `n30` và các counter đo **hoạt động khác nhau** |
| `corr(n30, n5)` | 0.094 | |
| `corr(n30, n11)` | 0.210 | |
| `corr(n30, n18)` | 0.024 | |
| `corr(n5, n11)` | 0.153 | xác nhận lại: hai biến độc lập |
| capacity TB khi `n30 = 1` | 5.09 | |
| capacity TB khi `n30 = 30` | 80.46 | nhưng **76.0%** (1,863/2,452) user `n30=30` **vẫn** fail capacity ⇒ phân bố lệch phải nặng |

**Đọc:** ngay cả trong nhóm hoạt động dày đặc nhất (`n30 = 30` — active mọi ngày),
đa số vẫn có ít event "có kế toán" hơn số ngày active. Điều này phù hợp với việc
`f5`/`f11`/`f18` đếm các **lớp hành động hẹp**, chứ không phải hoạt động tổng quát.

Lớp hành động hẹp ⇒ `P2` (taxonomy đầy đủ) **khó tin về mặt tiên nghiệm**, độc lập
với `f30` là gì. 6 feature không thể liệt kê toàn bộ hoạt động của một user e-commerce.

### 7.6 · Ba hypothesis — kết luận

| | Nội dung | Trạng thái |
|---|---|---|
| **H1** | Tồn tại lớp event: `→ f30`, `↛` counter T1 | **UNRESOLVED** — không kiểm chứng được từ repo (§7.3). Được **hỗ trợ** bởi loại suy §7.4 và §7.5, **chưa** được chứng minh |
| **H2** | `f30` không phải distinct active days ⇒ S-03 sai | **UNRESOLVED** — không bác được, cũng không chứng minh được. Trần đúng 30 **nhất quán** với S-03 nhưng cũng nhất quán với semantic khác |
| **H3** | `f5`/`f11`/`f18` không phải event counter như giả định | **REFINED, không bác** — cơ chế `ln`/`log10` là `[MEASURED]` chắc chắn; **cửa sổ** và **loại hành động** vẫn `[UNKNOWN]`. §7.5 gợi ý chúng hẹp |

### 7.7 · Ràng buộc kéo theo — **chỉ hiệu lực nếu FE-1 được chấp nhận**

| # | Ràng buộc |
|---|---|
| **FE-a** | `FREE_EVENT` phải thuộc event type **không counter T1 nào đếm** (§4.1) |
| **FE-b** | `FREE_EVENT` chỉ đặt trong cửa sổ 30 ngày và chỉ để thoả `n30` |
| **FE-c** | Phải đo `implied_unexplained_event_lower_bound` — §18.3 |
| **FE-d** | S-03 và FE-1 **liên đới**: bác FE-1 ⇒ phải bác S-03, và ngược lại (§7.1) |
| **FE-e** | `FREE_EVENT` **chưa** được nâng lên thành reconstruction primitive. Nó là **hypothesis**, chỉ thành primitive khi event taxonomy chứng minh được lớp event đó tồn tại |

### 7.8 · ★ Tách ba khái niệm — không được dùng thay thế nhau

| Khái niệm | Giá trị | Bản chất |
|---|---|---|
| **Structural conflict** (`n30 < \|D_forced\|`) | **8 user — 0.001%** | `[MEASURED]`, vô điều kiện. Đây là mâu thuẫn **thật** trong chính target |
| **Capacity infeasibility under current assumptions** | **440,713 — 47.56%** | `[MEASURED]` bất đẳng thức, nhưng **kết luận "không reconstruct được" là CÓ ĐIỀU KIỆN** trên P1 ∧ P2 |
| **Actual unsolvable reconstruction rate** | **`[UNKNOWN]`** | Chưa biết, và **không thể biết** cho tới khi event taxonomy được chốt |

> 🚫 Viết "47.56% user không tái tạo được" là **sai**. Đúng: *"47.56% user vi phạm
> capacity bound; bound đó chỉ chứng minh impossibility dưới hai tiền đề chưa chứng minh."*

---

## 8. Hard constraints

Vi phạm ⇒ nghiệm **không hợp lệ**. Không thương lượng.

| # | Ràng buộc | Assertion |
|---|---|---|
| **H-1** | `∀e ∈ E : e.event_ts < feature_ts` (nghiêm ngặt) | khớp `feat_user_realtime_pit.sql` dùng `<` |
| **H-2** | `d1 ≥ d2` | `[MEASURED]` đúng 100% dòng nguồn |
| **H-3** | Event `EVT_ORDER_PAID` sớm nhất tại đúng `feature_ts − d1 ngày` | `MIN(ts)` phải khớp chính xác |
| **H-4** | Event `EVT_ORDER_PAID` muộn nhất tại đúng `feature_ts − d2 ngày` | `MAX(ts)` khớp chính xác |
| **H-5** | `d1 = d2` ⇒ sinh **một** event, không phải hai | tránh MIN=MAX giả tạo |
| **H-6** | `count(EVT_F5) = n5` **chính xác** | không ±1 |
| **H-7** | `count(EVT_F11) = n11` chính xác | |
| **H-8** | `count(EVT_F18) = n18` chính xác | |
| **H-9** | `count(distinct date(ts)) trong cửa sổ 30d = n30` chính xác | **ngày phân biệt**, không phải số event |
| **H-10** | `n30 ≥ \|D_forced\|` | nếu sai ⇒ QUARANTINE (measured: 8 user) |
| **H-11** | Target **không bao giờ** bị sửa | |
| **H-12** | `label` / `is_treat` không vào solver | thực thi bằng chữ ký hàm |

---

## 9. Medium constraints

Nên thoả; vi phạm ⇒ ghi nhận, không fail.

| # | Ràng buộc |
|---|---|
| **M-1** | `event_id` duy nhất và deterministic (`uuid5`) |
| **M-2** | Không hai event trùng `(customer_id, event_type, event_ts)` tới mili giây |
| **M-3** | Event trải hợp lý trong ngày (không dồn hết vào 00:00:00) |
| **M-4** | Ngày active chọn ngoài `D_forced` phân bố đều, không dồn cuối cửa sổ |
| **M-5** | Free-event ratio của một user ≤ ngưỡng pilot (§18.3) |

---

## 10. Soft constraints

Chỉ áp dụng khi không mâu thuẫn H và M. **Track A không ưu tiên behavioral realism.**

| # | Ràng buộc |
|---|---|
| **S-1** | Giờ trong ngày theo phân bố traffic thực tế |
| **S-2** | Session có cấu trúc (`app_open` → `page_view` → …) |
| **S-3** | Khoảng cách giữa các event giống hành vi thật |

> Track A ưu tiên **correct reconstruction**. Behavioral realism thuộc **Track B**.
> Đánh đổi S để giữ H là **đúng**, không phải thoả hiệp.

---

## 11. Reconstruction algorithm concept

**Không greedy. Không "hy vọng khớp".** Bắt buộc có vòng verify.

```
   T(u) ── decode ──► (d1, d2, n5, n11, n18, n30)
                              │
                              ▼
   ┌──────────────────────────────────────────────────┐
   │ P0 · PRECHECK khả thi                            │
   │    D_forced = {d1,d2} ∩ [0,29]                   │
   │    n30 ≥ |D_forced| ?  ──── không ──► QUARANTINE │
   └──────────────────────┬───────────────────────────┘
                          ▼
   ┌──────────────────────────────────────────────────┐
   │ P1 · Đặt event RÀNG BUỘC CỨNG (không bậc tự do)  │
   │    EVT_ORDER_PAID @ feature_ts − d1              │
   │    EVT_ORDER_PAID @ feature_ts − d2 (nếu d1 ≠ d2)│
   └──────────────────────┬───────────────────────────┘
                          ▼
   ┌──────────────────────────────────────────────────┐
   │ P2 · Chọn tập ngày active                        │
   │    D_active = D_forced ∪ chọn(n30 − |D_forced|   │
   │                             từ [0,29] \ D_forced)│
   └──────────────────────┬───────────────────────────┘
                          ▼
   ┌──────────────────────────────────────────────────┐
   │ P3 · Phủ D_active bằng event                     │
   │    ưu tiên 1: event n5/n11/n18 (nếu cửa sổ cho)  │
   │    ưu tiên 2: FREE event (EVT_SESSION_STARTED)   │
   └──────────────────────┬───────────────────────────┘
                          ▼
   ┌──────────────────────────────────────────────────┐
   │ P4 · Đặt event counter còn lại                   │
   │    xếp chồng vào ngày ĐÃ active                  │
   │    ⇒ không tạo ngày active mới                   │
   └──────────────────────┬───────────────────────────┘
                          ▼
   ┌──────────────────────────────────────────────────┐
   │ P5 · CHẠY FEATURE ENGINE THẬT trên E             │
   │    F = FeatureEngine(E)                          │
   └──────────────────────┬───────────────────────────┘
                          ▼
   ┌──────────────────────────────────────────────────┐
   │ P6 · So sánh F[T1] vs T theo regime §15          │
   └──────────┬───────────────────────┬───────────────┘
              │ khớp                  │ lệch
              ▼                       ▼
            PASS              ┌───────────────┐
                              │ P7 · REPAIR   │──┐
                              └───────┬───────┘  │ ≤ K vòng
                                      │          │
                                      └──────────┘
                                      │ vẫn lệch
                                      ▼
                                  QUARANTINE
```

### 11.1 · Bất biến của P4

> **Xếp chồng vào ngày đã active** là bất biến then chốt: nó đảm bảo P4 **không bao
> giờ** phá `n30` đã thoả ở P3. Nhờ vậy thứ tự P2 → P3 → P4 là **an toàn một chiều**,
> không cần backtrack giữa chúng.

---

## 12. Repair strategy

Repair chỉ được sửa **`E`**, tuyệt đối không sửa `T`.

| Triệu chứng | Hành động | Vùng an toàn |
|---|---|---|
| `n30` thiếu `k` ngày | Thêm free event vào `k` ngày chưa active | ✅ không đụng counter |
| `n30` thừa `k` ngày | Xoá **free event** khỏi ngày chỉ có free event | ⚠️ **không** xoá được ngày trong `D_forced` |
| `n5` thiếu `k` | Thêm `k` × `EVT_F5` vào ngày **đã active** | ✅ `n30` không đổi |
| `n5` thừa `k` | Xoá `k` × `EVT_F5` từ ngày còn event khác | ⚠️ ngày không được rỗng đi |
| `n11`, `n18` lệch | Như `n5` | |
| `d1`/`d2` lệch | **Không repair được** — timestamp là hard constraint | → QUARANTINE |

### 12.1 · Quy tắc vùng an toàn

```
SAFE REPAIR ZONE = ngày đã active ∧ có ≥ 2 event
```

Sửa trong vùng này không đổi `n30`. Sửa ngoài vùng này **phải** kiểm lại `n30`.

### 12.2 · Giới hạn

- Tối đa `K` vòng repair (`K` chốt ở pilot, **chưa đặt số**)
- Mỗi vòng **phải** chạy lại feature engine thật — không được suy đoán kết quả
- Repair không hội tụ ⇒ QUARANTINE, không nới dung sai

---

## 13. Quarantine strategy

| Mã | Điều kiện | Ước lượng `[MEASURED]` |
|---|---|---|
| `Q_FORCED_DAYS` | `n30 < \|D_forced\|` (H-10) | **8 user (0.001%)** |
| `Q_CAPACITY` | `n30 > capacity_upper_bound` **và** FE-1 bị bác **và** S-03 được giữ | 440,713 (47.56%) — **0 nếu chấp nhận FE-1**. Con số này là **capacity infeasibility under assumptions**, không phải actual unsolvable rate (§7.8) |
| `Q_RANGE` | Target ngoài miền đã đo | 0 kỳ vọng |
| `Q_REPAIR` | Repair không hội tụ sau `K` vòng | **`[UNKNOWN]`** — pilot đo |
| `Q_TIMESTAMP` | `d1`/`d2` không đặt được | 0 kỳ vọng |

Ghi vào `biz.reject_customer(customer_id, reason_code, detail_json)`.

> 🚫 **Ngưỡng quarantine chưa được đặt.** Chưa có evidence cho `Q_REPAIR`.
> Đặt số trước pilot là bịa. Xem §18.

---

## 14. GATE A

Phạm vi và ba mức kiểm: xem `RECONSTRUCTION_CONTRACT.md` §14. Bổ sung của tài liệu này:

### 14.1 · Kiểm ở tầng **event**, không chỉ tầng feature

Assertion mạnh hơn, bắt lỗi sớm hơn:

```
assert count(E, EVT_F5)                               == n5
assert count(E, EVT_F11)                              == n11
assert count(E, EVT_F18)                              == n18
assert count(distinct date(ts) in 30d window)         == n30
assert (feature_ts − min(ts of EVT_ORDER_PAID)).days  == d1
assert (feature_ts − max(ts of EVT_ORDER_PAID)).days  == d2
assert max(ts) < feature_ts
```

Nếu tầng event pass mà tầng feature fail ⇒ **lỗi ở feature engine**, không phải generator.
Đây là cách phân biệt hai nguồn lỗi.

### 14.2 · Free event không được tính vào bất kỳ counter nào

```
assert count(E, EVT_SESSION_STARTED) không xuất hiện trong bất kỳ assertion counter nào
```

Nếu free event bị một counter đếm ⇒ vi phạm §4.1 ⇒ toàn bộ mô hình sụp.

---

## 15. LN & LOG10 comparison regimes

**First-class contract. Không dùng một luật cho tất cả.**

### 15.1 · Regime `LOG10` — `f18`, `f30`

```
decode:   n = round(10^f)
encode:   f' = round(log10(n), 6)
compare:  f' == f            ← ĐẲNG THỨC CHÍNH XÁC
```

`[MEASURED]` round-trip đúng **100.0000%** trên 926,669 dòng.
Lưu 6 chữ số ⇒ phép làm tròn hấp thụ toàn bộ sai số float.

### 15.2 · Regime `LN` — `f5`, `f11`

```
decode:   n = round(exp(f))
encode:   f' = ln(n)                    (float64, KHÔNG làm tròn)
compare:  |f' − f| < 1e-15 × max(|f|, 1)    ← DUNG SAI TƯƠNG ĐỐI
```

`[MEASURED]` đẳng thức chính xác chỉ đúng **93.05%** (`f5`) / **93.42%** (`f11`).
Ở dung sai tương đối `1e-15`: **100%**.

> 🚫 **Cấm** dùng `round(exp(f))` rồi giả định round-trip float64 sẽ exact.
> `[MEASURED]` nó **không** exact ở ~7% số dòng.
>
> 🚫 **Cấm** nới dung sai `LN` lên `1e-6` để "cho pass". Nếu `1e-15` fail thì
> **generator sai**, không phải dung sai sai.

### 15.3 · Regime `REC` — `f1`, `f2`

```
decode:   d = f            (đã nguyên)
encode:   d' = (feature_ts − ts).days
compare:  d' == d          ← ĐẲNG THỨC CHÍNH XÁC (số nguyên)
```

### 15.4 · Bảng tổng

| Regime | Cột | Decode | Encode | Compare |
|---|---|---|---|---|
| `LOG10` | `f18`, `f30` | `round(10^f)` | `round(log10(n), 6)` | `==` |
| `LN` | `f5`, `f11` | `round(exp(f))` | `ln(n)` float64 | `rel < 1e-15` |
| `REC` | `f1`, `f2` | `f` | `date_diff` | `==` |

---

## 16. Leakage & tautology prevention

| # | Ràng buộc |
|---|---|
| **X-1** | Solver **không** nhận `label`, `is_treat` — thực thi bằng chữ ký hàm, không bằng quy ước |
| **X-2** | Giá trị `f` gốc **không** xuất hiện trong `raw.events` |
| **X-3** | `FeatureEngine` là code path production; **không** có nhánh `if reconstruction_mode` |
| **X-4** | Trường `_gen_*` bị drop ở `stg_events` |
| **X-5** | Mọi event Track A `< feature_ts` (H-1) |
| **X-6** | `backfill_solver` **không** nhận behaviour model |

### 16.1 · Thừa nhận thành thật

> GATE A ở tầng này **gần với tautology theo thiết kế** — ta cố tình dựng ngược
> transformation. Nó chứng minh **pipeline chạy đúng**, không chứng minh **semantic đúng**.

Điều làm nó **không** vô nghĩa:

1. Nó bắt lỗi thật: sai cửa sổ, sai biên PIT, sai bảng mã hoá, mất chính xác float
2. Nó **có thể fail** — §6.2 cho thấy hệ vô nghiệm cho 47.56% user nếu không có FE-1.
   Một tautology thật thì không bao giờ fail được.

Validation thật là **Model-Level Validation** (`FEATURE_LINEAGE.md` §13.2).

---

## 17. Unresolved assumptions

| # | Assumption | Nhãn | Trạng thái |
|---|---|---|---|
| **S-01** | `f1` = `days_since_first_order` | `[ASSUMPTION]` | ⚠️ **Bị lung lay** — xem §17.1 |
| **S-02** | `f2` = `days_since_last_order` | `[ASSUMPTION]` | ⚠️ như trên |
| **S-03** | `f30` = `active_days_in_last_30d` | `[ASSUMPTION]` | **UNRESOLVED** — liên đới FE-1 (§7.1). Trần đúng 30 *nhất quán* nhưng không chứng minh |
| **S-04** | `f18` = counter hành động hiếm | `[ASSUMPTION]` | **REFINED** — cơ chế `log10` là `[MEASURED]`; loại hành động + cửa sổ `[UNKNOWN]` |
| **S-05** | `f5`/`f11` = 2 counter tần suất cao | `[ASSUMPTION]` | **REFINED** — cơ chế `ln` là `[MEASURED]`; §7.5 gợi ý lớp hành động **hẹp** |
| **S-06** | Cửa sổ 365 ngày cho `f5`/`f11`/`f18` | `[ASSUMPTION]` | **`[UNKNOWN]`** — không có evidence |
| **FE-1** | Tồn tại hoạt động không quan sát được | `[CONDITIONAL PROPOSITION]` | **UNRESOLVED** — không kiểm chứng được từ repo (§7.3). Suy ra được **nếu** giữ S-03 |

### 17.1 · ⚠️ Bằng chứng mới làm suy yếu S-01/S-02

`[MEASURED]` **`d1 = d2` ở 97.42% dòng** (902,731 / 926,669).

Dưới S-01/S-02, điều này nghĩa là **97.42% user có đúng MỘT đơn hàng trong đời**.
Với một sàn e-commerce, con số đó khó tin.

Đọc lại cho công bằng: `f1 = f2` ở 97.42% dòng chỉ nói hai đại lượng **gần như luôn
bằng nhau** và khác nhau đáng kể ở 2.58% (`avg(f1−f2) ≈ 123 ngày` trong nhóm đó).
Điều đó khớp với S-01/S-02 **chỉ khi** cohort này gồm phần lớn user một-đơn.

> **Ghi nhận, không kết luận.** Đây là lý do S-01/S-02 phải giữ nhãn `[ASSUMPTION]`.
> Cần thêm evidence để giữ hoặc bác.
>
> 🔎 Quan sát phụ (không phải bằng chứng): tỉ lệ "chỉ một lần" của `f18` là **95.5%**,
> của cặp `f1`/`f2` là **97.42%** — cùng bậc độ lớn. Có thể trùng hợp.

### 17.2 · `[UNKNOWN]` còn lại

- Semantic thật của cả 6 cột T1
- `n5`, `n11`, `n18` đếm **cái gì**; cửa sổ thời gian của chúng
- Vì sao dataset dùng **hai** cơ số log (`ln` vs `log10`)
- `f30` có thật sự là "ngày phân biệt" hay là một counter thường bị cap ở 30

---

## 18. Pilot experiment design

### 18.1 · Cấu hình

```
N          = 10,000 user
Nguồn      = full_trainset.csv, split='train' ONLY
Lấy mẫu    = ngẫu nhiên phân tầng theo n30 (đảm bảo phủ n30 = 1..30)
Seed       = cố định, ghi vào biz.generation_run
Scale      = KHÔNG production-scale
```

Phân tầng theo `n30` là quan trọng: mẫu ngẫu nhiên thuần sẽ thiếu user `n30` lớn,
mà đó chính là nhóm dễ fail capacity nhất.

### 18.2 · Chỉ số phải đo

| Nhóm | Chỉ số |
|---|---|
| **Khả thi** | `exactly_solvable` / `repairable` / `unsolvable` (số & %) |
| **Event** | tổng event; event/user (mean, p50, p99, max) |
| **Xung đột** | `Q_FORCED_DAYS` rate · `Q_CAPACITY` rate · **`f2 ↔ f30` conflict rate** |
| **GATE A** | pass rate tổng · **pass rate theo từng cột** (6 cột T1) |
| **Repair** | số vòng repair (mean, max); % user cần repair |
| **Regime** | `LN` pass ở `1e-15` · `LOG10` pass ở `==` |

### 18.3 · ★ Chỉ số chẩn đoán: `implied_unexplained_event_lower_bound`

> ⚠️ **Đã đổi tên.** Bản trước gọi là `free_event_ratio` — sai, vì nó giả định
> `FREE_EVENT` đã tồn tại. Chừng nào event taxonomy chưa được xác nhận (§7.3, §7.6),
> đại lượng này chỉ là **chẩn đoán dưới các assumption hiện tại**, không phải phép đếm
> một lớp event đã được chứng minh.

```
implied_unexplained_event_lower_bound
    = Σ_u max(0, n30(u) − capacity_upper_bound(u))
```

`[MEASURED]` dưới S-03 + capacity bound hiện tại:

```
≈ 2.39 triệu event
≈ 10% khối lượng event Track A đang đề xuất (≈ 23.1 triệu)
```

**Cách đọc đúng:** *"Dưới S-03 và P2, cần tối thiểu ~2.39M event mà mô hình hiện tại
không giải thích được."* Nó **không** nói "có 2.39M free event".

| Giá trị pilot | Diễn giải |
|---|---|
| Thấp (~10%) | Phần lớn event bị ràng buộc bởi dữ liệu ⇒ reconstruction mang nhiều thông tin |
| Cao (>50%) | Phần lớn event **không bị ràng buộc** ⇒ giá trị reconstruction thấp, bất kể GATE A pass |

Không nằm trong GATE A, nhưng quyết định reconstruction có ý nghĩa hay không.

### 18.4 · Đầu ra của pilot

Pilot **chốt** những con số hiện đang để trống:

```
[ ] ngưỡng quarantine (%)
[ ] K = số vòng repair tối đa
[ ] ngưỡng implied_unexplained_event_lower_bound chấp nhận được
[ ] quyết định giữ hay bác FE-1   ─┐ hai quyết định LIÊN ĐỚI:
[ ] quyết định giữ hay bác S-03   ─┘ không thể cùng bác (§7.1)
```

> Pilot **không** giải được §7.3 (event taxonomy của `f*` không quan sát được).
> Nó chỉ chốt được ta **chọn** nhánh nào của `¬P1 ∨ ¬P2`, và ghi lại lựa chọn đó
> như một `[ASSUMPTION]` tường minh.

> 🚫 **Không con số nào ở trên được đặt trước pilot.**

### 18.5 · Điều kiện dừng

```
Nếu unsolvable > 5% SAU KHI đã chấp nhận FE-1
    ⇒ DỪNG. Semantic assumption sai, không phải generator sai.
    ⇒ Báo cáo: UNSOLVABLE UNDER CURRENT SEMANTIC ASSUMPTIONS
    ⇒ KHÔNG sửa target. KHÔNG viết feature engine riêng.
```

Con số 5% ở đây là **ngưỡng dừng an toàn**, không phải ngưỡng chấp nhận — nó chỉ để
tránh chạy tiếp một hướng đã hỏng. Ngưỡng chấp nhận thật do §18.4 chốt.

---

## Liên quan

`RECONSTRUCTION_CONTRACT.md` · `DATA_GENERATION.md` · `FEATURE_LINEAGE.md` §12–13 ·
`FEATURE_DICTIONARY.md` · `AUDIT_KIEN_TRUC_VA_FEATURE.md`
