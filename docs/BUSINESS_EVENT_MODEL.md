# Business Event Model

> **Vai trò:** Layer 2 (Raw / Domain Event) — cầu nối `Operational DB → Feature Pipeline`.
>
> **Trạng thái:** đề xuất thiết kế. **Chưa sửa `schemas.py`.**
>
> **Nguyên tắc bất di bất dịch:**
> ```
> Raw Event ≠ Feature
> Event là SỰ KIỆN đã xảy ra, bất biến, có timestamp.
> Feature là HÀM của tập event, tính tại một thời điểm.
> ```

---

## Mục lục

1. [Nguyên tắc](#1-nguyên-tắc)
2. [Envelope chuẩn](#2-envelope-chuẩn)
3. [Danh mục event](#3-danh-mục-event)
4. [Ánh xạ transaction → event](#4-ánh-xạ-transaction--event)
5. [Customer journey theo thời gian](#5-customer-journey-theo-thời-gian)
6. [Kịch bản T0 → Tn cho online simulation](#6-kịch-bản-t0--tn-cho-online-simulation)
7. [So sánh với `schemas.AppEvent` hiện tại](#7-so-sánh-với-schemasappevent-hiện-tại)
8. [Ràng buộc topic & ordering](#8-ràng-buộc-topic--ordering)

---

## 1. Nguyên tắc

| # | Nguyên tắc | Vì sao |
|---|---|---|
| **E1** | Mọi event bắt nguồn từ **một transition trạng thái trong Layer 1** | Không có event "mồ côi" không tương ứng thay đổi nghiệp vụ nào |
| **E2** | Event mang **FK về entity**, không mang giá trị tính toán | `ORDER_PAID` mang `order_id` + `amount`, **không** mang `hist_gmv_30d` |
| **E3** | Event **bất biến**. Sửa = phát event mới | `ORDER_CANCELLED` chứ không phải update `ORDER_CREATED` |
| **E4** | `event_ts` = thời điểm **xảy ra**, `ingested_at` = thời điểm **nhận** | Point-in-time join dùng `event_ts`; đo lag dùng hiệu hai cái |
| **E5** | Event có **khoá dedup** (`event_id`) | Consumer at-least-once ⇒ trùng lặp là bình thường |
| **E6** | Event của cùng một customer phải **giữ thứ tự** | Kafka key = `customer_id` (repo hiện đã làm đúng) |
| **E7** | Đổi field ⇒ **bump `schema_version` + topic version**, không sửa tại chỗ | Đã là nguyên tắc trong `schemas.py` — giữ nguyên |

---

## 2. Envelope chuẩn

```jsonc
{
  // --- định danh & thời gian (bắt buộc cho MỌI event) ---
  "event_id":       "8f3a…",            // UUID, khoá dedup
  "event_type":     "ORDER_PAID",
  "event_ts":       1786531200.482,     // epoch giây, thời điểm XẢY RA
  "schema_version": 2,

  // --- chủ thể ---
  "customer_id":    "C0000123",         // luôn có
  "session_id":     "a91c…",            // null với event hệ thống
  "platform":       "android",

  // --- tham chiếu entity (chỉ điền cái liên quan) ---
  "sku_id":         null,
  "product_id":     null,
  "category_id":    null,
  "order_id":       "O0004412",         // ★ MỚI so với AppEvent
  "voucher_id":     null,               // ★ MỚI
  "issuance_id":    null,               // ★ MỚI
  "decision_id":    null,               // ★ MỚI — đóng closed loop

  // --- đại lượng ---
  "quantity":       0,
  "unit_price":     0.0,
  "amount":         458000.0,           // tổng tiền của event (order/payment)

  // --- payload tự do theo loại event ---
  "attrs": { "payment_method": "COD" }
}
```

> **Bốn field `★ MỚI`** là điều kiện cần để đóng closed loop §12 của project.
> Không có chúng, event `VOUCHER_CLAIMED` không truy được về treatment nào, và
> `ORDER_PAID` không truy được về voucher nào ⇒ không đo được uplift thật trong loop.

---

## 3. Danh mục event

### 3.1 · Nhóm SESSION & BROWSE

| Event | Sinh bởi (Layer 1) | Field bắt buộc thêm | Feature nào dùng |
|---|---|---|---|
| `SESSION_STARTED` | `SESSION.insert` | — | `rt_session_*`, tần suất phiên |
| `SESSION_ENDED` | `SESSION.ended_at` set | `attrs.duration_sec` | `rt_session_len_sec` |
| `SEARCH_PERFORMED` | (không đổi state) | `attrs.query`, `attrs.result_cnt` | ý định tìm kiếm |
| `PRODUCT_VIEWED` | (không đổi state) | `product_id`, `category_id` | `rt_page_view_1h`, category affinity |

> 🟨 `SEARCH_PERFORMED` là event **không có transition state** — ngoại lệ có ý thức
> với E1. Lý do: lưu mọi truy vấn tìm kiếm thành entity là phình ERD mà không thêm
> feature nào (ASSUMPTION A-05). Ghi nhận đây là ngoại lệ duy nhất.

### 3.2 · Nhóm CART

| Event | Sinh bởi | Field thêm | Feature |
|---|---|---|---|
| `ITEM_ADDED_TO_CART` | `CART_ITEM.insert` | `sku_id`, `quantity`, `unit_price` | `rt_add_to_cart_1h`, giá trị giỏ |
| `ITEM_REMOVED_FROM_CART` | `CART_ITEM.delete` | `sku_id` | tín hiệu do dự |
| `CART_VIEWED` | — | — | ý định checkout |

### 3.3 · Nhóm ORDER  *(đường sinh label)*

| Event | Sinh bởi | Field thêm | Ghi chú |
|---|---|---|---|
| `CHECKOUT_STARTED` | (bước UI) | `amount` (giá trị giỏ) | **Điểm quyết định voucher tự nhiên nhất** |
| `ORDER_CREATED` | `ORDER.insert` | `order_id`, `amount` | |
| `PAYMENT_INITIATED` | `PAYMENT.insert` | `order_id`, `attrs.method` | |
| `PAYMENT_FAILED` | `PAYMENT.status=FAILED` | `order_id`, `attrs.reason` | ma sát checkout |
| `PAYMENT_COMPLETED` | `PAYMENT.status=COMPLETED` | `order_id`, `amount` | |
| **`ORDER_PAID`** | `ORDER.status=PAID` | `order_id`, `amount`, `attrs.discount` | ★ **MỐC LABEL (A-01)** |
| `ORDER_CANCELLED` | `ORDER.status=CANCELLED` | `order_id` | |
| `SHIPMENT_CREATED` | `SHIPMENT.insert` | `order_id` | |
| `ORDER_DELIVERED` | `SHIPMENT.status=DELIVERED` | `order_id` | |
| `RETURN_REQUESTED` | `RETURN_LINE.insert` | `order_id` | |

> **`ORDER_CREATED` và `ORDER_PAID` là hai event khác nhau, cách nhau về thời gian.**
> Repo hiện tại chỉ có một event `order` — không phân biệt được đơn đã trả tiền
> hay chưa. Với COD phổ biến ở SEA, khoảng cách đó không hề nhỏ.

> ⚠️ Track B đang sinh event theo vocabulary business v2 (`SESSION_STARTED`,
> `PRODUCT_VIEWED`, `ITEM_ADDED_TO_CART`). Không publish các event này vào consumer v1
> hiện tại nếu chưa có schema/consumer v2; v1 sẽ reject vì `EVENT_TYPES` chỉ nhận
> `app_open`, `page_view`, `search`, `add_to_cart`, `checkout`, `order`,
> `voucher_view`, `voucher_claim`.

### 3.4 · Nhóm VOUCHER  *(đường sinh treatment)*

| Event | Sinh bởi | Field thêm | Ý nghĩa nhân quả |
|---|---|---|---|
| **`VOUCHER_ISSUED`** | `VOUCHER_ISSUANCE.insert` | `voucher_id`, `issuance_id`, `decision_id` | ★ **TREATMENT (A-02)** |
| `VOUCHER_VIEWED` | `ISSUANCE.status=VIEWED` | `issuance_id` | mediator |
| `VOUCHER_CLAIMED` | `VOUCHER_CLAIM.insert` | `issuance_id` | mediator |
| `VOUCHER_REDEEMED` | `VOUCHER_REDEMPTION.insert` | `issuance_id`, `order_id`, `amount` | mediator + chi phí thật |
| `VOUCHER_EXPIRED` | `ISSUANCE.status=EXPIRED` | `issuance_id` | |

> ⚠️ **`VOUCHER_ISSUED` hiện KHÔNG tồn tại trong repo.** `EVENT_TYPES` chỉ có
> `voucher_view` và `voucher_claim`. Nghĩa là **event mang treatment đang thiếu** —
> đây là gap của domain-event model; API/policy tạo treatment nằm ngoài scope.

### 3.5 · Nhóm DECISION  *(event hệ thống)*

| Event | Sinh bởi | Field thêm |
|---|---|---|
| `DECISION_MADE` | `DECISION.insert` | `decision_id`, `attrs.{uplift_score, decision, model_version, feature_version, policy_version, reason_code}` |

**Biện minh:** đây là event làm cho quyết định của model trở thành **dữ liệu quan sát
được trong chính stream**, không chỉ nằm trong Postgres. Nhờ nó, phân tích sau này
trả lời được: *"tại T=t, model thấy feature nào, chấm bao nhiêu điểm, và sau đó user
làm gì?"* — chỉ bằng cách đọc một stream duy nhất.

---

## 4. Ánh xạ transaction → event

Bảng dưới ánh xạ 1–1 với `ERD.md` §11.

### T1 · Browse → Cart

| # | Layer 1 | Event |
|---|---|---|
| 1 | `SESSION.insert` | `SESSION_STARTED` |
| 2 | — | `SEARCH_PERFORMED` |
| 3 | — | `PRODUCT_VIEWED` (×n) |
| 4 | `CART.upsert` + `CART_ITEM.insert` | `ITEM_ADDED_TO_CART` |

### T2 · Checkout → Paid

| # | Layer 1 | Event | Bất biến phải giữ |
|---|---|---|---|
| 1 | — | `CHECKOUT_STARTED` | `amount` = Σ cart_item |
| 2 | `ORDER.insert` + `ORDER_ITEM×n` | `ORDER_CREATED` | `amount` = `ORDER.gross_amount` |
| 3 | `VOUCHER_REDEMPTION.insert` | `VOUCHER_REDEEMED` | `amount` ≤ `VOUCHER.max_discount` |
| 4 | `CAMPAIGN.budget_spent +=` | — | `budget_spent ≤ budget_total` |
| 5 | `PAYMENT.insert` | `PAYMENT_INITIATED` | |
| 6 | `PAYMENT.status=COMPLETED` | `PAYMENT_COMPLETED` | `amount` = `ORDER.net_amount` |
| 7 | `ORDER.status=PAID` | **`ORDER_PAID`** | `paid_at ≥ created_at` |
| 8 | `CART_ITEM.delete` | `ITEM_REMOVED_FROM_CART` (×n) | |

### T3 · Decision → Issuance  *(closed loop)*

| # | Layer 1 | Event |
|---|---|---|
| 1 | `DECISION.insert` | `DECISION_MADE` |
| 2 | `VOUCHER_ISSUANCE.insert` | **`VOUCHER_ISSUED`** ← treatment |
| 3 | `ISSUANCE.status=VIEWED` | `VOUCHER_VIEWED` |
| 4 | `VOUCHER_CLAIM.insert` | `VOUCHER_CLAIMED` |
| 5 | → quay lại T2 | |

### T4 · Cancel / Return

| # | Layer 1 | Event |
|---|---|---|
| 1 | `ORDER.status=CANCELLED` | `ORDER_CANCELLED` |
| 2 | `RETURN_LINE.insert` | `RETURN_REQUESTED` |

---

## 5. Customer journey theo thời gian

```
 t
 │
 ├─ SESSION_STARTED ────────────────── SESSION.insert
 │
 ├─ SEARCH_PERFORMED  "tai nghe"
 │
 ├─ PRODUCT_VIEWED    sku=S00921
 ├─ PRODUCT_VIEWED    sku=S01144
 │
 ├─ ITEM_ADDED_TO_CART  sku=S00921 qty=1 ── CART_ITEM.insert
 │        │
 │        └──► rt_add_to_cart_1h += 1 ; giá trị giỏ = 458k
 │
 ├─ CHECKOUT_STARTED  amount=458000
 │        │
 │        └──►  ★ ĐIỂM QUYẾT ĐỊNH
 │                 downstream decision/policy component (ngoài scope)
 │
 ├─ DECISION_MADE   decision=SEND_VOUCHER  score=0.031
 ├─ VOUCHER_ISSUED  voucher=V_88_50K  issuance=I0091   ◄── TREATMENT
 │
 ├─ VOUCHER_VIEWED    issuance=I0091
 ├─ VOUCHER_CLAIMED   issuance=I0091   ── VOUCHER_CLAIM.insert
 │
 ├─ ORDER_CREATED     order=O4412  amount=458000
 ├─ VOUCHER_REDEEMED  order=O4412  amount=50000
 ├─ PAYMENT_INITIATED order=O4412
 ├─ PAYMENT_COMPLETED order=O4412  amount=408000
 ├─ ORDER_PAID        order=O4412                     ◄── LABEL = 1
 │
 ├─ SHIPMENT_CREATED  order=O4412
 └─ SESSION_ENDED
```

Không phải journey nào cũng đi hết. Các nhánh thoát hợp lệ:
`SESSION_ENDED` sau `PRODUCT_VIEWED` · `ORDER_CANCELLED` sau `ORDER_CREATED` ·
`VOUCHER_EXPIRED` sau `VOUCHER_ISSUED` · `PAYMENT_FAILED` rồi thử lại.

---

## 6. Kịch bản T0 → Tn cho online simulation

Đây là thứ scenario driver sẽ chạy. **Yêu cầu: mỗi mốc có event cụ thể và có thể
quan sát được feature + decision thay đổi.**

### 6.1 · Kịch bản S1 — "warming up" (kỳ vọng score tăng)

| Mốc | Δt | Event | Feature đổi | Quan sát |
|---|---|---|---|---|
| **T0** | 0 | *(chưa có event)* | tất cả `rt_* = 0` | `score₀`, `decision₀` |
| **T1** | +2' | `SESSION_STARTED`, `PRODUCT_VIEWED ×3` | `rt_events_1h`=4, `rt_page_view_1h`=3 | `score₁` |
| **T2** | +5' | `ITEM_ADDED_TO_CART ×2` | `rt_add_to_cart_1h`=2, `rt_events_1h`=6 | `score₂` |
| **T3** | +8' | `CHECKOUT_STARTED` | `rt_events_1h`=7 | `score₃` |
| **T4** | +9' | *(decision)* | — | **kỳ vọng `SEND_VOUCHER`** |
| **T5** | +12' | `VOUCHER_CLAIMED`, `ORDER_PAID` | `rt_order_1h`=1, `rt_gmv_1h`=458k | `score₅` |

**Assert:** `score₀ < score₃`. Nếu không đổi ⇒ gặp đúng rủi ro **G2** trong audit —
model không nhạy với feature event-derived, scenario vô nghĩa. **Phải kiểm trước
khi xây driver, không phải sau.**

### 6.2 · Kịch bản S2 — "cooling down" (kỳ vọng score giảm)

Bắt đầu từ trạng thái cuối của S1, **không** sinh event nào trong 65 phút, gọi
đánh giá state lại ở downstream. Các `rt_*` phải rơi về 0 theo cửa sổ trượt 1h.

**Assert:** `score_sau ≈ score₀`. Đây là bài test **cửa sổ trượt hoạt động đúng** —
và cần điều khiển được thời gian (gap **G6**).

### 6.3 · Kịch bản S3 — "closed loop" (bài test §12 thật sự)

```
T0  user duyệt                    → score thấp   → NO_VOUCHER
T1  user thêm giỏ 3 món           → score tăng   → SEND_VOUCHER
                                                 → VOUCHER_ISSUED ──┐
T2  event VOUCHER_ISSUED quay về Kafka ◄──────────────────────────┘
T3  feature `rt_voucher_active` = 1  (feature MỚI, xem FEATURE_DICTIONARY §5)
T4  downstream policy đánh giá lại state
                → reason_code = COOLDOWN → NO_VOUCHER
T5  user claim + mua              → ORDER_PAID
T6  feature cập nhật, campaign budget_spent tăng
```

> S3 là kịch bản **duy nhất** thực sự chứng minh closed loop. Nó đòi hỏi ba thứ hiện
> chưa có: event `VOUCHER_ISSUED`, đường quay ngược decision → Kafka, và policy có
> trạng thái (cooldown).

---

## 7. So sánh với `schemas.AppEvent` hiện tại

### 7.1 · Event type

| Hiện có (`EVENT_TYPES`) | Trong model này | Nhận xét |
|---|---|---|
| `app_open` | `SESSION_STARTED` | Đổi tên, cùng ý nghĩa |
| `page_view` | `PRODUCT_VIEWED` | |
| `search` | `SEARCH_PERFORMED` | |
| `add_to_cart` | `ITEM_ADDED_TO_CART` | |
| `checkout` | `CHECKOUT_STARTED` | |
| `order` | **tách thành** `ORDER_CREATED` + `ORDER_PAID` | ⚠️ Một event đang gộp hai sự kiện nghiệp vụ khác nhau |
| `voucher_view` | `VOUCHER_VIEWED` | |
| `voucher_claim` | `VOUCHER_CLAIMED` | |
| — | **`VOUCHER_ISSUED`** | ❌ **THIẾU — đây là event mang treatment** |
| — | `DECISION_MADE` | ❌ THIẾU — loop hở |
| — | `SESSION_ENDED` | ❌ THIẾU — `rt_session_len_sec` không có nguồn online |
| — | `ITEM_REMOVED_FROM_CART`, `PAYMENT_*`, `ORDER_CANCELLED`, `SHIPMENT_CREATED`, `RETURN_REQUESTED` | Bổ sung |

### 7.2 · Field

| Field | `AppEvent` | Envelope mới | Ghi chú |
|---|---|---|---|
| `event_id`, `user_id`, `event_type`, `event_ts`, `session_id`, `platform` | ✅ | ✅ | Giữ nguyên (`user_id` → `customer_id`) |
| `item_id` | ✅ TEXT tự do | `sku_id` FK | Phải trỏ SKU có thật |
| `category_id` | ✅ ngẫu nhiên | FK, **suy ra từ product** | Hiện đang bốc random, không liên quan `item_id` |
| `price` | ✅ random mỗi event | `unit_price` từ `SKU.list_price` | ⚠️ Hiện cùng item có 2 giá khác nhau |
| `quantity` | ✅ | ✅ | |
| `order_id` | ❌ | ✅ | **Chặn closed loop** |
| `voucher_id` / `issuance_id` | ❌ | ✅ | **Chặn closed loop** |
| `decision_id` | ❌ | ✅ | **Chặn closed loop** |
| `amount` | ❌ | ✅ | GMV hiện tính `price × quantity` ở dbt |
| `attrs` | ❌ | ✅ JSON | |

### 7.3 · Đánh giá công bằng về code hiện tại

Những thứ trong `schemas.py` / `stream_consumer.py` **đúng và phải giữ nguyên**:

- `validate_event()` + DLQ theo `reason` — đúng chuẩn
- `REQUIRED_FIELDS` tối thiểu, phần còn lại nullable — đúng
- Kafka key = user_id ⇒ ordering trong phạm vi user — đúng (E6)
- Dedup theo `event_id` ở `stg_app_events` — đúng (E5)
- Comment "đổi field = đổi version topic" — đúng nguyên tắc (E7), và migration này
  **phải tuân theo chính nó**: `v1 → v2`, không sửa tại chỗ

---

## 8. Ràng buộc topic & ordering

### 8.1 · Topic

| Topic | Nội dung | Key | 🟨 Đề xuất |
|---|---|---|---|
| `app.user.events.v1` | *(hiện tại)* — giữ chạy song song | `user_id` | Không đụng |
| `app.user.events.v2` | Envelope mới §2 | `customer_id` | Mới |
| `biz.decision.v1` | `DECISION_MADE` | `customer_id` | Mới |
| `biz.voucher.v1` | `VOUCHER_ISSUED/VIEWED/CLAIMED/REDEEMED` | `customer_id` | Mới |
| `app.user.events.dlq` | *(hiện tại)* | — | Giữ |

**Vì sao tách topic voucher/decision khỏi topic app:** hai luồng có nguồn phát khác
nhau (server-side vs client-side), tần suất khác nhau (thấp vs cao), và độ quan trọng
khác nhau (mất một `PRODUCT_VIEWED` không sao; mất một `VOUCHER_ISSUED` là mất một
bản ghi treatment). 🟦 Đây là INFERENCE về vận hành, không phải fact về Lazada.

### 8.2 · Ràng buộc bắt buộc — điều cấm số 6 của project

```
❌ SAI:  simulator ──────────────► sửa trực tiếp downstream state
✅ ĐÚNG: simulator ──► Kafka ──► stream processor ──► lake/MinIO ──► downstream state
```

Áp dụng **cả cho đường quay ngược của decision**:

```
❌ SAI:  decision component ──► sửa trực tiếp event history
✅ ĐÚNG: decision component ──► phát domain event có provenance
                  ──► publish DECISION_MADE + VOUCHER_ISSUED vào Kafka
                         ──► stream processor ──► lake/MinIO ──► downstream state
```

Chỉ đường thứ hai mới cho parity offline/online, vì lúc train ta cũng đọc chính
những event đó từ lake.

---

## Tiếp theo

- `FEATURE_DICTIONARY.md` — event nào sinh feature nào, với confidence
- `MIGRATION_PLAN.md` M3 — publish event v2 mà không làm hỏng pipeline đang chạy
