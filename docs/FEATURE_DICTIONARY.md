# Feature Dictionary

> **Vai trò:** Layer 3 (Feature Engineering / Feature Store) — hợp đồng của từng feature.
>
> **Trạng thái:** phần "hiện có" mô tả code đang chạy; phần "đề xuất" chưa code.
>
> **Nguyên tắc §15 của project:** mỗi feature có đủ 13 trường lineage.
> Feature chưa biết semantic ⇒ `business_definition: UNKNOWN`, `confidence: UNKNOWN`.
> **Không bịa.**

---

## Mục lục

1. [Thang confidence](#1-thang-confidence)
2. [Tổng quan 94 feature hiện có](#2-tổng-quan-94-feature-hiện-có)
3. [Track A — `f0`..`f82` (opaque)](#3-track-a--f0f82-opaque)
4. [Track B — feature event-derived hiện có](#4-track-b--feature-event-derived-hiện-có)
5. [Track C — feature đề xuất từ synthetic business system](#5-track-c--feature-đề-xuất-từ-synthetic-business-system)
6. [Lỗi contract phát hiện được](#6-lỗi-contract-phát-hiện-được)
7. [Feature bị loại và lý do](#7-feature-bị-loại-và-lý-do)

---

## 1. Thang confidence

| Mức | Định nghĩa | Được phép làm gì |
|---|---|---|
| **CONFIRMED** | Có công thức trong repo **hoặc** đo trực tiếp trên dữ liệu | Dùng tự do, viết test parity |
| **INFERRED** | Suy luận hợp lý, chưa chứng minh | Ghi vào doc, **không** hard-code semantic vào tên biến |
| **SYNTHETIC** | Do project thiết kế, lineage đầy đủ nhưng không đối chiếu được với LZD | Dùng tự do trong synthetic system |
| **UNKNOWN** | Không có bằng chứng | **Chỉ** seed nguyên trạng. Không diễn giải, không mô phỏng, không đặt tên gợi ý |

---

## 2. Tổng quan 94 feature hiện có

`spec.all_names` = 87 batch + 7 realtime = **94**.

| Track | Số cột | Nguồn | Lineage | Confidence chủ đạo |
|---|---|---|---|---|
| **A** — `f0`–`f82` | 83 | CSV DESCN | ❌ không có | UNKNOWN |
| **B** — `hist_*`/`tenure`/`voucher_used` | 4 | Event → dbt batch | ✅ đầy đủ | CONFIRMED (2 cột có lỗi contract) |
| **B** — `rt_*` | 7 | Event → stream + dbt PIT | ✅ đầy đủ | CONFIRMED (1 cột skew) |

---

## 3. Track A — `f0`..`f82` (opaque)

### 3.1 · Bản ghi lineage chung

```yaml
feature_name:        f0 .. f82        # 83 cột
business_definition: UNKNOWN
source_entity:       UNKNOWN          # nằm ngoài repo, bên trong Lazada
source_field:        UNKNOWN
source_event:        UNKNOWN
transformation:      UNKNOWN          # stg_user_snapshot.sql CHỈ cast(... as double)
window:              UNKNOWN
aggregation:         UNKNOWN
encoding:            UNKNOWN
batch_or_realtime:   BATCH (seed tĩnh — không bao giờ cập nhật)
offline_definition:  đọc nguyên trạng từ marts.feat_user_serving
online_definition:   đọc nguyên trạng từ Redis fs:{ver}:u:{uid}
confidence:          UNKNOWN
```

> **Ràng buộc thực thi:** không cột `f*` nào được đặt alias mang nghĩa nghiệp vụ ở
> bất kỳ đâu trong code. Chúng là **vector trạng thái lịch sử mờ**.

### 3.2 · Cấu trúc đo được (CONFIRMED — không phải semantic)

Đây là **tính chất thống kê**, không phải nghĩa nghiệp vụ. Nguồn:
`AUDIT_KIEN_TRUC_VA_FEATURE.md` §5.2 + đo bổ sung trong session này.

| Cột | Cấu trúc đo được | Bằng chứng | Confidence |
|---|---|---|---|
| `f40`–`f78` | **11 nhóm one-hot loại trừ**, `row_sum = 11.0` ở 100% dòng | 926,669/926,669 | CONFIRMED |
| `f68 ≡ f71 ≡ f77`, `f74` lệch 2 dòng | 8 cột mã hoá **1 bit** | `sum(abs(a−b)) = 0` | CONFIRMED |
| `f70` | **hằng số 1** — zero information | `min=max=1` | CONFIRMED |
| `f23`, `f25` | trùng nhau 99.78%, `corr = 0.999957` | query trực tiếp | CONFIRMED |
| `f79`–`f82` | 4 mã hoá của **cùng 1 biến 515 mức**, phân bố tần suất trùng khít | 663,495 / 28,969 / 3,947 giống hệt cả 4 cột | CONFIRMED |
| `f1`, `f2` | nguyên `[0,365]`, 366 giá trị, `f1 ≥ f2` luôn đúng | `min(f1−f2)=0` | CONFIRMED |
| `f30` | `10^f30` = số nguyên `[1,30]` ở **100%** dòng (tol. tương đối 1e-4); lưu 6 chữ số thập phân | đo lại trên 926,669 dòng — **nâng I2 → CONFIRMED** | CONFIRMED (cấu trúc) |
| `f27`, `f34` | nguyên `[0,100]`, 32 và 31 giá trị | query | CONFIRMED |

**Sau khi gộp trùng:** `f40`–`f78` (39 cột) chỉ mang **7 biến categorical thực sự**.

### 3.3 · Điều KHÔNG được suy ra từ §3.2

| Cấu trúc đo được | ❌ Kết luận **cấm** | Vì sao |
|---|---|---|
| `f30 = log10(n)`, `n ∈ [1,30]` | ~~"số đơn 30 ngày"~~ | Cũng khớp: số ngày hoạt động, số phiên, số sản phẩm xem — vô số khả năng |
| 1 biến 515 mức | ~~"category_id"~~ | Cũng khớp: thành phố, brand, seller cluster |
| `f1`,`f2` ∈ `[0,365]`, `f1 ≥ f2` | ~~"days_since_signup / days_since_last_order"~~ | Chỉ biết **có thứ tự**, không biết là gì |
| `f27`,`f34` ∈ `[0,100]` | ~~"điểm loyalty"~~ | Percentile? điểm? tỉ lệ? — không phân biệt được |

Cả bốn suy luận đều **hợp lý** và cả bốn đều **không có bằng chứng**.
Điều cấm số 1 của project áp dụng.

---

## 4. Track B — feature event-derived hiện có

### 4.1 · `user_tenure_days` ⚠️ **CÓ LỖI CONTRACT**

```yaml
feature_name:        user_tenure_days
business_definition: "Số ngày kể từ lần đầu nhìn thấy user"   # theo feature_spec.yml
source_entity:       EVENT
source_field:        event_ts
source_event:        (mọi loại)
transformation:      date_diff('day', min(event_ts), now())
window:              30 ngày   ← ★ ĐÂY LÀ VẤN ĐỀ
aggregation:         MIN → DATE_DIFF
encoding:            none
batch_or_realtime:   BATCH
offline_definition:  dbt/models/marts/feat_user_behaviour.sql
online_definition:   seed từ batch, không cập nhật
confidence:          CONFIRMED (công thức) / ⚠️ SAI so với business_definition
```

> **Lỗi:** CTE `events` lọc `event_ts >= now() - interval '30 days'` **trước** khi
> tính `min(event_ts)`. Nên `user_tenure_days` **bị chặn trên ở 30** — nó là
> *"số ngày kể từ event đầu tiên trong 30 ngày gần nhất"*, không phải tuổi tài khoản.
>
> **Sửa đúng:** lấy từ `CUSTOMER.registered_at` (ERD §3.1), không lấy từ event.
> Tuổi tài khoản là thuộc tính của entity, không phải hàm của event window.

### 4.2 · `hist_order_cnt_30d`

```yaml
business_definition: "Số đơn trong 30 ngày gần nhất"
source_entity:       ORDER  (qua EVENT)
source_event:        order            → ĐỀ XUẤT ĐỔI: ORDER_PAID
transformation:      count(*) filter (where event_type = 'order')
window:              30 ngày (var history_days)
aggregation:         COUNT
batch_or_realtime:   BATCH
offline_definition:  feat_user_behaviour.sql
online_definition:   seed từ batch, KHÔNG cập nhật realtime (đúng — lambda)
confidence:          CONFIRMED
```

> 🟦 **Đề xuất:** đổi nguồn sang `ORDER_PAID` thay vì `order`. Hiện tại `order` không
> phân biệt đơn đã trả tiền hay chưa (`BUSINESS_EVENT_MODEL.md` §7.1) ⇒ đang đếm cả
> đơn sẽ bị huỷ.

### 4.3 · `hist_gmv_30d`

```yaml
business_definition: "GMV 30 ngày gần nhất"
source_entity:       ORDER_ITEM (qua EVENT)
source_field:        price, quantity
transformation:      sum(price * quantity) filter (event_type='order')
                     -- gmv định nghĩa tại stg_app_events.sql
window:              30 ngày
aggregation:         SUM
batch_or_realtime:   BATCH
confidence:          CONFIRMED (công thức) / ⚠️ dữ liệu không nhất quán
```

> ⚠️ **Vấn đề nguồn:** `price` do `event_producer` sinh ngẫu nhiên **cho từng event**
> (`lognormvariate(3.2, 0.8)`). Cùng một `item_id` có giá khác nhau ở hai event khác
> nhau. GMV vì vậy không truy ngược về catalog nào. Sau khi có SKU (ERD §3.4) thì
> `unit_price` lấy từ `SKU.list_price` và GMV trở nên kiểm chứng được.

### 4.4 · `voucher_used_30d` ⚠️ **TÊN SAI SO VỚI CÔNG THỨC**

```yaml
business_definition: "Số voucher đã dùng 30 ngày"    # theo feature_spec.yml
transformation:      count(*) filter (where event_type = 'voucher_claim')   # ← đếm CLAIM
window:              30 ngày
confidence:          CONFIRMED (công thức) / ⚠️ SAI so với business_definition
```

> **CLAIM ≠ REDEEM.** Thu thập voucher là *ý định*; dùng voucher là *giao dịch có
> `order_id`* (`LAZADA_BUSINESS_DOMAIN.md` §7.2). Đây là hai feature khác nhau về
> sức dự báo và nên tồn tại song song:
>
> | Feature | Nguồn đúng |
> |---|---|
> | `voucher_claimed_30d` | `VOUCHER_CLAIMED` ← đây là thứ đang được tính |
> | `voucher_redeemed_30d` | `VOUCHER_REDEEMED` ← đây là thứ tên đang hứa |
>
> **Không đổi tên cột hiện có mà không bump `feature_spec.version`** (điều cấm số 10).
> Xem `MIGRATION_PLAN.md` §4.

### 4.5 · Nhóm `rt_*` (7 cột)

Cửa sổ 1h, chia **12 ô 5 phút** ở cả hai phía — đây là thiết kế chống skew rất tốt
đã có sẵn, giữ nguyên.

| Feature | source_event | aggregation | offline | online | confidence |
|---|---|---|---|---|---|
| `rt_events_1h` | mọi event | `COUNT(*)` | `feat_user_realtime_pit.sql` | `_update_realtime()` | **CONFIRMED** |
| `rt_page_view_1h` | `page_view` | `COUNT filter` | ✅ | ✅ `EVENT_TO_COUNTER` | **CONFIRMED** |
| `rt_add_to_cart_1h` | `add_to_cart` | `COUNT filter` | ✅ | ✅ | **CONFIRMED** |
| `rt_order_1h` | `order` | `COUNT filter` | ✅ | ✅ | **CONFIRMED** |
| `rt_gmv_1h` | `order` | `SUM(price×qty)` | ✅ | ✅ | **CONFIRMED** |
| `rt_session_len_sec` | mọi event | `date_diff('second', min, max)` | ✅ | ❌ **KHÔNG GHI** | ⚠️ **SKEW** |
| `rt_last_event_ts` | mọi event | `MAX(event_ts)` | ✅ | ✅ (set, không cộng dồn) | **CONFIRMED** |

#### `rt_session_len_sec` — training/serving skew có thật

```yaml
offline_definition:  date_diff('second', min(event_ts), max(event_ts))
                     trên cửa sổ 12 ô × 5 phút   → GIÁ TRỊ THẬT
online_definition:   (không tồn tại)             → LUÔN NHẬN DEFAULT 0.0
confidence:          CONFIRMED là có skew
```

`stream_consumer._update_realtime()` chỉ ghi `rt_events_1h`, 3 counter trong
`EVENT_TO_COUNTER`, `rt_gmv_1h`, `rt_last_event_ts`. **Không có** `rt_session_len_sec`.
Đây là audit C16 — độc lập với mọi kế hoạch mô phỏng, sửa rẻ, giá trị ngay.

> 🟦 **Cách sửa đúng về nghiệp vụ:** khi có entity SESSION (ERD §4.1), độ dài phiên
> tính từ `SESSION.started_at` chứ không từ `min(event_ts)` trong cửa sổ 1h. Hai công
> thức cho kết quả khác nhau với phiên kéo dài quá 1 giờ. **Chốt một định nghĩa duy
> nhất trước khi sửa**, nếu không sẽ sửa skew này bằng cách tạo ra skew khác.

### 4.6 · Vấn đề point-in-time của nhóm batch — **phát hiện mới**

| | Cửa sổ tính tại | Point-in-time? |
|---|---|---|
| `rt_*` (`feat_user_realtime_pit.sql`) | `feature_ts` của snapshot, làm tròn ô 5 phút | ✅ Đúng — `e.event_ts < s.feature_ts` |
| `hist_*`, `user_tenure_days` (`feat_user_behaviour.sql`) | **`now()` lúc dbt build** | ❌ **Không** |

`seed_loader.py` gán `feature_ts = now()` lúc nạp CSV. dbt chạy **sau đó**. Nên
`feat_user_behaviour` gộp cả event xảy ra **sau** `feature_ts` — tức là **sau mốc
point-in-time mà `rt_*` tôn trọng**.

Hệ quả: trong cùng một dòng `marts.training_dataset`, `rt_*` tôn trọng biên PIT còn
`hist_*` thì không. Mức nghiêm trọng phụ thuộc khoảng cách seed → dbt build; hiện
tại nhỏ vì user seed chưa có event nào (gap **G4**), nhưng sẽ thành leakage thật ngay
khi backfill event lịch sử.

**Sửa:** `feat_user_behaviour` phải join với snapshot và dùng `feature_ts` làm mốc,
đúng như `feat_user_realtime_pit` đang làm.

---

## 5. Track C — feature đề xuất từ synthetic business system

Đây là feature **có lineage đầy đủ từ Layer 1**, do synthetic business system sinh ra.
Tất cả ở mức `SYNTHETIC` — nghĩa là hợp lệ và kiểm chứng được **trong hệ thống này**,
nhưng không tuyên bố tương ứng với bất kỳ `f*` nào.

> **Chưa cột nào được thêm vào `feature_spec.yml`.** Danh sách này là đề xuất để bàn.
> Thêm feature ⇒ bump `feature_spec.version` ⇒ train lại. Xem `MIGRATION_PLAN.md` §4.

### 5.1 · Profile (từ `CUSTOMER`, batch)

| Feature | source | transformation | window | encoding |
|---|---|---|---|---|
| `cust_tenure_days_true` | `CUSTOMER.registered_at` | `date_diff('day', registered_at, feature_ts)` | — | none |
| `cust_member_tier` | `CUSTOMER.member_tier` | — | — | **one-hot (4)** |
| `cust_city_tier` | `CUSTOMER.city_tier` | — | — | ordinal |
| `cust_country` | `CUSTOMER.country` | — | — | one-hot (6) |

### 5.2 · RFM (từ `ORDER`, batch)

| Feature | source_event | aggregation | window |
|---|---|---|---|
| `recency_days_since_last_order` | `ORDER_PAID` | `date_diff('day', max(event_ts), feature_ts)` | 365d |
| `frequency_orders_90d` | `ORDER_PAID` | `COUNT` | 90d |
| `monetary_gmv_90d` | `ORDER_PAID` | `SUM(amount)` | 90d |
| `avg_order_value_90d` | `ORDER_PAID` | `SUM(amount)/COUNT` | 90d |
| `log_orders_90d` | ↑ | `log10(1 + frequency_orders_90d)` | 90d |

> `log_orders_90d` dùng log10 vì đó là dạng biến đổi **đã quan sát được** trong dataset
> (`f30`). Đây là quyết định thiết kế lấy cảm hứng từ cấu trúc LZD — ✅ hợp lệ.
> ❌ Nó **không** tuyên bố `log_orders_90d` tương ứng `f30`.

### 5.3 · Cart / intent (từ `CART`, gần realtime)

| Feature | source | window | mode |
|---|---|---|---|
| `cart_value_current` | `CART_ITEM × SKU.list_price` | trạng thái hiện tại | REALTIME |
| `cart_item_cnt_current` | `CART_ITEM` | hiện tại | REALTIME |
| `cart_age_hours` | `min(CART_ITEM.added_at)` | hiện tại | REALTIME |
| `cart_abandoned_cnt_30d` | giỏ có item nhưng không `ORDER_CREATED` | 30d | BATCH |

> **Đây là nhóm có sức dự báo uplift cao nhất về mặt nghiệp vụ** — user có giỏ giá
> trị cao để lâu chưa mua chính là *persuadable* điển hình
> (`LAZADA_BUSINESS_DOMAIN.md` §8.1). Không nhóm feature nào hiện có nắm được tín hiệu này.

### 5.4 · Voucher (từ Domain D)

| Feature | source_event | window | mode | Ghi chú |
|---|---|---|---|---|
| `voucher_issued_30d` | `VOUCHER_ISSUED` | 30d | BATCH | lịch sử **treatment** |
| `voucher_claimed_30d` | `VOUCHER_CLAIMED` | 30d | BATCH | = `voucher_used_30d` hiện tại |
| `voucher_redeemed_30d` | `VOUCHER_REDEEMED` | 30d | BATCH | thứ tên cũ đang hứa |
| `voucher_claim_rate_30d` | ↑ | 30d | BATCH | `claimed / issued`, ratio |
| `voucher_redeem_rate_30d` | ↑ | 30d | BATCH | `redeemed / claimed` |
| `rt_voucher_active_cnt` | `VOUCHER_ISSUED` − `REDEEMED`/`EXPIRED` | hiện tại | **REALTIME** | ★ policy cần cho cooldown |
| `days_since_last_voucher` | `VOUCHER_ISSUED` | 365d | BATCH | |

> ⚠️ **Cảnh báo nhân quả:** `voucher_*_30d` là **lịch sử treatment**. Đưa chúng vào
> model uplift phải cân nhắc — nếu chính sách phát voucher trong quá khứ đã targeted
> (như train set của LZD), thì các feature này mã hoá luôn *chính sách cũ*, và model
> có thể học "ai từng được phát thì phát tiếp" thay vì học uplift thật. Cần
> ablation trước khi đưa vào production. Ghi nhận, chưa quyết.

### 5.5 · Realtime mở rộng

| Feature | source_event | window | Ghi chú |
|---|---|---|---|
| `rt_checkout_started_1h` | `CHECKOUT_STARTED` | 1h | tín hiệu ý định mạnh nhất |
| `rt_search_1h` | `SEARCH_PERFORMED` | 1h | |
| `rt_cart_remove_1h` | `ITEM_REMOVED_FROM_CART` | 1h | do dự |
| `rt_payment_failed_1h` | `PAYMENT_FAILED` | 1h | ma sát |
| `rt_distinct_category_1h` | `PRODUCT_VIEWED` | 1h | `COUNT(DISTINCT category_id)` — duyệt rộng vs sâu |

Tất cả tuân đúng cơ chế **12 ô × 5 phút** đang có ⇒ parity offline/online miễn phí.
Ngoại lệ: `rt_distinct_category_1h` dùng `COUNT(DISTINCT)` **không cộng dồn được**
qua các ô — cần HyperLogLog trên Redis hoặc chấp nhận xấp xỉ. **Ghi nhận là feature
khó, cân nhắc bỏ nếu không muốn thêm phức tạp.**

---

## 6. Lỗi contract phát hiện được

Tổng hợp — xếp theo mức độ.

| # | Feature | Vấn đề | Mức | Sửa ở đâu |
|---|---|---|---|---|
| **D1** | `rt_session_len_sec` | Offline tính thật, online luôn 0 ⇒ **skew** | 🔴 | `stream_consumer._update_realtime()` |
| **D2** | `user_tenure_days` | Bị chặn ở 30 do lọc cửa sổ trước khi `min()`; không phải tuổi tài khoản | 🟠 | `feat_user_behaviour.sql` + cần `CUSTOMER.registered_at` |
| **D3** | `voucher_used_30d` | Tên nói "used", công thức đếm **claim** | 🟠 | Đặt lại tên khi bump spec version |
| **D4** | `hist_*` không point-in-time | Dùng `now()` lúc dbt build thay vì `feature_ts` ⇒ leakage khi có event lịch sử | 🟠 | `feat_user_behaviour.sql` |
| **D5** | `hist_gmv_30d`, `rt_gmv_1h` | `price` ngẫu nhiên mỗi event, không truy về catalog | 🟡 | Cần SKU (ERD §3.4) |
| **D6** | `f70` | Hằng số 1 — zero information, vẫn chiếm 1 cột trong 94 | 🟡 | Loại khi bump spec |
| **D7** | `f23`/`f25`, `f68`/`f71`/`f74`/`f77` | Trùng lặp đo được ⇒ importance đếm hai lần | 🟡 | Loại khi bump spec, **cần ablation trước** (điều cấm số 5) |

> **Không cột nào bị xoá trong tài liệu này.** Điều cấm số 5 của project: không xoá/gộp
> feature chỉ vì correlation cao mà chưa có baseline/ablation evidence. D6 là ngoại lệ
> hiển nhiên (`min = max = 1` ⇒ zero information theo định nghĩa), nhưng vẫn phải bump
> `feature_spec.version` chứ không sửa lén.

---

## 7. Feature bị loại và lý do

| Ứng viên | Lý do không đưa vào |
|---|---|
| Feature từ `REVIEW`/`RATING` | Entity đã bị loại (A-05) |
| `shipment_delay_days` | Xảy ra **sau** điểm quyết định ⇒ không dùng được lúc inference |
| `return_rate_90d` | Cần RETURN có dữ liệu thật; hiện RETURN chỉ để đóng lifecycle (ERD §5.4) |
| Embedding sản phẩm / text search | Vượt scope; thêm một hạ tầng model thứ hai |
| Feature mức `(user × category)` | Đổi entity key của feature store ⇒ thay đổi lớn (A-03) |

---

## Tiếp theo

- `FEATURE_LINEAGE.md` — sơ đồ đường đi đầy đủ của từng nhóm
- `MIGRATION_PLAN.md` §4 — thứ tự bump `feature_spec.version` an toàn
