# Lazada / E-commerce Business Domain

> Business alias note: `config/features/business_aliases.yml` maps selected `f*`
> columns to synthetic voucher-uplift business names. Those aliases are for this
> project's story only; they are not confirmed Lazada/DESCN semantics and should
> not be treated as facts about Lazada internal data.

> **Vai trò của tài liệu:** Layer 1 (Business / Operational) trong mental model
> `Business → Raw/Event → Feature → Model`.
>
> **Trạng thái:** thiết kế, chưa code.
>
> **Nguyên tắc:** mỗi phát biểu đều có nhãn.
>
> | Nhãn | Nghĩa |
> |---|---|
> | 🟩 **FACT** | Có nguồn công khai xác nhận. Có link. |
> | 🟦 **INFERENCE** | Suy luận nghiệp vụ e-commerce hợp lý, không có nguồn trực tiếp. |
> | 🟨 **SYNTHETIC** | Project tự thiết kế để mô phỏng. **Không phải Lazada thật.** |
>
> ⚠️ Không đoạn nào trong tài liệu này mô tả database nội bộ thật của Lazada.
> Toàn bộ phần 🟨 là **synthetic reconstruction**.

---

## Mục lục

1. [Ranh giới tài liệu](#1-ranh-giới-tài-liệu)
2. [Mô hình marketplace](#2-mô-hình-marketplace)
3. [Catalog: seller → store → product → SKU](#3-catalog-seller--store--product--sku)
4. [Customer journey](#4-customer-journey)
5. [Order lifecycle](#5-order-lifecycle)
6. [Payment lifecycle](#6-payment-lifecycle)
7. [Voucher & campaign — trọng tâm của bài toán](#7-voucher--campaign--trọng-tâm-của-bài-toán)
8. [Bài toán voucher uplift decisioning](#8-bài-toán-voucher-uplift-decisioning)
9. [Những gì dataset LZD nói về nghiệp vụ này](#9-những-gì-dataset-lzd-nói-về-nghiệp-vụ-này)
10. [Danh sách ASSUMPTION](#10-danh-sách-assumption)

---

## 1. Ranh giới tài liệu

```
   Nghiệp vụ Lazada công khai            Dataset LZD (DESCN)
   (web, seller docs, paper)             f0..f82 + label + is_treat
            │                                     │
            │  research                           │  forensic (đã làm)
            ▼                                     ▼
   ┌──────────────────────────┐          ┌──────────────────────┐
   │ FACT + INFERENCE         │          │ Đặc trưng thống kê   │
   └───────────┬──────────────┘          └──────────┬───────────┘
               │                                    │
               └────────────┬───────────────────────┘
                            ▼
                 🟨 SYNTHETIC BUSINESS SYSTEM
                    (Layer 1 của project này)
```

Tài liệu này sinh ra Layer 1. Nó **không** nhận `f0..f82` làm đầu vào thiết kế
(xem [§9](#9-những-gì-dataset-lzd-nói-về-nghiệp-vụ-này) — dataset chỉ ràng buộc
được rất ít về nghiệp vụ), và nó **không** đặt `f0..f82` vào bất kỳ entity nào.

---

## 2. Mô hình marketplace

### 2.1 · Ba kênh bán

🟩 **FACT** — Lazada chạy mô hình marketplace lai (hybrid), gồm ba nhánh:

| Kênh | Ai giữ hàng | Ai bán | Ghi chú |
|---|---|---|---|
| **Marketplace (3P)** | Seller | Seller | Seller tự đăng bán và tự fulfil |
| **Retail (1P)** | Lazada | Lazada | Lazada mua hàng về bán trực tiếp |
| **Cross-border / LazGlobal** | Seller nước ngoài | Seller | Chủ yếu từ Trung Quốc qua mạng lưới Alibaba |

Nguồn: [productmint](https://productmint.com/the-lazada-business-model-how-does-lazada-make-money/),
[Business Model Analyst](https://businessmodelanalyst.com/lazada-business-model/).

🟩 **FACT** — **LazMall** là tầng premium: hàng chính hãng từ brand/nhà phân phối
được uỷ quyền, cam kết đổi trả 15 ngày và giao hàng hôm sau.
Nguồn: [duoke](https://www.duoke.com/en/blog/article/166-Choosing-Your-Lazada-Path-Benefits-and-Requirements-of-Marketplace-LazMall-LazGlobal).

🟩 **FACT** — Doanh thu chính từ **commission theo category**, khoảng **1–4%**
tuỳ ngành hàng. Nguồn: như trên.

### 2.2 · Fulfillment

🟩 **FACT** — Ba lựa chọn fulfillment:

| Mô hình | Kho | Vận chuyển |
|---|---|---|
| **Seller-shipped** (mặc định) | Seller | Seller tự đặt courier |
| **Fulfilment by Lazada** | Kho Lazada | Lazada pick/pack + LEX last-mile |
| **LGS (Lazada Global Shipping)** | Seller | Lazada lo toàn bộ từ đóng gói tới vận chuyển quốc tế |

Nguồn: [Locad](https://www.golocad.com/e-commerce/lazada/),
[uParcel](https://www.uparcel.sg/blog/lazada-fulfillment-options-singapore).

### 2.3 · Điều này ảnh hưởng gì tới ERD

🟦 **INFERENCE** — Với bài toán **voucher uplift**, sự khác nhau giữa 1P/3P/LazMall
chỉ quan trọng ở **hai** chỗ:

1. **Ai trả tiền voucher** — voucher seller-funded vs platform-funded (xem §7).
   Ảnh hưởng trực tiếp tới ràng buộc budget của policy engine.
2. **Tín hiệu chất lượng** — LazMall là một thuộc tính categorical của store, hợp lý
   để làm feature (loại store ⇒ hành vi mua khác nhau).

🟨 **SYNTHETIC** — Kênh fulfillment (seller-shipped vs FBL vs LGS) **không** được đưa
vào ERD. Lý do: không feature nào trong scope voucher decisioning phụ thuộc vào nó,
và nó chỉ ảnh hưởng tới thời gian giao hàng — nằm sau điểm quyết định.
Nếu sau này cần feature "trải nghiệm giao hàng" thì bổ sung, không phải bây giờ.

---

## 3. Catalog: seller → store → product → SKU

🟦 **INFERENCE** — Cấu trúc catalog chuẩn của mọi marketplace, Lazada không ngoại lệ:

```
SELLER  1 ──< STORE  1 ──< PRODUCT  1 ──< SKU
                              │
                              └──> CATEGORY (cây phân cấp)
```

- **SELLER** — pháp nhân bán hàng, có hợp đồng với sàn.
- **STORE** — mặt tiền của seller trên sàn. 🟨 Project giả định **1 seller = 1 store**
  để giảm độ phức tạp; quan hệ 1–n vẫn được giữ trong schema để không phải migrate sau.
- **PRODUCT** — đơn vị hiển thị (một trang sản phẩm). Người dùng *xem* product.
- **SKU** — đơn vị tồn kho và **đơn vị có giá**. Ví dụ "áo thun — size M — màu đen".
  Người dùng *mua* SKU. Đây là chỗ `price` sống.
- **CATEGORY** — cây phân cấp, gắn ở mức product.

> ⚠️ **Điểm này repo hiện tại đang làm sai.** `event_producer.py` sinh
> `item_id = "item_" + random(1..500000)` và `price = lognormal(3.2, 0.8)` **độc lập
> cho từng event**. Nghĩa là cùng một `item_id` có thể có giá khác nhau ở hai event,
> và `category_id` được bốc ngẫu nhiên không liên quan tới item. Không có catalog nào
> tồn tại phía sau. Đây là gap Layer 1 trong `MIGRATION_PLAN.md` §3.

---

## 4. Customer journey

🟦 **INFERENCE** — Journey điển hình trên app e-commerce, khớp với `SESSION_FLOW`
đã có sẵn trong repo:

```
mở app  →  duyệt / tìm kiếm  →  xem sản phẩm  →  thêm giỏ
        →  [xem voucher → thu thập voucher]
        →  checkout  →  thanh toán  →  chờ giao  →  nhận  →  [đổi trả]
```

Đặc điểm cần mô hình hoá:

| Đặc điểm | Mô tả | 🟨 Tham số synthetic |
|---|---|---|
| **Phễu thu hẹp mạnh** | Tỉ lệ view→cart→order giảm theo bậc | Giữ nguyên `SESSION_FLOW` hiện có làm điểm khởi đầu, hiệu chỉnh sau |
| **Phân bố lệch** | Thiểu số user tạo đa số traffic | Repo hiện dùng 80/20 — giữ |
| **Nhiều phiên rời rạc** | User quay lại nhiều lần trước khi mua | Cần **CUSTOMER state bền vững** giữa các phiên — hiện chưa có |
| **Giỏ hàng bền** | Giỏ sống qua nhiều phiên | Cần entity CART — hiện chưa có |

> ⚠️ **Điểm repo hiện tại đang làm sai.** `_make_session()` sinh mỗi phiên **độc lập
> hoàn toàn**: không có giỏ hàng mang sang phiên sau, không có trạng thái customer,
> `add_to_cart` không dẫn tới `order` của cùng item. Một session là một chuỗi Bernoulli
> chứ không phải một transaction flow.

---

## 5. Order lifecycle

🟩 **FACT** (một phần) — Lazada Seller Center có các trạng thái quan sát được công khai:
`Pending` → `Ready to Ship (RTS)` → `Shipped` → `Delivered`. Khi seller đặt RTS, hệ
thống tự báo courier tới lấy (pickup) hoặc seller phải giao tới điểm dropoff trong SLA.
Nguồn: [Lazada Open Platform — Order Status Flow](https://open.lazada.com/apps/doc/doc?nodeId=29484&docId=120167),
[SetStatusToReadyToShip](https://lazada-sellercenter.readme.io/docs/setstatustoreadytoship).

🟨 **SYNTHETIC** — Enum đầy đủ project sẽ dùng (đơn giản hoá, đủ cho uplift):

```
                       ┌──────────────┐
                       │   CREATED    │  đơn vừa tạo, chưa trả tiền
                       └──────┬───────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
      ┌──────────────┐  ┌──────────┐   ┌────────────┐
      │ PENDING_PAY  │  │   PAID   │   │ CANCELLED  │ ← huỷ trước khi trả
      └──────┬───────┘  └────┬─────┘   └────────────┘
             │ hết hạn       │
             ▼               ▼
      ┌────────────┐   ┌──────────────┐
      │ CANCELLED  │   │ READY_TO_SHIP│
      └────────────┘   └──────┬───────┘
                              ▼
                       ┌──────────────┐
                       │   SHIPPED    │
                       └──────┬───────┘
                              ▼
                       ┌──────────────┐
                       │  DELIVERED   │
                       └──────┬───────┘
                     ┌────────┴────────┐
                     ▼                 ▼
              ┌────────────┐    ┌────────────┐
              │ COMPLETED  │    │  RETURNED  │
              └────────────┘    └────────────┘
```

### 5.1 · Vì sao lifecycle này quan trọng với uplift

🟦 **INFERENCE + quyết định thiết kế** — **Định nghĩa "conversion" phải chốt trên
lifecycle này, không được để mập mờ.** Ba lựa chọn khả dĩ:

| Định nghĩa | Ưu | Nhược |
|---|---|---|
| `ORDER_CREATED` tồn tại | Biết sớm nhất, độ trễ label thấp | Đếm cả đơn huỷ / không trả tiền |
| `PAID` đạt tới | Cân bằng, có tiền thật | Trễ vài phút–vài giờ |
| `COMPLETED` (đã giao, hết hạn trả) | Đúng giá trị kinh tế nhất | Trễ 7–15 ngày ⇒ không dùng được cho online loop |

🟨 **SYNTHETIC ASSUMPTION A-01** → xem [§10](#10-danh-sách-assumption).

---

## 6. Payment lifecycle

🟦 **INFERENCE** — Phương thức thanh toán phổ biến ở SEA:

| Method | 🟦 Đặc điểm ảnh hưởng feature |
|---|---|
| `COD` (ship COD) | Rất phổ biến ở SEA; tỉ lệ huỷ/hoàn cao hơn hẳn |
| `CARD` | Có thể dính bank voucher (xem §7) |
| `WALLET` | Ví điện tử |
| `BANK_TRANSFER` | Có bước chờ, dễ hết hạn |

🟨 **SYNTHETIC** — PAYMENT là entity riêng (1 order có ≥1 payment attempt) vì:
một attempt thất bại rồi thử lại là hành vi có thật và tạo ra event khác nhau
(`PAYMENT_FAILED` vs `PAYMENT_COMPLETED`). Nếu gộp payment vào ORDER thì mất thông tin này.

---

## 7. Voucher & campaign — trọng tâm của bài toán

Đây là phần nghiệp vụ **quan trọng nhất** với project, nên research kỹ nhất.

### 7.1 · Phân loại voucher

🟩 **FACT** — Lazada có các loại voucher sau:

| Loại | Phạm vi | Ai tài trợ |
|---|---|---|
| **Store voucher** | Gắn với một seller / một số sản phẩm | Seller |
| **Platform voucher** | Toàn sàn (sitewide) | Lazada |
| **Free shipping voucher** | Bù một phần hoặc toàn bộ phí ship | Lazada hoặc seller |
| **Bank voucher** | Khi thanh toán bằng thẻ của ngân hàng cụ thể | Ngân hàng |

Theo **cơ chế nhận**, voucher chia hai kiểu:
- **Collectible voucher** — user *thu thập* trước, hệ thống **tự áp dụng** lúc checkout
- **Code voucher** — user phải *gõ mã* thủ công

Theo **kiểu giảm giá**: phần trăm (`% off`), số tiền cố định (`$5 off`), miễn phí ship.

Nguồn: [ringgitplus](https://ringgitplus.com/en/blog/online-shopping/lazada-revises-ability-to-stack-different-types-of-vouchers-effective-immediately),
[Dobin](https://www.dobin.io/blog/lazada-promo-codes-and-discounts),
[Prosperna](https://www.prosperna.com/blog/shopee-lazada-seller-funded-vouchers-margins).

🟩 **FACT** — **Voucher seller-funded**: seller tạo voucher trong Seller Centre, và
**chi phí giảm giá trừ thẳng vào doanh thu của seller**.
Nguồn: [Prosperna](https://www.prosperna.com/blog/shopee-lazada-seller-funded-vouchers-margins).

🟩 **FACT** — **Stacking**: Lazada cho phép cộng dồn nhiều loại giảm giá trong một đơn
(seller voucher + platform voucher + free shipping + LazCoins), **nhưng có ngoại lệ** —
một số khuyến mãi ngân hàng không cho stack.
Nguồn: [ringgitplus](https://ringgitplus.com/en/blog/online-shopping/lazada-revises-ability-to-stack-different-types-of-vouchers-effective-immediately).

### 7.2 · Vòng đời voucher — điểm mấu chốt

🟦 **INFERENCE** — Một voucher đi qua **bốn** trạng thái với **bốn** thời điểm khác nhau.
Đây là điều mà repo hiện tại **không** phân biệt và cần sửa:

```
   ISSUED  ────►  VIEWED  ────►  CLAIMED  ────►  REDEEMED
     │              │              │               │
  hệ thống       user nhìn      user bấm       dùng vào 1
  phát cho       thấy nó        "thu thập"     đơn cụ thể
  user U                                        (có order_id)
     │                                              │
     └── ĐÂY LÀ TREATMENT ──────────┐               │
         (is_treat = 1)             │               └── ĐÂY LÀ ĐIỀU KIỆN
                                    │                   của conversion có
                                    │                   voucher
                                    ▼
                         Uplift đo: P(mua | ISSUED) − P(mua | không ISSUED)
```

> ⚠️ **Điểm cực kỳ quan trọng, repo hiện tại đang nhầm.**
>
> Legacy/full mart từng khai báo `voucher_used_30d` = *"Số voucher đã dùng 30 ngày"*,
> nhưng `feat_user_behaviour.sql` tính nó bằng
> `count(*) filter (where event_type = 'voucher_claim')`.
>
> **CLAIM ≠ USE.** Thu thập voucher là một hành vi ý định; dùng voucher là một hành vi
> giao dịch có `order_id`. Tỉ lệ claim→redeem trong thực tế thấp hơn 1 nhiều.
> Feature này đang bị **đặt tên sai so với công thức**. Xem `FEATURE_DICTIONARY.md`.

🟦 **INFERENCE** — Ràng buộc nghiệp vụ của một voucher (đều cần có trong ERD):

| Ràng buộc | Ví dụ |
|---|---|
| `min_spend` | Đơn tối thiểu 100k mới dùng được |
| `max_discount` (cap) | Giảm 20% nhưng tối đa 50k |
| `valid_from` / `valid_to` | Cửa sổ hiệu lực |
| `scope` | Toàn sàn / một store / một category / một danh sách SKU |
| `per_user_limit` | Mỗi user dùng tối đa n lần |
| `total_quota` | Tổng số lượt phát ra |
| `stackable` | Có cho cộng dồn với loại khác không |

### 7.3 · Campaign

🟦 **INFERENCE** — Campaign là **vỏ ngân sách và thời gian** bao quanh một tập voucher.
Ví dụ chiến dịch "8.8" chứa nhiều voucher khác nhau. Campaign giữ:
`budget_total`, `budget_spent`, `start_ts`, `end_ts`, `objective`.

Điều này quan trọng vì **policy engine cần budget để ra quyết định** — không thể phát
voucher cho mọi user có `uplift_score` cao nếu ngân sách hữu hạn.

---

## 8. Bài toán voucher uplift decisioning

### 8.1 · Phát biểu bài toán

🟩 **FACT** — Paper DESCN dùng dữ liệu production từ *"real voucher distribution
business scenario in Lazada"*.
Nguồn: [arXiv 2207.09920](https://arxiv.org/html/2207.09920) (bản PDF có sẵn trong `docs/`).

🟦 **INFERENCE** — Bài toán nghiệp vụ đằng sau:

> Sàn có ngân sách khuyến mãi hữu hạn. Phát voucher cho một user tốn tiền
> (giảm doanh thu trên đơn đó). Có bốn nhóm user:
>
> | | Mua nếu **có** voucher | Không mua nếu có voucher |
> |---|---|---|
> | **Mua** nếu không có voucher | *Sure thing* — phát là **lỗ** | *Do not disturb* |
> | **Không mua** nếu không có | *Persuadable* — **phát đúng người** | *Lost cause* — phát là **phí** |
>
> Mục tiêu: tìm nhóm **persuadable**. Đó chính là uplift dương.

### 8.2 · Vì sao *không* được dùng purchase probability

🟦 **INFERENCE** — Model dự đoán `P(mua)` sẽ xếp hạng cao nhất nhóm *sure thing* —
những người mua sẵn rồi. Phát voucher cho họ là **trợ giá cho doanh thu vốn đã có**.
Uplift model xếp hạng theo `τ(x) = E[Y|T=1,x] − E[Y|T=0,x]`, mới tìm ra *persuadable*.

Đây chính là ràng buộc §11 trong yêu cầu của project, và nó có hệ quả kiểm chứng được:
**bảng feature importance của một model `P(mua)` và của một uplift model không giống nhau.**

### 8.3 · Treatment ở đây chính xác là gì

🟨 **SYNTHETIC ASSUMPTION A-02** → xem [§10](#10-danh-sách-assumption).

---

## 9. Những gì dataset LZD nói về nghiệp vụ này

Đây là phần ràng buộc **thật** mà dataset áp lên thiết kế nghiệp vụ. Rất ít, và
phải nói rõ là rất ít.

### 9.1 · Ràng buộc đo được (từ `AUDIT_KIEN_TRUC_VA_FEATURE.md`)

| # | Đo được từ dataset | Ràng buộc lên thiết kế nghiệp vụ |
|---|---|---|
| 1 | 83 covariate, **không công bố semantic** | Không entity nào được gán `f*` |
| 2 | Train: 22.2% treated, CR 5.66% vs 0.94% | Train là **observational, có targeting bias** |
| 3 | Test: 52.1% treated, CR 3.70% vs 3.33% | Test là **RCT**, uplift thật ≈ **+0.37pp** |
| 4 | `f40`–`f78` = 11 nhóm one-hot | Có ≥7 thuộc tính **categorical** ở tầng nghiệp vụ |
| 5 | `f79`–`f82`: 1 biến 515 mức, 4 kiểu mã hoá | Có ≥1 categorical **cardinality cao** (~515) |
| 6 | `f1`,`f2` nguyên `[0,365]`, `f1 ≥ f2` luôn đúng | Có ≥2 đại lượng **kiểu ngày trong năm / recency** có thứ tự |
| 7 | `f30 = log10(n)`, `n` nguyên `[1,30]` (100% dòng) | Có ≥1 **counter chặn trên ở 30** |
| 8 | `f27`,`f34` nguyên `[0,100]`, 32/31 giá trị | Có ≥2 đại lượng thang `[0,100]` (điểm? phần trăm?) |

### 9.2 · Đọc ràng buộc này thế nào cho đúng

🟦 **INFERENCE** — Những gì §9.1 cho phép nói:

> Tầng nghiệp vụ phía sau dataset **có** categorical (một cái ~515 mức), **có**
> counter bị cap ở 30, **có** đại lượng recency thang 365 ngày, **có** đại lượng
> thang 0–100.

❌ Những gì §9.1 **không** cho phép nói:

> ~~"515 mức là category_id"~~ · ~~"f1 là days_since_signup"~~ ·
> ~~"f30 là số đơn 30 ngày"~~ · ~~"f27 là điểm loyalty"~~

Cả bốn đều **hợp lý** và cả bốn đều **không có bằng chứng**. Nguyên tắc §13.1 của
project cấm biến chúng thành fact.

### 9.3 · Hệ quả kiến trúc quan trọng nhất

> **`f0..f82` KHÔNG phải đầu ra của synthetic business system.**

Synthetic business system sinh ra **feature riêng của nó, có lineage đầy đủ**.
Dataset LZD đóng hai vai hoàn toàn khác:

```
   Vai 1 — OBSERVED REFERENCE / TARGET SPACE
   f0..f82 là vector quan sát trong CSV. Feature store selected chỉ lấy 36 cột
   đã chốt trong fs_2026_08_v1; không nạp nguyên trạng toàn bộ f0..f82 lên Redis.
   Không diễn giải phần ngoài selected set.

   Vai 2 — REFERENCE ĐỂ HIỆU CHỈNH
   Phân bố / cardinality / sparsity của LZD dùng để kiểm tra rằng feature do
   synthetic system sinh ra "trông giống dữ liệu thật ở mức thống kê".
   KHÔNG phải để chứng minh synthetic_f30 == LZD_f30.
```

Chi tiết cách hai vai này ghép lại: `FEATURE_LINEAGE.md` §2 (mô hình hai track).

---

## 10. Danh sách ASSUMPTION

Mọi assumption đều ghi đủ ba dòng theo §17 yêu cầu project.

---

### A-01 · Định nghĩa conversion = đơn đạt trạng thái `PAID`

**ASSUMPTION** — Label `Y = 1` khi user tạo một đơn đạt tới `PAID` trong cửa sổ
attribution sau treatment. `CREATED` chưa đủ; `COMPLETED` quá trễ.

**RATIONALE** — `CREATED` đếm cả đơn không bao giờ trả tiền (đặc biệt nghiêm trọng
ở SEA vì COD phổ biến — §6). `COMPLETED` trễ 7–15 ngày nên không dùng được trong
closed-loop online (§12 yêu cầu). `PAID` là điểm sớm nhất có giá trị kinh tế thật.

**IMPACT** — Order ở trạng thái `CANCELLED` trước khi `PAID` không tính là conversion.
Order `RETURNED` **vẫn** tính là conversion (vì đã `PAID`) — chấp nhận sai lệch này,
và ghi nhận nó như một hạn chế đã biết. Nếu sau này muốn label chặt hơn thì thêm
`label_strict` chứ **không** đổi định nghĩa `label` đang có.

---

### A-02 · Treatment = **VOUCHER_ISSUED** (phát), không phải claimed/redeemed

**ASSUMPTION** — `is_treat = 1` nghĩa là hệ thống **đã phát** voucher tới user
(voucher xuất hiện trong ví/trang voucher của user), bất kể user có thu thập hay dùng.

**RATIONALE** — Treatment phải là thứ **hệ thống kiểm soát được**, vì đó là biến
quyết định của policy engine. Claim và redeem là *hành vi của user* — chúng nằm
**sau** treatment trên đường nhân quả và là **mediator**, không phải treatment.
Nếu định nghĩa treatment = redeemed thì đã điều kiện hoá trên kết quả ⇒ ước lượng
uplift vô nghĩa.

**IMPACT** — Chuỗi `VOUCHER_ISSUED → VOUCHER_VIEWED → VOUCHER_CLAIMED → VOUCHER_REDEEMED`
phải là **bốn** event riêng biệt với **bốn** timestamp riêng (§7.2). Repo hiện tại
chỉ có `voucher_view` và `voucher_claim`, **thiếu hoàn toàn `voucher_issued`** —
tức là **thiếu chính event mang treatment**. Đây là gap chặn closed-loop.

---

### A-03 · Đơn vị quyết định = (customer, decision_ts), không phải (customer, voucher)

**ASSUMPTION** — Mỗi downstream decision record áp dụng cho **một customer tại một
thời điểm**: phát hay không phát *một* voucher đã chọn sẵn từ campaign đang chạy.
Không phải bài toán chọn voucher nào trong nhiều voucher.

**RATIONALE** — Dataset LZD có `is_treat` **nhị phân** — không có thông tin về loại
voucher, mệnh giá hay campaign. Model học từ dữ liệu đó chỉ trả lời được câu hỏi
nhị phân. Mở rộng sang multi-treatment sẽ vượt quá thứ dữ liệu chống đỡ được.

**IMPACT** — Feature store dùng entity key `user_id` (đúng như hiện tại, không đổi).
Voucher/campaign nằm ở tầng **policy**, không ở tầng model. Nếu sau này muốn
multi-treatment thì phải đổi entity key thành `(user_id, campaign_id)` — đây là thay
đổi lớn, ghi nhận trước để không bị bất ngờ.

---

### A-04 · Synthetic business system dùng ID space TÁCH BIỆT với user LZD

**ASSUMPTION** — Customer do synthetic business system sinh ra dùng tiền tố khác
(`C…`) với user seed từ dataset (`U…`), và **có một bảng ánh xạ tường minh** cho
những user được chọn đưa vào scenario.

**RATIONALE** — Trộn hai không gian ID một cách ngầm định chính là lỗi G3 mà audit
đã bắt: producer sinh `U0000000..U0019999` (pool 20,000) trong khi dataset có
1,108,338 user ⇒ ~98% user đã seed không bao giờ nhận event, và không có gì đảm bảo
một `user_id` do producer bịa ra thực sự tồn tại trong snapshot.

**IMPACT** — Cần một bảng `customer_identity_map(customer_id, lzd_user_id, source)`.
Scenario driver chỉ chạy trên user **có trong map**. Điều này cũng thoả điều cấm
số 7 của project ("không dùng random user ID không tồn tại").

---

### A-05 · Không mô hình hoá: review, chat, livestream, ví/LazCoins, payout seller

**ASSUMPTION** — Năm nhóm nghiệp vụ trên **không** vào ERD.

**RATIONALE** — Yêu cầu §7 nói rõ: *"Không được tạo ERD khổng lồ chỉ để trông giống
Lazada. Mỗi entity phải có lý do nghiệp vụ."* Không feature nào trong scope voucher
uplift decisioning cần tới chúng.

**IMPACT** — Nếu sau này cần feature kiểu "user hay đọc review trước khi mua" thì
phải mở rộng ERD — và phải mở rộng cả event model + feature pipeline cùng lúc.
Ghi nhận đây là biên giới có chủ ý, không phải thiếu sót.

---

## Nguồn tham khảo

- [Lazada Business Model — productmint](https://productmint.com/the-lazada-business-model-how-does-lazada-make-money/)
- [Lazada Business Model — Business Model Analyst](https://businessmodelanalyst.com/lazada-business-model/)
- [Lazada in E-commerce — Locad](https://www.golocad.com/e-commerce/lazada/)
- [Marketplace / LazMall / LazGlobal — duoke](https://www.duoke.com/en/blog/article/166-Choosing-Your-Lazada-Path-Benefits-and-Requirements-of-Marketplace-LazMall-LazGlobal)
- [Lazada Fulfillment Options — uParcel](https://www.uparcel.sg/blog/lazada-fulfillment-options-singapore)
- [Voucher stacking — ringgitplus](https://ringgitplus.com/en/blog/online-shopping/lazada-revises-ability-to-stack-different-types-of-vouchers-effective-immediately)
- [Seller-funded vouchers — Prosperna](https://www.prosperna.com/blog/shopee-lazada-seller-funded-vouchers-margins)
- [Promo codes & voucher types — Dobin](https://www.dobin.io/blog/lazada-promo-codes-and-discounts)
- [Order Status Flow — Lazada Open Platform](https://open.lazada.com/apps/doc/doc?nodeId=29484&docId=120167)
- [SetStatusToReadyToShip — Lazada Seller Center API](https://lazada-sellercenter.readme.io/docs/setstatustoreadytoship)
- [DESCN — arXiv 2207.09920](https://arxiv.org/html/2207.09920) (PDF local: `docs/2207.09920v3.pdf`)
