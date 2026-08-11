# Feature Lineage

> **Vai trò:** chứng minh đường đi `Business Entity → Raw Field/Event → Transformation
> → Feature → Feature Store → Model` cho **từng** feature — hoặc chứng minh rằng
> đường đi đó **không tồn tại**.
>
> **Trạng thái:** mô tả code hiện tại (Track A, B) + đề xuất (Track C).

---

## Mục lục

1. [Mental model bắt buộc](#1-mental-model-bắt-buộc)
2. [Mô hình hai track — quyết định kiến trúc trung tâm](#2-mô-hình-hai-track--quyết-định-kiến-trúc-trung-tâm)
3. [Lineage Track A — `f0`..`f82`](#3-lineage-track-a--f0f82)
4. [Lineage Track B — event-derived hiện có](#4-lineage-track-b--event-derived-hiện-có)
5. [Lineage Track C — synthetic business system](#5-lineage-track-c--synthetic-business-system)
6. [Ranh giới point-in-time](#6-ranh-giới-point-in-time)
7. [Vai trò của dataset LZD: seed vs reference](#7-vai-trò-của-dataset-lzd-seed-vs-reference)

---

## 1. Mental model bắt buộc

```
    ERD                    ≠   Feature Store
    Business Data          ≠   ML Feature
    Raw Event              ≠   Feature
    f0..f82                ≠   Business Columns
    LZD Dataset            ≠   LZD Internal Database
    Synthetic Reconstruction ≠ Historical Fact
```

Đọc theo chiều xuôi:

```
   Business Entity  ──(transition)──►  Domain Event
                                            │
                                            │ (transformation: window + aggregation
                                            │                  + encoding)
                                            ▼
                                        Feature
                                            │
                                            ▼
                                    Feature Store
                                     ┌──────┴──────┐
                                offline        online
                                     │             │
                                 training      inference
                                     └──────┬──────┘
                                            ▼
                                          Model
```

**Mọi feature phải chỉ ra được vị trí của mình trên đường này.**
Feature nào không chỉ ra được ⇒ `confidence: UNKNOWN` ⇒ chỉ được seed, không được mô phỏng.

---

## 2. Mô hình hai track — quyết định kiến trúc trung tâm

Đây là quyết định quan trọng nhất trong toàn bộ bộ tài liệu, và nó **cần bạn xác nhận**.

### 2.1 · Vấn đề

Yêu cầu §18 vẽ một vòng đời đi qua `f0...f82`:

```
CUSTOMER → BUSINESS ACTION → DOMAIN EVENT → RAW EVENT
        → FEATURE ENGINEERING → FEATURE STORE
        → f0...f82 / realtime features → UPLIFT MODEL → ...
```

Nhưng audit đã xác nhận (C1, C11): **`f0..f82` không có transformation nào trong repo,
và Lazada không công bố semantic của bất kỳ cột nào.** Không có raw để quay về.

⇒ Mũi tên `FEATURE ENGINEERING → f0..f82` **không thể tồn tại một cách trung thực.**
Vẽ nó ra là vi phạm điều cấm số 4 ("reverse `f*` thành raw rồi tuyên bố đó là dữ liệu gốc").

### 2.2 · Giải pháp: tách track, không tách hệ thống

```
┌─────────────────────────────────────────────────────────────────────┐
│                     SYNTHETIC BUSINESS SYSTEM                       │
│   CUSTOMER · SKU · CART · ORDER · VOUCHER · CAMPAIGN · DECISION      │
└───────────────────────────────┬─────────────────────────────────────┘
                                │ business action
                                ▼
                          DOMAIN EVENT ──► Kafka ──► lake
                                │
                    ┌───────────┴────────────┐
                    ▼                        ▼
        ┌───────────────────┐    ┌────────────────────────┐
        │ TRACK B           │    │ TRACK C                │
        │ 11 feature hiện có│    │ feature mới, lineage   │
        │ lineage đầy đủ    │    │ đầy đủ từ Layer 1      │
        │ CONFIRMED         │    │ SYNTHETIC              │
        └─────────┬─────────┘    └────────────┬───────────┘
                  └────────────┬──────────────┘
                               ▼
                    ┌──────────────────────┐
                    │    FEATURE STORE     │◄──────────────┐
                    └──────────┬───────────┘               │
                               │                           │
                               ▼                    ┌──────┴───────┐
                          UPLIFT MODEL              │  TRACK A     │
                               │                    │  f0..f82     │
                               ▼                    │  seed tĩnh   │
                            POLICY                  │  UNKNOWN     │
                               │                    │  KHÔNG có    │
                               ▼                    │  lineage     │
                        VOUCHER DECISION            └──────▲───────┘
                               │                           │
                               └───► DECISION_MADE ────────┘  ← chỉ nạp 1 lần
                                     VOUCHER_ISSUED             từ CSV,
                                          │                     không bao giờ
                                          └──► Kafka ──► loop    tính lại
```

| | Track A | Track B | Track C |
|---|---|---|---|
| Cột | `f0`–`f82` (83) | 11 event-derived hiện có | đề xuất, chưa có |
| Lineage | ❌ không tồn tại | ✅ đầy đủ | ✅ đầy đủ |
| Cập nhật theo event | ❌ không bao giờ | ✅ có | ✅ có |
| Vai trò | vector trạng thái lịch sử mờ | live signal (đã kiểm chứng được) | live signal (nghiệp vụ giàu hơn) |
| Confidence | UNKNOWN | CONFIRMED | SYNTHETIC |
| Test parity áp dụng? | chỉ **parity truyền tải** | ✅ parity tính toán | ✅ parity tính toán |

### 2.3 · Điều này thoả yêu cầu §18 như thế nào

Vòng đời §18 **vẫn đóng trọn vẹn** — chỉ là nó đóng qua Track B/C, còn Track A tham
gia với tư cách **điều kiện ban đầu tĩnh**:

```
CUSTOMER → BUSINESS ACTION → DOMAIN EVENT → FEATURE ENGINEERING → FEATURE STORE
                                                                        │
                                            f0..f82 (seed) ─────────────┤
                                                                        ▼
                                                                  UPLIFT MODEL
                                                                        ▼
                                                            UPLIFT SCORE → POLICY
                                                                        ▼
                                                            VOUCHER / NO VOUCHER
                                                                        ▼
                                                            CUSTOMER RESPONSE
                                                                        ▼
                                                                  NEW EVENT ──┐
                                                                              │
                                            FEATURE UPDATE ◄──────────────────┘
                                                    ▼
                                              NEW DECISION
```

Đây là cách trung thực duy nhất để có closed loop mà không bịa lineage cho `f*`.

### 2.4 · ⚠️ Cần bạn quyết

| Lựa chọn | Nội dung | Hệ quả |
|---|---|---|
| **(a)** Giữ 94 feature hiện tại, chỉ thêm Track C | Model đọc A + B + C | Track A vẫn chi phối importance (rủi ro G2 còn nguyên) |
| **(b)** Model song song: `model_full` (A+B+C) và `model_live` (B+C) | So sánh trực tiếp | Trả lời được G2 bằng thực nghiệm; tốn gấp đôi train |
| **(c)** Bỏ hẳn Track A khỏi model, chỉ dùng làm seed | Model chỉ đọc B+C | Mất tín hiệu từ 83 cột thật; nhưng lineage 100% sạch |

**Khuyến nghị: (b).** Nó là cách duy nhất *đo* được rủi ro G2 thay vì phỏng đoán, và
kết quả của nó quyết định luôn nên chọn (a) hay (c). Chi phí thấp vì hạ tầng train
dùng chung.

---

## 3. Lineage Track A — `f0`..`f82`

```
   ┌─────────────────────────────────────────────┐
   │  Hệ thống nội bộ Lazada                     │
   │  (ngoài repo, không quan sát được)          │
   └────────────────────┬────────────────────────┘
                        │  ✖ transformation KHÔNG BIẾT
                        ▼
   data/full_trainset.csv   ← điểm sớm nhất ta quan sát được
   data/full_testset.csv
                        │
                        │  seed_loader.load_csv_to_lake()
                        │  CHỈ: data_id → user_id, gắn split/dt/feature_ts
                        ▼
   s3://lakehouse/raw/user_snapshot/dt=…/{train,test}.parquet
                        │
                        │  stg_user_snapshot.sql
                        │  CHỈ: cast(f{i} as double) + dedup
                        ▼
   feat_user_serving.sql   ← copy nguyên trạng, không biến đổi
                        │
          ┌─────────────┴──────────────┐
          │ sync.py                     │ training_dataset.sql
          ▼                             ▼
   Redis fs:{ver}:u:{uid}        marts.training_dataset
          │                             │
          └─────────────┬───────────────┘
                        ▼
                      MODEL
```

**Kết luận lineage:** đường đi tồn tại **từ CSV trở đi** và hoàn toàn minh bạch —
mọi bước đều là copy/cast. Nhưng đường đi **phía trước CSV không tồn tại trong repo
và không được công bố**.

⇒ Với `f*`, thứ duy nhất kiểm chứng được là **parity truyền tải**:

```
   marts.training_dataset.f{i}   ==   Redis fs:{ver}:u:{uid}[f{i}]
   (bằng nhau từng bit, cho cùng user_id)
```

Không phải parity tính toán — vì không có phép tính nào để lặp lại.

---

## 4. Lineage Track B — event-derived hiện có

### 4.1 · Nhóm realtime `rt_*` — lineage tốt nhất trong repo

```
   EventProducer._make_session()        ⚠ chưa có business entity phía sau
            │
            ▼
   AppEvent{event_id,user_id,event_type,event_ts,session_id,platform,
            item_id,category_id,price,quantity}
            │
            │ Kafka  key=user_id  (giữ thứ tự trong phạm vi 1 user)
            ▼
   topic app.user.events.v1
            │
            ▼
   stream_consumer.run()
       ├─ validate_event()  ──✖──► DLQ (theo reason)
       │
       ├─(1)─► parquet s3://lakehouse/raw/app_events/dt=/hour=
       │              │
       │              ▼
       │       stg_app_events.sql   dedup theo event_id, gmv = price×quantity
       │              │
       │              ▼
       │       feat_user_realtime_pit.sql
       │           cửa sổ = [bucket_start(feature_ts) − 11×300s , feature_ts)
       │              │
       │              ▼           ═══ OFFLINE ═══
       │       marts.training_dataset.rt_*
       │
       └─(2)─► _update_realtime()
                  incr_realtime_counters(user, {rt_*: Δ})
                  update_realtime_bulk({rt_last_event_ts})
                       │
                       ▼
                  Redis rt:u:{uid}   HASH ô 5 phút, TTL 3600s
                       │
                       ▼
                  online_store.aggregate_realtime()
                       cộng các ô có bucket_start ≥ bucket_start(now) − 11×300
                       │
                       ▼           ═══ ONLINE ═══
                  spec.merge(batch ← realtime ← context)
                       │
                       ▼
                     MODEL
```

**Điểm parity:** hai nhánh (1) và (2) **cùng làm tròn về ô 5 phút**. Comment trong
`feat_user_realtime_pit.sql` ghi rõ: nếu offline dùng `feature_ts - interval '1 hour'`
thì hai bên lệch tới 5 phút dữ liệu. Đây là thiết kế đúng và cần được **cắm test tự
động canh giữ** — hiện chưa có (gap **G7**).

**Chỗ đứt:** `rt_session_len_sec` chỉ có nhánh (1), **không** có nhánh (2). Xem
`FEATURE_DICTIONARY.md` §4.5.

### 4.2 · Nhóm batch `hist_*`

```
   stg_app_events.sql
            │
            │  ⚠ lọc: event_ts >= now() − 30 days      ← now(), KHÔNG phải feature_ts
            ▼
   feat_user_behaviour.sql
       user_tenure_days   = date_diff('day', min(event_ts), now())   ⚠ chặn ở 30
       hist_order_cnt_30d = count(*) filter (event_type='order')
       hist_gmv_30d       = sum(gmv) filter (event_type='order')
       voucher_used_30d   = count(*) filter (event_type='voucher_claim')  ⚠ tên sai
            │
            │ left join using (user_id)      ⚠ KHÔNG join theo dt/feature_ts
            ▼
   feat_user_serving.sql   (cùng bảng được sync lên Redis)
            │
      ┌─────┴─────┐
      ▼           ▼
   Redis      training_dataset
   (batch)         │
      └──────┬─────┘
             ▼
           MODEL
```

**Ba chỗ đứt lineage, đều đã ghi ở `FEATURE_DICTIONARY.md` §6:** D2 (tenure bị chặn),
D3 (tên sai), D4 (không point-in-time).

### 4.3 · Chỗ đứt lớn nhất: **phía trên EventProducer không có gì**

```
   ???  ← KHÔNG CÓ BUSINESS ENTITY, KHÔNG CÓ OPERATIONAL DB
    │
    ▼
   EventProducer._make_session()
       for event_type, prob in SESSION_FLOW:
            if random() > prob: continue
            price    = lognormvariate(3.2, 0.8)    ← giá bịa cho từng event
            item_id  = "item_" + randint(1,500000) ← không có catalog
            category = choice(CATEGORIES)          ← không liên quan item_id
```

Đây là **lỗ hổng kiến trúc trung tâm** so với mental model của project:

| Yêu cầu | Thực tế repo |
|---|---|
| `Business Entity → Transaction → Event` | `random() → Event` |
| "Customer views product" ⇒ `PRODUCT_VIEWED` | Không có Customer, không có Product |
| "Customer places order" ⇒ `ORDER_CREATED, PAYMENT_COMPLETED, ORDER_PAID` | Một event `order` duy nhất, không có order nào tồn tại |

Layer 1 **hoàn toàn vắng mặt**. Repo hiện tại nhảy thẳng từ generator ngẫu nhiên
(giả danh Layer 2) sang Layer 3.

> **Nói cho công bằng:** đây không phải lỗi thiết kế của repo. Repo được xây làm
> *hạ tầng data pipeline*, và ở vai trò đó nó **rất tốt** — versioned feature store
> với atomic swap, PIT join, dedup, at-least-once, spec làm hợp đồng ba bên. Cái
> thiếu là một tầng nghiệp vụ ở trên, đúng thứ bộ tài liệu này thiết kế.

---

## 5. Lineage Track C — synthetic business system

### 5.1 · Ví dụ đầy đủ — `cart_value_current`

```
LAYER 1  ┌────────────────────────────────────────────────┐
         │ CART_ITEM.insert(cart_id, sku_id, qty)         │
         │ SKU.list_price                                 │
         └───────────────────┬────────────────────────────┘
                             │ transition
LAYER 2                      ▼
         ITEM_ADDED_TO_CART{customer_id, sku_id, quantity,
                            unit_price, event_ts}
                             │ Kafka → stream + lake
LAYER 3                      ▼
         offline:  SUM(unit_price × quantity)
                   trên CART_ITEM còn sống tại feature_ts
         online:   Redis HASH cart:u:{uid} cộng/trừ khi
                   ITEM_ADDED / ITEM_REMOVED / ORDER_PAID
                             │
                             ▼
         FEATURE STORE  cart_value_current
                             │
LAYER 4                      ▼
                          MODEL
```

⚠️ Đây là feature **trạng thái** (không phải counter cửa sổ). Nó không cộng dồn được
qua ô 5 phút ⇒ **cơ chế parity hiện tại không áp dụng trực tiếp**. Cần thiết kế
riêng: online giữ giá trị tuyệt đối, offline tái dựng trạng thái tại `feature_ts`
bằng cách replay event. **Đây là công việc thật, không miễn phí** — ghi nhận trước
khi cam kết.

### 5.2 · Ví dụ — `voucher_redeemed_30d` (đóng closed loop)

```
LAYER 1   DECISION.insert ──► VOUCHER_ISSUANCE.insert
                                      │
LAYER 2                        VOUCHER_ISSUED ──► Kafka biz.voucher.v1
                                      │
                              (user phản ứng)
                                      ▼
                              VOUCHER_CLAIMED
                                      ▼
                    VOUCHER_REDEMPTION.insert khi ORDER_PAID
                                      ▼
                              VOUCHER_REDEEMED{issuance_id, order_id, amount}
                                      │
LAYER 3            COUNT trên cửa sổ 30 ngày, point-in-time tại feature_ts
                                      ▼
                          voucher_redeemed_30d
                                      │
LAYER 4                            MODEL ──► score ──► POLICY ──► DECISION
                                      │                              │
                                      └──────────────────────────────┘
                                            ★ VÒNG LẶP ĐÓNG
```

Đây là feature **duy nhất** trong toàn bộ tài liệu mà lineage của nó **đi qua chính
đầu ra của model**. Nó là bằng chứng closed loop hoạt động — và cũng là chỗ nguy hiểm
nhất về nhân quả (xem cảnh báo `FEATURE_DICTIONARY.md` §5.4).

### 5.3 · Cảnh báo: đừng để Track C thành `f*` thứ hai

Track C dễ phình. Kỷ luật cần giữ:

1. Thêm feature ⇒ **phải** viết đủ 13 trường lineage **trước** khi code
2. Thêm feature ⇒ **phải** có cả `offline_definition` và `online_definition`; nếu
   chỉ có một ⇒ đó là skew được lên lịch sẵn (chính là bài học `rt_session_len_sec`)
3. Thêm feature ⇒ bump `feature_spec.version`, không sửa tại chỗ
4. Feature vào model ⇒ **phải** có ablation chứng minh nó đóng góp

---

## 6. Ranh giới point-in-time

Ranh giới PIT là thứ giữ cho mọi lineage ở trên trung thực.

```
                    quá khứ  ◄────────── feature_ts ──────────►  tương lai
                                              │
   ✅ được dùng để tính feature               │   ❌ TUYỆT ĐỐI KHÔNG
   event_ts < feature_ts                      │   event_ts >= feature_ts
                                              │
   ────────────────────────────────────────── │ ────────────────────────────
                                              │
                                              │   ✅ label lấy ở đây
                                              │   trong cửa sổ attribution
```

| Thành phần | Tôn trọng biên PIT? | Bằng chứng |
|---|---|---|
| `feat_user_realtime_pit.sql` | ✅ | `e.event_ts < s.feature_ts` |
| `online_store.aggregate_realtime()` | ✅ | cutoff theo `now()` lúc serve |
| `feat_user_behaviour.sql` | ❌ | dùng `now()` lúc dbt build — **D4** |
| `feat_user_serving.sql` | ⚠️ | join behaviour `using (user_id)`, không theo `dt` |
| `training_dataset.sql` | ✅ | join theo `(user_id, dt)`; label từ snapshot |
| `offline_store.iter_shard()` | ✅ | chỉ đọc `entity_key + batch_names` ⇒ **label không bao giờ tới Redis** |

Dòng cuối đáng nhấn mạnh: **ranh giới label đang đúng** và nhiều team làm sai chỗ này.
Đừng phá nó khi migrate.

---

## 7. Vai trò của dataset LZD: seed vs reference

Yêu cầu §5 nói: coi dataset LZD là **feature target / reference**. Cụ thể hoá:

### 7.1 · Vai 1 — SEED (đang hoạt động)

```
full_trainset.csv ──► lake ──► stg ──► feat_user_serving ──► Redis fs:{ver}:u:{uid}
```

1,108,338 user (train + test) đã được nạp vào Redis, không lọc split (audit C14).
Đây là **trạng thái lịch sử [A]** — điểm xuất phát của mọi scenario.
Không diễn giải. Không mô phỏng. Không tái tạo.

### 7.2 · Vai 2 — REFERENCE để hiệu chỉnh (chưa làm)

Sau khi synthetic business system chạy, so **đặc trưng thống kê** của feature Track C
với LZD:

| Chiều so sánh | So cái gì | Ngưỡng chấp nhận |
|---|---|---|
| Range | min/max của feature liên tục | cùng bậc độ lớn |
| Distribution | KS statistic / quantile | 🟨 cần chốt |
| Cardinality | số mức của categorical | cùng bậc |
| Sparsity | tỉ lệ 0 / null | ±10pp |
| One-hot structure | có nhóm loại trừ không | có/không |
| Treatment distribution | % treated | TARGETED ≈ 22%, RCT ≈ 52% |
| Outcome distribution | positive rate | train ≈ 2.0%, test ≈ 3.5% |
| Uplift thô | CR(treat) − CR(control) | TARGETED ≈ +4.7pp, RCT ≈ +0.37pp |

Bốn dòng cuối là ràng buộc **mạnh nhất và cụ thể nhất** mà dataset áp lên synthetic
system — chúng đo được, đã biết con số đích, và ép synthetic generator phải mô hình
hoá cả **targeting bias** lẫn **RCT** (đó là lý do `EXPERIMENT.mode` tồn tại trong ERD §7.2).

### 7.3 · Điều KHÔNG phải mục tiêu

```
❌ synthetic_f30 == LZD_f30
❌ "cột X của synthetic system tương ứng f79"
❌ khôi phục raw data đứng sau f0..f82
```

Yêu cầu §5 nói rõ: không cần chứng minh đẳng thức khi semantic chưa được xác nhận.
Thứ cần chứng minh là **mapping/hypothesis hợp lý và thống kê phù hợp**.

### 7.4 · Ràng buộc bảo toàn test set

```
full_testset.csv  =  RCT  =  tài sản đánh giá không thiên lệch DUY NHẤT
```

Uplift thật chỉ **+0.37pp** trên nền 3.3%. Với đủ 181,669 dòng: SE ≈ 0.087pp ⇒ z ≈ 4.3.
Cắt xuống 50k: SE ≈ 0.16pp ⇒ z ≈ 2.3 — **mất độ tin cậy**.

⇒ **Cấm dùng test set làm nguồn event stream** (điều cấm số 9). Scenario lấy user từ
**train** (926,669 user, dư thừa) qua `CUSTOMER_IDENTITY_MAP` với `lzd_split='train'`.

---

## Tiếp theo

- `MIGRATION_PLAN.md` — chuyển từ kiến trúc hiện tại sang kiến trúc này theo thứ tự nào
