# Audit kiến trúc & feature — trước khi code

> **Phạm vi:** đọc toàn bộ `src/`, `dbt/`, `airflow/dags/`, `config/`, paper DESCN,
> và chạy forensic trên `data/full_trainset.csv` (926,669 dòng) +
> `full_testset.csv` (181,669 dòng).
>
> **Chưa code gì.** Đây là tài liệu audit theo yêu cầu.
>
> ⚠️ **Tài liệu này thay thế `KE_HOACH_FEATURE_VA_SINH_DU_LIEU.md`.**
> Ba giả thuyết trung tâm của file đó đã bị dữ liệu bác bỏ. Xem [Phần 8](#8-những-gì-file-kế-hoạch-cũ-nói-sai).
>
> 📌 **Tài liệu này vẫn còn hiệu lực**, nhưng phạm vi của nó là *pipeline hiện tại*.
> Tầng nghiệp vụ (Layer 1) đứng trên nó được thiết kế ở bộ tài liệu mới:
> [`LAZADA_BUSINESS_DOMAIN.md`](LAZADA_BUSINESS_DOMAIN.md) → [`ERD.md`](ERD.md) →
> [`BUSINESS_EVENT_MODEL.md`](BUSINESS_EVENT_MODEL.md) →
> [`FEATURE_DICTIONARY.md`](FEATURE_DICTIONARY.md) →
> [`FEATURE_LINEAGE.md`](FEATURE_LINEAGE.md) → [`MIGRATION_PLAN.md`](MIGRATION_PLAN.md).

---

## 1. Tôi hiểu mục tiêu của bạn thế nào

Bạn muốn dựng một **bàn thử nghiệm online decisioning**, không phải một máy sinh dữ liệu giả.

Cụ thể: lấy một user **đã có thật** trong dataset, đặt trạng thái lịch sử của họ vào
online feature store, rồi cho họ **sống tiếp** — sinh hành vi mới theo thời gian, để
hành vi đó cập nhật feature state, và mỗi lần state đổi thì gọi model hỏi lại
"có phát voucher cho người này không?".

Câu hỏi bạn muốn trả lời là: **khi hành vi của cùng một người thay đổi theo thời gian,
quyết định của model có đổi theo đúng cách không?**

Ba điều bạn nhấn mạnh, và tôi hiểu vì sao:

**Một — feature phải có một định nghĩa duy nhất.** Nếu simulator tự tính feature theo
cách riêng còn training tính theo cách khác, thì bàn thử nghiệm đo chính nó chứ không
đo hệ thống. Bạn muốn chứng minh được `feature_lúc_train == feature_sinh_lại_từ_raw`.

**Hai — không được đoán semantic.** Bạn không muốn tôi nhìn `f30` rồi tự nhận là
"số đơn 30 ngày" và xây cả simulator lên trên phỏng đoán đó.

**Ba — reverse là để chạy lại pipeline, không phải để bịa lại quá khứ.** Mục đích của
việc map feature về raw là để có thể **chạy lại đúng feature engineering**, chứ không
phải để tạo ra một chuỗi event lịch sử nghe có vẻ hợp lý.

Và ranh giới ba loại dữ liệu phải rõ: **A** trạng thái lịch sử (từ dataset, làm điểm
xuất phát), **B** feature state hiện tại (Redis, thứ model đọc), **C** event tương lai
(simulator bịa ra, làm state chuyển động). Không trộn.

**Kết luận quan trọng nhất của audit này:** mục tiêu đó **khả thi**, phần lớn hạ tầng
**đã có sẵn và chạy được** — nhưng nó khả thi theo một đường khác với đường mà file kế
hoạch cũ vẽ ra, vì `f0..f82` **không có raw để quay về**. Chi tiết ở Phần 4 và 5.

---

## 2. Kiến trúc hiện tại (A)

### 2.1 · Luồng thật, đọc từ code

```
   data/full_trainset.csv  (926,669 dòng, 86 cột)
   data/full_testset.csv   (181,669 dòng, 86 cột)
            │
            │  seed_loader.load_csv_to_lake()      ← DAG 00_bootstrap_lake
            │  CHỈ: đổi data_id → user_id, gắn split/dt/feature_ts
            ▼
   s3://lakehouse/raw/user_snapshot/dt=.../{train,test}.parquet
            │
            │  stg_user_snapshot.sql   ← CHỈ cast sang double + dedup
            ▼
   ┌────────────────────────────────────────────────────┐
   │  feat_user_serving.sql                             │
   │    f0..f82           ← copy nguyên từ snapshot     │
   │    user_tenure_days  ┐                             │
   │    hist_order_cnt_30d│ ← feat_user_behaviour.sql   │
   │    hist_gmv_30d      │   TÍNH TỪ EVENT             │
   │    voucher_used_30d  ┘                             │
   └────────────────────────────────────────────────────┘
            │                              │
            │ sync.py (shard, versioned)   │ training_dataset.sql
            │  ← DAG 40                    │  + feat_user_realtime_pit.sql
            ▼                              ▼
     Redis fs:{ver}:u:{uid}          marts.training_dataset
     (87 field batch)                (94 feature + label + is_treat)
            │                              │
            │                              ▼  DAG 30 → train.py
            │                        MLflow Registry ❌ CHƯA IMPLEMENT
            │
            │
   ┌────────┴─────────────────────────────────────────┐
   │                                                  │
   │  event_producer.py ──Kafka──> stream_consumer.py │
   │  (session ngẫu nhiên)              │             │
   │                                    ├──> parquet raw/app_events
   │                                    └──> Redis rt:u:{uid}
   │                                         (7 counter, ô 5 phút)
   └──────────────────────────────────────────────────┘
            │
            ▼
   FastAPI /decide
      read_for_serving()  ← 1 round-trip Lua: active_version + batch + rt
      spec.merge()        ← batch ← realtime ← context
      model.predict_uplift()
      score >= uplift_threshold ? SEND_VOUCHER : NO_VOUCHER
      inference_logger.log() → Postgres
```

### 2.2 · Chất lượng hạ tầng hiện có

Cần nói rõ: **phần hạ tầng được xây rất tốt.** Những thứ sau đã đúng chuẩn production
và không cần sửa:

| Cơ chế | File | Vì sao đáng ghi nhận |
|---|---|---|
| Versioned feature store + atomic swap | `online_store.py` | `fs:meta:active_version` trỏ version; ghi xong mới swap; rollback được |
| Chống skew cửa sổ realtime | `feat_user_realtime_pit.sql` + `online_store.py` | Cả hai bên **cùng làm tròn về ô 5 phút**. Comment trong SQL ghi rõ nếu dùng `feature_ts - interval '1 hour'` thì lệch tới 5 phút dữ liệu |
| Point-in-time join | `feat_user_realtime_pit.sql` | Chỉ lấy event `event_ts < feature_ts` → không rò rỉ tương lai |
| Feature spec làm hợp đồng 3 bên | `spec.py` + `feature_spec.yml` | dbt / sync / API đọc chung; `validate_columns()` fail nếu lệch |
| At-least-once + dedup | `stream_consumer.py` + `stg_app_events.sql` | Commit offset sau cùng; dedup theo `event_id` |
| Idempotent sharded sync | `sync.py` | `is_shard_done()` → rerun an toàn |

**Ranh giới label đã đúng.** `feat_user_serving.sql` không chứa `label`/`is_treat`;
`offline_store.iter_shard()` chỉ đọc `entity_key + batch_names`. Nghĩa là **label
không bao giờ tới được Redis**. Đây là thứ nhiều team làm sai, ở đây đã đúng sẵn.

---

## 3. Kiến trúc bạn muốn (mục tiêu)

```
        DATASET (user đã tồn tại)
                 │
                 ▼
        [A] HISTORICAL SEED STATE
                 │
                 ▼
        ONLINE FEATURE STORE (Redis)
                 │
        [B] LATEST FEATURE STATE  ◄──────────┐
                 │                            │
                 ▼                            │
          Inference Service                   │
                 │                            │
          production model version            │
                 │                            │
             uplift score                     │  cập nhật
                 │                            │
            Policy Engine                     │
                 │                            │
        ┌────────┴────────┐                   │
        ▼                 ▼                   │
    VOUCHER          NO VOUCHER               │
                                              │
        [C] FUTURE SIMULATED EVENTS ──────────┘
              (T0 → T1 → T2 ...)
```

Điều kiện bắt buộc bạn đặt ra:

```
feature_lúc_train  ==  raw → feature_pipeline → feature_sinh_lại
```

---

## 4. Gap analysis (điểm cốt lõi)

### 4.1 · Phát hiện quyết định: có ba họ feature, nguồn gốc khác hẳn nhau

Model đọc **94 feature** (`spec.all_names`). Chúng chia làm ba họ hoàn toàn khác nhau
về khả năng tái tạo:

| Họ | Số cột | Nguồn | Có transformation trong repo? | Tái tạo được từ raw? |
|---|---|---|---|---|
| `f0`–`f82` | **83** | CSV DESCN | ❌ **Không có.** `stg_user_snapshot.sql` chỉ `cast(... as double)` | ❌ **Không** |
| `user_tenure_days`, `hist_order_cnt_30d`, `hist_gmv_30d`, `voucher_used_30d` | **4** | Event | ✅ `feat_user_behaviour.sql` | ✅ **Có** |
| `rt_*` (7 cột) | **7** | Event | ✅ `feat_user_realtime_pit.sql` (offline) + `online_store.py` (online) | ✅ **Có, và parity đã được thiết kế có chủ đích** |

### 4.2 · Vì sao `f0..f82` không thể reverse

Đây không phải thiếu sót của repo. Đây là **tính chất của dataset**:

- CSV được giao **ở mức feature đã tính xong**. Không có raw nào phía trước nó trong repo.
- Paper DESCN (`docs/2207.09920v3.pdf`, Bảng 1) xác nhận **"83 covariates"** cho
  Production Dataset, và **không công bố ý nghĩa của bất kỳ cột nào**. Section 4.1 chỉ
  mô tả cách thu thập, không có feature dictionary.
- Phép biến đổi tạo ra `f0..f82` xảy ra bên trong Lazada và không nằm trong repo này.

**Hệ quả trực tiếp:** điều kiện `original_train_feature == recomputed_from_raw` là

- ❌ **bất khả thi cho 83 cột `f*`** — không có raw, không có pipeline để chạy lại
- ✅ **đã khả thi và kiểm chứng được ngay cho 11 cột event-derived**

Nên "chứng minh feature parity" vẫn là deliverable thật, nhưng phạm vi đúng của nó là
**11 feature, không phải 94**.

### 4.3 · Tin tốt: mục tiêu của bạn không cần reverse `f*`

Đọc lại mục tiêu bạn viết ở mục 1 và 3: bạn cần **trạng thái lịch sử làm điểm xuất
phát**, rồi cần **feature thay đổi theo event tương lai**.

`f0..f82` đóng vai trò [A] — trạng thái lịch sử tĩnh. Chúng **không cần tái tạo**, chỉ
cần **seed đúng**. Và việc seed đó **đã chạy rồi**: `dag_40` → `sync.py` →
`feat_user_serving` → Redis, không lọc `split`, tức **cả 1,108,338 user train+test đều
đã được nạp vào Redis**.

Phần thay đổi theo event là 11 feature còn lại — chính là những feature **có lineage
chứng minh được**.

Vậy kiến trúc bạn muốn không cần reverse `f*`. Nó cần:

```
[A] f0..f82  = seed tĩnh từ dataset      → ĐÃ CÓ (dag_40)
[C] event    → 11 feature event-derived  → ĐÃ CÓ (stream_consumer + dbt)
[B] Redis latest state                    → ĐÃ CÓ (online_store)
    model                                 → ❌ CHƯA CÓ
    scenario driver T0→T1→T2              → ❌ CHƯA CÓ
```

### 4.4 · Bảng gap

| # | Gap | Mức độ | Chi tiết |
|---|---|---|---|
| **G1** | **Model không tồn tại** | 🔴 **Chặn toàn bộ** | `train.build_model()`, `fit_model()`, `evaluate()`, `MlflowUpliftModel.load()`, `.predict()` — tất cả `raise NotImplementedError`. Đang chạy `StubModel` (hàm băm). Không có model thì không có quyết định để test |
| **G2** | **Chưa biết model có nhạy với feature thay đổi không** | 🔴 **Rủi ro làm vô nghĩa cả scenario** | Xem 4.5 |
| **G3** | Không gian `user_id` lệch | 🟠 Cao | `_pick_user()` sinh `U0000000`–`U0019999` (pool 20,000) nhưng dataset có 1,108,338 user. ~98% user đã seed không bao giờ nhận event; và không kiểm tra user có tồn tại trong snapshot không |
| **G4** | User dataset không có lịch sử hành vi | 🟠 Cao | `feat_user_behaviour.sql` tính từ `stg_app_events`. User seed từ CSV chưa có event nào → cả 4 `hist_*` = 0 qua `coalesce`. Trạng thái lịch sử [A] rỗng một nửa |
| **G5** | Không có scenario driver | 🟠 Cao | Không có cách chạy **một user cụ thể** qua T0→T1→T2 rồi quan sát. Cần harness |
| **G6** | Không điều khiển được thời gian | 🟡 Trung bình | Mọi thứ dùng `now()`. Không tua nhanh được → không test nổi cửa sổ trượt 30 ngày |
| **G7** | Không có bài kiểm feature parity | 🟡 Trung bình | Chưa có test nào chứng minh `feat_user_realtime_pit` (offline) == `online_store.aggregate_realtime` (online). Comment trong SQL nói hai bên phải khớp từng giây, nhưng không có test tự động canh |
| **G8** | Policy engine quá đơn giản | 🟢 Thấp | `score >= uplift_threshold` hard-code trong `/decide`. Chưa có budget, cooldown, guardrail |

### 4.5 · G2 — rủi ro lớn nhất, cần nói thẳng

Bảng feature importance bạn gửi **chỉ chứa `f*` và các biến engineered từ `f*`**.
Không có một dòng nào cho `rt_events_1h`, `rt_order_1h`, `hist_gmv_30d`,
`user_tenure_days`.

Nghĩa là: hoặc 11 feature event-derived không có mặt lúc train, hoặc có mà xếp dưới
ngưỡng cắt. **Chưa xác định được.**

Điều này quan trọng vì: nếu model dồn gần hết trọng số vào 83 `f*` — vốn **đóng băng**
trong kịch bản của bạn — thì T0, T1, T2 sẽ cho ra **cùng một `uplift_score`, cùng một
quyết định**. Bàn thử nghiệm chạy trơn tru và không đo được gì.

> **Phải xác minh trước khi xây scenario driver:** giữ nguyên `f*`, chỉ đổi 11 feature
> event-derived trong khoảng thực tế của chúng, đo xem `uplift_score` dịch chuyển bao
> nhiêu. Nếu gần như không đổi thì phải xử lý (đưa `rt_*`/`hist_*` vào model với trọng
> số đủ lớn, hoặc chấp nhận rằng đây là bàn thử hạ tầng chứ không phải bàn thử quyết định).

---

## 5. Feature mapping (B) — chỉ ghi cái có bằng chứng

### 5.1 · Bảng mapping

| Feature | Raw source | Transformation | Semantic | Batch/Online | Event update được? | Bằng chứng |
|---|---|---|---|---|---|---|
| `f0`–`f82` (83) | **UNKNOWN** | **UNKNOWN** (ngoài repo) | **UNKNOWN** | Batch | ❌ Không | `stg_user_snapshot.sql` chỉ cast; paper Bảng 1 "83 covariates", không có dictionary |
| `user_tenure_days` | `stg_app_events.event_ts` | `date_diff('day', min(event_ts), now())` | Số ngày từ lần đầu thấy user | Batch | ⚠️ Gián tiếp (event đầu tiên) | `feat_user_behaviour.sql` |
| `hist_order_cnt_30d` | event `order`, 30 ngày | `count(*) filter (event_type='order')` | Số đơn 30 ngày | Batch | ✅ | `feat_user_behaviour.sql` + `var('history_days')=30` |
| `hist_gmv_30d` | event `order` | `sum(price*quantity) filter (order)` | GMV 30 ngày | Batch | ✅ | `feat_user_behaviour.sql`, `gmv` định nghĩa ở `stg_app_events.sql` |
| `voucher_used_30d` | event `voucher_claim` | `count(*) filter (voucher_claim)` | Voucher đã dùng 30 ngày | Batch | ✅ | `feat_user_behaviour.sql` |
| `rt_events_1h` | mọi event | `count(*)` cửa sổ 1h | Tổng event 1 giờ trượt | **Cả hai** | ✅ | `feat_user_realtime_pit.sql` + `stream_consumer._update_realtime()` |
| `rt_page_view_1h` | event `page_view` | `count(*) filter` | | **Cả hai** | ✅ | `EVENT_TO_COUNTER` trong `schemas.py` |
| `rt_add_to_cart_1h` | event `add_to_cart` | `count(*) filter` | | **Cả hai** | ✅ | `EVENT_TO_COUNTER` |
| `rt_order_1h` | event `order` | `count(*) filter` | | **Cả hai** | ✅ | `EVENT_TO_COUNTER` |
| `rt_gmv_1h` | event `order` | `sum(price*quantity)` | | **Cả hai** | ✅ | `stream_consumer.py` dòng tính `rt_gmv_1h` |
| `rt_session_len_sec` | mọi event | `date_diff('second', min, max)` | | Offline | ⚠️ Xem ghi chú | `feat_user_realtime_pit.sql` — **không thấy tính ở `_update_realtime()`** |
| `rt_last_event_ts` | mọi event | `max(event_ts)` | Freshness overlay | **Cả hai** | ✅ | `update_realtime_bulk()` |

> ⚠️ **Bất đối xứng phát hiện được:** `rt_session_len_sec` có công thức offline trong
> `feat_user_realtime_pit.sql` nhưng `stream_consumer._update_realtime()` chỉ cập nhật
> `rt_events_1h`, 3 counter trong `EVENT_TO_COUNTER`, `rt_gmv_1h` và `rt_last_event_ts`.
> Tức online **không bao giờ ghi** `rt_session_len_sec` → lúc serve luôn nhận default 0,
> lúc train nhận giá trị thật. **Đây là training/serving skew có thật, cần xác nhận và sửa.**

### 5.2 · Forensic trên dữ liệu thật — cấu trúc `f*`

Chạy trên toàn bộ 926,669 dòng train. Không suy diễn semantic, chỉ ghi cấu trúc đo được.

**Phát hiện 1 — `f40`–`f78` là 11 nhóm one-hot loại trừ lẫn nhau, KHÔNG phải 1 one-hot**

`row_sum(f40..f78) = 11.0` với **đúng 100% số dòng** (926,669/926,669). Kiểm tra loại
trừ theo cặp cho ra phân hoạch duy nhất:

| Nhóm | Cột | Số mức |
|---|---|---|
| G1 | `f40`, `f41`, `f42` | 3 |
| G2 | `f43`–`f52` | 10 |
| G3 | `f53`–`f62` | 10 |
| G4 | `f63`, `f64`, `f65` | 3 |
| G5 | `f66`, `f67` | 2 |
| G6 | `f68`, `f78` | 2 |
| G7 | `f69`, `f77` | 2 |
| G8 | `f70` | 1 — **hằng số = 1, không mang thông tin** |
| G9 | `f71`, `f72` | 2 |
| G10 | `f73`, `f74` | 2 |
| G11 | `f75`, `f76` | 2 |

**Phát hiện 2 — G6, G7, G9, G10 là CÙNG MỘT biến, lặp 4 lần**

```
f68 vs f71 : 0 dòng khác nhau
f68 vs f77 : 0 dòng khác nhau
f71 vs f77 : 0 dòng khác nhau
f68 vs f74 : 2 dòng khác nhau  (trên 926,669)
```

Vậy 8 cột `f68,f69,f71,f72,f73,f74,f77,f78` chỉ mã hoá **1 bit**. Sau khi gộp trùng và
bỏ `f70` hằng số: **`f40`–`f78` = 7 biến categorical thực sự, không phải 39 feature.**

**Phát hiện 3 — `f23` và `f25` gần như là cùng một cột**

```
f23 = f25 ở  924,595 / 926,669 dòng  (99.78%)
corr(f23, f25) = 0.999957
miền giá trị cả hai: [-7.346260241168491, 20.069979539571417]
```

**Phát hiện 4 — `f79`–`f82` là 4 cách mã hoá của cùng một biến 515 mức**

Cả bốn cột có **phân bố tần suất trùng khít**:

| Giá trị thứ | Số dòng (giống hệt cho cả f79, f80, f81, f82) |
|---|---|
| 1 | 663,495 (71.6%) |
| 2 | 28,969 |
| 3 | 3,947 |

Giá trị là float trong `[-0.67, 0.65]`, 515 giá trị phân biệt. **Không phải sentinel**
(giả thuyết cũ sai) — dạng này khớp với **target/mean encoding** của một biến
categorical 515 mức, mã hoá 4 kiểu khác nhau.

**Phát hiện 5 — `f30` là counter đã lấy log10**

```
f30: min=0.0   max=1.477121   n_distinct=30   825,425/926,669 dòng KHÔNG nguyên
log10(30) = 1.4771212547...
```

`max(f30)` khớp `log10(30)` tới 6 chữ số. Nhất quán với `f30 = log10(n)`, `n ∈ [1,30]`.
Là counter — nhưng **đã biến đổi log**, nên mọi cơ chế "cộng 1 khi có event" đều sai.

**Phát hiện 6 — `f1`, `f2` là số nguyên `[0, 365]`, và `f1 ≥ f2` luôn đúng**

```
f1: nguyên, [0, 365], 366 giá trị, avg 191.08
f2: nguyên, [0, 365], 366 giá trị, avg 187.90
f1 - f2: [0, 365], avg 3.18    ← không bao giờ âm
```

**Phát hiện 7 — `f27`, `f34` là số nguyên `[0, 100]`** với 32 và 31 giá trị phân biệt.
Miền `[0,100]` với ít giá trị rời rạc ⇒ **không phải** counter 0–30 như giả thuyết cũ.

### 5.3 · Hệ quả: bảng importance đang đếm trùng

Ghép Phát hiện 2, 3, 4 với bảng importance:

| Biến thực | Các cột trùng | Tổng gain bị cộng dồn |
|---|---|---|
| 1 biến liên tục | `f25` (8.53%) + `f23` (7.37%) | **15.90%** trên **một** biến |
| 1 biến nhị phân | `f68` (4.09%) + `f73` (1.01%) — `f73 ≈ 1 − f68` | **5.10%** trên **một** bit |
| 1 categorical 515 mức | `f80`+`f82`+`f79`+`f81` + 4 cờ `*_is_special` | rải rác nhiều dòng |

Nghĩa là **~21% "gain" ở đỉnh bảng thực chất thuộc về 2 biến**, không phải 6.

Thêm một điểm cần theo dõi: `f68` bằng 1 ở **98.89%** số dòng. Một cờ mất cân bằng
98.9/1.1 mà chiếm ~5% gain nghĩa là model đang tách trên nhóm thiểu số 1.11%
(~10,286 dòng). Có thể là tín hiệu thật, cũng có thể là overfit — **chưa xác định**.

### 5.4 · Bằng chứng từ paper + kiểm chứng lại trên CSV

Paper (Bảng 1) và CSV khớp nhau gần như tuyệt đối:

| Chỉ số | Paper | CSV thực đo | Khớp |
|---|---|---|---|
| Covariates | 83 | 83 (`f0`–`f82`) | ✅ |
| Train % treated | 22.1% | **22.2%** | ✅ |
| Test % treated | 51.6% | **52.1%** | ✅ |
| Train positive rate | 2.0% | **1.99%** | ✅ |
| Test positive rate | 3.5% | **3.52%** | ✅ |

CSV trong `data/` nhỏ hơn paper ~4.5 lần (926,669 vs 4.17M) ⇒ là **bản public
subsample**, nhưng giữ nguyên thiết kế thí nghiệm.

**Điều paper nói mà thay đổi kế hoạch:**

> *"the treatment assignment is selective due to the operation targeting strategy and
> we collect those data as our training set which includes strong treatment bias.
> We also have a slightly smaller size of users who are not affected by the targeting
> strategy and the treatment assignment follows the randomized controlled trials (RCT).
> We use them as our testing dataset."*

Tức: **train là dữ liệu quan sát có thiên lệch, test là RCT.** Đo trên CSV:

| | Train (observational) | Test (RCT) |
|---|---|---|
| CR treated | 5.66% | 3.70% |
| CR control | 0.94% | 3.33% |
| **Chênh lệch thô** | **+4.72 pp** | **+0.37 pp** |

**Chênh nhau 12.7 lần.** Con số +4.72pp ở train gần như hoàn toàn là treatment bias —
chiến lược targeting đã chọn sẵn người dễ chuyển đổi. Uplift thật chỉ **+0.37pp** trên
nền 3.3%.

> **Hệ quả:** `full_testset.csv` là **tài sản đánh giá không thiên lệch duy nhất của
> cả project**. Và tín hiệu trong nó rất mỏng.
>
> Với toàn bộ 181,669 dòng, sai số chuẩn của chênh lệch ≈ 0.087pp ⇒ z ≈ 4.3 — đo được.
> Nếu cắt xuống 50k như file kế hoạch cũ đề xuất, SE ≈ 0.16pp ⇒ z ≈ 2.3 — **mấp mé
> ngưỡng, không còn đủ tin cậy.**
>
> ⇒ **Không được đốt test set làm nguồn event stream.** Kịch bản online decisioning
> không cần label — nó cần feature state. Hãy lấy user từ **train** (926,669 user, dư
> thừa) để chạy scenario, và giữ nguyên vẹn test set cho việc đánh giá.

---

## 6. CONFIRMED / INFERRED / UNKNOWN

### ✅ CONFIRMED — có bằng chứng trực tiếp từ code, data hoặc paper

| # | Kết luận | Bằng chứng |
|---|---|---|
| C1 | `f0..f82` **không có** transformation nào trong repo | `stg_user_snapshot.sql` chỉ `cast(f{i} as double)` |
| C2 | 11 feature event-derived **có** lineage đầy đủ | `feat_user_behaviour.sql`, `feat_user_realtime_pit.sql`, `stream_consumer.py` |
| C3 | `f40`–`f78` = 11 nhóm one-hot loại trừ, `row_sum = 11.0` ở 100% dòng | Query trên 926,669 dòng |
| C4 | `f68 ≡ f71 ≡ f77`; `f74` lệch 2 dòng ⇒ 8 cột mã hoá 1 bit | `sum(abs(a-b))` = 0 |
| C5 | `f23 = f25` ở 99.78% dòng, `corr = 0.999957` | Query trực tiếp |
| C6 | `f79`–`f82` có phân bố tần suất trùng khít ⇒ 4 mã hoá của cùng 1 biến 515 mức | 663,495 / 28,969 / 3,947 giống hệt cả 4 cột |
| C7 | `f70` là hằng số 1 ⇒ zero information | `min=max=1`, `n_distinct=1` |
| C8 | `f1`, `f2` nguyên trong `[0,365]`; `f1 ≥ f2` luôn đúng | `min(f1-f2) = 0` |
| C9 | `f30` không nguyên, `max = 1.477121 = log10(30)` | Query |
| C10 | `f27`, `f34` nguyên trong `[0,100]`, 32/31 giá trị | Query |
| C11 | Paper: 83 covariates, **không công bố semantic** | `2207.09920v3.pdf` Bảng 1, §4.1 |
| C12 | Train = observational biased; Test = RCT | Paper §4.1 + đo lại: 22.2%/52.1% treated, +4.72pp/+0.37pp |
| C13 | Label **không** tới được Redis | `feat_user_serving.sql` không select label; `iter_shard()` chỉ đọc `batch_names` |
| C14 | Seed user state vào Redis **đã hoạt động**, không lọc split | `dag_40` + `sync.py` + `feat_user_serving.sql` |
| C15 | Model **chưa được implement** | `NotImplementedError` ở `train.py` (3 chỗ) và `model_loader.py` (2 chỗ) |
| C16 | `rt_session_len_sec` có ở offline, **không có** ở online update | So `feat_user_realtime_pit.sql` với `_update_realtime()` |
| C17 | `f30 = log10(n)` với `n` **nguyên trong [1,30]** — đúng **100%** số dòng | `10^f30` cho số nguyên ở 926,669/926,669 dòng (sai số tương đối < 1e-4). `f30` lưu 6 chữ số thập phân nên `max(10^f30) = 29.999982`, không phải sai lệch bản chất. **Cơ chế** confirmed; **semantic của `n`** vẫn UNKNOWN |

### 🟡 INFERRED — hợp lý nhưng CHƯA chứng minh

| # | Suy đoán | Dựa trên | Cần gì để xác nhận |
|---|---|---|---|
| I1 | `f1`, `f2` là chỉ số ngày trong năm | Nguyên, `[0,365]`, 366 giá trị, `f1 ≥ f2` | Không thể xác nhận nếu không có dictionary. **Chấp nhận là UNKNOWN khi thiết kế** |
| ~~I2~~ | ~~`f30` là `log10` của một bộ đếm ≤ 30~~ | → **đã nâng lên CONFIRMED (C17)**, xem bên dưới | ✅ đã kiểm |
| I3 | `f79`–`f82` là target/mean encoding | Phân bố tần suất trùng khít, float, 515 mức | Không thể xác nhận nếu không có dictionary |
| I4 | 7 nhóm one-hot là thuộc tính categorical nghiệp vụ | Cấu trúc one-hot | Không xác nhận được |
| I5 | `f68` mất cân bằng 98.9/1.1 mà gain cao là overfit | Tỉ lệ + share | Chạy thêm seed, xem ổn định |

### ❌ UNKNOWN — không có bằng chứng, không được đoán

- Ý nghĩa nghiệp vụ của **mọi** `f0`–`f82`
- Raw field nào sinh ra chúng
- Cửa sổ thời gian của các counter đã mã hoá (`f30` là log của cái gì? 30 ngày? 30 lần?)
- 515 mức của `f79`–`f82` là gì
- 7 biến categorical là những thuộc tính gì
- `f*` có được tính point-in-time đúng lúc Lazada tạo dataset không

> **Nguyên tắc áp dụng:** không cột `f*` nào được gán semantic trong code. Chúng là
> **vector trạng thái lịch sử mờ**, seed nguyên trạng, không diễn giải.

---

## 7. Phần C–G theo yêu cầu

### C · Reverse Feasibility

| Nhóm feature | Reverse về raw? | Bằng cách nào | Mất thông tin? |
|---|---|---|---|
| `f0`–`f82` (83) | ❌ **Không** | Không có transformation để đảo. CSV đã ở mức feature | Không áp dụng — **không reverse, chỉ seed nguyên trạng** |
| 4 `hist_*` / `tenure` | ✅ Có | Sinh event `order`/`voucher_claim` với `price`,`quantity`,`event_ts` phù hợp rồi chạy `feat_user_behaviour.sql` | Có — nhiều chuỗi event cho cùng một tổng. Không quan trọng: ta chỉ cần **tiến về phía trước**, không cần khôi phục quá khứ |
| 7 `rt_*` | ✅ Có | Sinh event trong cửa sổ 1h | Tương tự |

**Kết luận:** cái duy nhất cần "reverse" là 11 feature event-derived, và với chúng thì
không cần reverse thật — chỉ cần **sinh event tiến về phía trước** rồi để pipeline tính.

### D · Feature Reproduction — cách chứng minh parity

Vì `f*` không tái tạo được, bài kiểm parity phải nhắm vào chỗ có lineage. Ba mức:

**Mức 1 — parity offline↔online cho `rt_*` (quan trọng nhất, hiện chưa có test)**

```
sinh N event đã biết trước cho user U tại các mốc t
        │
        ├─→ Kafka → stream_consumer → Redis rt:u:U
        │      → online_store.aggregate_realtime()          → giá trị ONLINE
        │
        └─→ lake → stg_app_events → feat_user_realtime_pit  → giá trị OFFLINE

    assert ONLINE == OFFLINE  cho cả 7 rt_*
```

Đây chính là bài test sẽ bắt được C16 (`rt_session_len_sec` lệch). Ngưỡng dùng
`quality.online_offline_tolerance = 0.0001` đã khai báo sẵn trong spec.

**Mức 2 — parity batch cho 4 `hist_*`**

Chạy `feat_user_behaviour.sql` hai lần trên cùng tập event → phải bất biến. Rồi so với
giá trị đọc từ Redis sau khi `dag_40` sync.

**Mức 3 — parity seed cho `f*`**

Không phải parity tính toán, mà là **parity truyền tải**: giá trị `f*` trong
`marts.training_dataset` phải **bằng đúng từng bit** giá trị `f*` đọc từ Redis cho cùng
`user_id`. Nếu lệch, lỗi nằm ở đường sync chứ không ở định nghĩa feature.

### E · Online State

| Feature | Seed vào Redis | Update realtime | Event nào | Cửa sổ | TTL |
|---|---|---|---|---|---|
| `f0`–`f82` | ✅ `fs:{ver}:u:{uid}` | ❌ không bao giờ | — | — | không TTL khi active; `stale_ttl_seconds=86400` khi retire |
| `user_tenure_days` | ✅ batch | ❌ | — | từ `min(event_ts)` | như trên |
| `hist_order_cnt_30d` | ✅ batch | ❌ (chỉ batch hằng ngày) | `order` | 30 ngày | như trên |
| `hist_gmv_30d` | ✅ batch | ❌ | `order` | 30 ngày | như trên |
| `voucher_used_30d` | ✅ batch | ❌ | `voucher_claim` | 30 ngày | như trên |
| `rt_events_1h` | ❌ | ✅ `rt:u:{uid}` | mọi event | 12 ô × 5 phút | `realtime_ttl_seconds=3600` |
| `rt_page_view_1h` | ❌ | ✅ | `page_view` | như trên | 3600 |
| `rt_add_to_cart_1h` | ❌ | ✅ | `add_to_cart` | như trên | 3600 |
| `rt_order_1h` | ❌ | ✅ | `order` | như trên | 3600 |
| `rt_gmv_1h` | ❌ | ✅ | `order` (`price×quantity`) | như trên | 3600 |
| `rt_session_len_sec` | ❌ | ⚠️ **KHÔNG** — xem C16 | — | — | — |
| `rt_last_event_ts` | ❌ | ✅ (set, không cộng dồn) | mọi event | — | 3600 |

**Khoảng trống thiết kế cần bạn quyết:** 4 feature `hist_*` chỉ cập nhật **theo batch
hằng ngày**. Trong kịch bản T0→T1→T2 diễn ra trong vài phút, một đơn hàng mới sẽ làm
`rt_order_1h` tăng ngay nhưng `hist_order_cnt_30d` **không đổi** cho tới lần sync sau.

Đó là hành vi đúng của kiến trúc lambda, nhưng nó thu hẹp thứ scenario của bạn quan sát
được: chỉ 7 `rt_*` (thực tế 6, do C16) là thực sự chuyển động trong thời gian ngắn.

### F · Inference Flow

```
POST /decide {user_id}
   │
   ├─ store.read_for_serving(uid)          ← 1 round-trip Lua, atomic
   │     GET fs:meta:active_version
   │     HGETALL fs:{ver}:u:{uid}          → batch_raw  (87 field)
   │     HGETALL rt:u:{uid}                → rt_raw     (ô 5 phút)
   │
   ├─ store.aggregate_realtime(rt_raw)     ← cộng ô trong cutoff → 7 rt_*
   ├─ spec.merge(batch_raw, {**realtime, **req.context})
   │     batch ← realtime ghi đè ← context ghi đè
   │     thiếu → Feature.default
   │
   ├─ model.predict_uplift([merged])       ← ❌ StubModel
   │
   ├─ decision = score >= settings.uplift_threshold ? SEND_VOUCHER : NO_VOUCHER
   │     score is None → NO_DECISION
   │
   └─ inference_logger.log(...)            ← hàng đợi, worker ghi Postgres theo lô
```

**Về model version:** `model_loader.py` đã có sẵn đúng cấu trúc bạn muốn —
`MODEL_STAGE=Production` mặc định, comment gợi ý `get_model_version_by_alias()`, và
`feature_order` đọc từ `feature_list.json` trong artifact để chống lệch thứ tự cột.
Chưa implement, nhưng **thiết kế đã đúng hướng, không cần đổi**.

### G · Simulation Flow (đề xuất, chưa code)

```
1. chọn user U từ TRAIN split (không dùng test — xem 5.4)
2. dag_40 đã seed f0..f82 của U vào fs:{ver}:u:U          ← ĐÃ CÓ
3. [tuỳ chọn] backfill event lịch sử để 4 hist_* khác 0    ← giải G4
4. T0: POST /decide {U}         → ghi lại score₀, decision₀
5. simulator sinh event tương lai cho U (mù với label)     ← CHƯA CÓ
      → Kafka → stream_consumer → rt:u:U
6. T1: POST /decide {U}         → score₁, decision₁
7. lặp lại → T2, T3...
8. assert: score thay đổi theo hướng mong đợi khi event thay đổi
```

Bước 5 là thứ duy nhất phải viết mới. Bước 2, 6 đã chạy được ngay hôm nay.

---

## 8. Những gì file kế hoạch cũ nói sai

| Điều file cũ khẳng định | Thực tế đo được | Ảnh hưởng |
|---|---|---|
| `f40`–`f78` là **một** one-hot 39 mức | **11 nhóm** one-hot, `row_sum = 11` | Toàn bộ cách xử lý khối này sai |
| `f68`, `f73`, `f75` là "mức sống sót của cùng khối" | Thuộc **các nhóm khác nhau**; `f68≡f71≡f77` là 1 bit lặp 4 lần; `f73 ≈ 1−f68` | Đếm gain sai |
| 13 cột "vắng mặt" là bị drop vì all-zero | **Tất cả 39 cột đều có trong CSV**. Chúng bị pipeline importance loại (drop-one-level + lọc near-constant) | Suy luận về khối one-hot sai từ gốc |
| `f79`–`f82` là "sentinel-coded" | Float liên tục `[-0.67, 0.65]`, 4 mã hoá của cùng 1 biến 515 mức. **Không có sentinel** | Cơ chế mô phỏng đề xuất sai |
| `f27`/`f30`/`f34` là "bộ đếm cửa sổ ~1 tháng" | `f30` = log10, `[0, 1.477]`; `f27`/`f34` nguyên `[0,100]` — **ba thứ khác nhau** | "Event làm counter tăng 1" sai với cả ba |
| Model-32 ≈ 22 biến tiềm ẩn | Trùng lặp nhiều hơn nhiều: `f23=f25`, `f68` lặp 4 lần, `f79`–`f82` cùng 1 biến | Con số 32 phải tính lại |
| Chia test 130k stream / 50k holdout | Uplift thật chỉ +0.37pp; 50k đưa z từ 4.3 xuống 2.3 | **Không được đốt test set** |
| "Live-12" gồm `f30`,`f27`,`f34`,`f1`,`f2`,`f79`–`f82`… | Đây là **INFERRED**, không cột nào chứng minh được cơ chế | Live set đúng là **11 feature event-derived có lineage** |

Điểm cốt lõi: file cũ chọn feature "live" theo **semantic phỏng đoán**. Audit này cho
thấy đã tồn tại sẵn một live set **chứng minh được** — 11 feature event-derived — và đó
mới là chỗ nên xây.

---

## 9. Implementation plan

### Phase 0 — Forensics *(phần lớn đã xong trong audit này)*

- [x] `f40`–`f78`: xác định 11 nhóm one-hot
- [x] Phát hiện trùng lặp `f68/f71/f74/f77`, `f23/f25`, `f79`–`f82`
- [x] `f1`,`f2`,`f27`,`f30`,`f34`: đo miền giá trị và tính nguyên
- [x] Đối chiếu paper ↔ CSV; xác nhận train biased / test RCT
- [x] Kiểm `10^f30` có ra số nguyên 1..30 không → **CONFIRMED (C17)**, 100% số dòng
- [ ] Lặp lại toàn bộ trên `full_testset.csv` — xác nhận cấu trúc giống train
- [ ] Chạy thêm seed cho các feature `n_estimators=4`

### Phase 1 — Feature/Raw reconstruction

- [ ] Chốt: `f*` = seed tĩnh, **không diễn giải, không mô phỏng**
- [ ] Gộp cột trùng trước khi train: `f23`|`f25` giữ 1; `f68`,`f71`,`f74`,`f77` giữ 1;
      bỏ `f70`; mỗi nhóm one-hot bỏ 1 mức chuẩn
- [ ] Viết lại feature set dựa trên **biến**, không phải cột
- [ ] Giải **G4**: backfill event lịch sử cho user được chọn để 4 `hist_*` khác 0

### Phase 2 — Feature parity validation

- [ ] Test parity online↔offline cho 7 `rt_*` (Mức 1 ở phần D)
- [ ] **Sửa C16** — `rt_session_len_sec` thiếu ở online update
- [ ] Test parity truyền tải `f*`: `training_dataset` == Redis
- [ ] Cắm ngưỡng `quality.online_offline_tolerance` vào CI

### Phase 3 — Redis seed *(đã có, cần xác minh)*

- [ ] Xác nhận `dag_40` seed đúng user từ train split
- [ ] Giải **G3** — không gian `user_id`: producer phải lấy user **có thật** trong snapshot

### Phase 4 — Future event simulator

- [ ] `decompose`/`generate` với signature **không nhận `label`, không nhận `is_treat`**
- [ ] Sinh event tương lai cho user đã seed
- [ ] `PRODUCER_MODE` chọn giữa traffic ngẫu nhiên hiện tại và scenario-driven

### Phase 5 — Online feature update *(đã có, cần đo)*

- [ ] Đo độ trễ event → Redis
- [ ] Xác minh cửa sổ trượt 5 phút hoạt động đúng khi tua nhanh thời gian (**G6**)

### Phase 6 — Model inference

- [ ] **Implement model** (G1) — T-learner + LightGBM là đường ngắn nhất
- [ ] Implement `MlflowUpliftModel.load()` / `.predict()`, dùng alias Production
- [ ] Log `feature_list.json` lúc train để cố định thứ tự cột
- [ ] **Đo độ nhạy (G2)**: giữ `f*` cố định, quét 11 feature event-derived, xem
      `uplift_score` dịch bao nhiêu. **Nếu không dịch, scenario vô nghĩa — dừng và xử lý**

### Phase 7 — Voucher decision

- [ ] Tách policy khỏi `/decide` thành module riêng
- [ ] Thêm budget / cooldown / guardrail nếu cần

### Phase 8 — End-to-end validation

- [ ] Scenario driver: chạy 1 user qua T0→T1→T2, xuất bảng
      `(t, event mới, feature đổi, score, decision)`
- [ ] Chạy trên nhiều user, kiểm tra decision đổi đúng hướng
- [ ] Đánh giá model trên **test set nguyên vẹn** (AUUC/Qini)

### Phase 9+ — Chỉ làm sau khi core chạy

- [ ] Synthetic uplift ground truth `τ(x)`
- [ ] Optuna calibration
- [ ] Adversarial validation

---

## 10. Ba việc tôi đề nghị làm ngay

**1. Sửa C16** — `rt_session_len_sec` là training/serving skew có thật, đang tồn tại
trong code production, độc lập với mọi kế hoạch mô phỏng. Sửa rẻ, giá trị ngay.

**2. Trả lời G2 trước khi xây bất cứ thứ gì** — nếu `uplift_score` không phản ứng với
11 feature event-derived thì toàn bộ scenario T0→T1→T2 sẽ chạy đẹp mà không đo được gì.
Phải biết điều này **trước**, không phải sau.

**3. Gộp cột trùng rồi train lại** — `f23`/`f25` và bộ `f68` đang được đếm hai lần
trong importance. Feature set hiện tại chứa dư thừa đo được, và nó bóp méo mọi quyết
định chọn feature phía sau.

---

## Phụ lục · Cách tái lập forensic

```bash
# Materialize CSV → parquet (nhanh hơn ~30x khi query lặp)
python -c "
import duckdb; con=duckdb.connect()
con.execute(\"COPY (SELECT * FROM read_csv('data/full_trainset.csv',header=true,auto_detect=true,sample_size=200000)) TO 'train.parquet' (FORMAT PARQUET)\")
"

# Kiểm one-hot
SELECT f40+f41+...+f78 AS rs, count(*) FROM 'train.parquet' GROUP BY 1;

# Kiểm trùng lặp
SELECT sum(abs(f68-f77)), sum(abs(f23-f25)) FROM 'train.parquet';
```

Toàn bộ số liệu trong tài liệu này đo trên **toàn bộ** 926,669 dòng train, không lấy mẫu.
