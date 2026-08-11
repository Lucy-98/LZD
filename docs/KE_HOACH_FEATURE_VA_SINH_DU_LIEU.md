# Kế hoạch: chọn feature & sinh dữ liệu realtime

> # ⛔ TÀI LIỆU ĐÃ BỊ THAY THẾ — KHÔNG DÙNG ĐỂ IMPLEMENT
>
> **Thay bằng:** [`AUDIT_KIEN_TRUC_VA_FEATURE.md`](AUDIT_KIEN_TRUC_VA_FEATURE.md)
>
> Forensic chạy trên toàn bộ 926,669 dòng `full_trainset.csv` đã **bác bỏ** các
> giả thuyết trung tâm của tài liệu này:
>
> - `f40`–`f78` **không** phải một one-hot 39 mức — là **11 nhóm** one-hot
>   (`row_sum = 11.0` ở 100% số dòng)
> - `f79`–`f82` **không** sentinel-coded — là 4 mã hoá của cùng 1 biến 515 mức
> - `f27`/`f30`/`f34` **không** cùng loại — `f30` đã lấy log10, hai cái kia nguyên `[0,100]`
> - `f23` và `f25` **gần như trùng nhau** (99.78% số dòng) — đỉnh bảng importance đang đếm trùng
> - Đề xuất "stream 130k / holdout 50k" **sai** — uplift thật chỉ +0.37pp, cắt test set
>   làm hỏng tài sản đánh giá duy nhất
>
> Bảng đối chiếu đầy đủ: Phần 8 của tài liệu thay thế.
>
> Giữ file này lại chỉ để tham chiếu lịch sử.

> Tài liệu này gộp lại kết quả phân tích bảng feature importance (F1/F2) và
> thiết kế cách sinh dữ liệu realtime cho pipeline.
>
> **Trạng thái:** bản thiết kế, chưa code. Giai đoạn 0 phải chạy trước khi
> tin các con số ở Phần 2.

---

## Mục lục

1. [Vấn đề đang có](#1-vấn-đề-đang-có)
2. [Đọc được gì từ bảng importance](#2-đọc-được-gì-từ-bảng-importance)
3. [Chọn bao nhiêu feature](#3-chọn-bao-nhiêu-feature)
4. [Thiết kế sinh dữ liệu realtime](#4-thiết-kế-sinh-dữ-liệu-realtime)
5. [Ba ràng buộc bắt buộc](#5-ba-ràng-buộc-bắt-buộc)
6. [Lộ trình thực hiện](#6-lộ-trình-thực-hiện)
7. [Cách kiểm chứng](#7-cách-kiểm-chứng)
8. [Bẫy đã biết](#8-bẫy-đã-biết)

---

## 1. Vấn đề đang có

Đọc code hiện tại thấy hai luồng dữ liệu **không gặp nhau ở tầng feature**:

| Luồng | File | Sinh ra gì |
|---|---|---|
| Batch | `ingestion/seed_loader.py` | `f0..f82` nạp **một lần** từ CSV rồi đóng băng |
| Stream | `ingestion/event_producer.py` | Event ngẫu nhiên → chỉ ra `rt_*` counters |

Chúng chỉ gặp nhau ở `user_id` trên Redis, không gặp nhau ở tầng feature.

**Hệ quả:** `f25`, `f23`, `f30`, `f68` — đúng 4 feature mà model dựa vào nhiều
nhất — **không hề nhúc nhích trong realtime**. Pipeline đang serve một model
tĩnh với một lớp overlay trang trí bên cạnh.

Đó là lý do cần kế hoạch này.

---

## 2. Đọc được gì từ bảng importance

Cách xếp hạng: gộp F1 và F2, lấy trung bình `normalized_share` của mỗi
feature qua các set nó xuất hiện, rồi chuẩn hoá lại.

> **Vì sao không dùng `consensus_rank` trực tiếp:** rank chỉ nói vị trí,
> không nói độ lớn đóng góp. Ví dụ `f68` chỉ đứng rank 17/21 nhưng chiếm
> **4.1%** tổng gain, trong khi `f40` rank 33 chỉ được 0.9%.

### 2.1 · Bảng xếp hạng gộp

**Nhóm A — 20 feature mạnh nhất (59.6% tổng gain)**

| # | Feature | Share | rank F1→F2 | Ghi chú |
|---|---|---|---|---|
| 1 | `f25` | 8.53% | 4→1 | |
| 2 | `f23` | 7.37% | 11→10 | |
| 3 | `f68` | 4.09% | 17→21 | nhị phân, gain rất cao |
| 4 | `f30` | 3.92% | 3→2 | ổn định nhất bảng |
| 5 | `f1` | 3.34% | 2→13 | ⚠ spread 75 |
| 6 | `f2` | 3.08% | 1→9 | ⚠ spread 65 |
| 7 | `f31` | 3.05% | 7→6 | |
| 8 | `f80` | 2.68% | 5→3 | |
| 9 | `f9` | 2.66% | 9→5 | |
| 10 | `f16` | 2.46% | 6→4 | rất ổn định |
| 11 | `f53` | 2.20% | 20→18 | nhị phân, chỉ 4/5 seed |
| 12 | `f81_is_special` | 2.03% | –→52 | ⚠ chỉ 4/5 seed, spread 63 |
| 13 | `f27` | 2.01% | 13→8 | |
| 14 | `f26` | 1.84% | 8→7 | |
| 15 | `f29` | 1.84% | 26→23 | |
| 16 | `f28` | 1.77% | 24→28 | spread 69 |
| 17 | `f12` | 1.75% | 12→11 | |
| 18 | `f82` | 1.66% | 34→14 | nhảy 20 bậc khi có FE |
| 19 | `f13` | 1.64% | 10→12 | ổn định |
| 20 | `f6` | 1.63% | 15→16 | |

**Nhóm B — 10 feature tiếp theo → 74.1% tích luỹ**

`f11` `f3` `f17` `f37` `f21` `f22` `f4` `f34` `f24` `f10`

**Nhóm C — 5 feature nữa → 80.4% tích luỹ**

`f35` `f79` `f38` `f65` `f20`

**Đuôi — 27 feature, tổng cộng chỉ 4.9% gain**

```
f62 f44 f61 f59 f66 f43 f75 f60 f64 f46 f52 f58 f54 f48
f45 f49 f50 f57 f55 f56 f47 f51
f1_f2_gap  f34_is_special  f82_is_special  f80_is_special  f1_f2_is_gap
```

Đặc điểm chung: `mean_rank` 50–68 ở **cả hai** feature set, `n_uniq = 2`,
share riêng lẻ < 0.4%. Không cái nào bị hai set đánh giá mâu thuẫn → tín hiệu
bỏ khá chắc.

### 2.2 · Phát hiện quan trọng nhất: khối one-hot

**`f40`–`f66` là 27 cột liên tiếp, tất cả `n_uniq = 2`.**

Không dataset thật nào có 27 cờ nhị phân độc lập nằm cạnh nhau. Đây gần như
chắc chắn là **one-hot của MỘT biến categorical**.

Bằng chứng thêm: 13 cột vắng mặt khỏi cả hai bảng là
`f15, f32, f33, f39, f67, f69, f70, f71, f72, f74, f76, f77, f78`.

Để ý cụm `f67, f69–f72, f74, f76–f78` nằm **xen kẽ** giữa khối binary và
`f79`. Nghĩa là khối one-hot thật sự có thể là **`f40`–`f78` (39 mức)**,
trong đó 12 mức bị drop vì all-zero.

> ⚠️ **Điều này có nghĩa `f68`, `f73`, `f75` không phải cờ riêng lẻ —
> chúng là các mức sống sót của cùng khối đó.** Và `f68` chiếm 4.1% gain.
>
> Bạn không "chọn `f68`", bạn chọn cả biến gốc.

### 2.3 · Các họ feature khác đọc được từ `n_uniq`

| Nhóm | `n_uniq` | Diễn giải khả dĩ |
|---|---|---|
| `f1`, `f2` | 366, 366 | Hai mốc **ngày** (day-of-year / recency) |
| `f79`–`f82` | 515 × 4 | Một họ 4 biến **cùng thang đo** (4 cửa sổ? 4 kênh?) |
| `f27`,`f30`,`f34` | 32, 30, 31 | Bộ đếm cửa sổ ~1 tháng |
| `f0`, `f7` | 6, 6 | Cặp categorical nhỏ |
| `f3`,`f17` | 1898, 1882 | Gần như cặp |
| `f6`,`f14` | 851, 852 | Gần như cặp |

Việc `f1_f2_gap` đã được engineer sẵn cho thấy người làm cũng đoán `f1`/`f2`
là hai ngày — và đoán đúng, nhưng công thức chưa tối ưu (gap chỉ được 0.30%).

Cả 4 biến `f79`–`f82` đều có biến thể `*_is_special` ⇒ chúng có **giá trị
sentinel** (kiểu −1 / 999 đại diện cho "không có dữ liệu").

### 2.4 · Điều đã kiểm tra và bác bỏ

Ban đầu nghi `f23`/`f25` là ID rò rỉ (`n_uniq` = 256184/256185, chênh đúng 1).

**Đã bác bỏ.** Train có **926,669 dòng** nhưng `f25` chỉ 256,185 giá trị
unique (27.6%). Một ID phải unique ~100%. Vậy chúng là biến liên tục
cardinality cao — tiền hoặc timestamp — hoàn toàn hợp lệ.

Chi tiết chênh đúng 1 vẫn đáng chú ý, nhưng đọc theo hướng khác: hai biến
**dẫn xuất từ nhau** (giá gốc / giá sau giảm, hoặc hai mốc thời gian lệch
nhau). Cần xác nhận ở Giai đoạn 0.

### 2.5 · Cảnh báo về độ ổn định

| Vấn đề | Feature | Ý nghĩa |
|---|---|---|
| `n_estimators = 4` (không đủ 5 seed) | `f53` `f65` `f73` `f41` `f43` `f81_is_special` `f82_is_special` `f1_f2_gap` | Chạy thêm seed trước khi tin |
| Tụt hạng mạnh khi thêm FE | `f1` (2→13), `f2` (1→9) | Bị `*_is_special` ăn tín hiệu ⇒ tương quan cao |
| Nhảy hạng mạnh | `f82` (34→14) | |

`f81_is_special` đáng ngờ nhất: share 2.03% (top 12) nhưng chỉ 4/5 seed thấy
được và spread 63. Rất có thể một seed cho nó gain cực lớn còn các seed khác
gần như bỏ qua.

---

## 3. Chọn bao nhiêu feature

### 3.1 · Đếm bằng biến tiềm ẩn, không phải cột

Nếu giả thuyết one-hot đúng, `f40`–`f78` là **một biến**. Quy đổi:

| Biến tiềm ẩn | Cột thuộc về nó |
|---|---|
| Categorical 27–39 mức | `f40`–`f78` (gồm `f68`,`f53`,`f65`,`f63`,`f73`…) |
| Cặp ngày | `f1`, `f2` |
| Họ 4 cửa sổ | `f79`,`f80`,`f81`,`f82` |
| Bộ đếm tháng | `f27`, `f30`, `f34` |
| Cặp dẫn xuất | `f23`, `f25` |
| High-card | `f28`,`f29`,`f10`,`f20`,`f22` |
| Đơn lẻ (16 cái) | `f31`,`f9`,`f16`,`f26`,`f12`,`f13`,`f6`,`f11`,`f3`,`f17`,`f37`,`f21`,`f4`,`f24`,`f35`,`f38` |

**32 cột ≈ 22 biến tiềm ẩn.**

Điều này quan trọng vì chi phí calibrate simulator scale theo **biến tiềm ẩn**,
không theo cột — ma trận tương quan cần khớp là 22×22 chứ không 32×32. Đó là
khác biệt giữa "Optuna 200 trial là xong" và "không hội tụ".

### 3.2 · Thang bậc lồng nhau

Đừng chọn một con số. Định nghĩa 3 bộ **lồng nhau**, chạy cả 3 qua đúng pipeline:

| Bộ | Cột | Gain | Dùng để |
|---|---|---|---|
| **Guard-52** | top 52 (bỏ đuôi 27) | 95.1% | Baseline an toàn, chỗ lui về |
| **Model-32** | top 30 + one-hot gộp | ~75% | **Bộ chính** — train, serve, calibrate |
| **Live-12** | tập con của 32 | ~45% | Bộ được mô phỏng bằng cơ chế thật |

Kỳ vọng: Guard-52 và Model-32 chênh nhau **dưới 1% AUUC**. Nếu chênh nhiều
hơn, tức trong đuôi có tương tác mà importance đơn biến không thấy — cũng là
một phát hiện đáng giá.

### 3.3 · Live-12 gồm những gì

```
f30, f27, f34        <- bộ đếm cửa sổ, event tăng trực tiếp
f1, f2               <- cặp ngày, trôi theo đồng hồ
f79, f80, f81, f82   <- họ 4 cửa sổ (1 cơ chế, 4 output)
onehot_cat           <- biến categorical gộp (chứa f68, f53, f65)
f23, f25             <- tiền/thời gian
```

20 cột còn lại của Model-32 để **frozen**: replay từ `raw/user_snapshot` qua
batch layer, đúng như hiện tại. Chúng là thuộc tính tĩnh của user.

### 3.4 · Con số đổi theo model nào

`training/train.py` để ngỏ 3 hướng, chúng chịu được số feature rất khác nhau:

| Model | Số cột hợp lý | Lý do |
|---|---|---|
| **T-learner + LightGBM** | 50–60 | Cây rất chịu được feature thừa. Với 926k dòng thì thoải mái |
| **X-learner** | 32–40 | 4 sub-model, variance cộng dồn qua tầng imputation |
| **DESCN** | 32 | NN muốn đầu vào ít chiều; **xử lý one-hot tốt nhất** vì học được embedding cho biến 39 mức thay vì 39 cột thưa |

> Nếu train cả 3 để so sánh: **dùng chung một bộ 32 cột cho cả ba.**
> So sánh model mà đổi feature set thì kết quả vô nghĩa.

---

## 4. Thiết kế sinh dữ liệu realtime

### 4.1 · Nguyên tắc nền: train dạy cơ chế, test cấp đích đến

```
full_trainset.csv (926,669 dòng)  ->  học CƠ CHẾ  (event sinh ra feature thế nào)
full_testset.csv  (181,669 dòng)  ->  cấp ĐÍCH ĐẾN (feature phải bằng bao nhiêu)
```

**Vì sao phải tách nguồn:** nếu calibrate simulator trên train, rồi đánh giá
model (vốn cũng train trên train) bằng output của simulator, bạn có một
**vòng kín** — simulator học phân phối train, model học phân phối train, hai
bên đồng ý với nhau *theo cấu tạo*. Mọi metric đo được đều là hư ảo.

**Vì sao hai vai bổ sung nhau:** phân rã một dòng test thành event là bài toán
**thiếu xác định** — `f30 = 17` có vô số chuỗi event tạo ra nó. Cần cơ chế học
từ train để lấp các bậc tự do. Test một mình không sinh được event; train một
mình rơi vào vòng kín. Ghép lại mới chạy.

### 4.2 · Đơn giản hoá quan trọng nhất: seed rồi tiến hoá

`f30` là bộ đếm 30 ngày. Để tái tạo `f30 = 17` bằng event, phải rải 17 event
trên 30 ngày lịch sử. Nhưng stream chỉ chạy từ bây giờ trở đi.

**Đừng cố đảo ngược phép tổng hợp.** Dùng đúng kiến trúc hai tầng đã có
trong `config/features/feature_spec.yml`:

| Tầng | Redis key | Vai trò |
|---|---|---|
| Batch | `fs:{version}:u:{user_id}` | **Trạng thái khởi đầu** — ghi thẳng `f*` của dòng test |
| Overlay | `rt:u:{user_id}` | **Chuyển động** — chỉ phát phần tăng thêm từ giờ trở đi |

> Dòng test cấp **điểm xuất phát**, stream cấp **vận tốc**.
>
> Bài toán tụt từ "nghịch đảo thiếu xác định" xuống "sinh delta tiến" —
> dễ hơn cả bậc độ lớn.

Điều này cũng giải thích vì sao Live-12 là đúng bộ: chỉ feature có cơ chế tăng
trưởng rõ ràng mới cần delta. Phần frozen đã nằm sẵn ở tầng batch.

### 4.3 · Hai chế độ producer

Thêm một cờ env, hai chế độ phục vụ hai mục đích khác nhau — **cần cả hai**:

| Chế độ | Nguồn | Trả lời câu hỏi |
|---|---|---|
| `PRODUCER_MODE=replay` | Dòng thật từ test split | "Pipeline có chạy đúng không?" — freshness, skew, alert Grafana |
| `PRODUCER_MODE=synth` | Simulator calibrate từ train | "Model có học đúng uplift không?" |

**Vì sao cần `synth`:** chỉ có simulator mới bake được hàm uplift ground-truth
`τ(x)` vào dữ liệu. Với nó bạn đo được:

- Model có recover đúng `τ` không (không phải chỉ AUUC/Qini xấp xỉ)
- Qini curve **lý thuyết tối đa** là bao nhiêu → biết model còn cách trần bao xa
- Sleeping-dog có bị bắt đúng không — thứ mọi metric tổng hợp đều che giấu

Đây là thứ **dữ liệu thật không bao giờ cho được**, vì uplift cá nhân không
quan sát được.

Gợi ý `τ(x)` có cấu trúc thú vị, sát bài toán Lazada thật:
- **Persuadable** ở user tenure trung bình + GMV thấp
- **Sleeping-dog** ở user GMV cao (phát voucher cho khách sộp = mất biên)

### 4.4 · Vòng calibrate cho chế độ `synth`

Simulator có bộ tham số θ (~30–50 số): tần suất session, xác suất chuyển
trạng thái trong `SESSION_FLOW`, tham số lognormal của giá, phân phối tenure,
tỉ lệ claim voucher, phân phối biến categorical 39 mức...

```
loss(θ) = Σ_j  w_j · Wasserstein( f_j^sim(θ) , f_j^real )   # marginal
        + λ  · ‖ Σ_sim(θ) − Σ_real ‖_F                       # cấu trúc phụ thuộc
```

**`w_j` chính là cột `normalized_share`** từ bảng importance. Công sức hiệu
chỉnh tự động dồn vào nơi model quan tâm: `f25` (8.5%) được ưu tiên gấp 200
lần `f51` (0.03%).

Chạy bằng Optuna, 200–300 trial. Mỗi trial: sinh 50k user ảo → chạy qua dbt
→ tính loss.

> ⚠️ **Đừng bỏ hạng mục `λ`.** Nếu chỉ tối ưu marginal, bạn sẽ được 30
> histogram hoàn hảo và một ma trận tương quan bị phá nát — model train trên
> đó sẽ học tương tác sai hoàn toàn.

Đây là chỗ việc chọn feature trả tiền: bài toán từ bất khả thi (83 marginal +
ma trận 83×83) thành khả thi (32 marginal + 22×22).

### 4.5 · Ngân sách test set

Với `PRODUCER_EVENTS_PER_SECOND=25` và ~5 event/session → tiêu ~5 dòng/giây.

```
181,669 dòng  ->  ~10 tiếng
130,000 dòng (phần stream)  ->  ~7 tiếng
```

Đủ cho demo, **không đủ cho stack chạy thường trực** → đó là lý do thứ hai
cần chế độ `synth` (vô hạn).

**Lệch cần sửa:** `PRODUCER_USER_POOL=20000` trong khi test có 181k user.
`_pick_user()` đang bịa `user_id` trong không gian 20k, sẽ không khớp với
`user_id` dẫn từ `data_id` của dòng test.

### 4.6 · Hệ quả hay: tua nhanh thời gian

Khi làm theo kiến trúc seed + tiến hoá, `feature_ts` của dòng test trở thành
**t₀** của user đó. Nghĩa là có thể **tua nhanh**: đặt t₀ lùi 30 ngày và phát
event tốc độ 1000× → thấy `f30` trượt trọn cửa sổ trong 45 phút.

Đó là cách duy nhất test logic cửa sổ trượt, late event và out-of-order trong
`stream_consumer.py` mà không phải chờ một tháng.

---

## 5. Ba ràng buộc bắt buộc

### 5.1 · Hàm phân rã phải MÙ với `label`

Dòng test chứa `label` và `is_treat`. Khi phân rã thành event, **tuyệt đối
không được điều kiện hoá theo `label`**.

Cụ thể: `event_producer.py` sinh `order` với xác suất 0.08. Nếu bạn "làm cho
thật" bằng cách cho dòng `label=1` sinh `order` còn `label=0` thì không —
bạn vừa nhét đáp án vào feature stream. Model sẽ thấy `rt_order_1h = 1` và
dự đoán hoàn hảo, bạn sẽ tưởng mình vừa thắng lớn.

Nghe hiển nhiên nhưng nó lẻn vào rất êm: chỉ cần muốn "session của user mua
hàng thì dài hơn" là leak rồi, vì độ dài session tương quan với label.

**Ép bằng signature:**

```python
def decompose(features: dict[str, float]) -> list[AppEvent]:
    ...
```

Không truyền cả row vào. Không có `label` trong phạm vi hàm.

### 5.2 · Chia test set làm ba, giữ lại một phần không bao giờ chạm

Nếu test drive realtime stream, log inference qua `serving/inference_logger.py`
sẽ chảy về lake, rồi DAG retrain nuốt vào `training_dataset`. Test set nhiễm
vào train — **âm thầm**, không có gì báo.

| Phần | Số dòng | Vai trò |
|---|---|---|
| `train` | 926,669 | Reverse: học cơ chế, calibrate θ |
| `stream` | ~130,000 | Drive realtime event |
| `holdout` | ~50,000 | **Không bao giờ stream, không bao giờ log.** Trọng tài duy nhất |

Không có `holdout`, đến tuần thứ ba sẽ không còn cách nào biết model tốt hay xấu.

### 5.3 · Chỉ một định nghĩa feature

Nếu tính `f*` bằng Python trong simulator còn training tính bằng dbt, bạn vừa
tạo ra đúng thứ mà `feature_spec.yml` được viết ra để chống.

**Bắt buộc: event → dbt → `f*`, một đường duy nhất.**

---

## 6. Lộ trình thực hiện

### Giai đoạn 0 · Forensics — làm trước mọi thứ

> **Rẻ nhất, quyết định nhiều nhất.** Ba con số 52/32/12 đều phụ thuộc kết
> quả này. Ước lượng: nửa ngày.

Script chạy trên `data/full_trainset.csv`, xác nhận/bác bỏ:

- [ ] **`f40`–`f78` có phải one-hot mutually-exclusive không?** → check `row_sum == 1`
- [ ] **`f79`–`f82` giá trị sentinel là gì?** → xem histogram đuôi, tìm giá trị bất thường
- [ ] **`f23` và `f25` liên hệ thế nào?** → hiệu? tỉ số? một cái là hàm của cái kia?
- [ ] **`f1`/`f2` có phải ngày không?** → phân phối có chu kỳ tuần/tháng không
- [ ] **`f27`/`f30`/`f34` có phải bộ đếm không?** → giá trị có nguyên và không âm không

Kết quả (3) quyết định mô phỏng `f23`/`f25` như một cơ chế hay hai.

### Giai đoạn 1 · Chốt feature set

- [ ] Chạy thêm seed cho các feature `n_estimators = 4` (xem 2.5)
- [ ] Tính correlation giữa `f1`/`f2` và nhóm `*_is_special` → chọn giữ bên nào
- [ ] Nếu one-hot xác nhận: gộp `f40`–`f78` thành 1 cột categorical
- [ ] Ghi 3 bộ Guard-52 / Model-32 / Live-12 vào `config/features/feature_spec.yml`
- [ ] Train 3 lần, so AUUC/Qini

**Tiêu chí qua:** Guard-52 vs Model-32 chênh < 1% AUUC.

### Giai đoạn 2 · Chế độ `replay`

Cho pipeline chuyển động thật bằng dữ liệu thật. Không cần simulator.

- [ ] Chia `full_testset.csv` → `stream` (130k) / `holdout` (50k)
- [ ] Sửa `_pick_user()` để `user_id` khớp không gian `data_id` của test
- [ ] Viết `decompose(features) -> list[AppEvent]` (mù với label)
- [ ] `PRODUCER_MODE=replay` trong `event_producer.py`
- [ ] dbt model tính `f*` từ event lake

**Kết quả:** stack chạy được ~7 tiếng với dữ liệu chuyển động thật.

### Giai đoạn 3 · Cơ chế Live-12

- [ ] Khai báo `semantic:` cho từng feature trong `feature_spec.yml`
      (`binary_onehot_member`, `window_count_30d`, `day_index`,
      `monetary_continuous`, `sentinel_coded`)
- [ ] Cài cơ chế delta cho 12 feature Live
- [ ] Bật chế độ tua nhanh thời gian, test logic cửa sổ trượt

### Giai đoạn 4 · Chế độ `synth`

- [ ] `simulation/personas.py` — latent state: tenure, propensity, price_sens,
      voucher_affinity, cat_pref
- [ ] `simulation/mechanisms.py` — map archetype → cách event dịch chuyển `f*`
- [ ] `simulation/uplift_truth.py` — `τ(x)` ground truth + gán `is_treat`
- [ ] `simulation/calibrate.py` — Optuna loop, chấm điểm bằng `normalized_share`
- [ ] `PRODUCER_MODE=synth`

**Tiêu chí qua:** adversarial validation AUC < 0.65 (xem 7.1).

### Cấu trúc file mới

```
src/lzd_pipeline/simulation/
    personas.py
    mechanisms.py
    uplift_truth.py
    calibrate.py
scripts/
    forensics_dataset.py       # Giai đoạn 0
    split_testset.py           # Giai đoạn 2
```

Sửa đổi:
- `ingestion/event_producer.py` — thêm 2 chế độ
- `config/features/feature_spec.yml` — thêm block `semantic:`, 3 bộ feature
- `dbt/models/` — model tính `f*` từ event

---

## 7. Cách kiểm chứng

### 7.1 · Adversarial validation — diagnostic tốt nhất

Train một classifier phân biệt `real` vs `sim` trên 32 feature:

| AUC | Kết luận |
|---|---|
| ≈ 0.50 | Không phân biệt nổi — simulator đạt |
| 0.50–0.65 | Chấp nhận được |
| > 0.80 | Giả lộ liễu — **và feature importance của classifier chỉ thẳng vào feature bạn mô phỏng sai** |

Đây là vòng lặp tự chỉ đường. Chạy sau mỗi lần chỉnh θ.

### 7.2 · Kiểm chéo hai chiều

- Train trên real → score trên sim
- Train trên sim → score trên real

Nếu AUUC giữ được **cả hai chiều**, simulator đủ trung thực để làm môi trường test.

### 7.3 · Drift monitor thật

Cắm PSI/KS per-feature vào Grafana stack sẵn có. Test bằng cách **cố tình vặn
θ giữa lúc chạy** — alert phải kêu.

### 7.4 · Với chế độ `synth`: đo trực tiếp `τ`

- Sai số giữa `τ̂(x)` model dự đoán và `τ(x)` ground truth
- Qini thực tế / Qini trần lý thuyết
- Tỉ lệ bắt đúng nhóm sleeping-dog

---

## 8. Bẫy đã biết

| # | Bẫy | Cách tránh |
|---|---|---|
| 1 | **Leak `label` qua khâu phân rã** | Signature `decompose(features)`, không truyền row |
| 2 | **Nhiễm `holdout`** | Cắt riêng 50k, không bao giờ stream/log |
| 3 | **Định nghĩa feature bị nhân đôi** | event → dbt → `f*`, một đường duy nhất |
| 4 | **Mô phỏng `f68` như cờ độc lập** | Nếu one-hot đúng: sinh categorical rồi mới one-hot. Sinh 27 Bernoulli độc lập sẽ tạo row có 3 mức cùng bằng 1 — trạng thái không tồn tại trong dữ liệu thật |
| 5 | **Sim collapse** | Giữ hạng mục `λ‖Σ_sim − Σ_real‖` trong loss |
| 6 | **Xây quanh artifact thống kê** | `f81_is_special` chỉ 4/5 seed — chạy thêm seed trước khi hard-code cơ chế sentinel cho nó |
| 7 | **`τ` sống trong nhóm frozen** | `τ(x)` phải phụ thuộc chủ yếu vào Live-12, nếu không uplift thật không bao giờ đổi theo thời gian và pipeline realtime tuy chạy nhưng không test được gì |
| 8 | **Bỏ lẻ từng mức one-hot** | Row sẽ có `sum = 0` cho hầu hết user ⇒ gộp tất cả mức bị bỏ thành một mức "không xác định" chung |

---

## Phụ lục · Trạng thái các giả thuyết

| Giả thuyết | Trạng thái | Bằng chứng |
|---|---|---|
| `f23`/`f25` là ID rò rỉ | ❌ **Đã bác bỏ** | 926,669 dòng vs 256,185 unique = 27.6% |
| `f40`–`f66` là khối binary liên tiếp | ✅ **Xác nhận** | Đọc trực tiếp từ `n_uniq` |
| `f40`–`f78` là one-hot của 1 biến | ⏳ **Chưa kiểm tra** | Giai đoạn 0, check `row_sum == 1` |
| `f1`/`f2` là ngày | ⏳ **Chưa kiểm tra** | `n_uniq = 366` cho cả hai |
| `f79`–`f82` có sentinel | ⏳ **Chưa kiểm tra** | Cả 4 đều có biến thể `*_is_special` |
| `f27`/`f30`/`f34` là bộ đếm tháng | ⏳ **Chưa kiểm tra** | `n_uniq` = 32/30/31 |

Mọi con số ở Phần 3 giả định các giả thuyết ⏳ là đúng. **Chạy Giai đoạn 0 trước.**
