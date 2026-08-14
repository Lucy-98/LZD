# Uplift Model — DRLearner, 55 cột từ feature store

> **Trạng thái: ĐÃ LẮP + verify.** Model chạy trong `inference-api`, thay
> `StubModel`. Dự đoán **trùng khít bit-for-bit** với bản gốc.
>
> Nhãn: `[FACT]` `[MEASURED]` `[ASSUMPTION]` `[UNKNOWN]`

---

## 1. Model là gì

`[FACT]` Đọc từ `models/uplift_voucher/metadata.json`:

| | |
|---|---|
| Thuật toán | **DRLearner** (doubly-robust learner), base learner **LightGBM** |
| Nguồn | repo `hatuki0604/lzd-uplifting-model`, huấn luyện trong notebook |
| Cấu trúc | 350 cây, `objective=regression`, `max_depth=4` |
| Dữ liệu train | 926,669 dòng (`train.parquet` + `val.parquet`) |
| Đầu ra | CATE / uplift score — số thực, âm và dương |

`[MEASURED]` Metric trên `rct_holdout` (bootstrap 1000 lần), DRLearner là bản
quán quân trong 6 mô hình:

| Model | Qini | AUUC | uplift@10% |
|---|---|---|---|
| **DRLearner** | **0.02324** | **0.02541** | 0.01418 |
| SLearner | 0.02253 | 0.02420 | 0.01202 |
| CausalForestDML | 0.01901 | 0.02048 | 0.01461 |
| NonParamDML | 0.01732 | 0.01892 | 0.01471 |
| LinearDML | 0.01534 | 0.01667 | 0.00852 |
| TLearner | 0.01482 | 0.01600 | 0.01287 |

> ⚠️ KTC Qini của DRLearner là `[-0.00066, 0.04831]` — **chứa 0**. Nghĩa là ở
> mức holdout này chưa loại trừ được giả thuyết "không có uplift". Đây là số
> của bản gốc, ghi lại nguyên trạng, **không** phải khẳng định model có tác dụng.

---

## 2. Đầu vào: **76** cột, không phải 55

Đây là chỗ dễ hiểu nhầm nhất.

```
   55  từ Redis          DE tính và lưu (đúng bằng scope fs_2026_08_v2)
    7  fe_* dẫn xuất     inference-api TỰ TÍNH từ cột gốc
   14  điền mặc định     trung vị trên train+val
  ────
   76  vào model
```

`[MEASURED]` **55 cột Redis trùng khít 55/55 với `fs_2026_08_v2`.** Đây không
phải trùng hợp — danh sách `chia_viec.DE_tinh_va_luu_redis` trong
`feature_contract.json` chính là scope mà pipeline reconstruction sinh ra.
`test_55_cot_redis_khop_selected_feature_set` khoá bất biến này.

### 2.1 · 7 đặc trưng dẫn xuất

| Cột | idx | Công thức |
|---|---|---|
| `fe_ratio_f1_f2` | 69 | `f1 / (f2 + 1)` |
| `fe_ratio_f14_f16` | 70 | `f14 / (f16 + 1)` |
| `fe_ratio_f9_f27` | 71 | `f9 / (f27 + 1)` |
| `fe_inter_f9_f26` | 72 | `f9 * f26` |
| `fe_inter_f14_f27` | 73 | `f14 * f27` |
| `fe_nonzero_top10` | 74 | đếm khác 0 trong `f9 f16 f27 f26 f14 f3 f13 f10 f17 f7` |
| `fe_flag_sum` | 75 | `f66 + f68 + f75` |

> ⚠️ **`fe_ratio_f14_f16` và `fe_inter_f14_f27` LUÔN bằng 0 ở production.**
> Cả hai nhân `f14`, mà `f14` nằm trong 14 cột điền mặc định `= 0.0`. Đây là
> hệ quả của hợp đồng, không phải lỗi cài đặt.
>
> Tương tự `fe_flag_sum = 2 + f68`, vì `f66` và `f75` đều mặc định `1.0`.
> ⇒ Thực chất chỉ **4/7** đặc trưng dẫn xuất mang thông tin.

### 2.2 · 🔴 14 cột mặc định — cái giá đã đo được

```
f7  f14  f36  f48  f49  f50  f51  f55  f56  f60  f61  f63  f66  f75
```

`[MEASURED]` **9 trong 14 cột này** (`f48 f49 f50 f51 f55 f56 f60 f61 f63`)
chính là `intermediate_only` của `fs_2026_08_v2` — pipeline **đã tính sẵn**.
Điền `0.0` cho chúng khiến một số user có **cả group one-hot bằng 0**:

| Group | User bị cả group = 0 | |
|---|---|---|
| g2 (`f43..f52`) | 13,508 / 926,669 | 1.46% |
| **g3 (`f53..f62`)** | **244,475 / 926,669** | **26.38%** |
| g4 (`f63..f65`) | 28,951 / 926,669 | 3.12% |

Trạng thái "cả group = 0" **không tồn tại trong dữ liệu huấn luyện** — bất biến
one-hot đúng 100% trên train.

**Quyết định hiện tại: giữ mặc định.** Lý do: dự đoán trùng khít bản đã
benchmark, nên mọi con số Qini/AUUC ở §1 vẫn áp dụng được.

```
🚫 Đổi sang giá trị thật sẽ ĐỔI dự đoán. Phải đo lại Qini trên rct_holdout
   trước khi tin. Đừng đổi rồi giả định là tốt hơn.
```

---

## 3. Artifact: vì sao dùng booster chứ không phải `.pkl`

`[MEASURED]` Hai artifact cho kết quả **trùng khít bit-for-bit**:

```
predict_cate(X)  vs  booster.predict(X)      3,000 mẫu
max |lệch| = 0.0            array_equal = True
```

Vì `predict_cate` chỉ là một dòng:

```python
def predict_cate(self, X):
    return self.model.predict(X)      # self.model = LGBMRegressor
```

| | `model.pkl` | **`model_booster.txt`** |
|---|---|---|
| Định dạng | cloudpickle (nhúng code object) | text của LightGBM |
| Python | **phải đúng 3.10.9** | mọi bản |
| Dependency | cloudpickle + sklearn + lightgbm | lightgbm |
| Trên Python 3.14 | ❌ `TypeError: code() argument 13 must be str, not int` | ✅ |

Dùng `.pkl` sẽ buộc hạ `PYTHON_VERSION` của image `lzd/python-service` từ
3.11.10 xuống 3.10.9 — image đó dùng chung cho `event-producer`,
`stream-consumer` **và** `inference-api`. Không có lý do trả giá đó cho một
model cho kết quả y hệt.

---

## 4. Code

| File | Vai trò |
|---|---|
| [serving/feature_contract.py](../src/lzd_pipeline/serving/feature_contract.py) | đọc hợp đồng, dựng vector 76 chiều, tính 7 `fe_*` |
| [serving/model_loader.py](../src/lzd_pipeline/serving/model_loader.py) | `LightGBMUpliftModel` · `MlflowUpliftModel` · chuỗi nạp |
| [training/register.py](../src/lzd_pipeline/training/register.py) | đăng ký artifact vào MLflow Registry |
| `models/uplift_voucher/` | booster · contract · metadata · golden predictions |

### 4.1 · Chuỗi nạp model

```
1. MlflowUpliftModel     MLflow Registry (artifact trên MinIO)   ← production
2. LightGBMUpliftModel   booster bundled trong repo              ← chạy ngay
3. StubModel             CUỐI CÙNG, luôn kèm cảnh báo
```

`StubModel` giữ lại có chủ đích — nó cho phép đo trễ Redis / cache hit / lưu
lượng trước khi registry sẵn sàng. 🚫 Score của nó **không** dùng để ra quyết định.

### 4.2 · ⚠️ Thứ tự cột là một phần của hợp đồng

Contract ghi nguyên văn: *"Sai thứ tự cột sẽ cho dự đoán sai mà KHÔNG báo lỗi"*.
LightGBM nhận mảng số, không nhận tên. Đó là lý do:

- thứ tự đọc từ **artifact**, không chép tay
- `build_matrix()` là đường **duy nhất** để dựng đầu vào
- booster và contract log **cùng một `artifact_path`** trong MLflow

### 4.3 · Ghi chú kỹ thuật: `model_str` chứ không `model_file`

LightGBM mở file ở tầng thư viện C, tầng đó không xử lý được đường dẫn
non-ASCII trên Windows. Repo nằm trong `OneDrive\Máy tính` nên `model_file=`
đổ `Could not open`. Đọc bằng Python rồi truyền `model_str=` thì tránh được.

---

## 5. Kiểm chứng

### 5.1 · Golden test — bit-for-bit với bản gốc

`models/uplift_voucher/golden_predictions.json` sinh **một lần** từ `model.pkl`
gốc chạy trên **Python 3.10.20** (môi trường đóng gói của notebook), gồm 150
dòng phủ 4 chế độ: ngẫu nhiên `[0,1]`, biên `[0,50]`, toàn 0, và hỗn hợp.

Đường serving chạy Python khác, artifact khác, tự dựng vector 76 cột — và cho
kết quả **trùng khít tuyệt đối** (`rel=0.0, abs=0.0`).

⇒ Trùng khít chứng minh **cả ba** đều đúng: thứ tự cột, giá trị mặc định,
công thức dẫn xuất. Lệch thì test chỉ ra sai ở dòng nào.

### 5.2 · Suite

```
tests/serving/test_uplift_model.py     25 passed
toàn bộ suite                         305 passed
```

---

## 6. Chưa làm

| Hạng mục | Ghi chú |
|---|---|
| **Đăng ký thật vào MLflow** | `register.py` viết xong, `--verify-only` chạy được, nhưng **chưa chạy với MLflow server thật** (host không dựng được stack) |
| Huấn luyện trong repo | `train.py` vẫn còn 4 `TODO(model)`. Model hiện tại huấn luyện ở notebook ngoài |
| Cấp giá trị thật cho 9 cột one-hot | Xem §2.2 — cần đo lại Qini trước |
| `f36` là T3 ngoài scope | `f36` cần mặc định `0.965`, nhưng nó **không** nằm trong 55 cột. Nếu sau này muốn cấp thật thì phải mở scope |
| DAG đăng ký model | Chưa có; `register.py` mới chỉ có CLI |

---

## Liên quan

`SCOPE_EXPANSION_55F.md` (55 cột từ đâu ra) · `TECH_REFERENCE.md` §5 (package map) ·
`FEATURE_DICTIONARY.md` (từng cột một)
