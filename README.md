# LZD Uplift Feature Platform

[![CI Pipeline](https://github.com/Lucy-98/LZD/actions/workflows/ci.yml/badge.svg)](https://github.com/Lucy-98/LZD/actions)
![Python](https://img.shields.io/badge/Python-3.11%20%7C%203.12-blue?logo=python)
![FastAPI](https://img.shields.io/badge/FastAPI-Serving-009688?logo=fastapi)
![Redis](https://img.shields.io/badge/Redis-FeatureStore-DC382D?logo=redis)
![DuckDB](https://img.shields.io/badge/DuckDB-OLAP-FFF000?logo=duckdb)
![dbt](https://img.shields.io/badge/dbt-Transformation-FF694B?logo=dbt)
![Airflow](https://img.shields.io/badge/Airflow-Orchestration-017CEE?logo=apacheairflow)
![MLflow](https://img.shields.io/badge/MLflow-ModelRegistry-0194E2?logo=mlflow)
![Kafka](https://img.shields.io/badge/Kafka-Streaming-231F20?logo=apachekafka)

---

## 📌 Executive Summary (English for Reviewers & Recruiters)

**LZD Uplift Feature Platform** is a production-grade, end-to-end Data & ML Engineering platform designed for dynamic e-commerce voucher allocation based on **Uplift Modeling (Causal Inference)**.

### Key Architecture & Engineering Achievements
- **End-to-End Pipeline**: Raw Ingestion $\to$ MinIO S3 Lakehouse $\to$ dbt / DuckDB Feature Transformations $\to$ Redis Online Feature Store $\to$ FastAPI Serving.
- **Low Latency Serving**: Realtime decision endpoint (`POST /decide`) operating at **< 10 ms SLA** with zero-skew contract validation between training and serving vectors.
- **Causal Uplift Model**: DRLearner (Meta-learner with LightGBM base models) achieving superior **AUUC & Qini scores** over benchmark models (SLearner, TLearner, CausalForestDML).
- **Mathematically Verified Feature Reconstruction**: Deterministic dry-run engine proving backward feature-to-event reconstruction (Track A witness events & Track B future events) with 100% boundary constraint verification across active users.
- **Production Observability**: Full metric push & log tracking with Prometheus, Grafana, Loki, StatsD, and PostgreSQL audit logging.
- **Engineering Quality**: 347 passing unit/contract tests, PEP 517/518 packaging (`pyproject.toml`), and automated GitHub Actions CI.

---

Repo này dựng lại toàn bộ đường dữ liệu cho bài toán Lazada voucher uplift: nạp
dataset gốc, build feature bằng dbt/DuckDB, sync 55 selected feature lên Redis,
phục vụ quyết định phát voucher qua FastAPI, huấn luyện lại hằng tuần, và chứng
minh reconstruction từ feature train ngược về event.

## Mục lục

1. [Tổng quan cho người mới](#1-tổng-quan-cho-người-mới)
2. [Trạng thái hiện tại](#2-trạng-thái-hiện-tại)
3. [Bố cục thư mục](#3-bố-cục-thư-mục)
4. [Yêu cầu môi trường](#4-yêu-cầu-môi-trường)
5. [Chạy nhanh không cần Docker](#5-chạy-nhanh-không-cần-docker)
6. [Chạy nhanh với Docker](#6-chạy-nhanh-với-docker)
7. [Các service trong stack](#7-các-service-trong-stack)
8. [MinIO và Kafka](#8-minio-và-kafka)
9. [Thứ tự chạy Airflow](#9-thứ-tự-chạy-airflow)
10. [Luồng dữ liệu dbt](#10-luồng-dữ-liệu-dbt)
11. [Hợp đồng Redis](#11-hợp-đồng-redis)
12. [Hợp đồng API suy luận](#12-hợp-đồng-api-suy-luận)
13. [Reconstruction](#13-reconstruction)
14. [Vận hành thật](#14-vận-hành-thật)
15. [Lệnh hay dùng](#15-lệnh-hay-dùng)
16. [Xử lý sự cố](#16-xử-lý-sự-cố)
17. [Tài liệu chính](#17-tài-liệu-chính)

---

## 1. Tổng quan cho người mới

### Mục tiêu

- Một stack chạy được ở máy local để đồng đội/mentor tái tạo lại dữ liệu và quan
  sát luồng `CSV → lakehouse → dbt → Redis → API`.
- Giải thích rõ **55 feature nào** được sync lên Redis thay vì sync cả `f0..f82`.
- Huấn luyện lại hằng tuần có **cổng kiểm soát**, không tự động thay model đang
  phục vụ.
- Chứng minh reconstruction: từ selected feature trong tập train, sinh ngược
  Track A witness event, chạy lại dbt SQL, và so lại feature kỳ vọng/thực tế.

### Ai dùng repo này

- **Data/ML engineer** muốn xem feature lineage và hợp đồng sync.
- **Mentor/reviewer** muốn clone về, cài đặt, chạy test, xem artifact
  reconstruction.
- **Người vận hành** cần bật vòng train tuần, đổi model, hoặc rollback.

### Luồng chính

```text
data/full_trainset.csv
data/full_testset.csv
        │
        ▼
Airflow DAG 00_bootstrap_lake
        │
        ▼
MinIO  lakehouse/raw/user_snapshot/*.parquet
        │
        ▼
Airflow DAG 20_build_features_dbt
        │
        ▼
DuckDB  marts.feat_user_selected_serving  ·  marts.training_dataset
        │                                          │
        ▼                                          ▼
DAG 40_sync_features_to_redis            DAG 30_train_uplift_model
        │                                          │
        ▼                                          ▼
Redis  fs:{version}:u:{user_id}           MLflow Registry (alias Production)
        │                                          │
        └──────────────┬───────────────────────────┘
                       ▼
              FastAPI  POST /decide
```

Luồng realtime chạy riêng:

```text
event-producer → Kafka app.user.events.v1 → stream-consumer
        → MinIO lakehouse/raw/app_events
        → Redis rt:u:{user_id}
```

Reconstruction chạy riêng, hoàn toàn dry-run trên DuckDB in-memory:

```text
selected train features → Track A events → dbt reconstruction SQL
        → các gate → CustomerState(T0) → Track B future events
```

---

## 2. Trạng thái hiện tại

Cập nhật 2026-08-17. Mọi con số đều **đo trên máy thật**, không phải ước lượng.

### Đã chạy được end-to-end

| Hạng mục | Kết quả |
|---|---|
| Test | `346 passed, 1 skipped` — chạy bằng `.venv\Scripts\python` |
| Pipeline | DAG 00 → 20 → 40 xanh, feature lên Redis, `active_version` được kích hoạt |
| Serving | `/decide` 2.7–10.6 ms ấm, `cache_hit=true` |
| Model | DRLearner từ notebook, MLflow Registry `v1`, alias `Production` |
| Reconstruction | Track A + B trên 10 user thật, 10/10 qua hết gate |
| Track B | 92 event đáp xuống `raw/track_b_future/` (prefix riêng) |

Model trong registry tái tạo golden predictions với **`max |lệch| = 0.0`** trên
150 mẫu — qua cả chặng đổi định dạng (pkl → booster text), đổi Python
(3.10.9 → 3.11) và đổi đường truyền (file local → MinIO).

### Best model là DRLearner

Benchmark trên `rct_holdout`, bootstrap n=1000:

| Model | qini | auuc | uplift@10 |
|---|---|---|---|
| **DRLearner** | **0.02324** | **0.02541** | 0.014182 |
| SLearner | 0.02253 | 0.02420 | 0.012018 |
| CausalForestDML | 0.01901 | 0.02048 | 0.014608 |
| NonParamDML | 0.01732 | 0.01892 | 0.014705 |
| LinearDML | 0.01534 | 0.01667 | 0.008516 |
| TLearner | 0.01482 | 0.01600 | 0.012871 |

Cần đọc kèm dè dặt, và chính `metadata.json` cũng ghi ra: DRLearner chỉ vượt
**2/5** đối thủ một cách có ý nghĩa thống kê, và **khoảng tin cậy qini chứa 0**
(`[-0.00066, 0.04831]`) — với cả sáu model. Thứ *có* ý nghĩa là ATE:
`0.00376`, CI `[0.00137, 0.00615]`, không chứa 0. Tức voucher có tác dụng thật;
còn việc xếp hạng ai nên nhận thì DRLearner là lựa chọn tốt nhất trong sáu, chưa
phải bằng chứng mạnh.

### Chưa làm

- **Track A chưa chạy trên toàn bộ** `full_trainset.csv` (926,669 dòng). Bản
  pilot cũ 10,000 dòng đã bị xoá vì thuộc scope `fs_2026_08_v1`/36 cột, không còn
  đúng với scope hiện tại `fs_2026_08_v2`/55 cột. Ước tính bản đầy đủ: **~43
  phút, ~7.8 GB artifact, ~17.3 triệu event**.
- `feat_cfs_*` (đường reconstruction) vẫn bị `--exclude tag:reconstruction` trong
  DAG 20 — chúng cần `raw/events_v2/` mà Track A chưa ghi.
- Model đang phục vụ **không dùng feature realtime**. Xem [§12](#12-hợp-đồng-api-suy-luận).
- Chưa publish Track B vào Kafka topic v2.

---

## 3. Bố cục thư mục

```text
airflow/dags/                 Airflow DAG
config/features/              feature spec, tập 55 selected feature
config/reconstruction/        cấu hình runtime cho reconstruction
dbt/                          model + test dbt (DuckDB)
dbt/macros/                   macro dùng chung, xem ghi chú bên dưới
docker/                       Dockerfile cho Airflow/Python/MLflow
docs/                         tài liệu kiến trúc + snapshot review đã commit
scripts/                      script điều khiển stack + init
src/lzd_pipeline/             mã nguồn pipeline
tests/                        unit test + contract test
data/                         CSV nguồn (đã commit)
.tmp/                         output tạm lúc chạy, git bỏ qua
```

Ba macro dbt tồn tại vì lý do cụ thể, không phải tiện tay:

| Macro | Vì sao cần |
|---|---|
| `generate_schema_name.sql` | dbt mặc định ghép `<target>_<custom>` → tạo `main_marts`, còn `feature_spec.yml` khai `marts`. Hai bên gọi hai tên cho cùng một bảng |
| `external_source.sql` | DuckDB **ném lỗi** thay vì trả 0 dòng khi glob parquet không khớp file nào — đường batch chết chỉ vì stream chưa chạy lần nào |
| `generic_tests.sql` | test tự viết, tránh phải cài `dbt_utils` |

Lưu ý `data/` **không** bị git bỏ qua. Muốn mentor tái tạo đúng lần chạy từ bản
clone sạch thì hai file này phải có:

```text
data/full_trainset.csv
data/full_testset.csv
```

`.dockerignore` vẫn loại `data/` khỏi build context — có chủ đích: container đọc
`data/` lúc chạy qua bind mount, không cần nướng CSV vào image.

---

## 4. Yêu cầu môi trường

**Bắt buộc:**

- Windows 10/11 với PowerShell 5+ hoặc PowerShell 7+
- Python 3.11+
- Git
- Docker Desktop có Compose v2 (nếu chạy cả stack)

**Tài nguyên Docker Desktop nên cấp:**

- CPU: 4 nhân
- RAM: tối thiểu 8 GB, nên 12 GB
- Đĩa: trống 15 GB

**Đường dẫn repo nên dùng:**

```text
C:\dev\LZD
```

Repo chạy được từ OneDrive, nhưng Docker BuildKit và bind mount ổn định hơn khi
nằm ngoài OneDrive, vì OneDrive có thể đánh dấu file thành reparse point.

---

## 5. Chạy nhanh không cần Docker

Nên đi đường này trước. Nó chứng minh hợp đồng code/reconstruction mà không cần
Docker Hub, MinIO, Kafka, Redis hay Airflow.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements-dev.txt
```

Chạy test:

```powershell
.venv\Scripts\python -m pytest tests -q
```

> ⚠️ **Phải chạy bằng interpreter của `.venv`.** Dùng system Python thiếu
> `lightgbm` thì `test_uplift_model.py` skip **cả module** — mất luôn phép kiểm
> golden bit-for-bit, mà bộ test vẫn báo xanh.

Sinh snapshot reconstruction cỡ nhỏ đã commit:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 snapshot
```

Sinh Track A event từ dữ liệu train thật, giới hạn 1,000 dòng:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 track-a 1000
```

Kết quả ra `.tmp/reconstruction_track_a/`. `.tmp/` bị git bỏ qua vì Track A đầy
đủ sinh file rất lớn.

---

## 6. Chạy nhanh với Docker

Một lệnh, từ bản clone sạch:

```powershell
docker compose up -d
```

Đó là toàn bộ phần cài đặt. Không cần tạo `.env`, không cần build tay, không cần
cờ profile: mọi biến trong `docker-compose.yml` đều có giá trị mặc định chạy
được, và Compose tự build ba image local (`lzd/airflow`, `lzd/python-service`,
`lzd/mlflow`) ở lần chạy đầu. Lần đầu mất 5–15 phút, các lần sau vài giây.

Bạn nhận được Postgres, Redis, MinIO, Kafka, Airflow, MLflow, API suy luận và
toàn bộ observability. Thêm traffic realtime mô phỏng chỉ khi cần:

```powershell
docker compose --profile all up -d
```

Hoặc bỏ mười container observability khi máy yếu:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 up-core
```

`.env` là **tuỳ chọn**, chỉ dùng để ghi đè: port trùng, mật khẩu, hoặc
`AIRFLOW_UNPAUSE_DAGS`. Copy từ `.env.example` khi cần.

Script bọc vẫn hữu ích khi mạng chập chờn hoặc có proxy công ty: nó kéo image
tuần tự có retry, và phân biệt được lỗi TLS bị chặn giữa đường với lỗi rớt mạng.

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 doctor
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 up
```

Mọi DAG đều **paused khi tạo**, có chủ đích — bootstrap đọc file CSV 476 MB,
không nên tự chạy khi ai đó vừa `docker compose up` để xem thử. Bỏ pause trong
UI, hoặc khai trong `.env`:

```dotenv
AIRFLOW_UNPAUSE_DAGS=30_train_uplift_model
```

Xem trạng thái và sức khoẻ:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 status
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 health
```

Dừng (giữ dữ liệu) và reset (xoá volume):

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 down
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 reset
```

---

## 7. Các service trong stack

**Hạ tầng lõi:**

| Service | Vai trò | URL |
|---|---|---|
| Postgres | metadata Airflow/MLflow + schema audit | localhost:5432 |
| Redis | online feature store | localhost:6379 |
| MinIO | lake/model artifact tương thích S3 | http://localhost:9001 |
| Kafka | bus event realtime | localhost:29092 |
| Airflow | điều phối | http://localhost:8080 |

**ML và serving** (cũng chạy mặc định):

| Service | Vai trò | URL |
|---|---|---|
| inference-api | đọc feature từ Redis, ra quyết định | http://localhost:8000/docs |
| mlflow | experiment + model registry | http://localhost:5000 |

**Observability** (chạy mặc định):

| Service | Vai trò | URL |
|---|---|---|
| Grafana | dashboard + khám phá log | http://localhost:3000 |
| Prometheus | kho metric | http://localhost:9090 |
| Loki | backend log | http://localhost:3100 |
| Kafka UI | xem topic/message/lag | http://localhost:8082 |
| RedisInsight | xem key Redis | http://localhost:5540 |

**Sau profile `stream`** — chỉ chạy với `--profile all`, vì chúng bắn event liên
tục:

| Service | Vai trò |
|---|---|
| event-producer | bắn app event v1 mô phỏng vào Kafka |
| stream-consumer | ghi event Kafka v1 xuống MinIO và Redis overlay |

**Tài khoản mặc định:**

| Giao diện | Đăng nhập |
|---|---|
| Airflow | `admin/admin` |
| Grafana | `admin/admin` |
| MinIO | `minioadmin/minioadmin123` |

---

## 8. MinIO và Kafka

MinIO tạo 3 bucket:

```text
lakehouse  →  dữ liệu raw/staging/marts/export
models     →  bề mặt model artifact
mlflow     →  artifact root của MLflow
```

Trong `lakehouse`, script init tạo sẵn:

```text
raw/user_snapshot     ←  DAG 00 ghi vào
raw/app_events        ←  stream-consumer ghi vào
raw/events_v2         ←  Track A land vào (hiện chưa có dữ liệu)
raw/track_b_future    ←  Track B land vào, prefix RIÊNG
exports
```

Kafka tạo 2 topic:

```text
app.user.events.v1      partitions=3, retention=2 ngày
app.user.events.dlq.v1  partitions=1, retention=7 ngày
```

Key Kafka là `user_id`/`customer_id` để đảm bảo thứ tự theo từng user. Event
Track B hiện chỉ dry-run, chưa publish lên topic v2 production.

---

## 9. Thứ tự chạy Airflow

Mở Airflow tại `http://localhost:8080`, rồi chạy tay theo thứ tự cho đường batch:

1. `00_bootstrap_lake`
2. `20_build_features_dbt`
3. `40_sync_features_to_redis`
4. `50_data_quality`

DAG reconstruction chạy tay: `60_reconstruction_e2e` — chỉ kiểm hợp đồng ở chế
độ dry-run, **không** ghi MinIO/Kafka/Redis production.

### Vòng train hằng tuần

`30_train_uplift_model` chạy 03:00 thứ Hai (`0 3 * * 1`) sau khi được bỏ pause:

```text
check_training_data   từ chối nếu thiếu nhóm treated hoặc control
train                 DR-Learner + LightGBM, log MLflow, đăng ký version
                      kèm model_booster.txt và feature_contract.json trong
                      cùng một artifact path
promote_model         chỉ đổi alias Production khi qua CẢ BA cổng
notify_serving        POST /admin/reload-model, bỏ qua khi alias không đổi
```

**Từ chối promote không phải là lỗi.** Model kém hơn là kết quả bình thường của
một tuần và không được làm đỏ DAG; quyết định cùng cả hai giá trị metric nằm
trong task log và XCom trả về.

Model huấn luyện ở đây ăn vector khác model notebook (**62 cột** so với **76** —
xem `training/contract.py`), nên `promote_model` sẽ không âm thầm thay model
notebook đăng ký qua `register.py`: bản đó không mang `holdout_version` để so, và
cổng giữ lại thay vì đổi mù. Chuyển hẳn sang model train trong repo là quyết định
có chủ đích, sau khi benchmark.

### Track A + Track B trên user thật

Ba entry point reconstruction, khác nhau ở phạm vi:

| Entry point | User | Track |
|---|---|---|
| `reconstruction.e2e` | 1 target tổng hợp | A và B |
| `reconstruction.track_a_batch` | thật, `--limit N` | chỉ A |
| `reconstruction.track_ab_batch` | thật, `--limit N` | A và B |

```powershell
$env:PYTHONPATH="src"
.venv\Scripts\python -m lzd_pipeline.reconstruction.track_ab_batch --limit 10
```

Đo ngày 2026-08-15 trên 10 dòng đầu của `data/full_trainset.csv`: **10/10 user
qua hết gate**, 115 Track A witness event dựng lại từ quá khứ và 92 Track B event
sinh cho hai ngày tương lai.

`e2e.run_end_to_end()` mặc định gọi `engine.solve()`, mà bước đầu của nó là
**liệt kê toàn bộ** không gian nghiệm — được với target tổng hợp
(`window_days=4`), và cố ý như vậy để đối chiếu với argmin exhaustive. Một hàng
thật mang `window_days=30`, nơi không gian đó là median 10^9.5 candidate mỗi
target: không phải chậm, là *không bao giờ xong*. Nên `track_ab_batch` truyền
nghiệm của solver constructive vào qua tham số `outcome=`; mọi bước sau — dbt
marts, gate A–F, `CustomerState(T0)`, Track B — chạy y nguyên.

Tất cả đều dry-run trên DuckDB in-memory và **không cần container nào**.

---

## 10. Luồng dữ liệu dbt

`00_bootstrap_lake` nạp:

```text
data/full_trainset.csv → lakehouse/raw/user_snapshot/dt=<run_date>/train.parquet
data/full_testset.csv  → lakehouse/raw/user_snapshot/dt=<run_date>/test.parquet
```

> 🚫 **`user_id` phải mang tiền tố theo split.** `train_123 → U0000123`,
> `test_123 → T0000123`. Trước đây công thức chỉ lấy chữ số và vứt tiền tố, mà
> hai file CSV đánh số độc lập từ 0 — nên `train_0` và `test_0` cùng ra
> `U0000000`. `stg_user_snapshot` khử trùng theo `(user_id, dt)` giữ bản
> `feature_ts` mới nhất, và **181,669 dòng train bị xoá âm thầm** (còn lại
> 18,331/200,000, mất 91.7%). Model train trên phần sót lại cho `qini = 0.000128`.
> Không có cảnh báo nào: DAG xanh, dbt xanh, chỉ con số cuối cùng là sai.
> Bất biến này được canh bởi `tests/test_seed_user_id.py`.

`20_build_features_dbt` chạy `dbt run` và `dbt test` trên DuckDB. Output quan
trọng:

```text
staging.stg_user_snapshot
staging.stg_app_events
marts.feat_user_behaviour
marts.feat_user_realtime_pit
marts.feat_user_serving
marts.feat_user_selected_serving   ←  Redis sync chỉ đọc bảng này
marts.training_dataset             ←  DAG 30 train trên bảng này
marts.eval_holdout                 ←  tập đánh giá ĐÓNG BĂNG
```

`feat_user_selected_serving` chứa `user_id`, `dt`, `feature_ts` và 55 selected
feature. Nó **không** chứa `label` hay `is_treat`.

### Vì sao tập đánh giá phải tách và đóng băng

`training/promote.py` quyết định thay model bằng cách so `qini` của run mới với
run đang giữ alias `Production`. **Phép so đó chỉ có nghĩa nếu hai run đo trên
cùng một tập.** Nếu tập test cũng lớn lên theo từng tuần thì tuần sau đo trên một
tập khác tuần trước — hai con số không so được nữa, mà cổng promote vẫn cứ so và
vẫn cứ ra quyết định.

Nên:

| Bảng | Vật liệu hoá | Nội dung |
|---|---|---|
| `training_dataset` | `incremental`, khoá `(user_id, dt)` | chỉ `split='train'`, cửa sổ trượt 12 tuần |
| `eval_holdout` | `table`, lọc theo `holdout_dt` | chỉ `split='test'`, mang cột `holdout_version` |

`holdout_version` đi theo từng dòng và được log vào MLflow params. `promote.py`
**từ chối** so hai run khác `holdout_version` — đổi tập đánh giá không thể xảy ra
âm thầm.

---

## 11. Hợp đồng Redis

Key batch của selected feature:

```text
fs:{version}:u:{user_id}      kiểu HASH
```

Trường:

```text
f1 f2 f5 f11 f18 f19 f30
f37 f38 f79 f80 f81 f82 f40 f43 f44 f45 f64 f68
f3 f4 f8 f9 f10 f12 f13 f16 f20 f21 f22 f23 f25 f26 f28 f29 f31 f35
_v  _ts  _feature_set_id
```

Key metadata:

```text
fs:meta:active_version
fs:meta:{version}:status
fs:meta:{version}:shards
fs:meta:versions
```

Thiết kế sync:

- Version suy ra tất định từ ngày logic, ví dụ `v20260805`.
- Mỗi shard ghi idempotent bằng `HSET`.
- Shard hoàn tất được đánh dấu trong `fs:meta:{version}:shards`.
- `validate` so mẫu Redis với DuckDB **trước khi** kích hoạt, và có `retries=0` —
  validate fail là tín hiệu dữ liệu sai, retry chỉ che lỗi.
- Kích hoạt là một phép hoán đổi nguyên tử `fs:meta:active_version`.
- Version cũ được thu hồi sau khi version mới đã an toàn.

Key overlay realtime:

```text
rt:u:{user_id}               kiểu HASH
```

```text
rt_events_1h|<bucket_epoch>
rt_page_view_1h|<bucket_epoch>
rt_add_to_cart_1h|<bucket_epoch>
rt_order_1h|<bucket_epoch>
rt_gmv_1h|<bucket_epoch>
rt_session_len_sec|<bucket_epoch>
rt_last_event_ts
```

Consumer ghi event thô **trước**, cập nhật Redis overlay **sau**, rồi mới commit
offset Kafka. Nhờ vậy lake luôn là nguồn sự thật.

---

## 12. Hợp đồng API suy luận

```text
GET  /health  /ready  /metrics  /store/info  /features/{user_id}
POST /decide  /decide/batch  /admin/reload-model
```

`POST /decide` chỉ bắt buộc `user_id`:

```json
{"user_id": "U0000123", "context": {"rt_order_1h": 3}, "debug": false}
```

```json
{"user_id": "U0000123", "decision": "NO_VOUCHER",
 "uplift_score": 0.003643, "threshold": 0.02,
 "feature_version": "v20260805", "model_version": "1",
 "cache_hit": true, "features_missing": 0,
 "features_supplied": 55, "realtime_applied": 0,
 "latency_ms": 9.4, "features": null}
```

Độ trễ ấm đo được **9–24 ms**; lần gọi đầu sau khi restart tốn khoảng một giây
để nạp booster.

Khoá trong `context` **phải** có trong `feature_spec.yml` — khác đi là **422**
kèm danh sách khoá sai. Bỏ qua im lặng một khoá gõ nhầm nghĩa là bên gọi tin rằng
tín hiệu đã được gửi, trong khi không có gì tới model.

### `realtime_applied` — trường cần nhìn

Nó đếm số tín hiệu realtime **thực sự vào được model**, và với model notebook nó
**luôn bằng 0**: 76 cột của nó là 55 batch + 7 dẫn xuất + 14 điền mặc định,
không có ô nào cho `rt_*`.

Gửi `rt_order_1h=9, rt_gmv_1h=500` **không làm score đổi một chữ số**, trong khi
`features_missing` giảm 62→60 và trông y như đã có tác dụng. Chỉ model do
`train.py` huấn luyện (62 cột, 7 trong đó là `rt_*`) mới dùng đến event.

Nói cách khác: toàn bộ hạ tầng realtime (Kafka → consumer → Redis overlay →
`context`) hiện **không ảnh hưởng tới quyết định**. Đó là lý do sâu xa để có
`train.py`, chứ không phải chỉ để repo tự train được.

### Một đường duy nhất dựng vector

Cả hai endpoint đi qua `decide_input.build_model_row()`. Trước đây chúng khác
nhau — `-0.128802` so với `+0.003643` cho **cùng một user** — vì `spec.merge()`
điền `0.0` cho cột thiếu, mà một `0.0` *có mặt* sẽ che mất giá trị mặc định
(trung vị) của hợp đồng model. 28/55 cột chung lệch nhau theo kiểu đó: `f1` là
`0.0` so với trung vị `172.0`. Giờ cột thiếu bị **bỏ ra khỏi** row để hợp đồng
tự điền, đúng như nó được thiết kế.

---

## 13. Reconstruction

Phương pháp: **sinh witness nhất quán feature (CFS)**.

Đây **không** phải khôi phục thật lịch sử event Lazada. Nó dựng **một** tập event
giống-raw hợp lệ, tái tạo lại đúng vector feature đã chọn dưới logic feature của
dbt.

Lớp alias nghiệp vụ nằm ở `config/features/business_aliases.yml` và
`docs/BUSINESS_ALIAS_MAP.md`, ánh xạ tên kỹ thuật như `f30` sang tên nghiệp vụ
tổng hợp như `active_days_30d_log10`. Các alias này làm câu chuyện voucher-uplift
dễ đọc, nhưng **không** phải ngữ nghĩa Lazada/DESCN đã được xác nhận. Redis và
dbt vẫn dùng tên `f*`.

| Track | Ý nghĩa | Output |
|---|---|---|
| Track A | witness event trong quá khứ, trước `reference_ts` | `EVT_*` / `raw_events_v2` |
| Handoff | feature đã kiểm trở thành `CustomerState(T0)` | state object |
| Track B | event tương lai tổng hợp, sau `reference_ts` | business-v2 future event |

Phân tầng feature đã chọn:

- **T1** mức event: `f1,f2,f5,f11,f18,f19,f30`
- **T2** mức thuộc tính: `f37,f38,f79,f80,f81,f82,f40,f43,f44,f45,f64,f68`
- **T3** truyền thẳng: `f3,f4,f8,f9,f10,f12,f13,f16,f20,f21,f22,f23,f25,f26,f28,f29,f31,f35`

Alias tổng hợp mẫu:

| Feature | Alias |
|---|---|
| `f5` | `product_browse_intensity_365d_ln` |
| `f11` | `cart_checkout_intent_365d_ln` |
| `f18` | `promo_touch_intensity_365d_log10` |
| `f19` | `promo_redemption_intensity_365d_log10` |
| `f30` | `active_days_30d_log10` |
| `f37` | `price_sensitivity_segment_64_encoded` |
| `f38` | `promo_affinity_segment_241_encoded` |
| `f79..f82` | `preferred_leaf_category_515_enc_*` |
| `f68` | `discount_hunter_flag` |

Alias event Track A trong báo cáo dùng tên `CFS_*`. Ví dụ `EVT_ORDER_PAID` hiển
thị là `CFS_RECENCY_MARKER`; **không** được đọc đó như bằng chứng rằng `f1/f2` là
feature recency đơn hàng thật của Lazada.

Artifact review đã commit nằm ở `docs/reconstruction_snapshot/`: `README.md`,
`report.html`, `track_a_events.csv`, `track_b_events.csv`,
`features_expected_actual.csv`, `feature_pass.svg`, `timeline.svg`,
`summary.json`, `manifest.json`.

Track A ghi ra `.tmp/reconstruction_track_a/`:

| File | Ý nghĩa |
|---|---|
| `raw_events_v2.csv` | event dựng lại, schema hướng dbt |
| `track_a_events_audit.csv` | event thô kèm cột audit của solver |
| `biz_reconstruction_boundary.csv` | biên mốc thời gian target/reference |
| `biz_customer_attribute.csv` | thuộc tính nguồn T2 đã giải mã |
| `biz_encoding_map.csv` | bảng mã hoá giá trị phân loại đã fit |
| `biz_onehot_layout.csv` | layout one-hot đầy đủ |
| `biz_passthrough_source.csv` | giá trị nguồn T3 đã đóng băng |
| `targets_selected_features.csv` | payload 55 feature kỳ vọng + hash |
| `quarantine.csv` | dòng không dựng lại được |
| `manifest.json` | số đếm, cấu hình, kết quả mẫu gate |

---

## 14. Vận hành thật

Mọi mục trên chứng minh pipeline chạy được. Mục này dành cho lúc câu trả lời phải
**đúng**, chứ không chỉ **xanh**.

### Thứ tự quan trọng

```bash
docker compose up -d                       # 22 container, lần đầu sẽ build image

# 1. Nạp dữ liệu (row_limit=0 nghĩa là toàn bộ)
airflow dags test 00_bootstrap_lake 2026-08-05 --conf '{"row_limit": 0}'

# 2. Build feature. --exclude tag:reconstruction là có chủ đích: các model đó
#    đọc raw/events_v2, thứ chỉ tồn tại sau khi Track A đã land.
airflow dags test 20_build_features_dbt 2026-08-05

# 3. Đẩy 55 feature lên Redis rồi lật active version
airflow dags test 40_sync_features_to_redis 2026-08-05

# 4. Đưa model đã benchmark vào registry (một lần, không phải mỗi run)
python -m lzd_pipeline.training.register
```

Kiểm tra đã vào đúng chỗ:

```bash
curl -s localhost:8000/store/info | jq '{active_version, model}'
```

> `model.is_stub` **phải** là `false`. Nếu là `true` thì API đang phục vụ điểm
> số giả. Đây là tín hiệu duy nhất lộ ra ngoài — hai lỗi Docker từng làm chuỗi
> nạp tụt xuống `StubModel` mà API vẫn xanh và `/decide` vẫn trả về số.

### Ba cổng promote

| Cổng | Từ chối khi |
|---|---|
| tự kiểm | bất kỳ `sanity_*` nào sai — thước đo uplift không tự chứng minh được |
| cùng thước | hai run mang `holdout_version` khác nhau |
| tốt hơn | `qini` không vượt bản đang giữ alias |

Cổng "cùng thước" đứng **trước** cổng so `qini` có chủ đích: không so được thì
không được so, chứ không phải so rồi mới hỏi lại là có hợp lệ không.

### Đổi tập đánh giá

`holdout_dt` và `holdout_version` nằm trong `dbt/dbt_project.yml`. Đổi chúng
nghĩa là **mọi số liệu lịch sử không còn so sánh được với mọi số liệu tương lai**.
Cổng 2 làm việc đó lộ ra thay vì âm thầm, nhưng bump vẫn là một quyết định, không
phải một bước bảo trì. Muốn so qua một lần bump thì phải train lại bản đang chạy
trên holdout mới trước.

### Đổi cửa sổ huấn luyện

`training_window_weeks` (dbt) và `TRAIN_WINDOW_WEEKS` (`training/train.py`) phải
khớp nhau. dbt chỉ giữ bấy nhiêu tuần trong bảng, nên đọc rộng hơn không được
thêm dòng nào; đọc hẹp hơn thì âm thầm train trên ít hơn tưởng.

Mỗi run log `dt_from`, `dt_to`, `n_train`, `holdout_version` vào MLflow — nên câu
*"run này đã thấy dữ liệu gì"* luôn trả lời được.

### Rollback model

API đọc version đang giữ alias `Production` và giữ trong RAM. Đổi alias rồi bảo
nó nạp lại — không restart, không rớt request:

```bash
python - <<'PY'
from mlflow.tracking import MlflowClient
MlflowClient().set_registered_model_alias("uplift_voucher", "Production", "1")
PY
curl -sX POST localhost:8000/admin/reload-model
```

### Reconstruction lên hạ tầng thật

Cả hai entry point batch đều dry-run mặc định và không chạm hạ tầng. Land là
tuỳ chọn:

```bash
# Track A → raw/events_v2 + Postgres biz.* + DuckDB biz.*   (qua DAG 60, land=true)
# Track B → raw/track_b_future/                              (prefix RIÊNG)
python -m lzd_pipeline.reconstruction.track_ab_batch --limit 10 \
       --land /opt/lakehouse/track_b
```

Track B đi prefix riêng **có lý do**. Track A dựng lại hành vi **đã xảy ra** từ
feature có thật — dữ liệu huấn luyện chính đáng. Track B là hành vi
`RuleBasedBehaviour` **bịa ra**. Train trên nó là dạy model học lại luật của
chính nó, và kiểu hỏng đó **không có triệu chứng**: metric vẫn đẹp, có khi đẹp
hơn, vì model được chấm trên chính hành vi mà luật của nó sinh ra.

Dùng chung prefix thì mỗi tác giả sau này đều phải nhớ thêm
`where source_type != 'SYNTHETIC'`. Prefix riêng thì không có gì để quên, và
`dbt/tests/training_khong_nhiem_synthetic.sql` kiểm lại bất biến đó sau **mỗi**
lần `dbt test`.

> 🚫 Không bao giờ replay Track B vào `app.user.events.v1` — topic đó chạy thẳng
> vào `raw/app_events` → `feat_user_realtime_pit` → `training_dataset`. Muốn thử
> tải thì dùng topic riêng.

### Ước lượng chi phí

Đo trên máy này (VM Docker 3.97 GB):

| Công việc | Thời gian | Output |
|---|---|---|
| Build image lần đầu | 5–15 phút | 3 image, ~5.5 GB |
| DAG 00, `row_limit=200000` | ~6 phút | 50 MB parquet |
| DAG 00, toàn bộ 926,669 dòng | ~18 phút | ~140 MB parquet |
| DAG 20 | ~2 phút | 7 model |
| DAG 40 | ~10 phút | 200,000 user, 32 shard |
| Track A, toàn bộ 926,669 dòng | **~43 phút** | **~7.8 GB**, 17.3 triệu event |

Cả stack cần khoảng 2.4 GB thường trú. Nếu Docker thiếu chỗ, `make up-core` bỏ
mười container observability, và `docker builder prune` thu hồi cache — nhưng lưu
ý đĩa ảo WSL2 **không co lại** trên host, nó chỉ ngừng phình thêm.

---

## 15. Lệnh hay dùng

```powershell
# chẩn đoán Docker registry/proxy/CA
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 doctor

# build image
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 build

# trạng thái container
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 ps

# log của một service
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 logs airflow-scheduler

# mở redis-cli
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 redis

# liệt kê bảng DuckDB qua container Airflow
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 duckdb

# test local, không cần Docker
.venv\Scripts\python -m pytest tests -q
```

---

## 16. Xử lý sự cố

**PowerShell chặn script:**

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 <lệnh>
```

**Docker daemon không kết nối được:** mở Docker Desktop, đợi engine chạy, thử lại
`docker compose version`.

**WSL treo, Docker Desktop không khởi động được engine.** Triệu chứng: log báo
`wsl.exe -l -v --all: CommandTimedOut`, và `wsl --status` cũng treo. Mở PowerShell
**as Administrator**:

```powershell
Restart-Service WSLService -Force
wsl --shutdown
```

Nếu `wslservice.exe` sống sót cả `Stop-Process -Force` thì nó kẹt ở kernel —
**phải reboot**, không có lệnh user-space nào gỡ được.

**`docker pull` báo `x509: certificate signed by unknown authority`:** đây là vấn
đề trust/proxy/CA của Docker Desktop, không phải lỗi code. Nếu không cần proxy thì
tắt proxy trong Docker Desktop; nếu dùng proxy công ty thì import root CA vào
Windows `Certificates (Local Computer) → Trusted Root Certification Authorities`,
rồi restart Docker Desktop và kiểm bằng `docker pull redis:7.2-alpine`.

**Docker build lỗi `invalid file request` dưới OneDrive:** nên clone về
`C:\dev\LZD`. Giữ `.dockerignore` loại các thư mục bind-mount khỏi build context.

**Redis không có `fs:meta:active_version`:** chạy DAG `00_bootstrap_lake`,
`20_build_features_dbt`, `40_sync_features_to_redis` theo đúng thứ tự.

**`/store/info` báo `is_stub: true`:** model thật không nạp được. Hai nguyên nhân
đã gặp, cả hai đều hỏng im lặng:

1. `models/` không được mount vào container → `FileNotFoundError:
   feature_contract.json`
2. thiếu `libgomp1` ở runtime → `OSError: libgomp.so.1`. Lỗi này còn làm DAG 30
   chết ngay ở bước `import lightgbm`.

**MinIO/Kafka/Redis không đổi sau reconstruction dry-run:** đúng như thiết kế.
Dry-run chỉ kiểm hợp đồng; muốn ghi thật phải dùng `--land` hoặc DAG 60 với
`land=true`.

---

## 17. Tài liệu chính

- [Tech reference](docs/TECH_REFERENCE.md) — bản đồ mức code: module, config,
  hợp đồng, DAG, test. **Đọc file này đầu tiên nếu bạn sắp sửa code.**
- [Kiến trúc pipeline](docs/PIPELINE_ARCHITECTURE.md)
- [Luồng dữ liệu](docs/DATA_FLOW.md)
- [Bản đồ alias nghiệp vụ](docs/BUSINESS_ALIAS_MAP.md)
- [Reconstruction README](docs/RECONSTRUCTION_README.md)
- [Hợp đồng reconstruction](docs/RECONSTRUCTION_CONTRACT.md)
- [Đặc tả reconstruction](docs/RECONSTRUCTION_SPEC.md)
- [Feature lineage](docs/FEATURE_LINEAGE.md)
- [Từ điển feature](docs/FEATURE_DICTIONARY.md)
- [Runbook](docs/RUNBOOK.md)
