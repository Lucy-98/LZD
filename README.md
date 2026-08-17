# LZD Uplift Feature Platform

Repo này dựng lại toàn bộ đường dữ liệu cho bài toán Lazada voucher uplift: nạp
dataset gốc, build feature bằng dbt/DuckDB, sync 55 selected feature lên Redis,
phục vụ quyết định phát voucher qua FastAPI bằng best model đã chốt từ notebook,
và chứng minh reconstruction từ feature train ngược về event.

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
- Đóng gói best model từ notebook trực tiếp trong Docker image để cùng một image
  tag luôn phục vụ cùng một booster và feature contract.
- Chứng minh reconstruction: từ selected feature trong tập train, sinh ngược
  Track A witness event, chạy lại dbt SQL, và so lại feature kỳ vọng/thực tế.

### Ai dùng repo này

- **Data/ML engineer** muốn xem feature lineage và hợp đồng sync.
- **Mentor/reviewer** muốn clone về, cài đặt, chạy test, xem artifact
  reconstruction.
- **Người vận hành** cần build/deploy hoặc rollback image chứa model.

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
DuckDB  marts.feat_user_selected_serving
        │
        ▼
DAG 40_sync_features_to_redis
        │
        ▼
Redis  fs:{version}:u:{user_id} ───┐
                                   ├──► FastAPI POST /decide
Docker image: notebook best model ─┘
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
| Test | serving/model/package checks chạy xanh; reconstruction suite cần `duckdb` theo `requirements-dev.txt` |
| Pipeline | DAG 00 → 20 → 40 xanh, feature lên Redis, `active_version` được kích hoạt |
| Serving | `/decide` 2.7–10.6 ms ấm, `cache_hit=true` |
| Model | DRLearner từ notebook, bake trong `lzd-reconstruction/python-service` image |
| Reconstruction | Track A full train: 926.669 dòng đã xử lý, 926.661 solved, 8 quarantine; Gate A/A-T3 đạt 50/50 mẫu |
| Track B online demo | 10 user, 45 synthetic AppEvent qua Kafka; đủ 45 ở MinIO/dbt staging, 10 Redis overlay, 10 model score và campaign Top-K SEND/WAIT/SUPPRESS |

Model trong image tái tạo golden predictions với **`max |lệch| = 0.0`** trên
150 mẫu — qua cả chặng đổi định dạng (pkl → booster text), đổi Python
(3.10.9 → 3.11) và đóng gói vào image.

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

### Track A full train đã chạy

Run local ngày 2026-08-17 đã đọc đủ 926.669 dòng của `full_trainset.csv` theo
scope `fs_2026_08_v2`/55 cột, sinh 18.289.612 raw event và khoảng 8,0 GB
artifact trong `.tmp/reconstruction_track_a/`. Có 926.661 target solved; 8
target mâu thuẫn nội tại (`n30=1` nhưng ép hai ngày hoạt động khác nhau) được ghi
rõ vào `quarantine.csv`. Gate A và A-T3 đạt 50/50 dòng kiểm tra, không có failure.
Artifact runtime nằm trong `.tmp/` nên bị git-ignore; số liệu và danh sách file
được lưu ở `.tmp/reconstruction_track_a/manifest.json`.

### Chưa làm

- `feat_cfs_*` (đường reconstruction) vẫn bị `--exclude tag:reconstruction` trong
  DAG 20 — chúng cần artifact Track A được land vào `raw/events_v2/` trên lake.
- Model đang phục vụ **không dùng feature realtime**. Xem [§12](#12-hợp-đồng-api-suy-luận).
- Track B demo dùng adapter business-event → AppEvent v1 trong **stack biệt lập**;
  đây là dữ liệu synthetic để trình diễn luồng, không phải ground truth.

---

## 3. Bố cục thư mục

```text
airflow/dags/                 Airflow DAG
config/features/              feature spec, tập 55 selected feature
config/reconstruction/        cấu hình runtime cho reconstruction
dbt/                          model + test dbt (DuckDB)
dbt/macros/                   macro dùng chung, xem ghi chú bên dưới
docker/                       Dockerfile cho Airflow/Python service
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
được, và Compose tự build hai image local (`lzd-reconstruction/airflow`,
`lzd-reconstruction/python-service`) ở lần chạy đầu. Lần đầu mất 5–15 phút,
các lần sau vài giây.

Bạn nhận được Postgres kèm pgAdmin, Redis, MinIO, Kafka, Airflow, API suy luận và
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
không nên tự chạy khi ai đó vừa `docker compose up` để xem thử. Bỏ pause các
DAG dữ liệu cần thiết trong Airflow UI.

Xem trạng thái và sức khoẻ:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 status
powershell -ExecutionPolicy Bypass -File .\scripts\stack.ps1 health
```

Demo online bounded cho đúng 10 user Track A, không bật producer chạy liên tục:

```bash
make track-b-online-demo
# tương đương: sh scripts/run_track_b_online_demo.sh --users 10

# Sau khi E2E xong, in bảng inference/policy gọn để trình bày với mentor:
make show-inference-demo
```

Lệnh này dùng project/cổng trong `config/demo-stack.env`, tự build image và nối
toàn bộ service. Track A reconstruct 10 user → land raw `events_v2` vào MinIO →
dbt materialize mart 55F trong DuckDB → engine sync/validate `10 × 55 = 550`
giá trị lên Redis. Sau đó Track B mới publish event qua Kafka → MinIO + Redis
realtime → dbt staging → FastAPI/model đóng trong image. Report nằm ở
`.tmp/track_b_online_demo/<run_id>/report.json`.

Script cũng tự bật monitoring của chính stack demo: Grafana
http://localhost:13000 (`admin/admin`), Prometheus http://localhost:19090,
Loki http://localhost:13100 và Pushgateway http://localhost:19091. Dashboard đã
được provision trong folder **LZD Uplift Platform**, không cần import JSON thủ
công.

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
| Postgres | metadata Airflow + schema audit | localhost:5432 |
| pgAdmin | UI quản trị Postgres | http://localhost:5050 |
| Redis | online feature store | localhost:6379 |
| MinIO | data lake tương thích S3 | http://localhost:9001 |
| Kafka | bus event realtime | localhost:29092 |
| Airflow | điều phối | http://localhost:8080 |

**Serving** (cũng chạy mặc định):

| Service | Vai trò | URL |
|---|---|---|
| inference-api | đọc feature từ Redis, dùng model trong image để ra quyết định | http://localhost:8000/docs |

**Observability** (chạy mặc định):

| Service | Vai trò | URL |
|---|---|---|
| Grafana | UI dashboard metric + khám phá log (không phải nơi thu thập dữ liệu) | http://localhost:3000 |
| Prometheus | scrape exporter/`/metrics`, lưu time-series và đánh giá alert | http://localhost:9090 |
| Pushgateway | giữ metric của các job batch đã thoát để Prometheus scrape | http://localhost:9091 |
| StatsD exporter | chuyển Airflow StatsD sang metric Prometheus | nội bộ |
| Redis/Postgres/Kafka exporter | chuyển metric native của hạ tầng sang Prometheus | nội bộ |
| Promtail | thu log Docker/Airflow và đẩy sang Loki | nội bộ |
| Loki | lưu và query log cho Grafana | http://localhost:3100 |
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
| pgAdmin | `admin@lzd.example/admin123` |
| Grafana | `admin/admin` |
| MinIO | `minioadmin/minioadmin123` |

Trong pgAdmin, server **LZD Postgres** đã được khai báo sẵn với host
`postgres`, database/user `lzd`. Lần kết nối đầu tiên, nhập mật khẩu database
`lzd_secret` (hoặc giá trị `POSTGRES_PASSWORD` trong `.env`). Nếu đổi
`POSTGRES_USER` hay `POSTGRES_DB`, cập nhật tương ứng trong
`config/pgadmin/servers.json`.

---

## 8. MinIO và Kafka

MinIO tạo một bucket dữ liệu:

```text
lakehouse  →  dữ liệu raw/staging/marts/export
```

Model không nằm ở MinIO. `model_booster.txt`, `feature_contract.json` và
`metadata.json` được bake vào `lzd-reconstruction/python-service` khi build.

Trong `lakehouse`, script init tạo sẵn:

```text
raw/user_snapshot     ←  DAG 00 ghi vào
raw/app_events        ←  stream-consumer ghi vào
raw/events_v2         ←  Track A land vào (hiện chưa có dữ liệu)
raw/track_b_future    ←  artifact Track B dry-run/land riêng
exports
```

Kafka tạo 2 topic:

```text
app.user.events.v1      partitions=3, retention=2 ngày
app.user.events.dlq.v1  partitions=1, retention=7 ngày
```

Key Kafka là `user_id`/`customer_id` để đảm bảo thứ tự theo từng user. Online
demo chuyển bốn event Track B (`SESSION_STARTED`, `PRODUCT_VIEWED`,
`ITEM_ADDED_TO_CART`, `PURCHASE_COMPLETED`) sang AppEvent v1 hợp lệ rồi publish
vào topic v1 của **project demo biệt lập**.

---

## 9. Thứ tự chạy Airflow

Mở Airflow tại `http://localhost:8080`, rồi chạy tay theo thứ tự cho đường batch:

1. `00_bootstrap_lake`
2. `20_build_features_dbt`
3. `40_sync_features_to_redis`
4. `50_data_quality`

DAG reconstruction chạy tay: `60_reconstruction_e2e` — chỉ kiểm hợp đồng ở chế
độ dry-run, **không** ghi MinIO/Kafka/Redis production.

Project không có DAG train định kỳ. Dataset là bản nghiên cứu hữu hạn, không có
luồng quan sát mới liên tục đủ để biện minh cho retrain hằng tuần. Model được
chọn và benchmark trong notebook, sau đó đóng gói cùng image serving.

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

Để chứng minh đường online bằng đúng 10 user đó, dùng
`make track-b-online-demo`. Runner dựng Track A bằng dbt thật, materialize mart
`marts.demo_track_b_selected_serving`, sync đúng 55F sang Redis, bàn giao
`CustomerState(T0)`, rồi mới sinh một lô Track B bounded. Track A `EVT_*` không
được publish; chỉ AppEvent đã qua adapter/schema validation đi vào Kafka.

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
```

`feat_user_selected_serving` chứa `user_id`, `dt`, `feature_ts` và 55 selected
feature. Nó **không** chứa `label` hay `is_treat`; repo runtime cũng không dựng
training mart hay holdout mart vì không retrain model trong pipeline.

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
POST /decide  /decide/batch  /campaign/decide
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
một notebook tương lai huấn luyện với `rt_*` mới dùng đến event.

Nói cách khác: toàn bộ hạ tầng realtime (Kafka → consumer → Redis overlay →
`context`) hiện **không ảnh hưởng tới uplift score hoặc `decision` threshold của
hai endpoint model**. Trường `realtime_applied` làm giới hạn đó lộ rõ thay vì
để API tạo cảm giác realtime đang đi vào DRLearner.

Endpoint demo `POST /campaign/decide` tach rieng hai trach nhiem: model xep hang
ai nen uu tien, con `rt_*` synthetic chi gate thoi diem phat. Request:

```json
{"user_ids":["U0000000","U0000001","U0000002"],"budget":2}
```

Response tra rank/uplift va `campaign_action`: `SEND_VOUCHER` neu user nam trong
Top-K, uplift duong va co add-to-cart; `WAIT_FOR_INTENT` neu chua co intent;
`SUPPRESS_ALREADY_PURCHASED` neu da order. Day la policy demo co nhan
`synthetic_realtime=true`, khong phai threshold production da toi uu.

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
- **T2** mức thuộc tính: `f37,f38,f79,f80,f81,f82,f40,f41,f42,f43,f44,f45,f46,f47,f52,f53,f54,f57,f58,f59,f62,f64,f65,f68`
- **T3** truyền thẳng: `f0,f3,f4,f6,f8,f9,f10,f12,f13,f16,f17,f20,f21,f22,f23,f24,f25,f26,f27,f28,f29,f31,f34,f35`

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

Alias event Track A trong báo cáo dùng tên ecommerce có tiền tố `ASSUMED_*`.
Ví dụ `EVT_ORDER_PAID` hiển thị là `ASSUMED_ORDER_PAID`; **không** được đọc đó
như bằng chứng rằng `f1/f2` là feature recency đơn hàng thật của Lazada.

Artifact review đã commit nằm ở `docs/reconstruction_snapshot/`: `README.md`,
`report.html`, `track_a_events.csv`, `track_b_events.csv`,
`features_expected_actual.csv`, `feature_pass.svg`, `timeline.svg`,
`summary.json`, `manifest.json`.

Track A ghi ra `.tmp/reconstruction_track_a/`:

| File | Ý nghĩa |
|---|---|
| `raw_events_v2.csv` | event dựng lại, schema hướng dbt |
| `track_a_events_audit.csv` | event thô kèm cột audit của solver |
| `event_business_aliases.csv` | lookup tên kỹ thuật `EVT_*` sang alias trình bày `ASSUMED_*` |
| `biz_reconstruction_boundary.csv` | biên mốc thời gian target/reference |
| `biz_customer_attribute.csv` | thuộc tính nguồn T2 đã giải mã |
| `biz_encoding_map.csv` | bảng mã hoá giá trị phân loại đã fit |
| `biz_onehot_layout.csv` | layout one-hot đầy đủ |
| `biz_passthrough_source.csv` | giá trị nguồn T3 đã đóng băng |
| `targets_selected_features.csv` | payload 55 feature kỳ vọng + hash |
| `quarantine.csv` | dòng không dựng lại được |
| `manifest.json` | số đếm, cấu hình, kết quả mẫu gate |

Sau khi land các file trên, đường dbt reconstruction tạo bảng
`marts.feat_cfs_reconstructed_selected`: mỗi `target_id` có đúng 55 cột `f`
được join từ counter, recency, categorical và pass-through. Gate A đọc bảng hợp
nhất này; bảng không đọc `targets_selected_features.csv` để tính feature.

---

## 14. Vận hành thật

Mọi mục trên chứng minh pipeline chạy được. Mục này dành cho lúc câu trả lời phải
**đúng**, chứ không chỉ **xanh**.

### Thứ tự quan trọng

```bash
docker compose up -d                       # lần đầu sẽ build image

# 1. Nạp dữ liệu (row_limit=0 nghĩa là toàn bộ)
airflow dags test 00_bootstrap_lake 2026-08-05 --conf '{"row_limit": 0}'

# 2. Build feature. --exclude tag:reconstruction là có chủ đích: các model đó
#    đọc raw/events_v2, thứ chỉ tồn tại sau khi Track A đã land.
airflow dags test 20_build_features_dbt 2026-08-05

# 3. Đẩy 55 feature lên Redis rồi lật active version
airflow dags test 40_sync_features_to_redis 2026-08-05

# Model đã nằm trong image từ lúc build, không có bước đăng ký runtime.
```

Kiểm tra đã vào đúng chỗ:

```bash
curl -s localhost:8000/store/info | jq '{active_version, model}'
```

Kết quả `model.source` phải là `docker_image`. Nếu booster hoặc contract thiếu,
`/ready` trả 503; service không tạo score giả để che lỗi đóng gói.

### Đổi hoặc rollback model

Model version đi cùng image version:

1. Chốt best model trong notebook và xuất `model_booster.txt`,
   `feature_contract.json`, `metadata.json` vào `models/uplift_voucher/`.
2. Chạy golden test để bảo đảm booster mới đúng contract.
3. Build image bằng một tag mới, ví dụ
   `docker build -f docker/python-service/Dockerfile -t lzd-reconstruction/python-service:drlearner-20260813 .`.
4. Deploy tag đó. Rollback bằng cách deploy lại image tag trước.

Không có hot-reload: một process chỉ nạp model một lần lúc warm-up, nên cùng một
image digest không thể âm thầm đổi model giữa hai lần request.

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
feature có thật. Track B là hành vi `RuleBasedBehaviour` **bịa ra**, nên không
được dùng làm ground truth hay đưa ngược vào dataset nghiên cứu.

Dùng chung prefix thì mỗi tác giả sau này đều phải nhớ thêm
`where source_type != 'SYNTHETIC'`. Prefix riêng thì không có gì để quên.

Production không được replay Track B synthetic như dữ liệu thật. Ngoại lệ có
kiểm soát là `track-b-online-demo`: nó dùng Compose project/port/volume riêng,
chỉ gửi một lô bounded của 10 user và ghi rõ `source_type=SYNTHETIC` trong
lineage artifact. Không dùng output demo để train, đánh giá hoặc suy diễn lịch
sử người dùng.

### Ước lượng chi phí

Đo trên máy này (VM Docker 3.97 GB):

| Công việc | Thời gian | Output |
|---|---|---|
| Build image lần đầu | 5–15 phút | 2 image + model artifact |
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

**`/ready` trả 503 vì model không sẵn sàng:** đây là lỗi image và không bị che
bằng model giả. Kiểm tra hai nguyên nhân thường gặp:

1. Docker build context thiếu `model_booster.txt` hoặc `feature_contract.json`.
2. thiếu `libgomp1` ở runtime → `OSError: libgomp.so.1` khi import LightGBM.

Không sửa bằng cách mount `./models` từ host: mount làm cùng một image tag chạy
hai model khác nhau trên hai máy. Hãy sửa artifact rồi build image mới.

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
