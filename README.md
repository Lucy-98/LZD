# LZD Uplift Feature Platform

Nền tảng dữ liệu và phục vụ quyết định voucher bằng uplift modeling. Repo tích hợp Lakehouse (MinIO, DuckDB, dbt), Feature Store online (Redis), Airflow, Kafka, FastAPI, MLflow và Prometheus/Grafana/Loki.

## Business problem và đóng góp Data Engineering

### Bài toán cần giải quyết

Hệ thống voucher không chỉ cần dự đoán khách hàng có khả năng mua hàng, mà cần
ước lượng **tác động tăng thêm của voucher**: khách hàng nào sẽ chuyển đổi nhờ
voucher và khách hàng nào vốn đã mua dù không nhận ưu đãi. Quyết định sai gây
lãng phí ngân sách, giảm biên lợi nhuận hoặc bỏ lỡ khách hàng có thể được kích
hoạt đúng thời điểm.

Để mô hình uplift tạo ra quyết định có thể sử dụng trong sản phẩm, hệ thống dữ
liệu phải giải quyết đồng thời bốn vấn đề:

- tái dựng feature lịch sử có thể truy nguyên từ dữ liệu đã ẩn danh;
- bảo đảm cùng một ordered feature contract giữa training và serving;
- kết hợp batch profile với tín hiệu hành vi gần realtime mà không đếm trùng;
- chỉ kích hoạt feature/model version sau validation, đồng thời đo được
  freshness, completeness, skew và serving latency.

Khi các điều kiện này được đáp ứng, hệ thống có thể cung cấp feature nhất quán
cho quyết định voucher theo từng user, chạy lại pipeline có kiểm soát, rollback
version khi có sự cố và cung cấp bằng chứng vận hành thay vì chỉ dựa vào trạng
thái DAG màu xanh.

### Vai trò và phạm vi đóng góp

Phạm vi sở hữu chính của phần Data Engineering trong repo:

- thiết kế ingestion Kafka → immutable MinIO lake và DLQ;
- xây dựng reconstruction, dbt Bronze/Silver/Gold và PIT training dataset;
- xây dựng Redis online feature store gồm 71 batch fields + 12 realtime fields;
- orchestration DAG00/10/20/40/50/60/99, dependency và quality gates;
- versioned feature sync, checksum validation, atomic activation, rollback và GC;
- replay-idempotency theo `event_id`, event-time windows và watermark policy;
- model/feature compatibility metadata, data contracts và observability bằng
  Prometheus, Grafana, Loki và PostgreSQL audit.

FastAPI decision service và uplift model là các thành phần tích hợp ở ranh giới
phục vụ/ML. Phần DE chịu trách nhiệm cung cấp đúng dữ liệu, đúng thời điểm, đúng
version và contract để các thành phần đó có thể train, deploy và suy luận an
toàn; không dùng kết quả API/model để thay thế bằng chứng chất lượng dữ liệu.

### Giá trị đối với hệ thống tổng thể

| Năng lực DE | Đóng góp cho hệ thống |
|---|---|
| Immutable lake + deterministic object identity | Replay Kafka không làm mất dữ liệu hoặc âm thầm tạo raw payload khác |
| PIT training contract | Giảm leakage và training/serving skew |
| Versioned Redis feature store | Cutover/rollback feature không gây mixed-version traffic |
| Ordered contract + schema hash | Chặn model không tương thích trước khi nhận traffic |
| Quality gates và audit | Biết pipeline xanh có thực sự đủ freshness, completeness và parity hay không |
| Metrics, logs và dashboards | Rút ngắn thời gian phát hiện và khoanh vùng lỗi ingestion, sync hoặc serving |

### Bằng chứng demo nên thu thập

Để trình bày dự án, nên capture trực tiếp từ stack đang chạy:

1. Airflow Graph/Grid của DAG60 → DAG20 → DAG40 → DAG50 với run mới thành công.
2. MinIO Console hiển thị các partition `raw/user_snapshot`, `raw/events_v2`
   và `raw/app_events/.../topic=.../partition=.../offset-...parquet`.
3. DuckDB/dbt query thể hiện `marts.serving_features` có 71 batch fields và
   `marts.training_dataset` có đủ batch + realtime PIT + label/treatment.
4. RedisInsight gồm `fs:meta:active_version`, status metadata và một cặp key
   `fs:<version>:u:<user_id>` / `rt:u:<user_id>`.
5. Grafana dashboard 00 Pipeline Overview và 04 Data Quality & Skew.
6. API `/ready` thể hiện tuple feature/spec/model compatible; chỉ dùng làm bằng
   chứng tích hợp downstream, không phải trọng tâm ownership DE.
7. Một ảnh terminal chạy test suite và một ảnh kiến trúc target trong draw.io.

Model production đi kèm repo là **DRLearner–LightGBM, đúng 30 feature**. API chỉ phát voucher khi uplift score lớn hơn hoặc bằng:

```text
UPLIFT_DECISION_THRESHOLD=0.0070347543
```

## Trạng thái đã kiểm chứng

- Model nguồn: `https://github.com/hatuki0604/final-lazada-2.git`
- Commit model nguồn: `575fb1e0ea78ba0c9cb11876aac5984125308698`
- Model version khi phục vụ: `20260820-575fb1e0ea78`
- Feature contract reconstruction: `fs_2026_08_v4`, đúng 30 input model và đúng thứ tự
- Feature Store: 71 batch field (trong đó có nguyên vector 30F) + 12 realtime field
- Golden sentinel score: `0.004436201035688247`
- Sai số tuyệt đối cho sentinel: `1e-12`
- Toàn bộ test suite đã pass; trên macOS thiếu `libomp` có 2 test LightGBM bị skip
- Model thật đã được kiểm chứng trong Linux container

Model bundle nằm tại [`models/uplift_voucher_30f`](models/uplift_voucher_30f). `import_manifest.json` giữ nguồn, commit, threshold, sentinel và SHA-256 của từng artifact. Loader kiểm tra toàn bộ thông tin này khi API khởi động; artifact sai hoặc thiếu sẽ làm serving **fail-closed**, không tự chuyển sang model giả.

## Kiến trúc

```text
full_trainset.csv
      │
      ▼
Track A reconstruction ──► PostgreSQL biz.* / DuckDB / MinIO
                                      │
                                      ▼
                          dbt Bronze → Silver → Gold
                                      │
                        ┌─────────────┴─────────────┐
                        ▼                           ▼
              training_features (30F)     serving_features (71 batch fields)
                                                    │
                                                    ▼
                                          Redis versioned store
                                                    │
Kafka events ──► stream consumer ──► Redis realtime overlay
                                                    │
                                                    ▼
                                     FastAPI + LightGBM 30F
                                                    │
                                                    ▼
                                       PostgreSQL inference audit
```

Feature version trong Redis là phiên bản dữ liệu, khác với model version. Tên có hậu tố `smoke` chỉ dành cho dữ liệu kiểm thử thủ công, không phải dữ liệu production và không phải model giả.

### Các hàng rào triển khai đã được hiện thực hóa

- Stream landing dùng identity `topic/partition/offset-range`; replay cùng
  payload không tạo object mới, còn cùng offset nhưng khác payload sẽ fail-closed.
- Realtime counter được bucket theo `event_ts` và Lua áp dụng từng `event_id`
  đúng một lần. Event quá cửa sổ hoặc quá xa trong tương lai không đi vào
  online overlay và có metric riêng.
- Mỗi Redis feature version lưu `feature_spec_version`, `schema_hash` và
  `realtime_semantics_version`. `/ready` chỉ xanh khi tuple feature/spec/model
  tương thích.
- Model do DAG 30 train có deployment manifest, checksum và golden sentinel.
  Alias `Production` chỉ được đổi sau package/compatibility gate.
- Model deployment được stage vào volume `serving-models`, verify rồi atomic
  swap symlink `active`; API chỉ mount volume này read-only. Reload admin yêu
  cầu `X-Model-Reload-Token`.

Sau khi nâng cấp từ state cũ, phải chạy lại DAG 40 một lần để active feature
version có compatibility metadata mới; `/ready` cố ý trả 503 trước bước này.
Có thể chạy controller thủ công trong Airflow container:

```bash
docker compose exec -T airflow-scheduler python -m lzd_pipeline.serving.deployment \
  --root /opt/serving-models \
  --reload-url http://inference-api:8000/admin/reload-model
```

## Yêu cầu máy

- Git 2.x và Git LFS
- Docker Desktop hoặc Docker Engine với Compose v2
- Python 3.11 hoặc 3.12 nếu muốn chạy test ngoài Docker
- Docker được cấp ít nhất 4 CPU và 12 GB RAM; khuyến nghị 16 GB khi chạy
  backfill/realtime cùng lúc
- Ít nhất 25 GB ổ đĩa trống cho lượt smoke 5.000 dòng; full backfill cần nhiều hơn
- macOS, Linux hoặc Windows chạy Docker Desktop/WSL2

Kiểm tra nhanh:

```bash
git --version
git lfs version
docker version
docker compose version
```

## Dựng lại trên máy mới

### 1. Clone đúng phiên bản mã nguồn và dữ liệu LFS

Để hai máy cho cùng kết quả, không chạy trực tiếp từ một branch đang thay đổi. Dùng cùng commit hoặc release tag trên cả hai máy:

```bash
git lfs install
git clone https://github.com/Lucy-98/LZD.git
cd LZD
git checkout <RELEASE_TAG_HOAC_COMMIT>
git lfs pull
git status --short
git rev-parse HEAD
git lfs ls-files
```

`git status --short` phải rỗng. Lưu kết quả `git rev-parse HEAD` cùng báo cáo chạy để có thể truy nguyên chính xác phiên bản. Hai CSV lớn trong `data/` dùng Git LFS; nếu chỉ thấy file pointer vài trăm byte thì `git lfs pull` chưa hoàn tất.

Kích thước tham chiếu hiện tại sau khi pull là khoảng 476 MB cho
`full_trainset.csv` và 98 MB cho `full_testset.csv`. Có thể kiểm tra nhanh trên
macOS/Linux:

```bash
wc -c data/full_trainset.csv data/full_testset.csv
```

### 2. Tạo cấu hình

Compose có giá trị mặc định đủ để chạy local. Tạo `.env` nếu cần cấu hình rõ ràng:

```bash
cp .env.example .env
```

Giữ nguyên dòng sau nếu muốn tái lập quyết định hiện tại:

```dotenv
UPLIFT_DECISION_THRESHOLD=0.0070347543
```

Trên Linux native, sửa `AIRFLOW_UID` trong `.env` thành kết quả của `id -u` để
container ghi được bind mount `airflow/logs`. Trên macOS/Windows Docker Desktop
giữ `AIRFLOW_UID=50000`.

Xác nhận Compose đọc được toàn bộ cấu hình trước khi build:

```bash
docker compose --profile all config --quiet
```

Không sửa model bundle hoặc `feature_contract.json`. Không dùng credential mặc định trong môi trường public/production.

### 3. Build và khởi động stack

Trên macOS/Linux:

```bash
chmod +x scripts/stack.sh
./scripts/stack.sh doctor
./scripts/stack.sh init
./scripts/stack.sh up
```

Trên Windows PowerShell:

```powershell
.\scripts\stack.ps1 doctor
.\scripts\stack.ps1 init
.\scripts\stack.ps1 up
```

`up` dựng toàn bộ batch platform, Airflow, API và observability. Hai service tạo
traffic liên tục (`event-producer`, `stream-consumer`) chỉ chạy khi dùng
`up-all` hoặc `docker compose --profile all up -d`; chúng không cần thiết để
hoàn thành bootstrap batch ban đầu.

Nếu không dùng script, các lệnh tương đương là:

```bash
docker compose --profile all build --pull
docker compose up -d
```

Chờ các service khởi động rồi kiểm tra:

```bash
docker compose --profile all ps
curl --fail http://localhost:8000/health
```

Healthcheck đầy đủ dùng `./scripts/stack.sh health` trên macOS/Linux hoặc
`.\scripts\stack.ps1 health` trên Windows.

`airflow-init`, `kafka-init` và `minio-init` kết thúc với exit code 0 là bình
thường. Các service dài hạn phải ở trạng thái `running`/`healthy`. `/health`
chỉ cho biết process API còn sống; `/ready` chỉ thành công sau khi DAG 40 kích
hoạt một feature version thật trong Redis.

Chỉ khi chủ động xóa toàn bộ trạng thái local cũ mới chạy lệnh sau. Lệnh này
xóa PostgreSQL, Redis, Kafka, MinIO, DuckDB và các volume của project:

```bash
docker compose --profile all down -v --remove-orphans
```

### 4. Xác minh đúng model, không cần dữ liệu smoke

Kiểm tra model ngay trong môi trường Linux của container:

```bash
docker compose exec -T inference-api python -c "from lzd_pipeline.serving.model_loader import get_model; m=get_model(True); row={k:m.contract.defaults[k] for k in m.feature_order}; print(m.version, len(m.feature_order), m.predict_uplift([row])[0])"
```

Kết quả bắt buộc:

```text
20260820-575fb1e0ea78 30 0.004436201035688247
```

Kiểm tra log nạp model:

```bash
docker compose logs inference-api | grep model_loaded
```

Phải thấy `feature_count: 30` và không được có `model_not_configured`.

### 5. Tạo feature version chính thức

Đăng nhập Airflow tại <http://localhost:8080> bằng `admin/admin`, sau đó chạy theo thứ tự:

1. `00_bootstrap_lake` – tạo cấu trúc lake và nạp dữ liệu nguồn. Lượt smoke
   dùng `dt=2026-08-05`, `row_limit=5000`.
2. `60_reconstruction_e2e` – mặc định mới nhất đã là `mode=backfill`,
   `row_limit=5000`, `verify_limit=300`, `dt=2026-08-05`, `land=true`.
3. `20_build_features_dbt` – giữ `run_date=1970-01-01` để build toàn bộ dữ liệu
   hiện có. Task đầu tiên `validate_reconstruction_inputs` phải thành công.
4. `40_sync_features_to_redis` – để `dt` trống để tự chọn partition mới nhất,
   giữ `force_full_resync=false`.
5. `50_data_quality` – chạy kiểm tra chất lượng sau đồng bộ.

Ở DAG 60, `contract_dry_run` màu skipped là đúng khi chạy mode backfill;
`backfill_train_split` và `assert_sources_ready` phải màu xanh. Đừng đánh giá một
run cũ sau khi đổi cấu hình: mỗi lần cần tạo run mới và kiểm tra đúng `run_id`.
Không chạy DAG 20 hoặc DAG 40 trước khi `assert_sources_ready` thành công.

Muốn xử lý toàn bộ train split, đặt `row_limit=0` ở cả DAG 00 và DAG 60. Quá
trình reconstruction có thể sinh khoảng 17,9 triệu event, cần nhiều thời gian,
RAM và dung lượng hơn đáng kể so với lượt smoke.

Sau khi DAG 40 thành công:

```bash
curl --fail http://localhost:8000/ready
```

Response sẽ chứa feature version chính thức dạng `vYYYYMMDD`, không cần seed `vmodel30-smoke`.

Kiểm tra trực tiếp Redis:

```bash
docker compose exec -T redis redis-cli PING
docker compose exec -T redis redis-cli GET fs:meta:active_version
docker compose exec -T redis redis-cli ZRANGE fs:meta:versions 0 -1
```

Kết quả đầu phải là `PONG`, còn `active_version` phải có dạng `vYYYYMMDD`.
Trong RedisInsight tại <http://localhost:5540>, thêm database với Host `redis`,
Port `6379`, không username/password và Database Index `0`. Không dùng
`127.0.0.1` trong RedisInsight vì ứng dụng này cũng chạy trong container.

### 6. Gọi API quyết định

Lấy `example_user` từ XCom/log của task `smoke_test_serving` trong DAG 40. Cũng
có thể tìm một key `fs:<active_version>:u:<user_id>` trong RedisInsight, rồi gọi:

```bash
curl --fail --request POST http://localhost:8000/decide \
  --header 'content-type: application/json' \
  --data '{"user_id":"<USER_ID_DA_SYNC>","context":{}}'
```

Response phải có:

- `model_version: "20260820-575fb1e0ea78"`
- `threshold: 0.0070347543`
- `features_supplied: 30`
- `features_missing: 0` với một bản ghi đầy đủ
- `decision: "SEND_VOUCHER"` và `voucher_code: "voucher_30"` khi score
  `>= threshold`; ngược lại là `NO_VOUCHER` và `no_voucher`

Không so sánh `latency_ms`, timestamp hoặc ID run giữa hai máy: đây là dữ liệu runtime và đương nhiên thay đổi. Model version, thứ tự feature, threshold và uplift score cho cùng một vector đầu vào mới là hợp đồng cần giống nhau.

## Chạy test local

macOS/Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e '.[dev,ml]'
python -m pytest -q
```

Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev,ml]"
python -m pytest -q
```

LightGBM trên macOS cần OpenMP:

```bash
brew install libomp
```

Nếu không muốn cài `libomp`, vẫn có thể xác minh model bằng lệnh chạy trong `inference-api` ở bước 4.

Kiểm tra dbt và Airflow:

```bash
docker compose exec -T airflow-webserver bash -lc 'cd /opt/project/dbt && dbt parse --no-version-check'
docker compose exec -T airflow-webserver airflow dags list-import-errors
```

Lệnh thứ hai phải trả về `No data found`.

## Chứng minh artifact giống nhau

Các SHA-256 chuẩn nằm trong [`models/uplift_voucher_30f/import_manifest.json`](models/uplift_voucher_30f/import_manifest.json):

| Artifact | SHA-256 |
|---|---|
| `model_booster.txt` | `41c42a31b484670c085a234b490b9446cadc4a995d7aa5a4f7cedc48a908c1a5` |
| `feature_contract.json` | `869c7f22ce63988cdd3df2b597ce5cb646dca3402f05d35d8a18652c16615863` |
| `metadata.json` | `c519f7aaa3b689b3be84c1d8febd27676ac6b600dfef0f80467d995ef6cd2434` |
| `golden_predictions.csv` | `ae56e6fdd558df1bf24914f68d459dd394bae9f30adb36dc8835b6a145d2ed37` |
| `decision_threshold.json` | `d1360b1be9d89a8e8a9e534a05f4be44e56940801dca1f379ba4178d498de4d9` |

Loader tự kiểm tra các hash này mỗi lần process API nạp model. Vì vậy health check xanh nhưng `/ready` báo model chưa cấu hình là dấu hiệu cần xem ngay log `inference-api`, không được bỏ qua.

## Dịch vụ và cổng mặc định

| Dịch vụ | URL | Tài khoản local |
|---|---|---|
| Inference API | <http://localhost:8000/docs> | Không có |
| Airflow | <http://localhost:8080> | `admin/admin` |
| Grafana | <http://localhost:3000> | `admin/admin` |
| MLflow | <http://localhost:5001> | Không có |
| Kafka UI | <http://localhost:8082> | Không có |
| MinIO Console | <http://localhost:9001> | `minioadmin/minioadmin123` |
| Prometheus | <http://localhost:9090> | Không có |
| RedisInsight | <http://localhost:5540> | Không có |
| Redis (host client) | `127.0.0.1:6379` | DB `0`, không password |

## Lệnh vận hành

```bash
./scripts/stack.sh doctor
./scripts/stack.sh status
./scripts/stack.sh health
./scripts/stack.sh logs inference-api
./scripts/stack.sh up
./scripts/stack.sh up-all
./scripts/stack.sh down              # dừng, giữ volume
./scripts/stack.sh reset             # xóa toàn bộ volume và log
```

Trên Windows dùng các lệnh tương ứng trong `scripts/stack.ps1`.

## Giới hạn tái lập

Repo bảo đảm **tái lập kết quả suy luận** bằng commit mã nguồn, model hash, feature order, threshold và sentinel. Đây chưa phải build byte-for-byte hoàn toàn vì một số dependency Python dùng khoảng phiên bản và một số image dịch vụ được tham chiếu bằng tag. Để phát hành production nghiêm ngặt hơn cần:

1. Tạo release tag/commit chứa toàn bộ thay đổi.
2. Tạo dependency lock file với hash.
3. Pin mọi Docker image theo digest.
4. Lưu `docker compose config`, image digest và kiến trúc CPU cùng release.
5. Dùng snapshot dữ liệu đầu vào bất biến thay vì dữ liệu phát sinh theo thời gian.

Không nên tuyên bố hai lần chạy “y hệt” chỉ vì container đều healthy. Chỉ coi là đạt khi hash artifact khớp, sentinel khớp và cùng input cho cùng uplift score.

## Khắc phục lỗi nhanh

- `git lfs pull` chưa chạy: CSV trong `data/` chỉ là pointer và DAG bootstrap fail.
- `/ready` chưa ready: chưa có active feature version hoặc model không hợp lệ.
- DAG 60 xanh nhưng backfill không chạy: mở đúng run mới; mặc định hiện tại là
  `mode=backfill`, `land=true`. Ở mode backfill chỉ `contract_dry_run` được skip.
- DAG 20 báo thiếu `raw/events_v2`: DAG 60 chưa land thành công vào MinIO;
  kiểm tra `backfill_train_split` và `assert_sources_ready` của cùng một run.
- `model_not_configured`: kiểm tra mount `./models:/opt/project/models:ro` và hash.
- RedisInsight trong Docker: Host phải là `redis`, không phải `127.0.0.1`.
- Port đã dùng: đổi port tương ứng trong `.env`, không sửa port nội bộ container.
- macOS test LightGBM fail vì `libomp.dylib`: chạy `brew install libomp` hoặc test model trong container.
- Muốn dựng sạch hoàn toàn: sao lưu dữ liệu cần giữ rồi chạy `docker compose --profile all down -v --remove-orphans`.

## Bảo mật

Credential trong `.env.example` chỉ phục vụ local development. Khi triển khai thật phải thay mật khẩu PostgreSQL/MinIO/Grafana/Airflow, sinh Fernet key mới, đặt reverse proxy/TLS, giới hạn network và quản lý secret ngoài Git.
