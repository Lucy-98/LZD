# LZD Uplift Feature Platform

Nền tảng dữ liệu và phục vụ quyết định voucher bằng uplift modeling. Repo tích hợp Lakehouse (MinIO, DuckDB, dbt), Feature Store online (Redis), Airflow, Kafka, FastAPI, MLflow và Prometheus/Grafana/Loki.

Model production đi kèm repo là **DRLearner–LightGBM, đúng 30 feature**. API chỉ phát voucher khi uplift score lớn hơn hoặc bằng:

```text
UPLIFT_DECISION_THRESHOLD=0.0070347543
```

## Trạng thái đã kiểm chứng

- Model nguồn: `https://github.com/hatuki0604/final-lazada-2.git`
- Commit model nguồn: `575fb1e0ea78ba0c9cb11876aac5984125308698`
- Model version khi phục vụ: `20260820-575fb1e0ea78`
- Feature contract: `fs_2026_08_v4`, đúng 30 feature và đúng thứ tự
- Golden sentinel score: `0.004436201035688247`
- Sai số tuyệt đối cho sentinel: `1e-12`
- Test repo: 327 passed; 2 test LightGBM có thể bị skip trên macOS thiếu `libomp`
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
              training_features (30F)     serving_features
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

## Yêu cầu máy

- Git 2.x và Git LFS
- Docker Desktop hoặc Docker Engine với Compose v2
- Python 3.11 hoặc 3.12 nếu muốn chạy test ngoài Docker
- Docker được cấp ít nhất 4 CPU, 10 GB RAM và khoảng 25 GB ổ đĩa trống
- macOS, Linux hoặc Windows chạy Docker Desktop/WSL2

Kiểm tra nhanh:

```bash
git --version
git lfs version
docker version
docker compose version
```

## Dựng lại trên máy mới

### 1. Clone đúng phiên bản mã nguồn

Để hai máy cho cùng kết quả, không chạy trực tiếp từ một branch đang thay đổi. Dùng cùng commit hoặc release tag trên cả hai máy:

```bash
git lfs install
git clone https://github.com/Lucy-98/LZD.git
cd LZD
git checkout <RELEASE_TAG_HOAC_COMMIT>
git lfs pull
git status --short
git rev-parse HEAD
```

`git status --short` phải rỗng. Lưu kết quả `git rev-parse HEAD` cùng báo cáo chạy để có thể truy nguyên chính xác phiên bản. Hai CSV lớn trong `data/` dùng Git LFS; nếu chỉ thấy file pointer vài trăm byte thì `git lfs pull` chưa hoàn tất.

### 2. Tạo cấu hình

Compose có giá trị mặc định đủ để chạy local. Tạo `.env` nếu cần cấu hình rõ ràng:

```bash
cp .env.example .env
```

Giữ nguyên dòng sau nếu muốn tái lập quyết định hiện tại:

```dotenv
UPLIFT_DECISION_THRESHOLD=0.0070347543
```

Không sửa model bundle hoặc `feature_contract.json`. Không dùng credential mặc định trong môi trường public/production.

### 3. Xóa trạng thái cũ và build sạch

Lệnh dưới đây xóa database, Redis, Kafka, MinIO và các volume của project hiện tại. Chỉ dùng khi cần một lần dựng hoàn toàn mới:

```bash
docker compose --profile all down -v --remove-orphans
docker compose --profile all build --pull
docker compose --profile all up -d
```

Không cần realtime simulator thì dùng `docker compose up -d`.

Chờ các service khởi động rồi kiểm tra:

```bash
docker compose ps
curl --fail http://localhost:8000/health
```

`/health` chỉ cho biết process còn sống. `/ready` chỉ trả về `ready` sau khi có một feature version thật đã được đồng bộ và kích hoạt trong Redis.

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

1. `00_bootstrap_lake` – tạo cấu trúc lake và nạp dữ liệu nguồn.
2. `60_reconstruction_e2e` – chọn `mode=backfill`, `land=true`.
3. `20_build_features_dbt` – build và test Bronze/Silver/Gold.
4. `40_sync_features_to_redis` – validate, atomic activate feature version.
5. `50_data_quality` – chạy kiểm tra chất lượng sau đồng bộ.

Lần kiểm tra nhanh nên đặt `row_limit=5000`. Muốn xử lý toàn bộ train split đặt `row_limit=0`; quá trình này có thể sinh khoảng 17,9 triệu event và cần nhiều thời gian/dung lượng hơn. Không chạy DAG 20 hoặc DAG 40 trước khi Track A backfill và land thành công.

Sau khi DAG 40 thành công:

```bash
curl --fail http://localhost:8000/ready
```

Response sẽ chứa feature version chính thức dạng `vYYYYMMDD`, không cần seed `vmodel30-smoke`.

### 6. Gọi API quyết định

Lấy một `user_id` đã đồng bộ từ DAG 40 rồi gọi:

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
- `decision: "VOUCHER"` khi score `>= threshold`, ngược lại `NO_VOUCHER`

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
- `model_not_configured`: kiểm tra mount `./models:/opt/project/models:ro` và hash.
- Port đã dùng: đổi port tương ứng trong `.env`, không sửa port nội bộ container.
- macOS test LightGBM fail vì `libomp.dylib`: chạy `brew install libomp` hoặc test model trong container.
- Muốn dựng sạch hoàn toàn: sao lưu dữ liệu cần giữ rồi chạy `docker compose --profile all down -v --remove-orphans`.

## Bảo mật

Credential trong `.env.example` chỉ phục vụ local development. Khi triển khai thật phải thay mật khẩu PostgreSQL/MinIO/Grafana/Airflow, sinh Fernet key mới, đặt reverse proxy/TLS, giới hạn network và quản lý secret ngoài Git.
