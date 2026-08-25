# LZD Uplift Feature Platform

Nền tảng dữ liệu phục vụ quyết định phân bổ voucher bằng uplift modeling. Hệ
thống kết hợp batch feature, tín hiệu hành vi gần realtime và mô hình uplift để
quyết định có nên gửi voucher cho từng khách hàng hay không.

## 1. Bài toán

### Mục tiêu nghiệp vụ

Dự đoán khả năng mua hàng là chưa đủ để tối ưu ngân sách khuyến mãi. Hệ thống
cần ước lượng **tác động tăng thêm của voucher** đối với từng khách hàng:

- `SEND_VOUCHER`: khách hàng có khả năng chuyển đổi nhờ voucher;
- `NO_VOUCHER`: khách hàng vẫn mua mà không cần ưu đãi hoặc voucher không tạo
  thêm tác động đáng kể.

Quyết định sai có thể gây lãng phí ngân sách, giảm biên lợi nhuận hoặc bỏ lỡ
khách hàng có thể được kích hoạt đúng thời điểm.

### Bài toán dữ liệu

Để mô hình uplift có thể dùng trong sản phẩm, nền tảng phải bảo đảm:

- tái dựng feature lịch sử từ dữ liệu nguồn đã ẩn danh;
- tạo training dataset theo point-in-time để hạn chế leakage;
- dùng cùng thứ tự và định nghĩa feature khi training và serving;
- kết hợp batch profile với hành vi gần realtime mà không đếm trùng event;
- đồng bộ feature vào Redis theo version để có thể kích hoạt hoặc rollback;
- chỉ phục vụ khi feature version và model contract tương thích;
- theo dõi chất lượng dữ liệu, freshness, skew và độ trễ suy luận.

Đầu ra của hệ thống là một quyết định cho từng `user_id`, gồm uplift score,
ngưỡng quyết định, model version và kết quả gửi hoặc không gửi voucher.

## 2. Cách xây dựng hệ thống

### Luồng dữ liệu

```text
full_trainset.csv
      │
      ▼
Track A reconstruction ──► PostgreSQL / DuckDB / MinIO
                                      │
                                      ▼
                          dbt Bronze → Silver → Gold
                                      │
                        ┌─────────────┴─────────────┐
                        ▼                           ▼
              training_features           serving_features
                                                    │
                                                    ▼
                                          Redis Feature Store
                                                    │
Kafka events ──► stream consumer ──► realtime overlay
                                                    │
                                                    ▼
                                         FastAPI + uplift model
                                                    │
                                                    ▼
                                       PostgreSQL inference audit
```

### Thành phần chính

| Thành phần | Vai trò |
|---|---|
| MinIO | Lưu dữ liệu lakehouse và raw event bất biến |
| DuckDB + dbt | Biến đổi Bronze/Silver/Gold và tạo feature |
| Kafka | Nhận tín hiệu hành vi gần realtime |
| Redis | Phục vụ batch feature theo version và realtime overlay |
| Airflow | Điều phối reconstruction, dbt, training, sync và quality gate |
| MLflow | Quản lý model artifact và version |
| FastAPI | Nạp feature, chạy uplift model và trả quyết định voucher |
| PostgreSQL | Lưu dữ liệu nghiệp vụ và inference audit |
| Prometheus/Grafana/Loki | Thu thập metric, dashboard và log |

### Các bước xây dựng pipeline

1. Nạp dữ liệu nguồn vào lakehouse.
2. Tái dựng event và trạng thái nghiệp vụ từ tập dữ liệu đã ẩn danh.
3. Dùng dbt để chuẩn hóa dữ liệu và tạo training/serving feature.
4. Train hoặc nạp model uplift với ordered feature contract.
5. Đồng bộ batch feature vào một Redis version mới, kiểm tra rồi mới kích hoạt.
6. Nhận event Kafka và cập nhật realtime overlay theo `event_id` và event time.
7. FastAPI ghép batch/realtime feature, kiểm tra contract và trả quyết định.
8. Ghi audit, metric và log để theo dõi toàn bộ pipeline.

## 3. Cài đặt

### Yêu cầu

- Git 2.x và Git LFS;
- Docker Desktop hoặc Docker Engine với Docker Compose v2;
- tối thiểu 4 CPU, 12 GB RAM và 25 GB ổ đĩa trống;
- Python 3.11 hoặc 3.12 nếu chạy test ngoài Docker.

Kiểm tra môi trường:

```bash
git --version
git lfs version
docker version
docker compose version
```

### Tải mã nguồn và dữ liệu

```bash
git lfs install
git clone https://github.com/Lucy-98/LZD.git
cd LZD
git checkout <COMMIT_HOAC_TAG_CAN_CHAY>
git lfs pull
git status --short
```

`git status --short` nên rỗng. Nếu các CSV trong `data/` chỉ chứa vài dòng Git
LFS pointer, chạy lại `git lfs pull`.

### Tạo cấu hình

```bash
cp .env.example .env
docker compose --profile all config --quiet
```

Các giá trị trong `.env.example` dùng cho môi trường local. Trên Linux native,
đổi `AIRFLOW_UID` trong `.env` thành kết quả của lệnh `id -u`. Trên macOS và
Windows Docker Desktop có thể giữ `AIRFLOW_UID=50000`.

### Build

macOS/Linux:

```bash
chmod +x scripts/stack.sh
./scripts/stack.sh doctor
./scripts/stack.sh init
```

Windows PowerShell:

```powershell
.\scripts\stack.ps1 doctor
.\scripts\stack.ps1 init
```

Không dùng script thì chạy:

```bash
docker compose --profile all build
```

## 4. Chạy lại hệ thống

### Khởi động

Khởi động batch platform, Airflow, API và observability:

```bash
./scripts/stack.sh up
```

Windows PowerShell:

```powershell
.\scripts\stack.ps1 up
```

Muốn chạy thêm event producer và stream consumer:

```bash
./scripts/stack.sh up-all
```

Kiểm tra container và health endpoint:

```bash
docker compose --profile all ps
./scripts/stack.sh health
curl --fail http://localhost:8000/health
```

### Chạy pipeline dữ liệu

Mở Airflow tại <http://localhost:8080>, đăng nhập bằng `admin/admin` và chạy các
DAG theo thứ tự:

1. `00_bootstrap_lake` — tạo lake và nạp dữ liệu nguồn;
2. `60_reconstruction_e2e` — tái dựng dữ liệu nghiệp vụ và event;
3. `20_build_features_dbt` — chạy dbt và tạo training/serving feature;
4. `40_sync_features_to_redis` — tạo và kích hoạt Redis feature version;
5. `50_data_quality` — kiểm tra chất lượng sau đồng bộ.

Để chạy thử nhanh, dùng `dt=2026-08-05`, `row_limit=5000` cho DAG 00 và DAG 60.
Để xử lý toàn bộ train split, đặt `row_limit=0`; lượt chạy này cần nhiều RAM,
thời gian và dung lượng hơn đáng kể.

Sau khi DAG 40 thành công, kiểm tra feature store và trạng thái sẵn sàng:

```bash
docker compose exec -T redis redis-cli PING
docker compose exec -T redis redis-cli GET fs:meta:active_version
curl --fail http://localhost:8000/ready
```

### Gọi API quyết định

Lấy một `user_id` đã đồng bộ từ log/XCom của task `smoke_test_serving` trong
DAG 40, sau đó gọi:

```bash
curl --fail --request POST http://localhost:8000/decide \
  --header 'content-type: application/json' \
  --data '{"user_id":"<USER_ID_DA_SYNC>","context":{}}'
```

API trả về uplift score và một trong hai quyết định `SEND_VOUCHER` hoặc
`NO_VOUCHER`.

### Chạy test

Trong Docker:

```bash
./scripts/stack.sh test
```

Ngoài Docker:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e '.[dev,ml]'
python -m pytest -q
```

### Dừng hoặc chạy lại từ đầu

Dừng hệ thống nhưng giữ dữ liệu:

```bash
./scripts/stack.sh down
```

Xóa toàn bộ volume và dựng lại từ đầu:

```bash
./scripts/stack.sh reset
./scripts/stack.sh up
```

`reset` xóa dữ liệu local trong PostgreSQL, Redis, Kafka, MinIO, DuckDB và các
volume Docker của project.
