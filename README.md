# 🛍️ LZD Uplift Feature Platform & Event Reconstruction Engine

> **Nền tảng Dữ liệu & Học máy Nhân quả (Causal ML) Toàn diện cho Bài toán Lazada Voucher Uplift:**
> Tích hợp Kiến trúc Lakehouse Hiện đại (MinIO S3 + DuckDB OLAP + dbt 3 Tầng Medallion), Feature Store Thời gian thực (Redis + In-Memory Caching), Điều phối Tự động hóa (Apache Airflow 2.10), Đăng ký & Quản lý Mô hình (MLflow), Phục vụ Suy luận Độ trễ Thấp (FastAPI < 5ms), Động cơ Tái tạo Sự kiện Ngược (Track A Event Reconstruction Solver), cùng Hệ thống Giám sát Toàn diện (Prometheus + Grafana + Loki).

---

## 📑 Mục Lục

1. [Tổng quan Dự án & Kiến trúc 2 Luồng Dữ liệu](#1-tổng-quan-dự-án--kiến-trúc-2-luồng-dữ-liệu)
2. [Ý Nghĩa Toàn Bộ Input & Output của Dự Án](#2-ý-nghĩa-toàn-bộ-input--output-của-dự-án)
3. [Danh Sách Các Công Cụ Trong Stack & Đường Dẫn Truy Cập](#3-danh-sách-các-công-cụ-trong-stack--đường-dẫn-truy-cập)
4. [Hướng Dẫn Cài Đặt Cho macOS & Linux](#4-hướng-dẫn-cài-đặt-cho-macos--linux)
5. [Hướng Dẫn Cài Đặt Cho Windows (PowerShell & WSL2)](#5-hướng-dẫn-cài-đặt-cho-windows-powershell--wsl2)
6. [Cách Sử Dụng & Thao Tác Chi Tiết Từng Công Cụ](#6-cách-sử-dụng--thao-tác-chi-tiết-từng-công-cụ)
7. [Thứ Tự Vận Hành 8 Airflow DAGs Chuẩn Production](#7-thứ-tự-vận-hành-8-airflow-dags-chuẩn-production)
8. [Chạy Nhanh Local Không Cần Docker (Test Suite & Solver)](#8-chạy-nhanh-local-không-cần-docker-test-suite--solver)
9. [Xử Lý Sự Cố Thường Gặp (Troubleshooting)](#9-xử-lý-sự-cố-thường-gặp-troubleshooting)

---

## 1. Tổng quan Dự án & Kiến trúc 2 Luồng Dữ liệu

Dự án giải quyết bài toán **Tối ưu hóa Chi phí Khuyến mãi (Voucher Allocation)** thông qua **Mô hình Uplift (Causal Inference)**: Xác định chính xác nhóm khách hàng *Persuadables* (chỉ mua khi có voucher) thay vì lãng phí voucher cho nhóm *Sure Things* (không có voucher vẫn mua) hoặc nhóm *Lost Causes* / *Sleeping Dogs*.

Hệ thống được thiết kế với **2 luồng dữ liệu chuyên biệt**:

```
                                  ┌────────────────────────────────────────────────────────┐
                                  │                LZD UPLIFT ARCHITECTURE                 │
                                  └────────────────────────────────────────────────────────┘

    [ LUỒNG 1: EVENT RECONSTRUCTION & dbt 3-TIER MEDALLION ]

    data/full_trainset.csv ──► Track A Solver (DAG 60) ──► MinIO (raw/events_v2: Raw Events E*) + DuckDB (biz.*)
                                                                 │
                                                                 ▼ (DAG 20: dbt Medallion)
                                  🥉 TẦNG ĐỒNG: stg_events_v2, stg_app_events
                                                                 │
                                                                 ▼
                                  🥈 TẦNG BẠC: int_cfs_counter, int_cfs_recency, int_cfs_categorical,
                                               int_passthrough, int_event_behaviour
                                                                 │
                                  ┌──────────────────────────────┴──────────────────────────────┐
                                  ▼                                                             ▼
                   🥇 marts.training_features (30f: f0..f80)                      🥇 marts.serving_features (~41 features)
                                  │                                                             │ (Tên nghiệp vụ)
                                  ▼                                                             ▼ (DAG 40 sync 01:30 AM)
                   Jupyter Notebook / MLflow                                     Redis Feature Store (fs:{version}:u:{id})
                                  │                                                             │
                                  └──────────────────────────────┬──────────────────────────────┘
                                                                 ▼
                                                    FastAPI POST /decide (< 5ms)
                                                    Hybrid Trigger: voucher_30, voucher_15, no_voucher


    [ LUỒNG 2: NEAR-REALTIME STREAMING & IN-SESSION INTENT ]

    Hành vi User sau t₀ ──► Kafka (app.user.events.v1) ──► Stream Consumer
                                                                 │
                                ┌────────────────────────────────┴────────────────────────────────┐
                                ▼                                                                 ▼
                 MinIO Lake (raw/app_events/)                                      Redis Realtime: rt:u:{user_id}
                 (Immutable Source of Truth)                                       (TTL 48h, sliding window 5m + 1h)
```

---

## 2. Ý Nghĩa Toàn Bộ Input & Output của Dự Án

### 📥 2.1. Danh Sách Input (Dữ Liệu & Cấu Hình Đầu Vào)

| Tên Input | Định Dạng / Vị Trí | Ý Nghĩa Nghiệp Vụ & Kỹ Thuật | Nơi Sử Dụng |
|---|---|---|---|
| **`full_trainset.csv`** | CSV (`data/full_trainset.csv`) | Tập dữ liệu lịch sử của **926,669 người dùng**, chứa đặc trưng hành vi, cờ can thiệp voucher (`is_treat`), và nhãn chuyển đổi đơn hàng (`conversion`). | Track A Solver (`DAG 60`), `DAG 00_bootstrap_lake` |
| **`full_testset.csv`** | CSV (`data/full_testset.csv`) | Tập dữ liệu kiểm định độc lập (**181,669 người dùng**), dùng làm RCT Holdout chuẩn để đánh giá khách quan các mô hình Uplift. | dbt `eval_holdout.sql`, MLflow benchmark |
| **`AppEvent Stream`** | JSON Stream qua Kafka topic `app.user.events.v1` | Chuỗi sự kiện hành vi realtime phát sinh từ ứng dụng Web/Mobile: `page_view`, `add_to_cart`, `search`, `checkout`, `order`, v.v. | `event-producer`, `stream-consumer`, `DAG 10_ingest_stream_to_lake` |
| **`fs_2026_08_v3.yaml`** | YAML (`config/features/fs_2026_08_v3.yaml`) | **Hợp đồng Scope 30 Features** phân theo 3 tier: T1 (4 event-level), T2 (5 categorical segments), T3 (21 latent scores). | Track A Solver, dbt models |
| **`feature_spec.yml`** | YAML (`config/features/feature_spec.yml`) | **Hợp đồng Feature Store (Data Contract)** định nghĩa danh sách features tên nghiệp vụ sạch, kiểu dữ liệu, ngưỡng chặn Data Quality (min, max, null rate), và TTL Redis **48 giờ**. | dbt models, Feature Sync (DAG 40), API Serving |
| **`uplift_training.yml`** | YAML (`config/training/uplift_training.yml`) | Cấu hình huấn luyện mô hình: siêu tham số LightGBM, thuật toán DR-Learner / T-Learner / X-Learner, tỷ lệ train/val, và tiêu chuẩn qua cổng promote (`qini > 0.015`, `uplift@10 > 0.01`). | Jupyter Notebook, `DAG 30_train_uplift_model` |

---

### 📤 2.2. Danh Sách Output (Kết Quả & Sản Phẩm Đầu Ra)

| Tên Output | Định Dạng / Nơi Lưu Trữ | Ý Nghĩa Nghiệp Vụ & Kỹ Thuật | Ai / Hệ Thống Nào Sử Dụng |
|---|---|---|---|
| **Raw Events Lake** | Parquet trên MinIO (`s3://lakehouse/raw/events_v2/`) | Chuỗi Raw Events ($E^*$) do Track A Solver tái tạo từ dataset, làm nguồn dữ liệu thô cho toàn bộ luồng dbt. | dbt `stg_events_v2`, DuckDB Engine |
| **Training Features Mart** | Bảng DuckDB (`marts.training_features`) | Chứa đúng **30 cột `f*`** được tính toán từ events và profile. | Jupyter Notebook đọc để huấn luyện mô hình offline |
| **Serving Features Mart** | Bảng DuckDB (`marts.serving_features`) | Chứa **~41 cột tên nghiệp vụ** (`customer_value_score`, `order_cnt_7d`, `browse_intensity_365d`...). | Airflow Sync (`DAG 40`) đồng bộ lên Redis |
| **Online Feature Store Keys** | Hash Map trong Redis (`localhost:6379`) | <ul><li>`fs:{version}:u:{user_id}`: Hash chứa ~41 features tên nghiệp vụ phục vụ đọc < 1ms.</li><li>`rt:u:{user_id}`: Realtime sliding window counters (5m + 1h), TTL 48h.</li><li>`fs:meta:active_version`: Con trỏ phiên bản active (Atomic Swap).</li></ul> | FastAPI Inference API (`/decide`, `/features/{user_id}`, `/trigger/*`) |
| **Trained Uplift Model** | Docker Image & MLflow Registry (`http://localhost:5001`) | Best model được xuất artifact từ Notebook và đóng gói vào Docker Image của serving service. | FastAPI Inference API (`ModelLoader`) |
| **Quyết Định Phân Phối Voucher** | JSON Response từ FastAPI (`POST /decide`) | Kết quả tính toán: `decision` (`SEND_VOUCHER` / `NO_VOUCHER`), `voucher_code` (`voucher_30`, `voucher_15`, `no_voucher`), `uplift_score`, `latency_ms`. | Lazada App Checkout / Giỏ hàng / Notification |
| **Giám Sát & Cảnh Báo Hệ Thống** | Prometheus Metrics & Grafana Dashboards | Theo dõi trực quan: Tốc độ ghi Redis Shard, Tỷ lệ lỗi task, Độ trễ suy luận P95/P99, Data Drift, Null Rate, và CPU/RAM container. | Đội ngũ Vận hành SRE / Data Platform |

---

## 3. Danh Sách Các Công Cụ Trong Stack & Đường Dẫn Truy Cập

| Công Cụ | Biểu Tượng / Vai Trò | Địa Chỉ Truy Cập (URL) | Tài Khoản Mặc Định | Chức Năng Chính |
|---|---|---|---|---|
| **Apache Airflow** | ⏱️ Điều phối Pipeline | [http://localhost:8080](http://localhost:8080) | `admin` / `admin` | Lập lịch, giám sát và quản lý 8 DAGs xử lý dữ liệu tự động. |
| **FastAPI Inference** | ⚡ Serving Suy Luận | [http://localhost:8000/docs](http://localhost:8000/docs) | *Không yêu cầu* | Swagger UI cung cấp API ra quyết định tặng voucher độ trễ < 5ms. |
| **MLflow Registry** | 🧪 Quản lý Experiment & Model | [http://localhost:5001](http://localhost:5001) | *Không yêu cầu* | Theo dõi tham số huấn luyện, so sánh mô hình và quản lý Model Registry. |
| **MinIO Storage** | 🪣 S3 Object Lakehouse | [http://localhost:9001](http://localhost:9001) | `minioadmin` / `minioadmin123` | Giao diện quản trị Data Lake, xem các file Parquet raw/staging. |
| **RedisInsight** | 🔴 Quản trị Redis UI | [http://localhost:5540](http://localhost:5540) | *Không yêu cầu* | Duyệt key `fs:*`, xem chi tiết Hash features tên nghiệp vụ của từng `user_id`. |
| **Kafka UI** | 📬 Giám sát Message Bus | [http://localhost:8082](http://localhost:8082) | *Không yêu cầu* | Xem message streaming realtime, topics, partitions và consumer lag. |
| **Grafana** | 📊 Dashboards Trực quan | [http://localhost:3000](http://localhost:3000) | `admin` / `admin` | Xem các bảng điều khiển Feature Sync, Serving Performance, Data Drift. |
| **Prometheus** | 📈 Thu thập Metrics | [http://localhost:9090](http://localhost:9090) | *Không yêu cầu* | Truy vấn PromQL và kiểm tra trạng thái targets/alert rules. |
| **DuckDB OLAP** | 🦆 Embedded Lakehouse Engine | `/opt/lakehouse/warehouse.duckdb` | CLI / Read-only | Kho dữ liệu nhúng hiệu năng cao, thực thi các truy vấn dbt transform. |
| **dbt-duckdb** | 🏗️ Biến đổi Dữ liệu | `dbt/` (Model + Macros) | CLI | Quản lý data lineage 3 tầng (Đồng $\rightarrow$ Bạc $\rightarrow$ Vàng) và schema tests. |

---

## 4. Hướng Dẫn Cài Đặt Cho macOS & Linux

### 4.1. Yêu cầu hệ thống
- **Hệ điều hành**: macOS (Apple Silicon M1/M2/M3/M4 hoặc Intel) hoặc Linux (Ubuntu 20.04+, Debian, Fedora).
- **Phần mềm**: Docker Desktop (hoặc Docker Engine + Compose v2), Python 3.11+, Git, Make.
- **Tài nguyên Docker khuyên nghị**: CPU: 4–6 cores, RAM: tối thiểu 8 GB (khuyên dùng 12 GB), Disk: trống tối thiểu 15 GB.

### 4.2. Các bước cài đặt từng bước

```bash
# Bước 1: Clone kho mã nguồn về máy
git clone <REPO_URL> LZD
cd LZD

# Bước 2: Tạo môi trường ảo Python 3.11+ và kích hoạt
python3 -m venv .venv
source .venv/bin/activate

# Bước 3: Nâng cấp pip và cài đặt dependencies cho phát triển & testing
pip install --upgrade pip
pip install -r requirements-dev.txt
pip install -e .

# Bước 4: Tạo file cấu hình môi trường từ mẫu
cp .env.example .env

# Bước 5: Chạy chẩn đoán môi trường trước khi khởi động
./scripts/stack.sh doctor

# Bước 6: Khởi động toàn bộ nền tảng (Full Stack kèm Observability & Stream)
make up-all
```

Kiểm tra trạng thái sau khi khởi động:
```bash
make status
make health
```

---

## 5. Hướng Dẫn Cài Đặt Cho Windows (PowerShell & WSL2)

### 5.1. Phương Án 1: Dùng Windows PowerShell Thuần (Native)

Mở **PowerShell** (Run as Administrator):

```powershell
# Bước 1: Cho phép PowerShell thực thi script
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass

# Bước 2: Di chuyển vào thư mục dự án
cd LZD

# Bước 3: Tạo môi trường ảo Python và kích hoạt
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Bước 4: Cài đặt dependencies
pip install --upgrade pip
pip install -r requirements-dev.txt
pip install -e .

# Bước 5: Tạo file .env từ file mẫu
Copy-Item .env.example .env

# Bước 6: Chạy kiểm tra Docker và kéo images
.\scripts\stack.ps1 doctor

# Bước 7: Khởi động toàn bộ stack
.\scripts\stack.ps1 up-all
```

Kiểm tra trạng thái hệ thống:
```powershell
.\scripts\stack.ps1 status
.\scripts\stack.ps1 health
```

---

## 6. Cách Sử Dụng & Thao Tác Chi Tiết Từng Công Cụ

### 6.1. FastAPI Inference API & Hybrid Trigger
- **Truy cập tài liệu Swagger UI**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **Kiểm tra thông tin Feature Store**:
  ```bash
  curl http://localhost:8000/store/info
  ```
- **Gửi request quyết định cấp voucher cho 1 User**:
  ```bash
  curl -X POST http://localhost:8000/decide \
       -H "Content-Type: application/json" \
       -d '{"user_id": "U0000001", "context": {"rt_page_view_5m": 3}}'
  ```
  *Kết quả trả về:*
  ```json
  {
    "user_id": "U0000001",
    "decision": "SEND_VOUCHER",
    "voucher_code": "voucher_30",
    "uplift_score": 0.0542,
    "threshold": 0.02,
    "feature_version": "v20260805",
    "model_version": "DRLearner-LightGBM",
    "cache_hit": true,
    "latency_ms": 2.1
  }
  ```
- **Đánh giá phiên người dùng qua Hybrid Trigger**:
  ```bash
  curl -X POST http://localhost:8000/trigger/evaluate-session \
       -H "Content-Type: application/json" \
       -d '{"user_id": "U0000001"}'
  ```

---

### 6.2. DuckDB & dbt (Kho Dữ Liệu & Biến Đổi 3 Tầng)
Kho dữ liệu lưu trữ tại `/opt/lakehouse/warehouse.duckdb`.

#### Truy vấn SQL nhanh trong DuckDB:
```bash
# Xem 30 features f* trong training mart
docker compose exec airflow-scheduler python -c "from lzd_pipeline.common.clients import duckdb_conn; con=duckdb_conn().__enter__(); print(con.execute('SELECT user_id, f0, f1, f5, f18, f80 FROM marts.training_features LIMIT 5').fetchdf())"

# Xem features tên nghiệp vụ trong serving mart
docker compose exec airflow-scheduler python -c "from lzd_pipeline.common.clients import duckdb_conn; con=duckdb_conn().__enter__(); print(con.execute('SELECT user_id, customer_value_score, order_cnt_7d, browse_intensity_365d FROM marts.serving_features LIMIT 5').fetchdf())"
```

#### Chạy dbt:
```bash
# Chạy staging, intermediate và marts
docker compose exec airflow-scheduler bash -c "cd /opt/project/dbt && dbt run && dbt test"
```

---

### 6.3. Redis Feature Store
```bash
# Xem phiên bản feature đang active
docker compose exec redis redis-cli get fs:meta:active_version

# Lấy features tên nghiệp vụ của user
docker compose exec redis redis-cli hgetall fs:v20260805:u:U0000001
```

---

## 7. Thứ Tự Vận Hành 8 Airflow DAGs Chuẩn Production

```
[DAG 60_reconstruction_e2e] ──► [DAG 20_build_features_dbt] ──► [DAG 40_sync_features_to_redis] ──► [DAG 50_data_quality]
                                              │
                                              ▼
                                   [DAG 30_train_uplift_model]
```

### Chi tiết nhiệm vụ từng DAG:
1. **`60_reconstruction_e2e`** (Track A Reconstruction):
   - Đọc dataset CSV, giải mã ngược ra chuỗi Raw Events ($E^*$) đẩy vào MinIO `raw/events_v2/` và nạp bảng điều khiển `biz.*`.
2. **`20_build_features_dbt`** (dbt 3 Tầng Medallion):
   - Chạy các model staging $\rightarrow$ intermediate $\rightarrow$ marts trên DuckDB, xây dựng bảng `training_features` (30f) và `serving_features` (tên nghiệp vụ).
3. **`40_sync_features_to_redis`** (Đồng bộ Online Feature Store - 01:30 AM):
   - Đọc bảng `marts.serving_features` từ DuckDB, chia đều **32 shards** nạp song song vào Redis (`HSET`), kiểm tra toàn vẹn và thực hiện **Atomic Swap** con trỏ `fs:meta:active_version`.
4. **`30_train_uplift_model`** (Khung Huấn Luyện MLOps):
   - Khung huấn luyện DR-Learner đọc `marts.training_dataset`, đánh giá chất lượng và đăng ký mô hình vào MLflow Registry.
5. **`50_data_quality`** (Kiểm Tra Chất Lượng Dữ Liệu):
   - Đo lường freshness, tỷ lệ null, độ lệch phân phối và cảnh báo qua Prometheus.
6. **`10_ingest_stream_to_lake`** (Micro-batch Streaming):
   - Định kỳ nạp sự kiện realtime từ Kafka vào MinIO.
7. **`00_bootstrap_lake`** (Khởi tạo Lakehouse):
   - Nạp ban đầu các bảng nền tảng.
8. **`99_ops_toolbox`** (Công Cụ Bảo Trì):
   - Hỗ trợ xóa cache Redis, chạy lại shard lỗi, reload service.

---

## 8. Chạy Nhanh Local Không Cần Docker (Test Suite & Solver)

Bạn có thể chạy toàn bộ unit tests và contract tests ngay trên máy tính:

```bash
# 1. Kích hoạt môi trường ảo
source .venv/bin/activate       # Trên macOS/Linux
# .\.venv\Scripts\Activate.ps1   # Trên Windows PowerShell

# 2. Chạy toàn bộ 329 Unit Tests
pytest tests/ -v
```

---

## 9. Xử Lý Sự Cố Thường Gặp (Troubleshooting)

### 9.1. Lỗi Docker daemon không chạy
- **Hiện tượng**: `Cannot connect to the Docker daemon at unix:///...`
- **Cách xử lý**: Mở ứng dụng **Docker Desktop** và đợi biểu tượng chuyển sang màu xanh lá cây.

### 9.2. Trùng Cổng (Port Conflict)
- **Hiện tượng**: Lỗi `bind: address already in use` khi khởi động container.
- **Cách xử lý**: Mở file `.env` và đổi port tương ứng (ví dụ: `POSTGRES_PORT=5433`, `REDIS_PORT=6380`).

### 9.3. Dừng Hệ Thống & Reset Dữ Liệu Sạch
```bash
# Dừng tất cả container:
make down

# Xóa sạch toàn bộ container và dữ liệu volumes để bắt đầu lại:
make reset
```

---

## 📚 Tài Liệu Kỹ Thuật Bổ Trợ

- 📊 [docs/DATA_FLOW.md](file:///Users/user/Intern/LZD/docs/DATA_FLOW.md): Sơ đồ luồng dữ liệu chi tiết giữa các tầng.
- 🔍 [docs/CHI_TIET_AIRFLOW_DAGS_VA_DBT.md](file:///Users/user/Intern/LZD/docs/CHI_TIET_AIRFLOW_DAGS_VA_DBT.md): Chi tiết cấu trúc 8 Airflow DAGs và dbt 3 tầng.
- 📖 [docs/HUONG_DAN_CHAY_END_TO_END.md](file:///Users/user/Intern/LZD/docs/HUONG_DAN_CHAY_END_TO_END.md): Hướng dẫn vận hành end-to-end.
- 📐 [docs/PIPELINE_ARCHITECTURE.md](file:///Users/user/Intern/LZD/docs/PIPELINE_ARCHITECTURE.md): Bản vẽ thiết kế kiến trúc kỹ thuật.
