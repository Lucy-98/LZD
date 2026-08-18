# Kịch bản Thuyết trình & Demo Thực chiến (End-to-End Presentation Script)

> **Dự án:** LZD Uplift Feature Platform & Reconstruction Pipeline  
> **Thời lượng gợi ý:** 10 – 15 phút (bao gồm 5 phút giải thích giải pháp + 8 phút Live Demo + 2 phút Q&A)

---

## 🎯 Cấu trúc Buổi Trình bày (Agenda)
1. **Phần 1: Đặt vấn đề & Bài toán Kinh doanh** *(1.5 phút)*
2. **Phần 2: Kiến trúc Hệ thống 5 Tầng** *(2 phút)*
3. **Phần 3: LIVE DEMO TỪNG BƯỚC THỰC TẾ** *(8 phút)*
   - **Demo 1:** Kiểm định Hợp đồng Toàn diện (`make test`)
   - **Demo 2:** Điều phối & Chuyển đổi Dữ liệu với Airflow + dbt
   - **Demo 3:** Khám phá Kho Đặc trưng Trực tuyến trên RedisInsight
   - **Demo 4:** Luồng Sự kiện Thời gian thực qua Kafka-UI
   - **Demo 5:** Gọi API Suy luận & Ra Quyết định Tặng Voucher (< 3ms)
   - **Demo 6:** Chạy Kịch bản Tích hợp Toàn diện (Track A & Track B)
   - **Demo 7:** Bảng Giám sát Vận hành Thời gian thực trên Grafana
4. **Phần 4: Tổng kết các Điểm sáng Công nghệ** *(1.5 phút)*

---

# BÀI NÓI VÀ THAO TÁC CHI TIẾT

---

## PHẦN 1: ĐẶT VẤN ĐỀ & BÀI TOÁN KINH DOANH (1.5 phút)

🗣️ **Lời thoại thuyết trình:**
> *"Kính chào anh/chị và các bạn. Trong các sàn TMĐT lớn như Lazada, việc phát voucher giảm giá tràn lan thường gây lãng phí ngân sách lớn mà không mang lại hiệu quả gia tăng (Incremental GMV). Có 4 nhóm khách hàng:*
> 1. **Persuadables (Cần thuyết phục):** Khách chỉ mua nếu có voucher $\rightarrow$ **Đây là nhóm duy nhất ta cần nhắm tới.**
> 2. **Sure Things (Chắc chắn mua):** Dù không có voucher họ vẫn mua $\rightarrow$ Tặng voucher là lãng phí.
> 3. **Lost Causes (Không bao giờ mua):** Tặng bao nhiêu cũng không mua.
> 4. **Sleeping Dogs (Khách phản cảm):** Tặng voucher spam thông báo có thể khiến họ khó chịu.
>
> *Để giải quyết bài toán này, dự án của chúng tôi xây dựng một nền tảng **Uplift Feature Platform & Machine Learning Serving** dựa trên bộ dữ liệu thực nghiệm 1.1 triệu người dùng từ Alibaba/Lazada, kết hợp xử lý Batch & Streaming để ra quyết định tặng voucher thời gian thực với độ trễ dưới 5ms."*

---

## PHẦN 2: KIẾN TRÚC HỆ THỐNG (2 phút)

🗣️ **Lời thoại thuyết trình:**
> *"Hệ thống được thiết kế theo kiến trúc Lambda hiện đại gồm 5 tầng khép kín:*
> - **1. Data Lakehouse (MinIO S3 + DuckDB):** Lưu trữ 1.1 triệu bản ghi snapshot và sự kiện stream.
> - **2. Data Transformation & DQ (dbt):** Chuẩn hóa dữ liệu, loại bỏ label để chống Data Leakage, xây dựng 55 đặc trưng chuẩn và kiểm định qua 33 bài test tự động.
> - **3. Online Feature Store (Redis):** Cơ chế chia 32 shards song song, hoán đổi phiên bản nguyên tử (Zero-downtime Atomic Swap) phục vụ 1.1 triệu người dùng.
> - **4. Realtime Streaming (Kafka + Stream Consumer):** Xử lý luồng hành vi thời gian thực và cập nhật bộ đếm sliding window 1h vào Redis.
> - **5. ML Serving & Observability (FastAPI + LightGBM + Prometheus + Grafana):** Mô hình Doubly Robust (DR-Learner) 350 cây đưa ra quyết định với SLA cực nhanh."*

---

## PHẦN 3: LIVE DEMO TỪNG BƯỚC (8 phút)

### 🔹 Demo 1: Kiểm định Hợp đồng Toàn diện (`make test`)
🗣️ **Lời thoại:** *"Đầu tiên, tôi sẽ chứng minh tính toàn vẹn của mã nguồn, các hợp đồng toán học và các kiểm định golden bit-for-bit qua bộ test tự động."*

💻 **Thao tác Terminal:**
```bash
make test
```
👉 **Điểm nhấn mạnh:** Toàn bộ **324 bài test PASS 100%** trong chưa đầy 4 giây, chứng minh hệ thống không hề có sai lệch toán học hay lỗi cú pháp.

---

### 🔹 Demo 2: Điều phối & Chuyển đổi Dữ liệu với Airflow + dbt
🗣️ **Lời thoại:** *"Tiếp theo là tầng điều phối dữ liệu Airflow. Chúng ta hãy quan sát các DAG xử lý batch."*

💻 **Thao tác Trình duyệt:**
- Mở: **[http://localhost:8080](http://localhost:8080)** (Tài khoản: `admin` / `admin`).
- Mở DAG **`20_build_features_dbt`** $\rightarrow$ Chọn tab **Graph**:
  - Chỉ cho người nghe thấy: `dbt_debug` $\rightarrow$ `dbt_run` $\rightarrow$ `assert_spec_contract` $\rightarrow$ `dbt_test` (33 tests pass) $\rightarrow$ `publish_dbt_results` $\rightarrow$ `profile_features`.
- Mở DAG **`40_sync_features_to_redis`** $\rightarrow$ Tab **Graph**:
  - Chỉ cho người nghe thấy: `prepare` $\rightarrow$ 32 `sync_shard` song song $\rightarrow$ `validate` (đối chiếu 1,000 users) $\rightarrow$ `activate` (đổi con trỏ) $\rightarrow$ `smoke_test_serving`.

👉 **Điểm nhấn mạnh:** Cơ chế **Idempotency** chống ghi đè lặp lại và kiểm tra hợp đồng trước khi swap version để đảm bảo **Zero-downtime**.

---

### 🔹 Demo 3: Khám phá Online Feature Store trên Redis
🗣️ **Lời thoại:** *"Dữ liệu sau khi sync từ DuckDB sẽ nằm tại Redis Online Store. Ta mở giao diện trực quan RedisInsight để kiểm tra."*

💻 **Thao tác Trình duyệt:**
- Mở: **[http://localhost:5540](http://localhost:5540)**.
- Chọn database **LZD Feature Store** (hoặc gõ host `redis`, port `6379`).
- Tại ô tìm kiếm:
  - Gõ `fs:meta:active_version` $\rightarrow$ Trị số là `v20260805`.
  - Gõ `fs:v20260805:u:U0000001` $\rightarrow$ Mở ra xem đầy đủ 55 đặc trưng (`f1`, `f2`, `f5`, `f30`...).
  - Gõ `rt:u:*` $\rightarrow$ Thấy các bộ đếm realtime 1 giờ do luồng stream Kafka cập nhật.

👉 **Điểm nhấn mạnh:** Redis đang lưu trữ và phục vụ hơn **1.1 triệu hồ sơ người dùng** với cấu trúc Hash tối ưu bộ nhớ.

---

### 🔹 Demo 4: Luồng Sự kiện Thời gian thực qua Kafka-UI
🗣️ **Lời thoại:** *"Bên cạnh dữ liệu tĩnh, các hành vi người dùng trên app như xem trang, thêm giỏ hàng được bắn liên tục vào Kafka."*

💻 **Thao tác Trình duyệt:**
- Mở: **[http://localhost:8082](http://localhost:8082)** (Kafka UI).
- Nhấp vào **Topics** $\rightarrow$ Chọn topic **`app.user.events.v1`** $\rightarrow$ Chọn tab **Messages**.
- Cho người nghe thấy các sự kiện JSON thời gian thực (`page_view`, `add_to_cart`, `order`...) đang được gửi liên tục.

---

### 🔹 Demo 5: Gọi API Suy luận & Ra Quyết định Tặng Voucher
🗣️ **Lời thoại:** *"Bây giờ, hãy đóng vai ứng dụng Lazada gửi một yêu cầu ra quyết định tặng voucher cho khách hàng `U0000001`."*

💻 **Thao tác Terminal:**
```bash
curl -s -X POST http://localhost:8000/decide \
  -H "Content-Type: application/json" \
  -d '{"user_id": "U0000001"}' | python3 -m json.tool
```

📋 **Kết quả hiển thị trên màn hình:**
```json
{
  "user_id": "U0000001",
  "decision": "NO_VOUCHER",
  "uplift_score": 0.004738,
  "threshold": 0.02,
  "feature_version": "v20260805",
  "model_version": "DRLearner-20260813",
  "cache_hit": true,
  "features_supplied": 55,
  "realtime_applied": 0,
  "latency_ms": 2.14
}
```

👉 **Điểm nhấn mạnh:**
- Model LightGBM thật (không phải stub) đã nạp 55 đặc trưng từ Redis.
- Tính toán điểm `uplift_score = 0.004738` (thấp hơn ngưỡng `0.02` nên không tặng để tiết kiệm ngân sách).
- **Thời gian xử lý chỉ mất ~2ms**, hoàn toàn đáp ứng chuẩn SLA khắt khe của hệ thống sàn TMĐT.

---

### 🔹 Demo 6: Chạy Kịch bản Tích hợp Toàn diện (Track A & Track B)
🗣️ **Lời thoại:** *"Để chứng minh tính liền mạch, tôi sẽ chạy kịch bản tích hợp toàn diện: từ giải mã quá khứ (Track A) $\rightarrow$ sinh sự kiện tương lai (Track B) $\rightarrow$ bắn Kafka $\rightarrow$ cập nhật Redis $\rightarrow$ và ra quyết định."*

💻 **Thao tác Terminal:**
```bash
PYTHONPATH=src .venv/bin/python scripts/demo_track_ab_e2e.py
```

👉 **Điểm nhấn mạnh:** 
- **10/10 users** giải mã quá khứ đều **PASS 100% 6 cổng kiểm định (Gate A..F)**.
- 92 sự kiện tương lai được sinh ra, bắn qua Kafka, stream consumer cập nhật ngay lập tức vào Redis `rt:u:U0000001`, và API trả về quyết định chuẩn xác.

---

### 🔹 Demo 7: Giám sát Trực quan Thời gian thực trên Grafana
🗣️ **Lời thoại:** *"Cuối cùng, mọi hoạt động của toàn bộ nền tảng đều được quan sát tập trung trên Grafana."*

💻 **Thao tác Trình duyệt:**
- Mở: **[http://localhost:3000](http://localhost:3000)** (Tài khoản: `admin` / `admin`).
- Chọn Dashboard: **`00 · End-to-End Pipeline Overview`**.
- Góc trên cùng bên phải: Chọn khoảng thời gian **`Last 15 minutes`**, bật **Auto-refresh `5s`**.

📊 **Các chỉ số trực quan cần chỉ trên màn hình:**
1. **Event produced /s & Event consumed /s:** Đang chạy ổn định ở mức ~24 req/s.
2. **Feature Store Synced Rows:** Hiển thị trọn vẹn **1,108,338 dòng**.
3. **API Throughput & Latency P99:** Đồ thị độ trễ suy luận luôn dưới ngưỡng 5ms.
4. **Realtime Overlay Keys:** Hàng ngàn key realtime đang hoạt động trên Redis.

---

## PHẦN 4: TỔNG KẾT & CÂU HỎI (1.5 phút)

🗣️ **Lời thoại thuyết trình:**
> *"Tóm lại, dự án đã hiện thực hóa thành công một nền tảng Data & AI Platform toàn diện:*
> 1. **Hiệu quả kinh doanh:** Áp dụng mô hình **Causal Uplift (DR-Learner)** giúp tối ưu hóa ngân sách khuyến mãi, chỉ tặng voucher cho nhóm khách hàng mang lại doanh thu gia tăng thực sự.
> 2. **Chất lượng dữ liệu:** Đảm bảo **Zero Training/Serving Skew** nhờ cơ chế quản lý hợp đồng `feature_spec.yml` đồng nhất giữa offline dbt và online Redis.
> 3. **Hiệu năng & Độ tin cậy:** Hệ thống phục vụ 1.1 triệu người dùng với độ trễ suy luận **< 3ms**, đồng bộ dữ liệu song song không gây gián đoạn và có hệ thống giám sát cảnh báo tự động 24/7.
>
> *Em xin chân thành cảm ơn anh/chị đã lắng nghe. Em rất mong nhận được câu hỏi và góp ý từ mọi người ạ!"*
