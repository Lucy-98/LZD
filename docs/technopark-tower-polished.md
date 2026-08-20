# BIÊN BẢN THẢO LUẬN CHI TIẾT
## Hệ Thống Real-time Serving và Data Pipeline Cho Mô Hình Khuyến Mại (Promotion Models)

---

### I. THIẾT KẾ LUỒNG DỮ LIỆU FEATURE SERVING (DATA PIPELINE)

Hệ thống được thiết kế tích hợp song song hai luồng xử lý dữ liệu để tối ưu hóa việc cung cấp đặc trưng (features) cho mô hình dự đoán:

1. **Luồng dữ liệu Batch (Offline):**
   * **Đặc điểm:** Phục vụ tính toán các đặc trưng dài hạn (như tổng giá trị đơn hàng, giá trị trung bình đơn hàng - AOV trong vòng 14 hoặc 30 ngày qua) [1, 5].
   * **Cơ chế hoạt động:** Dữ liệu được xử lý hồi tố từ ngày $t-1$ trở về trước [1, 5]. Quy trình tính toán định kỳ qua công cụ DVT và lưu trữ trên bộ lưu trữ đối tượng MinIO [9, 11]. Dữ liệu sau đó được đồng bộ một lần duy nhất lên Redis làm dữ liệu nền [5, 22].
   * **Lịch chạy (DAG Airflow):** Thiết lập chạy định kỳ vào lúc **1:00 AM** hàng ngày (giờ hệ thống/UTC) để xử lý dữ liệu của ngày $t-1$ [19, 21].

2. **Luồng dữ liệu Cận thời gian thực (Near Real-time / Online Serving):**
   * **Đặc điểm:** Thu thập và xử lý các sự kiện hành vi tương tác mới nhất của người dùng trên hệ thống (ví dụ: các sự kiện click, bỏ sản phẩm vào giỏ hàng nhưng chưa thanh toán trong phiên truy cập hiện tại) [2, 5].
   * **Cơ chế hoạt động:** Hệ thống liên tục quét hành vi của những người dùng có tương tác mới và cập nhật trực tiếp lên Redis dưới dạng các cặp Key-Value với thời gian sống (TTL) giới hạn (ví dụ: 48 giờ) để dữ liệu tự động giải phóng khi hết hạn [2, 5].
   * **Tần suất cập nhật:** Thiết lập tần suất quét và tính toán **khoảng 10 phút một lần** (thay vì quá ngắn gây tải hệ thống hoặc quá dài làm mất tính cập nhật của phiên truy cập người dùng) nhằm cập nhật nhanh nhất hành trình khách hàng [19, 20].

---

### II. GIẢ LẬP LUỒNG DỮ LIỆU ĐỂ KIỂM THỬ (SIMULATION WORKFLOW)

Để phục vụ công tác kiểm thử và đánh giá hệ thống một cách độc lập mà không gây rò rỉ dữ liệu (data leakage) giữa quá trình huấn luyện offline và suy diễn online, dữ liệu thô được phân tách thành hai nhánh giả lập chính (Track A và Track B) [7, 8]:

* **Track A (Dữ liệu Quá khứ - Historical Simulation):**
   * Giả lập các hành vi lịch sử trước mốc thời gian $t_0$ [9].
   * Luồng dữ liệu đi qua nhánh offline: được lưu trữ vào bucket `app-event` trên MinIO, chạy qua DVT để làm sạch, trích xuất đặc trưng phục vụ trực tiếp cho quá trình huấn luyện (training) mô hình [9, 11, 21].
* **Track B (Dữ liệu Tương lai - Future Event Simulation):**
   * Giả lập các hành vi phát sinh sau mốc thời gian $t_0$ (như tương tác lướt trang, thêm sản phẩm vào giỏ hàng trong thời gian thực) [9, 10].
   * Dữ liệu này tuyệt đối **không đi vào luồng offline** để tránh làm sai lệch độ chính xác thực tế của mô hình (tránh việc mô hình học trước hành vi tương lai) [8].
   * Thay vào đó, dữ liệu được đẩy trực tiếp qua hệ thống hàng đợi thông điệp **Kafka** và lưu trữ tạm thời tại bucket `bucket-event-v2` trên MinIO để phục vụ trực tiếp cho cơ chế suy diễn trực tuyến (online serving) [9, 10, 11].

---

### III. NGHIỆP VỤ KHUYẾN MẠI & TỐI ƯU HÓA CHI PHÍ (UPLIFT MODELING)

Hệ thống hướng tới việc giải quyết bài toán tối ưu hóa hiệu quả kinh tế của các chương trình khuyến mại thay vì phân phối voucher một cách đại trà [13, 14]:

1. **Nguyên lý Phân loại Khách hàng (Uplift Framework):**
   Tập khách hàng được phân chia khoa học thành 4 nhóm dựa trên phản ứng đối với khuyến mại [14, 15]:
   * **Nhóm Chắc chắn mua (Sure Things):** Nhóm khách hàng dù có nhận được voucher hay không thì vẫn sẽ thực hiện giao dịch [14]. Việc gửi voucher cho nhóm này gây lãng phí ngân sách [14].
   * **Nhóm Không phản hồi (Lost Causes):** Nhóm khách hàng hoàn toàn không có ý định mua hàng dù có nhận được voucher hay không [14].
   * **Nhóm Chống đối (Sleeping Dogs):** Nhóm khách hàng có xu hướng bị làm phiền hoặc từ chối mua hàng nếu nhận được khuyến mại.
   * **Nhóm Thuyết phục được (Persuadables):** Nhóm khách hàng chỉ thực hiện giao dịch khi và chỉ khi nhận được khuyến mại (đặc trưng bởi chỉ số Uplift dương rõ rệt) [14, 15]. Đây là **nhóm mục tiêu duy nhất** mà mô hình cần tập trung phân phối voucher [14].

2. **Bài toán Hiệu quả Doanh thu (Delta Value):**
   * Quyết định phân phối voucher dựa trên việc so sánh giữa phần doanh thu tăng thêm (Delta Value) và chi phí phát hành khuyến mại [16].
   * **Công thức đánh giá:**
     $$\Delta \text{ Value} = (\text{Doanh thu khi có khuyến mại} - \text{Doanh thu khi không có khuyến mại}) > \text{Chi phí khuyến mại}$$
     *Ví dụ thực tế:* Nếu gửi voucher cho 100 người, doanh số tăng từ 50 đơn hàng (trị giá 100 triệu) lên 60 đơn hàng (trị giá 130 triệu) trong khi chi phí voucher tiêu tốn 10 triệu, thì Delta Value ròng mang lại vẫn dương (+20 triệu). Đây là một chương trình có lời [16].

3. **Chuyển đổi Điểm số Mô hình sang Quyết định Nhị phân:**
   * Đầu ra của mô hình (API Score) ban đầu là một điểm số liên tục (ví dụ: xác suất từ $0.0$ đến $1.0$) [5, 6].
   * Hệ thống áp dụng cơ chế cắt ngưỡng (thresholding) dựa trên ngân sách hiện có để đưa ra quyết định nhị phân cuối cùng ($y=1$: áp dụng voucher, $y=0$: không áp dụng voucher) [5, 6, 26].
   * **Phản hồi API:**
     * Nếu $y=1$: Trả về mã voucher tương ứng (ví dụ: `voucher_30` - giảm giá 30%) để backend hiển thị trên giao diện khách hàng [26, 27].
     * Nếu $y=0$: Trả về trạng thái không áp dụng voucher (`no_voucher`) [26].

---

### IV. HỆ THỐNG GIÁM SÁT & KẾ HOẠCH BÁO CÁO (MONITORING & SLIDES)

1. **Hệ thống Giám sát (Monitoring Dashboard):**
   Xây dựng hệ thống giám sát toàn diện theo từng công đoạn xử lý dữ liệu [24]:
   * Giám sát thời gian thực luồng dữ liệu truyền tải (data ingestion) thông qua Kafka [23].
   * Giám sát trạng thái hoạt động và lịch sử ghi nhận log (lần chạy thành công, lỗi, thời gian chạy) của các DAGs trong Airflow [12, 24].
   * Giám sát việc cập nhật đặc trưng và tần suất đồng bộ hóa dữ liệu lên Redis [24].

2. **Phân công Nội dung Slide Báo cáo:**
   Nhóm thống nhất gộp chung nội dung vào một Slide tổng thể nhưng phân định rõ vai trò thuyết trình của từng thành viên để đảm bảo tính mạch lạc [24, 25]:
   * **Nhung (Phần 1 - Data Pipeline):** Trình bày kiến trúc tổng thể tích hợp luồng dữ liệu và mô hình; chi tiết về luồng xử lý Batch, Near Real-time; cơ chế hoạt động của Airflow, DVT, MinIO và hệ thống Dashboard giám sát dữ liệu [24, 25].
   * **Kiên (Phần 2 - Model Serving & Demo):** Giới thiệu tập dữ liệu benchmark, phân tích trực giác thuật toán (intuition) và lý do lựa chọn các mô hình, phương pháp tối ưu hóa hyperparameter; giới thiệu cơ chế đóng gói mô hình tĩnh vào Docker Image phục vụ online serving; thực hiện demo tích hợp API gửi Request và nhận Response thực tế [25, 26].

3. **Mốc thời gian quan trọng:**
   * Hoàn thiện slide và tài liệu đi kèm để chạy thử demo tích hợp toàn diện trước ngày **Thứ Sáu** [26, 29].
   * Chủ động liên hệ bên liên quan để làm rõ thời hạn chốt báo cáo cuối cùng [29].
