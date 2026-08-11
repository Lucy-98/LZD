# Hệ thống dữ liệu cho mô hình phát voucher (Uplift Model)

### Báo cáo: Kiến trúc · Mục tiêu · Đầu vào – Đầu ra · Tiến độ & Kết quả

**Người thực hiện:** Quang Anh · **Ngày báo cáo:** 10/08/2026 · **Giai đoạn:** xây dựng xong hệ thống, chuẩn bị chạy thật

---

## 1 · Đề bài được giao

> Xây dựng **data pipeline mức production** cho bài toán Uplift Model, gồm **hai luồng dữ liệu song song**:
> - **Luồng offline (huấn luyện):** kho dữ liệu → làm sạch → tạo bộ dữ liệu huấn luyện → lưu mô hình.
> - **Luồng online (phục vụ):** kho dữ liệu → tính sẵn hồ sơ khách hàng hằng ngày → đẩy sang bộ nhớ đọc nhanh → dịch vụ trả lời quyết định trong **dưới 10 mili giây**.
>
> Toàn bộ chạy được trên máy local bằng Docker, mô phỏng đúng cách Lazada làm thật.

**Bối cảnh nghiệp vụ:** Lazada dùng Uplift Model để quyết định *có phát voucher cho một khách hàng hay không*. Khi khách mở app, hệ thống phải trả lời trong **dưới 100 mili giây**. Không thể hỏi thẳng kho dữ liệu lớn mỗi lần (vừa chậm vừa tốn tiền) → phải **tính trước mỗi đêm, để sẵn ở nơi đọc nhanh**.

Mẫu thiết kế này gọi là **Online Feature Store**: kho dữ liệu lớn là *nguồn sự thật*, bộ nhớ nhanh chỉ là *bản sao để phục vụ*.

**Bốn yêu cầu bắt buộc của công việc đồng bộ hằng đêm:**

| Yêu cầu | Nghĩa dễ hiểu |
|---|---|
| **Consistency** | Số liệu ở bản sao phải khớp với kho gốc |
| **Freshness** | Dữ liệu phải mới, không được cũ quá X giờ |
| **Idempotency** | Chạy lại nhiều lần không được nhân đôi dữ liệu |
| **Fault tolerance** | Chạy dở bị lỗi thì phục hồi được, không phải làm lại từ đầu |

---

## 2 · Bài toán thực chất là gì

Đây **không phải** bài toán *"ai có khả năng mua hàng"*, mà là *"voucher có làm người này **đổi ý** không"*.

| Nhóm khách | Phát voucher thì sao |
|---|---|
| Kiểu gì cũng mua | **Lỗ** — mất tiền vô ích |
| Kiểu gì cũng không mua | **Vô nghĩa** — không đổi được gì |
| Ở giữa: có voucher thì mua, không có thì thôi | ✅ **Đúng nhóm cần phát** |

Vì vậy dữ liệu phải giữ được cả hai thông tin: *người này có được phát voucher không* (`is_treat`) và *người này có mua không* (`label`) — để mô hình học được phần **chênh lệch** giữa hai tình huống.

**Hai ràng buộc khó nhất:**
1. **Phải trả lời cực nhanh** — khách đang mở app, không thể bắt chờ.
2. **Dữ liệu lúc học và lúc dùng phải giống hệt nhau.** Nếu lệch, hệ thống ra quyết định sai mà **không hề báo lỗi**. Đây là rủi ro lớn nhất của cả dự án.

---

## 3 · Phạm vi công việc (đã chốt với mentor)

| Phần | Ai làm | Ghi chú |
|---|---|---|
| Toàn bộ đường ống dữ liệu, hạ tầng, lịch chạy, giám sát | **Tôi** | Nội dung của báo cáo này |
| Huấn luyện mô hình Uplift | Đồng đội | Chỗ ghép đã chuẩn bị sẵn |
| Phần chấm điểm bên trong API | Đồng đội | API và đường lấy dữ liệu đã xong |

**Giới hạn phạm vi có chủ ý:** chỉ phục vụ **khách hàng cũ** (đã có lịch sử trong hệ thống), **không xử lý khách mới hoàn toàn**. Lý do: khách mới chưa có hồ sơ trong bộ nhớ nhanh, cần một cơ chế xử lý riêng — để giai đoạn sau cho gọn phạm vi.

---

## 4 · Đầu vào – Đầu ra

### 4.1 Đầu vào

| Nguồn | Nội dung | Quy mô thật |
|---|---|---|
| **Bộ dữ liệu gốc** (`full_trainset.csv`, `full_testset.csv`) | Hồ sơ khách hàng đã qua xử lý, dùng để huấn luyện | **1.108.338 khách hàng**, 86 cột |
| **Sự kiện thời gian thực** | Khách mở app, xem, tìm kiếm, thêm giỏ, nhận voucher, đặt hàng | Sinh liên tục để mô phỏng |

Cấu trúc bộ dữ liệu gốc:

```
data_id | label | is_treat | f0 | f1 | ... | f82
```

- `label` — khách có mua hay không
- `is_treat` — khách có được phát voucher hay không
- `f0…f82` — 83 đặc trưng **bị ẩn tên** (nhà cung cấp dữ liệu che đi vì lý do bảo mật)

### 4.2 Đầu ra

| Đầu ra | Dạng | Dùng để làm gì |
|---|---|---|
| **Bảng dữ liệu huấn luyện** | Bảng trong kho dữ liệu | Đồng đội lấy để huấn luyện mô hình |
| **Hồ sơ khách hàng phục vụ** | Bản ghi trong bộ nhớ nhanh, mỗi khách một bản | Mô hình đọc để chấm điểm |
| **Câu trả lời quyết định** | `{ "user_id": ..., "phát voucher": có/không, "điểm": ..., "phiên bản mô hình": ... }` | App gọi khi khách mở ứng dụng |
| **Màn hình giám sát** | 5 bảng theo dõi + nhật ký tập trung | Biết dữ liệu đang chạy tới đâu, tắc ở đâu |

### 4.3 Cam kết về tốc độ *(mục tiêu thiết kế — chưa đo thật)*

| Chỉ tiêu | Mục tiêu |
|---|---|
| Đọc hồ sơ từ bộ nhớ nhanh | < 10 mili giây |
| Trả lời trọn vẹn một quyết định | < 100 mili giây |
| Sự kiện thời gian thực phản ánh vào hồ sơ | < 30 giây |
| Dữ liệu hằng ngày làm mới xong | Trước 2 giờ sáng |

---

## 5 · Kiến trúc đã xây

### 5.1 Ý tưởng cốt lõi

> **Không tính toán lúc khách hỏi — tính trước, để sẵn.**

```
Hàng đêm:   Dữ liệu thô → làm sạch → tính sẵn hồ sơ mỗi khách → đặt vào bộ nhớ đọc nhanh
Thời gian thực:  Khách click/mua → cập nhật hồ sơ trong vòng 30 giây
Khi có câu hỏi:  Lấy hồ sơ có sẵn → mô hình chấm điểm → trả lời ngay
```

Giống như nhà hàng: thay vì nấu từ đầu khi khách gọi món, ta **sơ chế sẵn nguyên liệu từ đêm trước** — khách gọi thì chỉ việc ra món.

### 5.2 Ba luồng dữ liệu

```
 App khách hàng ──► Hàng đợi sự kiện ──┬──► Kho dữ liệu thô (lưu trữ gốc)
                                       └──► Bộ nhớ nhanh: cập nhật nóng (sống 1 giờ)
                                                    ▲
 Dữ liệu gốc ──► Kho thô ──► Kho trung tâm ──► Làm sạch & tính hồ sơ
                                                    │            │
                                                    │      [Đồng bộ hằng đêm]
                                                    │            ▼
                                                    │   Bộ nhớ nhanh: hồ sơ chính thức
                                                    │            │
                                          Bảng huấn luyện        ▼
                                                    │      API trả quyết định
                                                    └──► Huấn luyện mô hình ──►┘
```

| # | Luồng | Đường đi | Nhịp |
|---|---|---|---|
| 1 | **Offline — huấn luyện** | Kho thô → kho trung tâm → làm sạch → bảng huấn luyện → mô hình | Hằng ngày |
| 2 | **Online — phục vụ** | Kho trung tâm → đồng bộ → bộ nhớ nhanh → API | Hằng ngày 01:30 |
| 3 | **Thời gian thực** | App → hàng đợi → xử lý → bộ nhớ nhanh (lớp phủ, sống 1 giờ) | Liên tục, trễ < 30s |

### 5.3 Công nghệ dùng và vai trò từng thứ

| Công nghệ | Vai trò trong hệ thống này |
|---|---|
| **Kafka** | Đường ống nhận sự kiện khách hàng theo thời gian thực. Nhận trước, xử lý sau → app không bị chậm vì chờ |
| **MinIO** | Kho lưu dữ liệu thô, giữ nguyên bản gốc để sau này kiểm chứng lại được |
| **DuckDB** | Nơi tính toán trên dữ liệu lớn — đóng vai kho dữ liệu trung tâm, **nguồn sự thật** (thay cho BigQuery khi chạy local) |
| **dbt** | Biến dữ liệu thô thành hồ sơ khách hàng dùng được qua 3 tầng: *thô → sạch → sẵn dùng*, kèm luôn kiểm tra chất lượng |
| **Redis** | Bộ nhớ đọc cực nhanh, giữ bản sao hồ sơ khách để trả lời trong vài mili giây |
| **Airflow** | Người quản đốc: chạy công việc theo lịch, tự thử lại khi lỗi, báo khi hỏng |
| **FastAPI** | Cửa nhận yêu cầu từ app: *"khách này có nên phát voucher không?"* |
| **MLflow** | Nơi lưu mô hình và kết quả huấn luyện — biết bản nào đang chạy, bản nào tốt hơn |
| **Grafana + Prometheus + Loki** | Màn hình giám sát và nhật ký: dữ liệu chạy tới đâu, tắc chỗ nào, chậm chỗ nào |
| **Postgres** | Sổ ghi chép vận hành: lịch sử đồng bộ, kết quả kiểm tra chất lượng, nhật ký quyết định |
| **Docker** | Đóng gói tất cả, bật lên bằng một câu lệnh, máy ai chạy cũng như nhau |

### 5.4 Ba quyết định thiết kế quan trọng nhất

**① Một bản "hợp đồng dữ liệu" duy nhất**

Danh sách đặc trưng được khai báo ở **đúng một chỗ**. Cả ba nơi — bước làm sạch, bước đồng bộ, và API — đều đọc chung file đó. Muốn thêm/bớt đặc trưng chỉ sửa một file, toàn hệ thống tự khớp theo.

*Vì sao quan trọng:* đây là cách chặn tận gốc rủi ro lớn nhất — dữ liệu lúc học và lúc dùng bị lệch nhau.

**② Thay dữ liệu mà không gián đoạn phục vụ**

Mỗi đêm phải thay hồ sơ của hơn 1,1 triệu khách. Nếu ghi đè trực tiếp thì giữa chừng sẽ có người dùng hồ sơ mới, người dùng hồ sơ cũ → quyết định không nhất quán.

Cách làm: **ghi bộ mới vào chỗ khác → kiểm tra kỹ → rồi đổi biển chỉ đường trong một nhịp.**

| Tình huống | Hệ thống xử lý thế nào |
|---|---|
| Kiểm tra không đạt | **Không đổi** — khách vẫn dùng bộ cũ, không ai bị ảnh hưởng |
| Đổi xong mới phát hiện sai | **Trỏ ngược lại** — phục hồi ngay lập tức |
| Chạy nửa chừng bị lỗi | Chạy lại **chỉ làm phần còn thiếu**, không làm lại từ đầu |

Dữ liệu được chia thành **32 phần độc lập**, lỗi phần nào chỉ làm lại phần đó.

**③ Bốn yêu cầu của đề bài được cài đặt ở đâu**

| Yêu cầu | Cách hiện thực |
|---|---|
| **Consistency** | Đối chiếu mẫu giữa bản sao và kho gốc + so tổng số dòng **trước khi** cho phép đổi sang bộ mới |
| **Freshness** | Mỗi bộ dữ liệu mang tên theo ngày; có cảnh báo tự động nếu dữ liệu cũ quá 26 giờ |
| **Idempotency** | Tên bộ dữ liệu sinh theo quy tắc cố định; phần nào ghi xong được đánh dấu, chạy lại sẽ bỏ qua |
| **Fault tolerance** | 32 phần độc lập, thử lại từng phần; biển chỉ đường chỉ đổi ở bước cuối nên lỗi giữa chừng **không ảnh hưởng khách hàng** |

---

## 6 · Tiến độ hiện tại

### 6.1 Trạng thái từng phần

| Phần việc | Trạng thái |
|---|---|
| Thiết kế kiến trúc tổng thể | ✅ **Xong** |
| Hạ tầng đóng gói (24 thành phần, bật bằng 1 lệnh) | ✅ **Xong** |
| 30 bài kiểm thử tự động (chạy không cần bật hệ thống) | ✅ **Đã chạy, xanh hết** |
| Bộ tài liệu vận hành (kiến trúc, luồng dữ liệu, xử lý sự cố, giám sát) | ✅ **Xong** |
| Nhận dữ liệu thời gian thực | 🟡 Code xong, **chưa chạy thật** |
| Làm sạch & tính hồ sơ khách hàng | 🟡 Code xong, **chưa chạy thật** |
| Đồng bộ hồ sơ lên bộ nhớ nhanh | 🟡 Code xong, **chưa chạy thật** |
| API trả lời quyết định | 🟡 Code xong, **chưa chạy thật** |
| Lịch chạy tự động (7 quy trình) | 🟡 Đã viết, **chưa chạy lần nào** |
| Màn hình giám sát (5 bảng) | 🟡 Đã cấu hình sẵn, **chưa có dữ liệu để hiện** |
| **Chọn 20–30 đặc trưng & gán nghiệp vụ Lazada** | 🔄 **Đang làm** — đã có bảng xếp hạng độ quan trọng, đang chốt danh sách |
| **Dựng ngược sự kiện thô từ bộ dữ liệu** | ⏳ Đã có kế hoạch, **chưa khởi động** |
| Mô hình dự đoán | ⏳ Chờ đồng đội — chỗ ghép đã chuẩn bị sẵn |

**Ý nghĩa của 🟡:** logic đã được kiểm tra ở mức từng phần, nhưng **chưa có bằng chứng các phần ghép lại chạy được với nhau**. Đây là khoảng cách thật giữa *"viết xong"* và *"chạy được"*.

### 6.2 Việc đang làm: chọn đặc trưng và dựng ngược dữ liệu

Bộ dữ liệu gốc có 83 đặc trưng **bị ẩn tên** (`f0`…`f82`) — không biết mỗi cột nghĩa là gì. Theo hướng dẫn của mentor, công việc gồm hai bước:

1. **Chọn ~20–30 đặc trưng quan trọng nhất** và **gán ý nghĩa nghiệp vụ Lazada** cho chúng (ví dụ: *số đơn 30 ngày gần nhất*, *giá trị đơn trung bình*, *số voucher đã dùng*…).
   → Hiện đã chạy xong bước xếp hạng độ quan trọng bằng nhiều mô hình, có bảng thứ hạng đồng thuận. **Đang ở khâu chốt danh sách cuối.**

2. **Dựng ngược ra sự kiện thô** tương ứng, để dữ liệu đi qua đủ chặng của đường ống (hàng đợi → kho thô → kho trung tâm → làm sạch) và **tính ra đúng bằng bảng gốc**.
   → Mục đích: chứng minh đường ống tính ra kết quả chính xác, chứ không phải chỉ chép dữ liệu qua.

---

## 7 · Kết quả đạt được

### 7.1 Những gì đã kiểm chứng được

**30 bài kiểm thử tự động chạy xanh**, phủ đúng các điểm rủi ro cao nhất:

| Nhóm | Kiểm tra điều gì |
|---|---|
| Hợp đồng dữ liệu | Danh sách đặc trưng khai báo đúng, ba nơi đọc ra giống nhau |
| Ghép hồ sơ | Hồ sơ chính thức + cập nhật nóng + giá trị mặc định ghép đúng thứ tự ưu tiên |
| Chạy lại không nhân đôi | Chạy lần hai không làm dữ liệu bị cộng dồn |
| Đổi biển chỉ đường | Đổi trong một nhịp, quay lui được, dữ liệu bộ mới **không lộ ra** trước khi sẵn sàng |
| Dọn dẹp | Bộ đang phục vụ không bao giờ bị xoá nhầm |
| Dữ liệu hỏng | Sự kiện sai định dạng bị đẩy sang hàng đợi lỗi riêng, không làm chết luồng chính |

### 7.2 Năm lỗi tự tìm ra và đã sửa

Không phải lỗi được ai báo — là lỗi tự rà ra khi soát lại toàn bộ hệ thống.

| Lỗi | Nếu không sửa thì sao |
|---|---|
| Mỗi lần trả lời khách lại mở một kết nối ghi nhật ký mới | Tiêu tốn 5–20% ngân sách thời gian, nguy cơ vượt cam kết tốc độ |
| **Con số "1 giờ vừa rồi" thực ra cộng dồn cả ngày** | **Lúc học thấy 12, lúc dùng thấy 400** — mô hình sai lệch nghiêm trọng |
| Hỏi bộ nhớ nhanh 2 lần thay vì 1 lần | Chậm gấp đôi, và có khe hở trả lời sai |
| Kết quả kiểm tra chất lượng không hiện lên màn hình giám sát | Dữ liệu hỏng mà không ai biết |
| Một truy vấn thủ công có thể khoá cả kho dữ liệu | Quy trình đêm gãy, sáng ra không có dữ liệu |

**Lỗi số 2 nguy hiểm nhất** — nó *không làm hệ thống báo lỗi*, chỉ âm thầm làm mô hình học sai. Đã viết thêm 5 bài kiểm thử riêng để nó không tái diễn.

### 7.3 Tối ưu để gọn và rẻ hơn

| | Trước | Sau |
|---|---|---|
| Dữ liệu thừa gửi đi mỗi lần đóng gói | 549 MB × 3 lần | **0,27 MB** |
| Sửa 1 dòng cấu hình rồi đóng gói lại | Làm lại từ đầu | **Vài giây** (dùng lại phần cũ) |
| Phát hiện xung đột thư viện | Lúc 1h30 sáng khi công việc chạy | **Ngay lúc đóng gói** |
| Giới hạn bộ nhớ | Không có — một thành phần tự chiếm 1/4 máy | **Có giới hạn rõ ràng** cho 21/24 thành phần |
| Tắt hệ thống giữa lúc đang ghi dữ liệu | Bị cắt ngang sau 10 giây | **Chờ đủ 90 giây** để ghi xong |

---

## 8 · Việc tiếp theo

| Bước | Nội dung | Ước tính |
|---|---|---|
| 1 | Chốt danh sách 20–30 đặc trưng + gán ý nghĩa nghiệp vụ | 1 ngày |
| 2 | Dựng ngược sự kiện thô, kiểm tra tính ra đúng bảng gốc | 1,5 ngày |
| 3 | Bật hệ thống, kiểm tra 24 thành phần khoẻ mạnh | 0,5 ngày |
| 4 | Nạp dữ liệu, chạy làm sạch + tính hồ sơ | 0,5 ngày |
| 5 | Đẩy hồ sơ lên bộ nhớ nhanh, gọi thử API, **đo tốc độ thật** | 0,5 ngày |
| 6 | Diễn tập sự cố: bơm lỗi giữa chừng, kiểm tra phục hồi | 0,5 ngày |
| 7 | Chốt số liệu thật, cập nhật lại báo cáo | 0,5 ngày |

**Rủi ro đã lường trước:** lần chạy đầu gần như chắc chắn sẽ lòi ra lỗi ghép nối — cấu hình sai, thiếu quyền, tràn bộ nhớ. Đó là chuyện bình thường và chính là lý do phải chạy thật. Đã để dư thời gian cho việc này.

---

## 9 · Bốn vấn đề cần cấp trên cho ý kiến

Đây không phải lỗi kỹ thuật — là **những thứ hệ thống hiện chưa có chỗ để đặt vào**. Quyết sớm thì rẻ, để lâu thì phải làm lại.

| # | Vấn đề | Hiện trạng |
|---|---|---|
| **1** | **Đo hiệu quả thật** ⚠️ *Quan trọng nhất* | Hệ thống ghi lại **quyết định đã phát**, nhưng không ghi lại **kết quả** (khách có dùng voucher, có mua không) → **không tự biết mình đúng hay sai**, không cải thiện được |
| **2** | **Ngân sách voucher** | Hiện xét từng người độc lập. Thực tế mỗi tháng có hạn mức → cần chuyển sang *"chọn N người đáng nhất trong ngân sách"* |
| **3** | **Nhóm đối chứng** | Muốn chứng minh hệ thống thực sự mang lại tiền, cần một nhóm nhỏ **cố tình không phát voucher** để so sánh |
| **4** | **Chống lạm dụng** | Hiện một người bấm 100 lần sẽ nhận voucher 100 lần → cần cơ chế *"đã phát rồi"* |

**Đề xuất thứ tự xử lý:** 1 → 2 → 3 → 4

---

## 10 · Tóm lại

✅ **Đã chắc chắn**
Kiến trúc hai luồng offline/online hoàn chỉnh; hạ tầng 24 thành phần bật bằng một lệnh; 7 quy trình tự động; 5 màn hình giám sát; 30 bài kiểm thử xanh; 5 lỗi tự tìm đã sửa; cơ chế thay dữ liệu không gián đoạn đã thiết kế và kiểm thử.

🔄 **Đang làm**
Chốt 20–30 đặc trưng và gán ý nghĩa nghiệp vụ, sau đó dựng ngược sự kiện thô cho dữ liệu chạy qua đủ đường ống.

🟡 **Chưa chứng minh**
Hệ thống **chưa chạy thật lần nào** — mọi con số về tốc độ hiện là **mục tiêu thiết kế, chưa phải số đo**. Đây là điều tôi muốn nêu rõ thay vì báo cáo là đã hoàn thành.

⏭️ **Kế hoạch**
Khoảng **5 ngày** để chạy end-to-end lần đầu và báo cáo lại bằng số liệu thật.

❓ **Cần quyết định**
Bốn vấn đề ở mục 9 — đặc biệt **vấn đề 1: làm sao đo được hiệu quả thật của hệ thống**.
