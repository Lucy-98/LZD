# Hệ thống phát voucher thông minh
### Báo cáo tiến độ · 07/08/2026

**Việc được giao:** xây hệ thống trả lời câu hỏi *"có nên phát voucher cho người này không?"* — trong **dưới 1/10 giây**, cho hơn **1,1 triệu khách hàng**.

**Tình hình thật:**
- ✅ Toàn bộ code đã viết xong, **30 bài kiểm thử tự động chạy xanh**.
- 🟡 **Chưa chạy thật lần nào** — chưa bật hệ thống lên, chưa có dữ liệu chạy qua đường ống.
- ⏳ Mô hình dự đoán: chờ bạn cùng nhóm.

> Việc tiếp theo của tôi là **bật lên và chạy end-to-end lần đầu**. Chi tiết ở slide 6.

---

## 1 · Bài toán thực ra là gì

Không phải *"ai có khả năng mua hàng"* — mà là *"voucher có làm người này **đổi ý** không"*.

- Người **kiểu gì cũng mua** → phát voucher là lỗ, mất tiền vô ích.
- Người **kiểu gì cũng không mua** → phát cũng vô nghĩa.
- Chỉ nhóm **ở giữa** mới đáng phát.

Hai ràng buộc khó:
1. **Phải trả lời cực nhanh** — khách đang mở app, không thể bắt họ chờ.
2. **Dữ liệu lúc học và lúc dùng phải giống hệt nhau** — nếu lệch, hệ thống sai mà không ai biết. Đây là rủi ro lớn nhất.

---

## 2 · Cách giải quyết

**Không tính toán lúc khách hỏi — tính trước, để sẵn.**

```
Hàng đêm:  Dữ liệu thô → làm sạch → tính sẵn hồ sơ mỗi khách → đặt vào bộ nhớ đọc nhanh
Realtime:  Khách click/mua → cập nhật hồ sơ trong vòng 30 giây
Khi hỏi:   Lấy hồ sơ có sẵn → mô hình chấm điểm → trả lời
```

Giống như thay vì nấu ăn khi khách gọi món, ta **chuẩn bị sẵn nguyên liệu từ đêm trước** — khách gọi thì chỉ việc ra món.

Mục tiêu thiết kế: mỗi câu trả lời mất **vài mili giây** thay vì vài giây. *(Con số này là thiết kế, chưa đo thật.)*

---

## 3 · Công nghệ dùng và vai trò của từng thứ

| Công nghệ | Nó làm gì trong hệ thống này |
|---|---|
| **Kafka** | Đường ống nhận sự kiện khách hàng (click, thêm giỏ, đặt hàng) theo thời gian thực. Nhận vào trước, xử lý sau — app không bị chậm vì chờ |
| **MinIO** | Kho lưu dữ liệu thô, giữ nguyên bản gốc để về sau kiểm chứng lại được |
| **DuckDB** | Nơi tính toán trên dữ liệu lớn. Đóng vai kho dữ liệu trung tâm — **nguồn sự thật** |
| **dbt** | Biến dữ liệu thô thành hồ sơ khách hàng dùng được, qua 3 tầng: thô → sạch → sẵn dùng. Kèm luôn kiểm tra chất lượng |
| **Redis** | Bộ nhớ đọc cực nhanh. Giữ bản sao hồ sơ khách để trả lời trong vài mili giây |
| **Airflow** | Người quản đốc: chạy các công việc theo lịch, tự thử lại khi lỗi, báo khi hỏng |
| **FastAPI** | Cửa nhận yêu cầu từ app: "khách này có nên phát voucher không?" |
| **MLflow** | Nơi lưu mô hình và kết quả huấn luyện — để biết bản nào đang chạy, bản nào tốt hơn |
| **Grafana + Prometheus** | Màn hình giám sát: dữ liệu chạy tới đâu, có tắc chỗ nào, có chậm không |
| **Docker** | Đóng gói tất cả lại, bật lên bằng 1 câu lệnh, máy ai chạy cũng giống nhau |

---

## 4 · Trạng thái thật của từng phần

| Phần | Trạng thái |
|---|---|
| 30 unit test (chạy không cần bật hệ thống) | ✅ **Đã chạy, xanh hết** |
| Nhận dữ liệu realtime (Kafka) | 🟡 Code xong, chưa chạy thật |
| Làm sạch & tính hồ sơ (dbt trên DuckDB) | 🟡 Code xong, chưa chạy thật |
| Đưa hồ sơ lên bộ nhớ nhanh (Redis) | 🟡 Code xong, chưa chạy thật |
| API trả lời quyết định (FastAPI) | 🟡 Code xong, chưa chạy thật |
| Lịch chạy tự động (Airflow) | 🟡 6 quy trình đã viết, chưa chạy lần nào |
| Màn hình giám sát (Grafana) | 🟡 5 màn hình đã cấu hình sẵn, chưa có dữ liệu để hiện |
| Mô hình dự đoán | ⏳ Chờ bạn cùng nhóm — chỗ ghép đã chuẩn bị sẵn |

**Ý nghĩa của 🟡:** logic đã được kiểm tra ở mức từng phần, nhưng **chưa có bằng chứng các phần ghép lại chạy được với nhau**. Đây là khoảng cách thật giữa "viết xong" và "chạy được".

---

## 5 · Phần khó nhất đã thiết kế xong: thay dữ liệu mà không gián đoạn

Mỗi đêm phải thay hồ sơ của 1,1 triệu khách trên Redis. Nếu ghi đè trực tiếp thì **giữa chừng sẽ có người dùng hồ sơ mới, người dùng hồ sơ cũ** → quyết định không nhất quán.

Cách làm: **ghi bộ mới vào chỗ khác, kiểm tra kỹ, rồi đổi biển chỉ đường trong 1 nhịp.**

- Kiểm tra không đạt → **không đổi**, khách vẫn dùng bộ cũ, không ai bị ảnh hưởng.
- Đổi xong phát hiện sai → **trỏ ngược lại**, phục hồi ngay lập tức.
- Chạy nửa chừng bị lỗi → chạy lại **chỉ làm phần còn thiếu**, không làm lại từ đầu.

Phần logic này **đã được unit test phủ** (đổi biển chỉ đường, quay lui, chạy lại không nhân đôi dữ liệu). Còn thiếu: **diễn tập thật** — công cụ bơm lỗi đã viết sẵn nhưng chưa bấm chạy.

---

## 6 · Việc tiếp theo — kế hoạch chạy thật

| Bước | Nội dung | Ước tính |
|---|---|---|
| 1 | Bật hệ thống lên, kiểm tra 24 thành phần khoẻ mạnh | 0,5 ngày |
| 2 | Nạp dữ liệu vào kho, chạy làm sạch + tính hồ sơ | 0,5 ngày |
| 3 | Đẩy hồ sơ lên Redis, gọi thử API, **đo tốc độ thật** | 0,5 ngày |
| 4 | Diễn tập sự cố: bơm lỗi giữa chừng, kiểm tra phục hồi | 0,5 ngày |
| 5 | Chốt số liệu thật để báo cáo lại | 0,5 ngày |

**Rủi ro đã lường trước:** lần chạy đầu gần như chắc chắn sẽ lòi ra lỗi ghép nối — cấu hình sai, thiếu quyền, tràn bộ nhớ. Đó là chuyện bình thường và chính là lý do phải chạy. Tôi để dư thời gian cho việc này.

**Sau bước 5, tài liệu này sẽ được cập nhật bằng số đo thật** thay cho số thiết kế.

---

## 7 · Đã tự tìm ra và sửa 5 lỗi (khi soát lại code)

Không phải lỗi được báo — là lỗi tự rà ra khi đọc lại toàn bộ hệ thống.

| Lỗi | Nếu không sửa thì sao |
|---|---|
| Mỗi lần trả lời khách lại mở một kết nối ghi nhật ký mới | Chậm thêm 5–20% ngân sách thời gian, có nguy cơ vượt cam kết |
| Con số "1 giờ vừa rồi" thực ra cộng dồn cả ngày | **Lúc học thấy 12, lúc dùng thấy 400** — mô hình sai lệch nghiêm trọng |
| Hỏi Redis 2 lần thay vì 1 lần | Chậm gấp đôi + có khe hở trả lời sai |
| Kết quả kiểm tra chất lượng của dbt không hiện lên Grafana | Dữ liệu hỏng mà không ai biết |
| Truy vấn tay có thể khoá cả kho DuckDB, làm chết job đang chạy | Pipeline đêm gãy, sáng ra không có dữ liệu |

Lỗi số 2 nguy hiểm nhất — nó **không làm hệ thống báo lỗi**, chỉ âm thầm làm mô hình học sai. Đã viết thêm 5 bài kiểm thử riêng để nó không tái diễn.

---

## 8 · Dọn dẹp cho gọn và rẻ hơn

| | Trước | Sau |
|---|---|---|
| Dữ liệu thừa gửi đi mỗi lần đóng gói | 549 MB × 3 lần | 0,27 MB |
| Sửa 1 dòng cấu hình rồi đóng gói lại | Làm lại từ đầu | Vài giây (dùng lại phần cũ) |
| Phát hiện xung đột thư viện | Lúc 1h30 sáng khi job chạy | Ngay lúc đóng gói |
| Bộ nhớ | Không giới hạn, Kafka tự chiếm 1/4 máy | Có giới hạn rõ ràng |

*Đây là thay đổi cấu hình — con số 549 MB tính từ kích thước file thật, các mục còn lại sẽ xác nhận bằng số đo sau lần chạy đầu.*

---

## 9 · Bốn câu hỏi cần anh cho ý kiến

Đây không phải lỗi kỹ thuật — là **những thứ hệ thống hiện chưa có chỗ để đặt vào**. Quyết sớm thì rẻ, để lâu thì phải đập đi làm lại.

**1. Ngân sách voucher?**
Hiện hệ thống xét từng người độc lập. Thực tế mỗi tháng có một hạn mức — cần chuyển sang *"chọn N người đáng nhất trong ngân sách"*.

**2. Chống lạm dụng?**
Hiện một người bấm 100 lần sẽ nhận voucher 100 lần. Cần cơ chế "đã phát rồi".

**3. Làm sao biết hệ thống đúng hay sai?** ⚠️ *Quan trọng nhất*
Hiện ta ghi lại **quyết định đã phát**, nhưng không ghi lại **kết quả** — người đó có dùng voucher không, có mua không.
→ Không đo được hiệu quả thật, không cải thiện được mô hình. Hệ thống hiện **không tự biết nó đúng hay sai**.

**4. Có giữ nhóm đối chứng không?**
Muốn chứng minh hệ thống thực sự mang lại tiền, cần một nhóm nhỏ **cố tình không phát voucher** để so sánh.

**Đề xuất thứ tự:** 3 → 1 → 4 → 2

---

## Tóm lại

✅ **Chắc chắn:** code đầy đủ, 30 bài kiểm thử xanh, 5 lỗi tự tìm đã sửa, thiết kế chống gián đoạn đã có.

🟡 **Chưa chứng minh:** hệ thống chưa chạy thật lần nào — mọi con số về tốc độ hiện là thiết kế, chưa phải số đo.

⏭️ **Tuần tới:** chạy end-to-end lần đầu (~2,5 ngày), rồi báo cáo lại bằng số thật.

❓ **Cần anh quyết:** 4 câu ở trên — đặc biệt **câu 3: làm sao đo được hiệu quả thật**.
