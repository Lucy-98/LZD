from __future__ import annotations

import datetime
import os
import zipfile
from pathlib import Path
from xml.sax.saxutils import escape


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "Tai-lieu-ky-thuat-LZD-Data-Pipeline.docx"


def run(text: str, *, bold: bool = False, italic: bool = False, size: int | None = None,
        color: str | None = None) -> str:
    props = []
    if bold:
        props.append("<w:b/>")
    if italic:
        props.append("<w:i/>")
    if size:
        props.append(f'<w:sz w:val="{size}"/><w:szCs w:val="{size}"/>')
    if color:
        props.append(f'<w:color w:val="{color}"/>')
    p = f"<w:rPr>{''.join(props)}</w:rPr>" if props else ""
    return f'<w:r>{p}<w:t xml:space="preserve">{escape(text)}</w:t></w:r>'


def para(text: str = "", *, style: str | None = None, align: str | None = None,
         bold: bool = False, italic: bool = False, keep_next: bool = False,
         before: int = 0, after: int = 120, line: int = 300) -> str:
    ppr = []
    if style:
        ppr.append(f'<w:pStyle w:val="{style}"/>')
    if align:
        ppr.append(f'<w:jc w:val="{align}"/>')
    if keep_next:
        ppr.append("<w:keepNext/>")
    ppr.append(f'<w:spacing w:before="{before}" w:after="{after}" w:line="{line}" w:lineRule="auto"/>')
    ppr.append('<w:widowControl/>')
    return f"<w:p><w:pPr>{''.join(ppr)}</w:pPr>{run(text, bold=bold, italic=italic)}</w:p>"


def bullet(text: str, level: int = 0) -> str:
    return (
        '<w:p><w:pPr><w:pStyle w:val="ListParagraph"/>'
        f'<w:numPr><w:ilvl w:val="{level}"/><w:numId w:val="1"/></w:numPr>'
        '<w:spacing w:after="80" w:line="280" w:lineRule="auto"/></w:pPr>'
        f'{run(text)}</w:p>'
    )


def equation(text: str) -> str:
    return para(text, align="center", italic=True, before=80, after=120)


def page_break() -> str:
    return '<w:p><w:r><w:br w:type="page"/></w:r></w:p>'


def table(rows: list[list[str]], widths: list[int]) -> str:
    out = ['<w:tbl><w:tblPr><w:tblStyle w:val="TableGrid"/><w:tblW w:w="0" w:type="auto"/>'
           '<w:tblLook w:val="04A0" w:firstRow="1" w:lastRow="0" w:firstColumn="1" '
           'w:lastColumn="0" w:noHBand="0" w:noVBand="1"/></w:tblPr><w:tblGrid>']
    out.extend(f'<w:gridCol w:w="{w}"/>' for w in widths)
    out.append('</w:tblGrid>')
    for i, row in enumerate(rows):
        out.append('<w:tr>')
        for j, cell in enumerate(row):
            shade = '<w:shd w:fill="D9EAF7"/>' if i == 0 else ''
            out.append(f'<w:tc><w:tcPr><w:tcW w:w="{widths[j]}" w:type="dxa"/>{shade}</w:tcPr>')
            out.append(para(cell, bold=i == 0, after=40, line=250))
            out.append('</w:tc>')
        out.append('</w:tr>')
    out.append('</w:tbl>')
    return ''.join(out)


def image_paragraph(rid: str, name: str, cx: int, cy: int, docpr_id: int) -> str:
    return f'''<w:p><w:pPr><w:jc w:val="center"/><w:spacing w:before="80" w:after="80"/></w:pPr><w:r><w:drawing>
<wp:inline distT="0" distB="0" distL="0" distR="0"><wp:extent cx="{cx}" cy="{cy}"/>
<wp:effectExtent l="0" t="0" r="0" b="0"/><wp:docPr id="{docpr_id}" name="{escape(name)}"/>
<wp:cNvGraphicFramePr><a:graphicFrameLocks xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" noChangeAspect="1"/></wp:cNvGraphicFramePr>
<a:graphic xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main"><a:graphicData uri="http://schemas.openxmlformats.org/drawingml/2006/picture">
<pic:pic xmlns:pic="http://schemas.openxmlformats.org/drawingml/2006/picture"><pic:nvPicPr><pic:cNvPr id="0" name="{escape(name)}"/><pic:cNvPicPr/></pic:nvPicPr>
<pic:blipFill><a:blip r:embed="{rid}"/><a:stretch><a:fillRect/></a:stretch></pic:blipFill>
<pic:spPr><a:xfrm><a:off x="0" y="0"/><a:ext cx="{cx}" cy="{cy}"/></a:xfrm><a:prstGeom prst="rect"><a:avLst/></a:prstGeom></pic:spPr>
</pic:pic></a:graphicData></a:graphic></wp:inline></w:drawing></w:r></w:p>'''


body: list[str] = []
images: list[tuple[Path, str, str]] = []


def add_heading(text: str, level: int = 1) -> None:
    body.append(para(text, style=f"Heading{level}", keep_next=True, before=180, after=100))


def add_figure(filename: str, caption: str) -> None:
    rid = f"rId{10 + len(images)}"
    path = ROOT / "docs" / filename
    images.append((path, rid, filename))
    # 6.45 inch wide; dimensions are preserved from known repository screenshots.
    dims = {
        "01-architecture.png": (2588, 1536), "02-airflow-pipeline.png": (2618, 1410),
        "03-minio-offset-range.png": (2646, 1436), "04-dbt-pit-query.png.png": (1698, 1292),
        "05-redis-batch.png": (2810, 1446), "05-redis-realtime.png": (2638, 1440),
        "06-grafana-overview.png": (2938, 1630), "07-Inference-no.png": (2500, 1474),
        "07-Inference-send.png": (2504, 1476), "08-Kafka.png": (2532, 1554),
    }
    pxw, pxh = dims[filename]
    cx = 5_898_000
    cy = int(cx * pxh / pxw)
    body.append(image_paragraph(rid, filename, cx, cy, len(images)))
    body.append(para(caption, style="Caption", align="center", italic=True, after=160, line=250))


# Cover
body.append(para("TÀI LIỆU KỸ THUẬT", style="Title", align="center", bold=True, before=900, after=220))
body.append(para("THIẾT KẾ VÀ TRIỂN KHAI NỀN TẢNG DỮ LIỆU PHỤC VỤ TỐI ƯU PHÂN BỔ VOUCHER TRONG THƯƠNG MẠI ĐIỆN TỬ", style="Subtitle", align="center", bold=True, after=320))
body.append(para("Phạm vi: Data Engineering, tái dựng dữ liệu, feature platform, orchestration, chất lượng dữ liệu và quan sát vận hành", align="center", italic=True, after=480))
body.append(para("Người thực hiện", align="center", bold=True, before=260, after=60))
body.append(para("LƯƠNG THỊ HỒNG NHUNG", align="center", bold=True, after=180))
body.append(para("Mentor", align="center", bold=True, after=60))
body.append(para("NGUYỄN BẢO LONG", align="center", bold=True, after=320))
body.append(para("Mã nguồn khảo sát: LZD Uplift Feature Platform", align="center"))
body.append(para("Phiên bản khảo sát: commit bb61edf", align="center"))
body.append(para(f"Ngày lập tài liệu: {datetime.date.today():%d/%m/%Y}", align="center"))
body.append(para("Ghi chú phạm vi: quá trình xây dựng mô hình học máy và phát triển API suy luận không thuộc phần công việc được tuyên bố trong tài liệu này.", align="center", italic=True, before=520))
body.append(page_break())

add_heading("TÓM TẮT", 1)
body.append(para(
    "Tài liệu trình bày thiết kế và hiện thực một nền tảng dữ liệu phục vụ bài toán phân bổ voucher theo tác động tăng thêm trong thương mại điện tử. Khác với hệ thống dự đoán xác suất mua hàng thuần túy, bài toán nghiệp vụ cần nhận diện nhóm khách hàng có hành vi thay đổi do ưu đãi; vì vậy tính đúng đắn của dữ liệu lịch sử, biên thời gian và tính nhất quán giữa dữ liệu huấn luyện với dữ liệu phục vụ có ý nghĩa quyết định. Kiến trúc được xây dựng theo hướng lakehouse nhẹ trên MinIO–Parquet–DuckDB, kết hợp Kafka cho sự kiện, dbt cho biến đổi phân tầng, Redis cho feature online, Airflow cho điều phối, PostgreSQL cho sổ cái kiểm toán và Prometheus–Grafana–Loki cho quan sát vận hành. Một cơ chế tái dựng sự kiện xác định được phát triển để khôi phục lịch sử có thể kiểm chứng từ bảng đặc trưng ẩn danh; kết quả được kiểm soát bằng hợp đồng feature, point-in-time boundary, provenance, idempotency, checksum và quality gate. Thiết kế batch–realtime sử dụng namespace theo phiên bản và con trỏ kích hoạt nguyên tử nhằm tránh mixed-version traffic và hỗ trợ rollback. Kết quả của công trình là một data platform tái lập được, có thể truy nguyên, có ranh giới trách nhiệm rõ ràng với thành phần mô hình và API."
))
body.append(para("Từ khóa—uplift modeling; causal inference; event reconstruction; lakehouse; feature store; point-in-time correctness; data quality; MLOps.", italic=True))

add_heading("PHẠM VI VÀ PHƯƠNG PHÁP KHẢO SÁT", 1)
body.append(para(
    "Tài liệu được tổng hợp từ việc đọc mã nguồn Python, mô hình SQL/dbt, cấu hình feature, DAG Airflow, schema PostgreSQL, Docker Compose và bộ kiểm thử của repo. Các phát biểu về số lượng feature và cơ chế vận hành được đối chiếu trực tiếp với feature_spec.yml, fs_2026_08_v4.yaml, các mart Gold, mã sync Redis và stream consumer. Cơ sở lý thuyết được đặt trên các công trình học thuật về hiệu ứng điều trị cá thể, lakehouse, dataflow theo thời gian sự kiện, data validation và technical debt của hệ thống ML [1]–[7]."
))
body.append(table([
    ["Trong phạm vi đóng góp", "Ngoài phạm vi tuyên bố"],
    ["Ingestion Kafka; immutable lake; DLQ; offset identity", "Huấn luyện/thiết kế thuật toán uplift"],
    ["Reconstruction; dbt Bronze–Silver–Gold; PIT dataset", "Phát triển business logic của API suy luận"],
    ["Redis feature store; versioning; sync; rollback", "Tối ưu hyperparameter và đánh giá mô hình"],
    ["Airflow; data quality; audit; observability", "Thiết kế endpoint hoặc giao diện người dùng"],
], [4700, 4700]))

body.append(page_break())
add_heading("1. CƠ SỞ LÝ THUYẾT VÀ LẬP LUẬN THIẾT KẾ", 1)
add_heading("1.1. Bài toán nghiệp vụ: từ dự đoán phản hồi đến tác động tăng thêm", 2)
body.append(para(
    "Trong một chiến dịch voucher, tối đa hóa xác suất mua hàng không đồng nhất với tối đa hóa hiệu quả ngân sách. Một khách hàng có xác suất chuyển đổi cao có thể vẫn mua nếu không nhận voucher; ưu đãi cho trường hợp này làm giảm biên lợi nhuận mà không tạo doanh thu tăng thêm. Ngược lại, khách hàng có xác suất mua trung bình nhưng nhạy với khuyến mại có thể là đối tượng ưu tiên. Bài toán vì thế được mô hình hóa theo hiệu ứng điều trị có điều kiện (conditional average treatment effect—CATE), trong đó treatment T biểu diễn việc gửi voucher và outcome Y biểu diễn chuyển đổi."
))
body.append(equation("τ(x) = E[Y(1) − Y(0) | X = x]"))
body.append(para(
    "Đại lượng τ(x) đo chênh lệch kỳ vọng giữa hai kết quả tiềm năng của cùng một hồ sơ khách hàng x. Do không thể quan sát đồng thời Y(1) và Y(0) trên cùng cá thể, dữ liệu treatment/control và biên thời gian phải được bảo toàn nghiêm ngặt [1], [2]. Vai trò của nền tảng dữ liệu không phải suy ra τ(x), mà là tạo ra X đúng thời điểm, gắn đúng treatment/label, giữ provenance và cung cấp cùng một vector feature cho cả hai chế độ offline–online."
))
body.append(para("Từ mục tiêu nghiệp vụ, bốn yêu cầu kỹ thuật được suy ra:"))
body.extend([
    bullet("Tính nhân quả và chống leakage: không feature nào được sử dụng thông tin xảy ra sau thời điểm ra quyết định."),
    bullet("Tính nhất quán: tên, kiểu, thứ tự, giá trị mặc định và semantics của feature phải là một hợp đồng có phiên bản."),
    bullet("Tính kịp thời: trạng thái dài hạn được tính batch, tín hiệu ý định ngắn hạn được cập nhật theo event time."),
    bullet("Tính kiểm toán: mọi lần chạy, shard, version, quality check và quyết định tích hợp phải có dấu vết truy nguyên."),
])

add_heading("1.2. Point-in-time correctness và chống training–serving skew", 2)
body.append(para(
    "Với nhãn quan sát tại thời điểm tᵢ, tập feature hợp lệ chỉ gồm những giá trị có thời điểm hiệu lực không muộn hơn tᵢ. Điều kiện này tương đương một phép AS-OF join theo khóa thực thể và timestamp. Nếu dùng trạng thái mới nhất để ghép với nhãn quá khứ, thông tin tương lai bị rò rỉ vào tập huấn luyện, làm sai lệch đánh giá ngoại tuyến và suy giảm khi triển khai [6], [7]. Trong repo, training_dataset ghép serving_features với feat_user_realtime_pit trên user_id, dt và feature_ts; dữ liệu realtime được dựng với điều kiện nghiêm ngặt trước feature_ts."
))
body.append(equation("feature_time ≤ label_time;    event_time < feature_ts"))
body.append(para(
    "Feature contract phiên bản 4 quy định 71 trường batch và 12 trường realtime. Trong 71 trường batch có vector 30 feature bất biến dùng tại ranh giới mô hình. Việc giữ nguyên tên f* trên hot path tránh suy đoán alias lúc phục vụ; các tên nghiệp vụ bổ sung được duy trì để diễn giải và tái sử dụng. Schema hash, feature_spec_version và realtime_semantics_version tạo thành bộ nhận dạng tương thích. Đây là cơ chế phòng thủ trước schema drift và training–serving skew, phù hợp với quan điểm coi dữ liệu đầu vào là tài sản production ngang hàng với thuật toán [4]."
))
add_figure("04-dbt-pit-query.png.png", "Hình 1. Truy vấn kiểm chứng point-in-time và cấu trúc tập dữ liệu huấn luyện.")

add_heading("1.3. Lakehouse và phân tách storage–compute", 2)
body.append(para(
    "Kiến trúc lakehouse kết hợp định dạng mở và khả năng truy cập trực tiếp của data lake với quản trị, kiểm toán và tối ưu truy vấn vốn thuộc data warehouse [3]. Repo hiện thực biến thể gọn cho môi trường nghiên cứu: MinIO cung cấp object storage tương thích S3; Parquet là định dạng cột bất biến; DuckDB là engine phân tích; dbt quản lý đồ thị biến đổi và kiểm thử. Việc tách storage khỏi compute cho phép dữ liệu raw được giữ nguyên, nhiều tác vụ có thể đọc lại cùng nguồn và pipeline có thể replay mà không phụ thuộc trạng thái một tiến trình."
))

add_heading("1.4. Event time, idempotency và tính xác định", 2)
body.append(para(
    "Trong streaming, processing time phản ánh lúc hệ thống nhận sự kiện, còn event time phản ánh lúc hành vi thực sự xảy ra. Sự kiện trễ, gửi lặp và lệch đồng hồ là trạng thái bình thường của hệ thống phân tán; do đó cửa sổ feature phải dựa trên event_ts, đi kèm allowed lateness và future-skew policy. Mỗi event_id chỉ được áp dụng một lần; sự kiện quá hạn hoặc quá xa tương lai bị loại khỏi online overlay và được đo riêng. Cách nhìn thời gian logic/virtual time trong dataflow tạo nền tảng cho xử lý nhất quán khi thứ tự đến không ổn định [5]."
))
body.append(para(
    "Đối với dữ liệu lịch sử đã ẩn danh, yêu cầu xác định còn mạnh hơn: cùng target, contract, seed và runtime configuration phải sinh cùng tập sự kiện và event_id. Reconstruction engine vì thế cưỡng chế chuỗi P1–P4: feasibility → lexicographic optimization → seeded selection trong optimal pool → materialization → canonical ordering. Seed chỉ tác động sau khi đã có tập nghiệm tối ưu; UUIDv5 và khóa cấu hình bảo đảm replay không tạo danh tính mới."
))

add_heading("1.5. Reliability by design và ranh giới ML", 2)
body.append(para(
    "Hệ thống ML tích lũy technical debt không chỉ ở mã mô hình mà còn ở phụ thuộc dữ liệu, cấu hình, glue code và các consumer ngầm [4]. Kiến trúc vì vậy áp dụng fail-closed tại các ranh giới quan trọng: thiếu nguồn reconstruction thì dừng trước dbt; sai contract thì không sync; checksum hoặc sample parity không đạt thì không kích hoạt version; metadata không tương thích thì downstream không được coi là sẵn sàng. Model và API được xem là consumer ở biên hệ thống, không phải bằng chứng thay thế cho chất lượng data pipeline."
))

body.append(page_break())
add_heading("2. KIẾN TRÚC DATA PIPELINE VÀ Ý NGHĨA THIẾT KẾ", 1)
add_figure("01-architecture.png", "Hình 2. Kiến trúc tổng thể của LZD Uplift Feature Platform.")

add_heading("2.1. Kiến trúc logic nhiều lớp", 2)
body.append(table([
    ["Lớp", "Thành phần", "Trách nhiệm và ý nghĩa"],
    ["Ingestion", "Producer, Kafka, consumer, DLQ", "Thu nhận sự kiện; xác thực schema; tách lỗi; bảo toàn offset và event identity."],
    ["Storage", "MinIO, Parquet", "Raw bất biến, partition theo thời gian/topic/partition; nền tảng replay và audit."],
    ["Processing", "DuckDB, dbt", "Chuẩn hóa Bronze–Silver–Gold; PIT feature; kiểm thử contract và chất lượng."],
    ["Online feature", "Redis", "Đọc độ trễ thấp; tách batch namespace theo version và realtime overlay theo user."],
    ["Orchestration", "Airflow", "Quản lý dependency, retry, pool khóa DuckDB writer, lịch batch và quality gate."],
    ["Control plane", "PostgreSQL ops.*", "Sổ cái run, shard sync, DQ và audit tích hợp downstream."],
    ["Observability", "Prometheus, Grafana, Loki", "Metrics, dashboard và log tập trung để phát hiện/khoanh vùng sự cố."],
], [1400, 2300, 5700]))

add_heading("2.2. Hai đường dữ liệu bổ sung: Track A và Track B", 2)
body.append(para(
    "Track A giải quyết cold-start lịch sử. Từ full_trainset.csv, hệ thống tạo target có định danh, giải mã từng regime feature, tìm nghiệm sự kiện thỏa hard constraint, tối ưu theo thứ tự từ điển và materialize event có provenance. Dữ liệu được land vào raw/events_v2 và các bảng biz.* trước khi dbt tái tính feature. Gate A so sánh feature tái tính với target, nhờ đó kiểm chứng pipeline bằng phép round-trip thay vì chỉ kiểm tra job hoàn thành. Trường hợp vô nghiệm bị quarantine; target không bị sửa để làm đẹp kết quả."
))
body.append(para(
    "Track B tiếp nhận app event đang phát sinh từ Kafka. Consumer xác thực envelope, ghi raw Parquet theo topic/partition/offset-range và cập nhật realtime overlay bằng thao tác Lua nguyên tử. Hai track hội tụ tại lớp feature: Track A cung cấp lịch sử xác định cho backfill và huấn luyện; Track B cung cấp hành vi mới cho feature batch kế tiếp và tín hiệu realtime hiện tại. Cấu trúc này tránh trộn dữ liệu tổng hợp với dữ liệu quan sát mà không có provenance."
))
add_figure("08-Kafka.png", "Hình 3. Kafka topic và trạng thái luồng sự kiện trong môi trường triển khai.")
add_figure("03-minio-offset-range.png", "Hình 4. Object raw trên MinIO được định danh bằng topic/partition/offset-range để hỗ trợ replay có kiểm soát.")

add_heading("2.3. Biến đổi Bronze–Silver–Gold và hợp đồng dữ liệu", 2)
body.append(para(
    "Lớp staging chuẩn hóa snapshot và event schema. Lớp intermediate tách các phép biến đổi theo semantics: recency, counter, categorical, passthrough và behavior aggregate. Lớp Gold tạo hai sản phẩm có mục đích khác nhau: training_features giữ đúng vector 30F; serving_features chứa 71 trường batch; training_dataset bổ sung 12 feature realtime PIT cùng label, treatment và split. Phân tách sản phẩm ngăn bảng phục vụ bị gắn nhãn/điều trị không cần thiết và cho phép contract test riêng theo consumer."
))
body.append(para(
    "dbt được chọn vì đồ thị phụ thuộc rõ ràng, SQL có khả năng kiểm tra và test có thể chạy trong cùng DAG. Các macro generic kiểm tra uniqueness, non-null, range và đối chiếu cột với spec. Airflow thực hiện fail-fast trước dbt nếu raw/events_v2 hoặc biz.* chưa sẵn sàng, tránh trạng thái DAG xanh từng phần nhưng dữ liệu Gold vô nghĩa."
))

add_heading("2.4. Feature store batch–realtime và atomic activation", 2)
body.append(para(
    "Online store dùng hai namespace: fs:{version}:u:{user_id} cho batch hash và rt:u:{user_id} cho realtime overlay. Con trỏ fs:meta:active_version là điểm chuyển đổi duy nhất. Một version mới đi qua các trạng thái IN_PROGRESS → VALIDATING → ACTIVE; dữ liệu được chia 32 shard, có audit độc lập, có thể resume những shard chưa DONE. Sau khi so sánh row count, checksum và sample giữa DuckDB–Redis, con trỏ mới được đổi nguyên tử. Version cũ được giữ theo chính sách retention để rollback và garbage collection có kiểm soát."
))
add_figure("05-redis-batch.png", "Hình 5. Batch feature hash theo version và user trong Redis.")
add_figure("05-redis-realtime.png", "Hình 6. Realtime overlay theo user, tách khỏi batch version để cập nhật độc lập.")
body.append(para(
    "Ý nghĩa của thiết kế này là loại bỏ mixed-version traffic: không consumer nào đọc một phần user từ version cũ và phần còn lại từ version mới. Realtime overlay có TTL 3.600 giây, batch stale TTL 86.400 giây; dedup state được giữ 86.400 giây. Chính sách noeviction trên Redis làm lỗi dung lượng hiển thị rõ thay vì âm thầm loại key và tạo feature missing không xác định."
))

add_heading("2.5. Điều phối, chất lượng và khả năng quan sát", 2)
body.append(para(
    "Các DAG tạo thành chuỗi kiểm soát: DAG 00 bootstrap fixture và schema; DAG 60 reconstruction end-to-end; DAG 20 build/test dbt; DAG 40 sync và kích hoạt Redis; DAG 50 đánh giá freshness, completeness, drift/skew; DAG 10 giám sát/compact streaming; DAG 99 cung cấp thao tác vận hành. DuckDB chỉ cho một writer nên các task ghi dùng Airflow pool duckdb_writer; max_active_runs=1 ngăn hai lần build tranh chấp. Retry được áp dụng ở ranh giới có thể phục hồi, trong khi vi phạm contract dùng fail-fast."
))
add_figure("02-airflow-pipeline.png", "Hình 7. Trạng thái pipeline và quan hệ điều phối trên Airflow.")
body.append(para(
    "Prometheus thu metrics từ service, exporter và Pushgateway; Grafana hợp nhất chỉ báo pipeline, Kafka, Redis, MinIO, Airflow và data quality; Loki nhận structured log qua Promtail. PostgreSQL ops.pipeline_run, ops.feature_sync_audit, ops.feature_sync_shard và ops.dq_result đóng vai trò control plane có thể truy vấn. Nhờ vậy, quan sát không dừng ở trạng thái task mà trả lời được: dữ liệu nào, version nào, bao nhiêu dòng, shard nào lỗi và quality gate nào không đạt."
))
add_figure("06-grafana-overview.png", "Hình 8. Dashboard tổng quan vận hành và chất lượng của data pipeline.")

body.append(page_break())
add_heading("3. TRIỂN KHAI", 1)
add_heading("3.1. Yêu cầu và cấu hình môi trường", 2)
body.append(para(
    "Môi trường tham chiếu dùng Docker Compose v2, Git LFS, tối thiểu 4 CPU và 12 GB RAM (khuyến nghị 16 GB), cùng dung lượng đĩa phù hợp cho Parquet và volume. Hai tập CSV lớn được quản lý bằng Git LFS; trước khi chạy cần xác nhận không còn LFS pointer. Các credential mặc định chỉ phù hợp local. Trong production cần thay khóa Airflow, mật khẩu PostgreSQL/MinIO, reload token và bổ sung quản lý secret, TLS, network policy và backup."
))
body.append(table([
    ["Nhóm", "Thiết lập chính"],
    ["Thời gian", "TZ; reference_ts; allowed lateness 600 s; future skew 300 s"],
    ["Feature", "feature_spec.yml v4; fs_2026_08_v4; 71 batch + 12 realtime"],
    ["Sync", "32 shard; batch size 1.000; giữ 2 version; sample validation 500"],
    ["Retention", "batch stale TTL 86.400 s; realtime TTL 3.600 s; dedup TTL 86.400 s"],
    ["Reproducibility", "constraint/encoding/objective/selection/solver/behavior version + seed"],
], [2700, 6700]))

add_heading("3.2. Trình tự dựng hệ thống", 2)
body.append(para("Quy trình triển khai local có thể tái lập theo thứ tự sau:"))
body.extend([
    bullet("Clone đúng commit/release, chạy git lfs pull, kiểm tra kích thước data/full_trainset.csv và full_testset.csv."),
    bullet("Tạo .env từ mẫu nếu cần ghi đè; xác thực docker compose --profile all config --quiet."),
    bullet("Chạy scripts/stack.sh doctor, init và up; profile all chỉ cần khi muốn phát sinh traffic realtime liên tục."),
    bullet("Kích hoạt DAG 00 để nạp fixture; DAG 60 ở mode backfill với land=true để land Track A."),
    bullet("Chạy DAG 20 để build/test hai Gold contract; sau đó DAG 40 để sync và atomic activate feature version."),
    bullet("Chạy DAG 50 và kiểm tra Grafana/Loki/PostgreSQL audit; chỉ coi hệ thống sẵn sàng khi quality gate đạt."),
])

add_heading("3.3. Cơ chế reconstruction có thể tái lập", 2)
body.append(para(
    "Feature set fs_2026_08_v4 gồm T1=2, T2=7 và T3=21, tổng cộng 30 trường. Các regime LN, REC, CAT và PASS có hàm decode/encode và tiêu chí so sánh riêng. Runtime fingerprint bao gồm phiên bản constraint model, encoding, objective, selection policy, solver, behavior policy và seed. Với mỗi target, engine tạo tập feasible, lấy toàn bộ argmin theo objective từ điển, chọn xác định trong optimal pool, sinh occurrence/event_id và sắp thứ tự canonical. Provenance phân biệt OBSERVED, DETERMINISTIC_DERIVED và SYNTHETIC; mọi future event bắt buộc mang nguồn SYNTHETIC."
))
body.append(para(
    "Các gate end-to-end kiểm tra: feature tái tính khớp target; reconstruction không dùng label/is_treat; số lượng sự kiện nằm trong giới hạn; future event không vi phạm reference timestamp; source readiness đạt trước dbt. Kiểu kiểm chứng này mạnh hơn việc đối chiếu file đầu ra vì nó xác minh các bất biến nghiệp vụ và thời gian."
))

add_heading("3.4. Ingestion và xử lý realtime", 2)
body.append(para(
    "Producer phát event envelope có event_id, user_id, event_type, event_ts và payload. Consumer kiểm tra schema; lỗi được chuyển DLQ thay vì làm dừng luồng hợp lệ. Micro-batch Parquet dùng object key chứa offset đầu–cuối. Replay cùng offset và cùng payload không tạo object mới; cùng offset nhưng nội dung khác bị fail-closed. Realtime counter dùng bucket theo event time và Lua để hợp nhất check-dedup/update/TTL thành thao tác nguyên tử. DAG 10 định kỳ đo p50/p95/max ingestion lag, cảnh báo giờ không có file và compact part file theo event_id."
))

add_heading("3.5. Đồng bộ feature và phát hành version", 2)
body.append(para(
    "DAG 40 mở audit cho feature_version xác định theo logical date, tạo trước 32 shard, chỉ chạy shard chưa DONE và ghi số dòng/thời gian/lỗi của từng shard. Sau sync, job đối chiếu expected_rows với written_rows, checksum tổng hợp và mẫu user giữa offline store và Redis. Metadata của version chứa schema hash và semantics version. Khi validation đạt, active pointer được đổi; nếu lỗi, version không nhận traffic. Cách phát hành này tương đương blue–green deployment ở lớp dữ liệu."
))

add_heading("3.6. Kiểm chứng tích hợp tại ranh giới model/API", 2)
body.append(para(
    "Mặc dù model và API không nằm trong phạm vi công việc được tuyên bố, hai ảnh suy luận được sử dụng như bằng chứng tích hợp downstream: cùng một feature platform có thể dẫn đến NO_VOUCHER hoặc SEND_VOUCHER tùy kết quả của consumer. Giá trị của phần Data Engineering nằm ở việc API nhận đúng user state, đúng active feature version, đúng contract và có audit; không nằm ở thuật toán tính score hoặc logic endpoint."
))
add_figure("07-Inference-no.png", "Hình 9. Bằng chứng tích hợp downstream với kết quả không gửi voucher; API chỉ là consumer của feature platform.")
add_figure("07-Inference-send.png", "Hình 10. Bằng chứng tích hợp downstream với kết quả gửi voucher; không được diễn giải là đóng góp phát triển model/API.")

add_heading("3.7. Kiểm thử và tiêu chí nghiệm thu", 2)
body.append(para(
    "Bộ kiểm thử bao phủ schema event, stream consumer, seed identity, feature spec, business alias, sync audit, compatibility và toàn bộ reconstruction: contract, solver ordering, snapshot, persistence, handoff, sink, warehouse readiness và end-to-end. Tiêu chí nghiệm thu đề xuất gồm: (i) test suite pass; (ii) dbt run/test pass cho +serving_features và +training_dataset; (iii) 71/12 feature đúng spec; (iv) checksum và sample parity đạt trước activation; (v) replay không đổi event/object identity; (vi) dashboard có freshness, completeness, skew và latency; (vii) rollback về version trước thực hiện được mà không mixed traffic."
))

add_heading("3.8. Giới hạn và hướng hoàn thiện", 2)
body.extend([
    bullet("Stack hiện tối ưu cho môi trường nghiên cứu/single-node; Kafka replication factor 1, DuckDB single-writer và credential local chưa đáp ứng HA production."),
    bullet("MinIO–Parquet chưa có transaction log kiểu Delta/Iceberg; atomicity được hiện thực ở object identity và Redis activation, không phải ACID table toàn cục."),
    bullet("Dữ liệu ẩn danh buộc reconstruction dùng giả thuyết semantics; cần phân biệt sự kiện tổng hợp với quan sát thật và không sử dụng chúng như bằng chứng hành vi thực."),
    bullet("Cần bổ sung kiểm thử tải dài hạn, disaster recovery, backup/restore, secret rotation, encryption và SLO/error budget trước khi triển khai quy mô lớn."),
    bullet("Nên lượng hóa chi phí voucher, uplift calibration và policy constraint ở lớp quyết định bởi nhóm model/product; nền tảng dữ liệu chỉ cung cấp contract và audit tương ứng."),
])

body.append(page_break())
add_heading("4. KẾT LUẬN VÀ ĐÓNG GÓP", 1)
body.append(para(
    "Công trình chuyển một bài toán tối ưu khuyến mại có yếu tố phản thực thành một hệ thống dữ liệu có thể vận hành và kiểm chứng. Lập luận kiến trúc bắt đầu từ yêu cầu nhân quả: feature phải đúng thời điểm; từ yêu cầu chi phí: quyết định cần trạng thái dài hạn lẫn ý định ngắn hạn; từ yêu cầu production: mọi thay đổi phải có version, quality gate, rollback và audit. Do đó, lakehouse, event-time streaming, PIT transformation, feature contract và atomic activation không phải tập hợp công nghệ rời rạc mà là các cơ chế cùng bảo vệ tính đúng đắn của quyết định voucher."
))
body.append(para("Các đóng góp kỹ thuật chính của phần công việc Data Engineering gồm:"))
body.extend([
    bullet("Đề xuất và hiện thực kiến trúc hợp nhất batch–stream với raw lake bất biến, hai track dữ liệu có provenance và một lớp feature thống nhất."),
    bullet("Xây dựng reconstruction engine xác định theo hard constraint, lexicographic optimization, seeded selection sau argmin, UUIDv5 và canonical ordering."),
    bullet("Thiết kế dbt pipeline Bronze–Silver–Gold tạo training/serving product có point-in-time correctness và contract kiểm chứng được."),
    bullet("Xây dựng online feature store 71 batch + 12 realtime, namespace theo version, shard-resume, checksum validation, atomic activation, rollback và GC."),
    bullet("Hiện thực idempotency theo event_id và offset-range, event-time windows, lateness/future-skew policy và DLQ."),
    bullet("Thiết lập orchestration, sổ cái audit và observability end-to-end để trạng thái vận hành có bằng chứng định lượng."),
])
body.append(para(
    "Đóng góp cốt lõi không phải một mô hình dự đoán mới hay một API mới, mà là hạ tầng dữ liệu làm cho mô hình bất kỳ tại ranh giới tương thích có thể được huấn luyện, phát hành và giám sát trên dữ liệu nhất quán. Đây là điều kiện cần để kết quả nghiên cứu uplift chuyển thành năng lực sản phẩm có thể tái lập, truy nguyên và bảo trì."
))

add_heading("TÀI LIỆU THAM KHẢO", 1)
refs = [
    "[1] E. Diemert, A. Betlei, C. Renaudin, and M. Amini, “A Large Scale Benchmark for Individual Treatment Effect Prediction and Uplift Modeling,” Proc. AdKDD, 2018.",
    "[2] E. W. Zhao, A. Harinen, and others, “Uplift Modeling with Multiple Treatments and General Response Types,” Proc. IEEE ICDM, 2017.",
    "[3] M. Armbrust, A. Ghodsi, R. Xin, and M. Zaharia, “Lakehouse: A New Generation of Open Platforms that Unify Data Warehousing and Advanced Analytics,” CIDR, 2021.",
    "[4] D. Sculley et al., “Hidden Technical Debt in Machine Learning Systems,” Advances in Neural Information Processing Systems, vol. 28, 2015.",
    "[5] M. Abadi and M. Isard, “Timely Dataflow: A Model,” Formal Techniques for Distributed Objects, Components, and Systems, pp. 131–145, 2015.",
    "[6] E. Breck, M. Zinkevich, N. Polyzotis, S. Whang, and S. Roy, “Data Validation for Machine Learning,” Proc. SysML, 2019.",
    "[7] A. N. Modi et al., “TFX: A TensorFlow-Based Production-Scale Machine Learning Platform,” Proc. KDD, 2017.",
    "[8] LZD Data Platform Team, “LZD Uplift Feature Platform,” repository commit bb61edf, khảo sát ngày 22-08-2026.",
]
body.extend(para(x, after=80, line=260) for x in refs)


sect = '''<w:sectPr><w:pgSz w:w="11906" w:h="16838"/><w:pgMar w:top="1134" w:right="1134" w:bottom="1134" w:left="1134" w:header="708" w:footer="708" w:gutter="0"/><w:cols w:space="708"/><w:docGrid w:linePitch="360"/></w:sectPr>'''
document = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships" xmlns:wp="http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing" xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" xmlns:pic="http://schemas.openxmlformats.org/drawingml/2006/picture"><w:body>{''.join(body)}{sect}</w:body></w:document>'''

styles = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:styles xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
<w:docDefaults><w:rPrDefault><w:rPr><w:rFonts w:ascii="Times New Roman" w:hAnsi="Times New Roman" w:eastAsia="Times New Roman"/><w:sz w:val="24"/><w:szCs w:val="24"/><w:lang w:val="vi-VN"/></w:rPr></w:rPrDefault><w:pPrDefault><w:pPr><w:jc w:val="both"/></w:pPr></w:pPrDefault></w:docDefaults>
<w:style w:type="paragraph" w:default="1" w:styleId="Normal"><w:name w:val="Normal"/><w:qFormat/><w:pPr><w:jc w:val="both"/></w:pPr></w:style>
<w:style w:type="paragraph" w:styleId="Title"><w:name w:val="Title"/><w:basedOn w:val="Normal"/><w:qFormat/><w:rPr><w:b/><w:sz w:val="36"/><w:color w:val="17365D"/></w:rPr></w:style>
<w:style w:type="paragraph" w:styleId="Subtitle"><w:name w:val="Subtitle"/><w:basedOn w:val="Normal"/><w:qFormat/><w:rPr><w:b/><w:sz w:val="28"/><w:color w:val="1F4E79"/></w:rPr></w:style>
<w:style w:type="paragraph" w:styleId="Heading1"><w:name w:val="heading 1"/><w:basedOn w:val="Normal"/><w:next w:val="Normal"/><w:qFormat/><w:pPr><w:outlineLvl w:val="0"/></w:pPr><w:rPr><w:b/><w:sz w:val="28"/><w:color w:val="17365D"/></w:rPr></w:style>
<w:style w:type="paragraph" w:styleId="Heading2"><w:name w:val="heading 2"/><w:basedOn w:val="Normal"/><w:next w:val="Normal"/><w:qFormat/><w:pPr><w:outlineLvl w:val="1"/></w:pPr><w:rPr><w:b/><w:sz w:val="25"/><w:color w:val="1F4E79"/></w:rPr></w:style>
<w:style w:type="paragraph" w:styleId="Caption"><w:name w:val="Caption"/><w:basedOn w:val="Normal"/><w:rPr><w:i/><w:sz w:val="20"/><w:color w:val="404040"/></w:rPr></w:style>
<w:style w:type="paragraph" w:styleId="ListParagraph"><w:name w:val="List Paragraph"/><w:basedOn w:val="Normal"/><w:pPr><w:ind w:left="720" w:hanging="360"/></w:pPr></w:style>
<w:style w:type="table" w:styleId="TableGrid"><w:name w:val="Table Grid"/><w:tblPr><w:tblBorders><w:top w:val="single" w:sz="4" w:color="B4C6E7"/><w:left w:val="single" w:sz="4" w:color="B4C6E7"/><w:bottom w:val="single" w:sz="4" w:color="B4C6E7"/><w:right w:val="single" w:sz="4" w:color="B4C6E7"/><w:insideH w:val="single" w:sz="4" w:color="B4C6E7"/><w:insideV w:val="single" w:sz="4" w:color="B4C6E7"/></w:tblBorders></w:tblPr></w:style>
</w:styles>'''

numbering = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?><w:numbering xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:abstractNum w:abstractNumId="0"><w:multiLevelType w:val="hybridMultilevel"/><w:lvl w:ilvl="0"><w:start w:val="1"/><w:numFmt w:val="bullet"/><w:lvlText w:val="•"/><w:lvlJc w:val="left"/><w:pPr><w:tabs><w:tab w:val="num" w:pos="720"/></w:tabs><w:ind w:left="720" w:hanging="360"/></w:pPr><w:rPr><w:rFonts w:ascii="Symbol" w:hAnsi="Symbol"/></w:rPr></w:lvl></w:abstractNum><w:num w:numId="1"><w:abstractNumId w:val="0"/></w:num></w:numbering>'''

content_types = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Default Extension="png" ContentType="image/png"/><Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/><Override PartName="/word/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.styles+xml"/><Override PartName="/word/numbering.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.numbering+xml"/><Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/><Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/></Types>'''

root_rels = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/><Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/></Relationships>'''

doc_rels_parts = [
    '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>',
    '<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/numbering" Target="numbering.xml"/>',
]
for path, rid, name in images:
    doc_rels_parts.append(f'<Relationship Id="{rid}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/image" Target="media/{escape(name)}"/>')
doc_rels = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">' + ''.join(doc_rels_parts) + '</Relationships>'

now = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
core = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?><cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><dc:title>Thiết kế và triển khai nền tảng dữ liệu phục vụ tối ưu phân bổ voucher</dc:title><dc:subject>Technical documentation - Data Engineering</dc:subject><dc:creator>LZD Data Platform Team</dc:creator><dc:description>Tài liệu kỹ thuật phạm vi Data Engineering; không tuyên bố phần model và API.</dc:description><dcterms:created xsi:type="dcterms:W3CDTF">{now}</dcterms:created><dcterms:modified xsi:type="dcterms:W3CDTF">{now}</dcterms:modified></cp:coreProperties>'''
app = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes"><Application>Microsoft Office Word</Application><DocSecurity>0</DocSecurity><ScaleCrop>false</ScaleCrop><Company>LZD Data Platform Team</Company><AppVersion>16.0000</AppVersion></Properties>'''

OUT.parent.mkdir(parents=True, exist_ok=True)
with zipfile.ZipFile(OUT, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("[Content_Types].xml", content_types)
    z.writestr("_rels/.rels", root_rels)
    z.writestr("word/document.xml", document)
    z.writestr("word/styles.xml", styles)
    z.writestr("word/numbering.xml", numbering)
    z.writestr("word/_rels/document.xml.rels", doc_rels)
    z.writestr("docProps/core.xml", core)
    z.writestr("docProps/app.xml", app)
    for path, _, name in images:
        z.write(path, f"word/media/{name}")

print(OUT)
print(f"size={OUT.stat().st_size} images={len(images)}")
