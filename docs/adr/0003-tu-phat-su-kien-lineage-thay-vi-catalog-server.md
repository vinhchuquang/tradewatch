# ADR 0003 — Tự phát sự kiện lineage theo chuẩn OpenLineage, không dựng catalog server

- **Ngày:** 2026-09-18
- **Trạng thái:** Chấp nhận
- **Loại:** **ADR bác bỏ** — loại DataHub / OpenMetadata / Amundsen / Marquez, kèm thứ thay thế

## Bối cảnh

Pipeline hiện có bốn chỗ giữ schema: Kafka topic, Parquet footer, `features/`, và Postgres. Khi một
trong bốn chỗ đó đổi mà ba chỗ còn lại không biết, hệ thống **không hỏng ngay** — nó tiếp tục chạy và
trả ra số sai. Đây là chế độ hỏng im lặng, và hiện tại repo không có cơ chế nào phát hiện nó.

Ba câu hỏi chưa trả lời được:

1. Bảng này lấy dữ liệu từ đâu, và job nào đang đọc nó? (**lineage**)
2. Schema khai báo trong `contracts/` có còn khớp với schema thật đang chạy không? (**catalog drift**)
3. Bảng này còn tươi không, hay stream đã chết từ tối qua mà query vẫn ra số? (**freshness**)

## Quyết định

**Không dựng catalog server.** Thay vào đó:

- Lấy **OpenLineage làm đặc tả định dạng sự kiện** (`job`, `run`, `inputs[]`, `outputs[]`, `facets`).
  Đây là một spec mở, không phải một service — dùng nó không kéo theo hạ tầng nào
- Mỗi job tự `INSERT` một dòng `lineage_event` vào **Postgres đang có**, ở cuối mỗi micro-batch
- Catalog không phải là bản ghi schema nào cả. Catalog là **phép trừ** giữa hai nguồn:
  `declared` (từ `contracts/`) và `observed` (từ `information_schema`, Kafka, Parquet footer)
- Truy vấn lineage bằng SQL đệ quy (`WITH RECURSIVE`) trên chính bảng đó

**Không thêm container nào.** Kiến trúc vẫn là 2 container.

## Lý do

**Không khớp ngân sách tài nguyên.** DataHub tối thiểu cần Kafka riêng + Elasticsearch + MySQL + GMS
+ frontend. Elasticsearch một mình đã đòi 2GB heap. Cộng vào ngân sách RAM hiện tại thì pipeline và
môi trường làm việc tranh nhau — vi phạm ưu tiên số một ở `CLAUDE.md`. OpenMetadata và Amundsen cùng
hình dạng. Marquez nhẹ hơn (Postgres + API + web) nhưng vẫn là 2–3 container cho một việc mà 1 bảng làm được.

**Không chặn được lỗi cụ thể nào mà thứ nhẹ hơn không chặn được.** Luật của repo: *mỗi thành phần chỉ
được vào nếu nó chặn được một lỗi cụ thể*. Ba câu hỏi ở trên đều trả lời được bằng SQL trên một bảng.
Cái mà catalog server thêm vào là **giao diện web và tích hợp nhiều nguồn** — giá trị thật khi có
hàng trăm dataset và nhiều team, không phải ở quy mô này.

**Lineage tự sinh làm hỏng chỗ có giá trị nhất.** `OpenLineageSparkListener` sinh lineage bằng cách
parse query plan của Spark. Nó bỏ qua đúng phần chứa quyết định: *cái gì đáng gọi là một dataset*,
*ranh giới một run ở đâu*, *facet nào cần ghi để sau này trả lời được câu hỏi vận hành*. Tự phát sự
kiện bắt phải trả lời những câu đó bằng tay — và đó là toàn bộ nội dung kỹ thuật của lineage.

**Ảnh hưởng disk:** một dòng `lineage_event` ≈ 400 byte. Trigger 10s → ~8.600 dòng/ngày → **<5MB/ngày,
<150MB/tháng**. Dưới 1,5% ngân sách. Xoá bằng `DROP PARTITION` theo tháng như mọi bảng khác, không `DELETE`.

## Hệ quả

- **Việc phát sự kiện phải nằm trong T0**, không lắp sau. Lineage lắp vào sau là khảo cổ học: dữ liệu
  cần để dựng lại đồ thị chưa từng được ghi, nên không dựng lại được. Đây là hệ quả cứng nhất của ADR này
- Truy vấn đồ thị cần **recursive CTE** — kỹ thuật SQL trước đó không nằm trong phạm vi. Phạm vi SQL của
  dự án mở rộng theo
- Không có giao diện web. Lineage đọc bằng SQL, hoặc render thành DOT/Mermaid trong README của thí nghiệm
- **Khi `observed` lệch `declared`: `observed` thắng về sự thật, `declared` thắng về thẩm quyền.**
  Hệ thống báo động và buộc con người hoặc sửa contract hoặc rollback migration. Catalog **không được**
  tự cập nhật `declared` cho khớp thực tế — làm thế thì nó thành cái gương, và gương không bao giờ
  báo được là mặt bẩn
- Chỉ phủ được lineage ở mức **dataset**, không mức **cột**. Chấp nhận có chủ đích: column-level lineage
  cần phân tích query plan, tức là đúng thứ vừa bác bỏ

## Bài học kiến trúc rút ra

**"Chúng ta cần data catalog" hầu như luôn là triệu chứng, không phải nhu cầu.** Triệu chứng của việc
hệ thống đã chạy nhiều năm mà không phát metadata nào. Lúc đó mua catalog server cũng không dựng lại
được quá khứ — nó chỉ bắt đầu ghi từ ngày cài. Chi phí thật của lineage không nằm ở công cụ, nằm ở
việc **quyết định phát sự kiện từ ngày đầu**.

## Xem lại khi nào

- Số dataset vượt ~30, hoặc có người thứ hai cần tra cứu mà không đọc được SQL
- Hoặc: đến khi làm freshness mà truy vấn SQL thủ công **không** trả lời nổi câu hỏi "bảng này tươi
  tới đâu" — lúc đó mới có bằng chứng cụ thể để cân nhắc Marquez (nhẹ nhất trong nhóm, cùng dùng Postgres)
