# ADR 0002 — Không dùng Cloudflare để chạy pipeline; chỉ dùng làm mặt tiền

- **Ngày:** 2026-09-05
- **Trạng thái:** Chấp nhận
- **Loại:** **ADR bác bỏ** — ghi lại một hướng đã bị loại, và vì sao

> ADR bác bỏ giá trị hơn ADR đồng ý. Nó ngăn người sau (kể cả chính bạn 3 tháng nữa) đi lại con đường đã biết là sai.

## Bối cảnh

Ràng buộc: chi phí hạ tầng bằng 0, không gắn payment method, disk local hạn chế. Cloudflare có gói free không đòi payment method. Câu hỏi tự nhiên: đẩy pipeline lên Cloudflare cho khỏi tốn tài nguyên local?

## Quyết định

**Không.** Pipeline (Kafka + Spark) chạy 100% local. Cloudflare chỉ giữ vai **mặt tiền công khai**: Worker nhận aggregate → D1 → Pages hiển thị dashboard. Cộng Tunnel để demo Spark UI khi cần.

## Lý do

**Kỹ thuật — Cloudflare không thể chạy được:**

- Workers là isolate ngắn: **10ms CPU/lần gọi**. Kafka consumer và Spark job là process chạy dài. Không khớp về bản chất, không phải về cấu hình
- Không có JVM trên Workers. Spark cần JVM
- Cloudflare Containers **không có free tier**
- R2 (object storage) **bắt buộc gắn payment method** dù chỉ dùng free tier → vi phạm ràng buộc trên
- Vectorize chỉ có ở gói paid. Workers AI không nằm trong danh sách free plan hiện tại

**Khả năng chẩn đoán — quan trọng hơn:**

Managed broker không cho kill broker để quan sát failover, không expose log segment, không cho tune retention. Bộ thí nghiệm 01–07 dựa hoàn toàn vào khả năng **làm hệ thống hỏng có chủ đích** rồi đo hậu quả. Đưa Kafka sang managed service là xóa sổ toàn bộ bộ thí nghiệm đó.

## Hệ quả

- Máy local là điểm chết duy nhất. Chấp nhận — không có SLA với ai
- Dashboard chỉ có dữ liệu khi máy đang chạy. Chấp nhận, và ghi rõ trên dashboard
- Ngân sách ghi của D1 (100k row/ngày ≈ 1,15 row/giây) trở thành **ràng buộc thiết kế cứng**: serving layer buộc phải là aggregate, không được raw. Đây là thí nghiệm 12
- Endpoint Worker phải có shared secret (trust boundary — không được lazy chỗ này)

## Bài học kiến trúc rút ra

**Biết cái gì không chạy được ở đâu là kỹ năng kiến trúc.** Vẽ ranh giới đúng ngay từ đầu tiết kiệm vài tối so với việc cố nhồi Kafka lên Workers rồi mới phát hiện.

## Xem lại khi nào

Nếu Cloudflare mở free tier cho Containers, hoặc nếu dự án cần chạy 24/7 cho người khác dùng.
