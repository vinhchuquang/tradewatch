# ADR 0001 — Chọn at-least-once + idempotent sink, không dùng Kafka transaction

- **Ngày:** 2026-09-05
- **Trạng thái:** Đề xuất (chưa triển khai — chốt sau thí nghiệm 01 và 07)
- **Liên quan:** thí nghiệm 01, 07; `src/tradewatch/sinks/`

## Bối cảnh

Pipeline `Kafka → Spark Structured Streaming → Postgres` cần đảm bảo mỗi trade xuất hiện **đúng một lần** trong sink, kể cả khi job chết giữa micro-batch.

Có ba đường:

1. **Kafka transaction / exactly-once semantics** ở tầng broker + producer
2. **Spark checkpoint + sink transactional** (two-phase commit)
3. **At-least-once + sink idempotent** (upsert theo khóa tự nhiên)

## Quyết định

Chọn **(3)**: Spark ghi at-least-once, sink dùng `INSERT ... ON CONFLICT (trade_id) DO UPDATE`.

## Lý do

- Binance đã cấp `trade_id` — **khóa tự nhiên có sẵn**, idempotency gần như miễn phí. Không phải tự sinh dedup key
- Spark checkpoint tự nó **chỉ** cho at-least-once với sink tùy ý. Muốn exactly-once phải có sink idempotent hoặc transactional — nghĩa là (1) không cứu được vế sink
- (2) đúng về lý thuyết nhưng phức tạp hơn nhiều lần cho cùng một kết quả quan sát được
- **Lý do quan trọng nhất:** đường (3) buộc ranh giới giữa "Spark bảo đảm gì" và "sink bảo đảm gì" phải hiện rõ trong code. Đường (1) che ranh giới đó sau một config flag — hôm nó hỏng thì không ai biết vế nào hỏng

## Hệ quả

- Sink **buộc** phải có unique constraint trên `trade_id`. Thiếu nó là mất bảo đảm — cần một test chặn trong CI
- Số liệu trung gian có thể thấy record trùng trước khi upsert. Metric phải đọc *sau* sink, không phải trước
- Nếu thêm sink không hỗ trợ upsert (ví dụ append vào Parquet), **bảo đảm này không tự động chuyển sang** — đó là nội dung thí nghiệm 08

## Cách bác bỏ quyết định này

Nếu thí nghiệm 01 cho thấy còn duplicate sót lại *sau* upsert, ADR này sai và phải mở lại. Ghi kết quả vào `experiments/01-*/README.md`.
