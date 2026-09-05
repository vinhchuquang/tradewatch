# tradewatch

Pipeline phát hiện bất thường realtime trên luồng giao dịch: `Kafka → Spark Structured Streaming → Postgres`, cộng một dashboard công khai chạy trên Cloudflare Workers + D1.

Điểm khác biệt của repo này không phải là pipeline — mà là **bộ thí nghiệm tái lập được chứng minh pipeline vẫn đúng khi hệ thống hỏng**. Mỗi thí nghiệm là một câu hỏi, một cách chạy, và một câu trả lời viết ra.

## Chạy thử

```bash
cp .env.example .env      # không cần key: nguồn Binance là public
make up                   # kafka + postgres (2 container; Spark chạy trên host)
make ingest               # bắt đầu nhận trade thật từ Binance WebSocket
make stream               # chạy streaming job
make down                 # dừng
make clean                # dọn volume, trả lại disk
```

Yêu cầu: Docker, `uv`, JRE 17 headless, **≥9GB disk trống** (dùng ~5,5GB, chừa ≥3GB). Spark cài trực tiếp bằng `uv`, không đóng container — image `apache/spark` ~2GB so với ~700MB. Trên Windows chạy trong WSL2 — xem `docs/runbook.md`.

## Kiến trúc

```
[Binance WS] → ingest → mess → [Kafka KRaft] → [Spark] ─┬→ [Postgres]  (sink idempotent, serving)
                                                         └→ [Parquet]   (lakehouse, backfill)
                                                              │
                                              aggregate 10s   ▼
                                            [CF Worker] → [D1] → [Pages: dashboard]
```

`mess/` là fault injector tự viết: chèn record trễ, duplicate, null, và schema drift. Các API realtime công khai sạch hơn dữ liệu production, nên độ bẩn phải tự tạo.

Pipeline chạy hoàn toàn local. Cloudflare chỉ làm mặt tiền — lý do ở [ADR 0002](docs/adr/0002-khong-dung-cloudflare-cho-pipeline.md).

## Thí nghiệm

Mỗi thí nghiệm chạy bằng `make exp-NN`, kết quả trong `experiments/NN-*/README.md`.

| # | Câu hỏi |
|---|---|
| 01 | Job chết giữa micro-batch: mất record hay nhân đôi record? |
| 02 | Record đến trễ 3 giờ thì bị drop hay sửa kết quả cũ? Cái giá của watermark là gì? |
| 03 | Thiếu watermark thì state phình tới đâu trước khi hết disk? |
| 04 | Một key chiếm phần lớn traffic: đo hot partition thế nào, sửa bằng gì? |
| 05 | Consumer lag tăng — nút cổ chai thật nằm ở đâu? |
| 06 | Đổi schema: thay đổi nào backward-compatible, thay đổi nào phá downstream? |
| 07 | Exactly-once đến từ Spark hay từ sink? |
| 08 | Backfill 3 tháng mà không dừng stream và không đếm trùng |
| 09 | Feature lúc train và lúc serve có giống nhau? Chứng minh bằng gì? |
| 10 | Small files: chậm thêm bao nhiêu, compact xong nhanh lại bao nhiêu? |
| 11 | Model xấu đi mà không ai biết: alert có kêu không? |
| 12 | Serving layer trong ngân sách ghi cố định (D1: 100k row/ngày) |

## Quyết định thiết kế

- [ADR 0001](docs/adr/0001-at-least-once-plus-idempotent-sink.md) — at-least-once + idempotent sink, không dùng Kafka transaction
- [ADR 0002](docs/adr/0002-khong-dung-cloudflare-cho-pipeline.md) — không chạy pipeline trên Cloudflare

Nguyên tắc chọn tool: **mỗi thành phần chỉ được vào nếu nó chặn được một lỗi cụ thể.** Đã bác bỏ có chủ đích: ClickHouse, Delta/Iceberg, Schema Registry (server), Debezium, Airflow, Feast, MLflow, Prometheus/Grafana, MinIO, HDFS, Kubernetes, Terraform. Lý do từng cái nằm trong ADR khi được viết.

## Definition of Done

Một thí nghiệm tính là xong khi: test pass trong CI, chạy lại được bằng một lệnh, và có **câu trả lời viết ra** — không chỉ có số.

## Cấu trúc

```
src/tradewatch/
  contracts/   schema Avro có version; compatibility được test trong CI
  ingest/      Binance WS + REST → Kafka
  mess/        fault injection
  stream/      Spark Structured Streaming jobs
  features/    hàm feature dùng chung cho CẢ train và serve — parity sống ở đây
  ml/          train (batch) + score (online)
  sinks/       idempotent upsert
  publish/     đẩy aggregate lên Cloudflare Worker
tests/         unit / contract / integration
experiments/   13 thí nghiệm
infra/         cloudflare worker + pages + schema D1
docs/          adr / runbook / postmortem
```

`features/` là module dùng chung có chủ đích: **parity giữa training và serving là vấn đề cấu trúc repo, không phải vấn đề ML.** Để hai đường tự tính feature thì không quy trình nào cứu được.
