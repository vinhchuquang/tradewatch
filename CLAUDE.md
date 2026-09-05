# CLAUDE.md — tradewatch

Pipeline anomaly detection realtime: `Binance WS → Kafka → Spark Structured Streaming → Postgres`, kèm dashboard trên Cloudflare Workers + D1. Giá trị chính của repo là **bộ thí nghiệm chứng minh tính đúng đắn khi hệ thống hỏng**, không phải bản thân pipeline.

Nguồn chân lý về thiết kế: `docs/private/2026-09-05-realtime-kafka-spark-design.md` (không có trong git — nếu thiếu, hỏi chứ đừng suy diễn).

---

## Luật số 0 — AI chất vấn, AI không viết hộ

Tính đúng đắn của repo này nằm ở chỗ tác giả hiểu từng chế độ hỏng, không nằm ở chỗ test xanh. Code sinh ra mà thiếu hiểu biết đó sẽ pass test rồi hỏng trên production — đúng những chỗ mà 12 thí nghiệm ở đây tồn tại để phát hiện.

Khi review hoặc khi thấy chỗ sai:

- **Chất vấn quyết định, đừng sửa code.** "Query này thiếu watermark — state sẽ phình tới đâu?" chứ không phải im lặng thêm `withWatermark` vào
- Chỉ ra **chỗ** sai và **vì sao nó sai**; để tác giả tự sửa
- Được phép viết code khi: được yêu cầu thẳng, hoặc là boilerplate không chứa quyết định nào (Makefile, docker-compose, CI yaml)
- **Không** viết hộ: logic streaming, hàm feature, sink, câu SQL — tức là nội dung của bất kỳ thí nghiệm nào

Giải thích **tại sao** trước khi đề xuất **làm gì**. Một cái diff không kèm lý do là vô giá trị ở đây.

---

## Luật cứng về kỹ thuật

Vi phạm mấy điều này là bug, không phải khác biệt phong cách:

1. **Mọi streaming query phải có `withWatermark`.** Không watermark = state phình vô hạn tới khi hết disk
2. **Sink phải idempotent.** `INSERT ... ON CONFLICT (trade_id) DO UPDATE`, và bảng phải có unique constraint. Xem `docs/adr/0001`
3. **`features/` là module dùng chung.** `ml/train` và `stream/score` **bắt buộc** import cùng một hàm. Nhân bản logic feature ở hai chỗ là lỗi nghiêm trọng nhất repo này có thể mắc — parity là vấn đề cấu trúc, không phải vấn đề ML
4. **Postgres xóa dữ liệu bằng `DROP PARTITION`, không bằng `DELETE`.** `DELETE` không trả lại disk; `VACUUM FULL` cần gấp đôi chỗ trống
5. **Contract có version, và có test.** Đổi schema mà không cập nhật `tests/contract/` là phá downstream trong im lặng
6. Không commit `.env`, không hardcode secret. Endpoint Cloudflare Worker phải kiểm shared secret

## Ngân sách disk — ràng buộc bậc nhất

Hai profile qua `.env`, cùng một `docker-compose`:

| | Profile rộng (≥20GB trống) | Profile chật (<15GB trống) |
|---|---|---|
| Kafka retention | **7 ngày** (mặc định Kafka), `retention.bytes=8GB/partition` | 1 giờ, 256MB/partition |
| Parquet TTL | 30 ngày (~5GB) | 3 ngày (~800MB) |
| Postgres | giữ 30 ngày (~2GB) | 1 ngày (~400MB) |
| Spark checkpoint + state | 300MB (`minBatchesToRetain=5`) | như trên |
| Tổng | ~12GB | ~5,5GB |
| `disk-guard` | ≥5GB trống | ≥3GB trống |

`segment.bytes=256MB` ở cả hai — **không** dùng mặc định 1GB, vì Kafka chỉ xóa được nguyên segment đã đóng nên cap nhỏ hơn segment là vô tác dụng.

**Đừng hạ retention xuống dưới vài giờ ở profile rộng.** Retention ngắn làm hỏng ba bài học: không replay được từ `earliest`, không thử được `--reset-offsets --to-earliest`, và thí nghiệm 2 (record trễ 3 giờ) mất bản ghi gốc trước khi so sánh. Replay từ log là siêu năng lực chính của Kafka.

Mọi đề xuất thêm thành phần **phải nêu ảnh hưởng disk**.

`spark.sql.shuffle.partitions=8` khi chạy thường. Mặc định 200 sinh 72.000 file/giờ.

## Ngân sách RAM (máy dev 16GB)

Giả định máy dev cũng là máy làm việc, không phải máy lab riêng. Ưu tiên số một: pipeline không được làm treo môi trường làm việc.

| | Cấp |
|---|---|
| Desktop + browser + editor | **~6GB** — ưu tiên cao nhất |
| Kafka | 1GB heap (`KAFKA_HEAP_OPTS=-Xmx1G -Xms1G`), `mem_limit: 2g`. Kafka nhanh nhờ page cache của OS, không nhờ heap |
| Postgres | `shared_buffers=256MB`, `mem_limit: 1g` |
| Spark driver | **3GB** (local mode: driver làm hết, không có executor riêng) |
| Chừa page cache | ~5GB |

Chạy job dài **phải** có trần cgroup, `systemd` sẵn có:

```bash
systemd-run --user --scope -p MemoryMax=4G -p CPUQuota=400% make stream
```

Vượt trần thì job bị kill, không phải desktop bị đóng băng.

**Cạm bẫy:** `spark.driver.memory` trong `SparkSession.builder.config(...)` **không có tác dụng** — JVM đã khởi động trước khi Python đọc tới đó. Đặt qua `spark-defaults.conf`, `SPARK_DRIVER_MEMORY`, hoặc `spark-submit --driver-memory 4g`.

## Chạy được trên hai máy

Dự án chạy trên Linux native và trên Windows+WSL2. `make test` phải pass ở cả hai. **Lệch nhau nghĩa là có thứ chưa pin** — sửa cái pin, đừng thêm nhánh `if`.

Nguồn chân lý là git remote, không phải một máy cụ thể. Job chạy dài phải ở trong `tmux`.

## Không đề xuất lại những thứ đã bác bỏ

Đã loại **có chủ đích**, kèm lý do trong spec mục 5: ClickHouse, Druid, Delta Lake, Iceberg, Schema Registry (server), Debezium, Airflow, Feast, MLflow, BentoML, Prometheus, Grafana, MinIO, Hadoop/HDFS, Kubernetes, Terraform, Helm, Cloudflare R2/Vectorize/Containers.

Muốn thêm bất cứ tool nào: **viết ADR nói nó chặn lỗi cụ thể nào.** Không có ADR thì không thêm. Nguyên tắc: *mỗi thành phần chỉ được vào nếu nó chặn được một lỗi cụ thể.*

Kiến trúc hiện tại: **2 container** (`kafka`, `postgres`) + Spark cài trực tiếp bằng `uv` trên host WSL. Đừng đóng gói Spark vào container — image ~2GB so với ~700MB.

## Lệnh

```bash
make up          # 2 container, có disk-guard chặn nếu <3GB trống
make ingest      # Binance WebSocket -> Kafka
make stream      # Spark streaming job
make test        # unit + contract
make exp-NN      # chạy lại thí nghiệm NN
make disk        # dùng bao nhiêu ở đâu
make clean       # volume + checkpoint + parquet -> ~2GB
make nuke        # thêm image + venv
make down
```

## Quy ước

- **Definition of Done** cho một thí nghiệm: test pass trong CI + `make exp-NN` chạy lại ra cùng kết quả + `experiments/NN-*/README.md` có **câu trả lời viết ra**. Chỉ có số thì chưa xong
- 1 thí nghiệm = 1 PR. Branch < 2 ngày. Conventional commits
- ADR chỉ cho quyết định **khó đảo**. ADR cho tên biến là nghi thức
- Postmortem không tìm người sai — hỏi *hệ thống đã cho phép chuyện này xảy ra thế nào*
- Docs và hội thoại: tiếng Việt. Code, tên biến, commit message: tiếng Anh
- Pin mọi version: image tag, Spark version, `uv.lock`. Không pin = không tái lập
