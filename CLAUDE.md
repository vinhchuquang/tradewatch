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
| spark-master | **512MB** — chỉ điều phối, không chạy task |
| spark-worker × 2 | **2GB mỗi cái** — nơi task thật sự chạy |
| Spark driver (`stream`) | **2GB** — submit lên master, không phải `local[*]` |
| Chừa page cache | ~5GB |

Trần RAM là **bắt buộc**, không phải tuỳ chọn. Mọi service khai `mem_limit` trong `docker-compose`:

```yaml
stream:
  mem_limit: 3g
  cpus: 4.0
```

Vượt trần thì container bị OOM-kill, không phải desktop bị đóng băng. Đây là lý do **không** chạy Spark trên host: trên host phải nhớ bọc `systemd-run --scope`, quên một lần là treo máy; trong compose thì trần nằm sẵn trong file, không quên được.

**Hai cạm bẫy RAM:**

1. `spark.driver.memory` trong `SparkSession.builder.config(...)` **không có tác dụng** — JVM khởi động trước khi Python đọc tới đó. Đặt qua biến môi trường `SPARK_DRIVER_MEMORY` trong compose
2. `mem_limit` phải **lớn hơn** `SPARK_DRIVER_MEMORY`. JVM còn metaspace, thread stack, buffer ngoài heap. Đặt bằng nhau là container chết vì OOM trong khi heap vẫn còn chỗ — và log Spark sẽ không nói gì, container chỉ biến mất

## Chạy được trên hai máy

Dự án chạy trên Linux native và trên Windows+WSL2. `make test` phải pass ở cả hai. **Lệch nhau nghĩa là có thứ chưa pin** — sửa cái pin, đừng thêm nhánh `if`. Đóng gói runtime vào image là cách pin mạnh nhất: nó pin cả JVM và thư viện hệ điều hành, thứ mà `uv.lock` không chạm tới.

Nguồn chân lý là git remote, không phải một máy cụ thể. Job chạy dài phải ở trong `tmux`.

## Không đề xuất lại những thứ đã bác bỏ

Đã loại **có chủ đích**, kèm lý do trong spec mục 5: ClickHouse, Druid, Delta Lake, Iceberg, Schema Registry (server), Debezium, Airflow, Feast, MLflow, BentoML, Prometheus, Grafana, MinIO, Hadoop/HDFS, Kubernetes, Terraform, Helm, Cloudflare R2/Vectorize/Containers.

Muốn thêm bất cứ tool nào: **viết ADR nói nó chặn lỗi cụ thể nào.** Không có ADR thì không thêm. Nguyên tắc: *mỗi thành phần chỉ được vào nếu nó chặn được một lỗi cụ thể.*

Kiến trúc hiện tại: **3 image, 7 service** — `kafka`, `postgres`, `spark-master`, `spark-worker-1`, `spark-worker-2`, `ingest`, `stream`. Ba cái cuối cùng dùng chung image `app`. Xem [ADR 0004](docs/adr/0004-spark-trong-container.md).

**Không dùng `local[*]`.** Local mode giấu đi tách driver/executor, và ba thí nghiệm skew / chẩn đoán lag / state phình mất phần lớn ý nghĩa vì chuyện đó.

**Checkpoint, Parquet, dữ liệu Kafka/Postgres đi vào named volume, không bind mount.** Bind mount trên Windows đi qua lớp 9P: thao tác file nhỏ chậm ~250 lần so với ext4 (đo thật). Chỉ bind mount source code — vài chục file, không đáng kể.

## Lệnh

```bash
make up          # 7 service, có disk-guard chặn nếu <3GB trống
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
