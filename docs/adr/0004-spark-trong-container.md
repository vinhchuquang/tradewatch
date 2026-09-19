# ADR 0004 — Spark chạy thành cluster trong container, không local mode trên host

- **Ngày:** 2026-09-19
- **Trạng thái:** Chấp nhận
- **Thay thế:** đoạn "Spark cài trực tiếp bằng `uv` trên host, đừng đóng container" trong `CLAUDE.md`

## Bối cảnh

Quyết định cũ: Kafka + Postgres trong container, Spark cài bằng `uv` trên host. Lý do duy nhất được ghi
lại là **kích thước image** — ~2GB cho image Spark so với ~700MB cho pyspark + JRE.

Đó là lý do yếu, và nó là lý do *duy nhất*. Một quyết định kiến trúc chỉ đứng trên một con số chênh
1,3GB thì đáng bị chất vấn lại khi có thêm dữ liệu.

## Quyết định

**Spark chạy thành cluster standalone trong container.** Kiến trúc: **3 image, 7 service**.

```
kafka · postgres
spark-master · spark-worker-1 · spark-worker-2     <- cùng image `app`
ingest · stream                                     <- cùng image `app`, khác lệnh
```

`stream` submit lên `spark://spark-master:7077`. **Không dùng `local[*]`.**

**Dữ liệu nóng đi vào named volume, không bind mount:** checkpoint, Parquet, Kafka log, Postgres data.
Chỉ bind mount source code.

## Lý do

**1. Image pin được thứ `uv.lock` không chạm tới.**
`uv.lock` pin thư viện Python. Nó không pin JVM, không pin glibc, không pin locale, không pin timezone
database. Spark là ứng dụng JVM — phiên bản JVM ảnh hưởng tới hành vi GC, tới cách xử lý timestamp, tới
cả kết quả sắp xếp chuỗi. Luật của repo là *"không pin = không tái lập"*; chạy trên host là để hở đúng
tầng quan trọng nhất.

**2. Trần RAM nằm trong file thay vì nằm trong trí nhớ.**
Trên host, trần RAM đến từ việc **nhớ** bọc lệnh trong `systemd-run --scope`. Quên một lần là Spark ăn
hết RAM và treo môi trường làm việc — đúng cái rủi ro mà mục "Ngân sách RAM" tồn tại để chặn. Trong
compose, `mem_limit` là một dòng trong file, luôn có hiệu lực, không phụ thuộc người gõ lệnh nhớ hay quên.

**3. Hệ thống thật chạy Spark trong container, và chạy nhiều executor.**
Kubernetes, EMR, Databricks — đều là container hoặc cluster manager. `pip install pyspark` trên máy dev
là thói quen phát triển local, không phải hình dạng vận hành.

Quan trọng hơn: **local mode giấu đi khái niệm trung tâm nhất của Spark — tách driver và executor.**
Local mode là một JVM giả lập cluster bằng thread. Shuffle trong đó là copy bộ nhớ; shuffle thật là ghi
đĩa rồi kéo qua mạng. Ba thí nghiệm mất phần lớn ý nghĩa nếu chạy local mode:

| Thí nghiệm | Local mode đo được | Cluster đo được |
|---|---|---|
| Skew | Vài thread trong một JVM lệch nhau | Một executor cày, các executor khác ngồi chơi — thấy trong Spark UI |
| Chẩn đoán lag | Không có phân bố task giữa executor để đọc | Task distribution, executor nào chậm, shuffle read/write |
| State phình | State trong một process | State phân tán theo partition trên từng executor |

Đóng gói Spark vào container mà vẫn chạy local mode là mới đi được nửa đường.

**4. Filesystem — số đo, không phải cảm giác.**
Trên Windows + WSL2, thao tác file đi qua ổ Windows phải qua lớp 9P. Đo trên cùng một máy:

| Phép đo | ext4 trong VM | qua lớp 9P | Chậm hơn |
|---|---|---|---|
| Tạo 1000 file nhỏ | 0,03s | 6,90s | **253×** |
| Ghi tuần tự 100MB | 0,10s | 1,69s | 17× |
| Xoá 1000 file | 0,02s | 2,66s | 130× |

Checkpoint của Spark và output Parquet đều là **nhiều file nhỏ** — đúng cột chậm 253 lần. Named volume
đặt chúng vào ext4 bên trong VM, nên vấn đề biến mất mà không cần ràng buộc chỗ đặt source code.

Quan trọng hơn tốc độ: thí nghiệm **small files** và thí nghiệm **state phình** đo hành vi filesystem.
Đo chúng qua lớp 9P là đo cây cầu, không phải đo thứ định đo — vẫn ra số, vẫn viết được kết luận, và sai.

**5. Chi phí disk thật sự nhỏ hơn con số ban đầu.**
Image thay thế venv chứ không cộng thêm vào: **+1,3GB ròng** (~2GB image so với ~700MB venv). Trên ngân
sách ~12GB của profile rộng là ~11%. Chấp nhận được, và đổi lại bốn điều ở trên.

## Hệ quả

- **RAM phải nâng trần WSL.** Tổng `mem_limit` của 7 service là ~9,5GB. Mặc định WSL2 chỉ cấp ~50% RAM
  host, thường không đủ. Đặt trong `.wslconfig`. Đây là ràng buộc cứng khi chạy trên Windows — trên Linux
  native không có vấn đề này
- **Spark UI có hai chỗ:** master ở `:8080`, driver ở `:4040`. Hai cái trả lời hai câu khác nhau —
  master nói về worker và tài nguyên, driver nói về job và stage đang chạy
- **Không xem được checkpoint và Parquet bằng file explorer.** Named volume nằm trong VM. Phải dùng
  `docker exec` hoặc một service phụ mount volume đó. Đây là mất mát thật với một repo mà giá trị nằm ở
  việc nhìn tận mắt state phình và small files — chấp nhận có chủ đích, vì số liệu đo sai còn tệ hơn
- **`make disk` phải đọc được named volume** (`docker system df -v`), không chỉ `du` trên đường dẫn host.
  Đây là ràng buộc cứng của T0
- **`make clean` xoá volume**, không xoá thư mục. `docker volume rm` chứ không `rm -rf`
- **Spark UI phải publish port** mới xem được từ trình duyệt host
- **Thí nghiệm 1 (kill giữa micro-batch) dùng `docker kill --signal=KILL`** thay cho `kill -9`. Sạch hơn:
  nó giết đúng PID 1 của container, không phụ thuộc việc tìm đúng process trên host
- Source code bind mount → sửa code không phải build lại image. Chỉ build lại khi đổi dependency
- Cài `openjdk` và `uv` trên host trở thành **không cần thiết** cho pipeline. Giữ lại cũng được, chúng
  không xung đột với gì

## Bài học kiến trúc rút ra

**Một quyết định kiến trúc đứng trên đúng một lý do là một quyết định chưa được kiểm.** Lý do cũ ở đây —
"image to hơn venv" — đúng về mặt số học và vẫn sai về mặt kết luận, vì nó cân một thứ đo được dễ
(dung lượng) với ba thứ đo được khó (tính tái lập, an toàn bộ nhớ, tương đồng với môi trường thật).
Thứ dễ đo thắng không phải vì nó quan trọng hơn.

## Xem lại khi nào

- Nếu vòng lặp sửa-chạy-xem chậm tới mức cản việc, và nguyên nhân truy được về container chứ không về Spark
- Nếu cần chạy Spark ở chế độ nhiều executor thật — lúc đó bàn cluster manager, không quay về host
