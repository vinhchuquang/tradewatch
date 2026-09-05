# Runbook

Mục tiêu: người khác đọc file này là chạy được và tự thoát khỏi sự cố, không cần hỏi ai.

---

## 1. Chuẩn bị máy

### 1a. Ubuntu native (đường chính)

```bash
sudo apt update && sudo apt install -y openjdk-17-jre-headless tmux
curl -LsSf https://astral.sh/uv/install.sh | sh
uv sync
```

Docker: theo hướng dẫn chính thức của Docker Engine, rồi `sudo usermod -aG docker $USER` và đăng nhập lại.

Cấp phát RAM (máy 16GB) — hai JVM tranh nhau nên phải đặt tường minh:

```bash
export KAFKA_HEAP_OPTS="-Xmx1G -Xms1G"     # trong docker-compose
export SPARK_DRIVER_MEMORY=4g              # KHÔNG đặt được qua SparkSession.config
```

> `spark.driver.memory` đặt trong `SparkSession.builder.config(...)` **không có tác dụng**: JVM đã khởi động trước khi Python đọc tới dòng đó. Dùng biến môi trường, `spark-defaults.conf`, hoặc `spark-submit --driver-memory`.

**Chạy mọi job dài trong `tmux`** — SSH đứt là job chết theo:

```bash
tmux new -s tw          # Ctrl-b d để thoát, job vẫn chạy
tmux a -t tw            # quay lại
```

### 1b. Windows + WSL2 (đường phụ)

```powershell
wsl --install -d Ubuntu          # ~2GB. Chỉ có distro docker-desktop là KHÔNG đủ
```

Cap RAM cho WSL — `%USERPROFILE%\.wslconfig`:

```ini
[wsl2]
memory=16GB
processors=8
sparseVhd=true
```

Trong Ubuntu:

```bash
sudo apt install -y openjdk-17-jre-headless   # JRE, không cần JDK full
curl -LsSf https://astral.sh/uv/install.sh | sh
uv sync                                        # pyspark + fastavro theo uv.lock
```

**Đặt code trong filesystem của Linux (`~/tradewatch`), không phải `/mnt/c` hay `/mnt/d`.** Qua `/mnt` thì I/O chậm nhiều lần và tốn thêm chỗ.

Không chạy PySpark trên Windows trực tiếp: nó đòi `winutils.exe` + `hadoop.dll`, và đó là một ổ mất thời gian không liên quan gì tới streaming.

---

## 2. Disk — phần cắn nhiều nhất

### Ngân sách

Hai profile qua `.env`, cùng một `docker-compose`:

| | Profile rộng (≥20GB trống) | Profile chật (<15GB trống) |
|---|---|---|
| Kafka retention | 7 ngày, 8GB/partition | 1 giờ, 256MB/partition |
| Parquet TTL | 30 ngày | 3 ngày |
| Tổng dùng | ~12GB | ~5,5GB |
| `disk-guard` | ≥5GB trống | ≥3GB trống |

`make up` từ chối chạy nếu không đủ chỗ trống. Chạy được ở cả hai profile là một phần của yêu cầu di động — lệch nhau nghĩa là có config bị hardcode.

**Docker và WSL ghi vào ổ C:, không phải ổ chứa code.** Kiểm tra đúng ổ:

```powershell
Get-PSDrive C | Select-Object @{n='FreeGB';e={[math]::Round($_.Free/1GB,1)}}
```

### Kiểm tra dùng bao nhiêu ở đâu

```bash
make disk                    # theo từng thành phần của dự án
docker system df -v          # image / container / volume / build cache
du -h -d1 ~ | sort -h | tail -20
```

Trên Windows, để nhìn toàn ổ: `winget install AntibodySoftware.WizTree` — đọc trực tiếp MFT, quét ổ 380GB trong vài giây.

### Thu hồi disk (theo thứ tự hiệu quả)

**Bước 1 — dọn trong Docker.** Build cache thường là thủ phạm lớn nhất.

```bash
docker builder prune -af
docker image prune -af
docker volume prune -f        # cẩn thận: xóa mọi volume không container nào dùng
```

**Bước 2 — compact vhdx. Chỉ áp dụng Windows/WSL2; Linux native không có vhdx.** Đây là bước hay bị bỏ sót.

> Xóa file trong WSL **không** trả disk về Windows. vhdx đã phình rồi thì không tự co lại.

```powershell
wsl --shutdown
```

Windows 11 **Home không có `Optimize-VHD`** (module Hyper-V, chỉ Pro có). Dùng `diskpart` trong PowerShell admin:

```
diskpart
  select vdisk file="%LOCALAPPDATA%\Docker\wsl\disk\docker_data.vhdx"
  attach vdisk readonly
  compact vdisk
  detach vdisk
  exit
```

Rồi bật sparse để lần sau tự thu hồi (distro phải đang `Stopped`):

```powershell
wsl --manage docker-desktop --set-sparse true
wsl --manage Ubuntu --set-sparse true
```

**Bước 3 — dọn Windows.**

```powershell
Dism /Online /Cleanup-Image /AnalyzeComponentStore
Dism /Online /Cleanup-Image /StartComponentCleanup /ResetBase   # WinSxS, thường 2-5GB
npm cache clean --force
cleanmgr /sageset:1 ; cleanmgr /sagerun:1
```

**Không xóa tay `C:\Windows\Installer`.** Đó là cache patch MSI; xóa tay là vỡ uninstall và Windows Update.

### Ba mức dọn của dự án

| Lệnh | Xóa gì | Còn lại |
|---|---|---|
| `make clean` | volume + checkpoint + Parquet | ~2GB |
| `make nuke` | thêm image + venv | ~0 |

---

## 3. Sự cố thường gặp

### Máy vừa mất điện (unclean shutdown)

Đây là **chaos test miễn phí** — đúng cái mà thí nghiệm 1 phải dàn dựng bằng `kill -9`. Đừng bỏ qua, hãy đo.

Thứ tự làm:

```bash
make up                    # Kafka + Postgres tự lên (restart: unless-stopped)
docker logs kafka | grep -i "recovery\|truncat"   # xem có segment nào bị truncate
psql -c "select count(*) from trades"             # đối chiếu với offset nguồn
```

- **Postgres**: crash-safe qua WAL, tự recover, không cần làm gì
- **Kafka**: chạy log recovery khi khởi động. Nếu segment cuối hỏng thì nó truncate — thiệt hại bị chặn trong phạm vi retention
- **Spark**: job **không** tự sống lại (tmux mất theo điện). Chạy lại từ checkpoint bằng `make stream`

Không viết auto-restart cho Spark. Chính lúc khởi động lại thủ công mới là lúc đo được, và YAGNI.

Sau mỗi lần mất điện, ghi vào `experiments/01-*/README.md`: mất record hay nhân đôi record, bao nhiêu, và vì sao. Ba lần mất điện là ba mẫu dữ liệu thật mà người khác không có.

### Job Spark làm máy chậm/treo

Máy dev thường cũng là máy làm việc. Luôn chạy job dài qua cgroup:

```bash
systemd-run --user --scope -p MemoryMax=4G -p CPUQuota=400% make stream
```

Vượt trần thì job bị kill thay vì desktop bị đóng băng. Nếu đã treo: `pkill -f pyspark`, rồi đặt trần trước khi chạy lại.

### Job không khởi động sau khi sửa query

```
Cannot start query with id ... as it was not stopped cleanly / offsets do not match
```

Checkpoint giữ cả **kế hoạch của query cũ**. Đổi logic mà giữ checkpoint cũ là không khởi động được. Xóa checkpoint của query đó rồi chạy lại — chấp nhận mất vị trí đọc, hoặc chỉ định lại offset.

### Kafka không xóa dữ liệu dù đã đặt `retention.bytes`

Kafka chỉ xóa được **segment đã đóng, nguyên cả segment**. `segment.bytes` mặc định 1GB, nên cap 256MB không bao giờ có tác dụng. Kiểm tra:

```bash
docker exec kafka kafka-configs.sh --bootstrap-server localhost:9092 \
  --describe --entity-type topics --entity-name trades
```

Phải thấy cả `retention.bytes`, `retention.ms`, **và** `segment.bytes=67108864`.

### Postgres đầy dù đã `DELETE`

`DELETE` không trả lại disk — chỉ đánh dấu dead tuple. `VACUUM FULL` thu hồi được nhưng cần **gấp đôi** chỗ trống, đúng lúc không có.

Dự án này xóa dữ liệu bằng `DROP PARTITION` (partition theo ngày). Nếu ai đó thêm `DELETE` vào job dọn, đó là bug.

### Consumer lag tăng không giảm

Đọc theo thứ tự, đừng đoán:

```bash
docker exec kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group tradewatch          # lag theo TỪNG partition
```

- Lag lệch giữa các partition → **skew**, vấn đề ở key design (thí nghiệm 04)
- Lag đều trên mọi partition → nghẽn ở xử lý. Xem Spark UI: `batch duration` vs `trigger interval`, và `processing rate` vs `input rate` (thí nghiệm 05)

### Sinh hàng chục nghìn file nhỏ

`spark.sql.shuffle.partitions` mặc định 200. Trigger 10 giây → 72.000 file/giờ. Đặt về 8 khi chạy thường (thí nghiệm 10 cố tình bật lại 200 để đo).

### Disk cạn giữa lúc chạy → WSL treo

Đây là lý do có `disk-guard`. Nếu đã treo: `wsl --shutdown` từ PowerShell, dọn theo mục 2, rồi khởi động lại.

---

## 4. Cloudflare

- Endpoint Worker nhận push **phải kiểm shared secret**. Không có secret thì không nhận write
- Tunnel chỉ bật khi demo, **tắt ngay sau đó**. Không để Spark UI mở ra internet thường trực
- Ngân sách D1: 100k row ghi/ngày ≈ 1,15 row/giây. Chỉ đẩy aggregate, không đẩy raw. Vượt hạn mức là write bị từ chối, không phải bị tính tiền
