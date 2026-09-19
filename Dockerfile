# Image `app` — dùng chung cho spark-master, spark-worker, ingest, stream.
#
# Pin bằng DIGEST, không chỉ bằng tag: tag có thể bị đẩy đè, digest là hash
# nội dung nên bất biến. Xem ADR 0004.
#
# Bên trong image này (đã xác minh, không phải phỏng đoán):
#   SPARK_HOME = /opt/spark          JAVA_HOME = /opt/java/openjdk (Java 17.0.19)
#   user       = uid 185 (spark)     python3   = 3.10.12
#   entrypoint = /opt/entrypoint.sh  workdir   = /opt/spark/work-dir
FROM apache/spark:4.0.4-python3@sha256:94ad730f7510002d8a1615de269f27cdeca4d4eef51657384db3fa9246b5a4d8

# pip cần quyền ghi vào site-packages -> đổi sang root cho bước cài,
# rồi TRẢ LẠI uid 185. Để container chạy bằng root là mở một lỗ bảo mật
# không có lý do gì để mở.
USER root

# --no-cache-dir: không giữ cache wheel trong layer. Cache nằm lại image
# thì image phình mà không ai dùng tới nó lần nữa.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

# Thư mục cho code bind-mount vào. Phải chown 185 vì process chạy bằng uid đó.
RUN mkdir -p /app && chown -R 185:185 /app

USER 185
WORKDIR /app

# KHÔNG đặt CMD ở đây. Mỗi service trong docker-compose khai lệnh riêng:
# master, worker, ingest, stream — cùng một image, bốn vai trò.
