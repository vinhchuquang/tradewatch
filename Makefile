SHELL := /bin/bash
.DEFAULT_GOAL := help
.PHONY: help up down ingest stream test disk clean nuke ps logs guard

# Ngưỡng disk trống tối thiểu. Profile rộng: 5GB. Profile chật: 3GB.
MIN_FREE_GB ?= 5

# ─────────────────────────────────────────────────────────────────────────────
# disk-guard — chỗ dễ viết sai nhất trong file này.
#
# `df /` BÊN TRONG WSL hoặc bên trong container báo dung lượng của Ổ ẢO, mà ổ
# ảo là sparse: nó nói còn ~1000GB trong khi máy thật sắp hết chỗ. Guard đọc
# con số đó thì KHÔNG BAO GIỜ KÊU — test vẫn xanh, make up luôn chạy, và cái
# chặn tồn tại như trang trí cho tới hôm Kafka làm đầy ổ hệ thống.
#
# Trên WSL, /mnt/c là ổ C: thật của Windows -> df ở đó ra số thật.
# Trên Linux native, không có /mnt/c -> đọc phân vùng chứa dữ liệu Docker.
# ─────────────────────────────────────────────────────────────────────────────
define FREE_GB
$$(if [ -d /mnt/c ]; then df -BG --output=avail /mnt/c | tail -1 | tr -dc '0-9'; \
   else df -BG --output=avail /var/lib/docker 2>/dev/null || df -BG --output=avail / | tail -1 | tr -dc '0-9'; fi)
endef

help:  ## Liệt kê các lệnh
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

guard:  ## Kiểm disk trống trước khi làm gì nặng
	@free=$(FREE_GB); \
	if [ -z "$$free" ]; then echo "disk-guard: KHONG DOC DUOC dung luong -> dung lai"; exit 1; fi; \
	echo "disk-guard: con $${free}GB tren may that (nguong $(MIN_FREE_GB)GB)"; \
	if [ "$$free" -lt "$(MIN_FREE_GB)" ]; then \
		echo "disk-guard: TU CHOI CHAY. Chay 'make clean' hoac don dep truoc."; exit 1; fi

up: guard  ## Lên 5 service nền (kafka, postgres, spark master + 2 worker)
	docker compose up -d --build
	@echo "Spark master UI: http://localhost:8080  (phai thay DU 2 worker)"

down:  ## Tắt, giữ nguyên volume
	docker compose down

ps:  ## Trạng thái service
	docker compose ps

logs:  ## Theo log (make logs S=stream)
	docker compose logs -f $(S)

ingest: guard  ## Binance WebSocket -> Kafka
	docker compose --profile run up -d ingest

stream: guard  ## Spark Structured Streaming
	docker compose --profile run up -d stream
	@echo "Spark driver UI: http://localhost:4040  (xem task chia tren executor nao)"

test:  ## unit + contract
	docker compose run --rm --no-deps stream python3 -m pytest tests/ -q

# ─────────────────────────────────────────────────────────────────────────────
# Dữ liệu nằm trong NAMED VOLUME, không nằm trên đường dẫn host.
# `du` trên thư mục dự án sẽ ra gần 0 và làm bạn tưởng chưa dùng gì.
# Phải hỏi Docker. Xem ADR 0004.
# ─────────────────────────────────────────────────────────────────────────────
disk:  ## Dung lượng theo từng thành phần
	@echo "=== Volume ==="
	@docker system df -v 2>/dev/null | awk '/VOLUME NAME/{p=1} p&&/tradewatch/{printf "  %-28s %s\n",$$1,$$3}'
	@echo "=== Image ==="
	@docker image ls --format '  {{.Repository}}:{{.Tag}}  {{.Size}}' | grep -E 'spark|kafka|postgres|tradewatch' || true
	@echo "=== May that ==="
	@echo "  con $(FREE_GB)GB"

clean:  ## Xoá volume dữ liệu (GIỮ image). Đo trước/sau.
	@echo "Truoc: $(FREE_GB)GB"
	docker compose down -v
	@echo "Sau:   $(FREE_GB)GB"

nuke: clean  ## clean + xoá luôn image đã build
	docker image rm tradewatch/app:local 2>/dev/null || true
	docker builder prune -af

exp-%: guard  ## Chạy lại thí nghiệm NN (vd: make exp-03)
	@d=$$(ls -d experiments/$**/ 2>/dev/null | head -1); \
	if [ -z "$$d" ]; then echo "Khong thay experiments/$**/"; exit 1; fi; \
	echo "== $$d =="; $(MAKE) -C "$$d" run
