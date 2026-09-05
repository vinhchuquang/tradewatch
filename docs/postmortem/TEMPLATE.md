# Postmortem — <tiêu đề ngắn: hiện tượng, không phải nguyên nhân>

- **Ngày xảy ra:**
- **Thời gian phát hiện → khắc phục:**
- **Bài toán / component liên quan:**

> **Luật duy nhất của tài liệu này: không tìm người sai.**
> Câu hỏi đúng là *hệ thống đã cho phép chuyện này xảy ra như thế nào.*
> Nếu bản postmortem kết luận "do tôi quên" thì nó chưa xong — hỏi tiếp: tại sao hệ thống cho phép cái quên đó gây hậu quả.

## 1. Hiện tượng

Cái bạn *quan sát* được, không phải cái bạn *suy ra*. Số liệu, log, screenshot.

## 2. Ảnh hưởng

Mất bao nhiêu record? Sai bao nhiêu? Trong bao lâu? Nếu đây là production thật thì ai bị ảnh hưởng?

## 3. Dòng thời gian

| Thời điểm | Việc xảy ra |
|---|---|
| | |

## 4. Nguyên nhân gốc

Đi tới khi câu trả lời không còn là "vì có người làm sai". Ví dụ:

- ❌ "Tôi quên đặt watermark"
- ✅ "Không có gì chặn một streaming query thiếu watermark được merge. Watermark là bắt buộc về mặt đúng đắn nhưng chỉ là quy ước về mặt code."

## 5. Vì sao không phát hiện sớm hơn

Thiếu metric nào? Thiếu alert nào? Hay có alert nhưng không ai đọc?

## 6. Hành động

Mỗi hành động phải **chặn được cả lớp lỗi**, không chỉ lần này.

| Hành động | Chặn lớp lỗi nào | Xong chưa |
|---|---|---|
| | | |

Ưu tiên theo thứ tự: **(1) làm lỗi không thể xảy ra được** > (2) làm lỗi tự phát hiện > (3) ghi vào runbook.
Nếu hành động duy nhất là "lần sau cẩn thận hơn" thì chưa có hành động nào.

## 7. Điều gì đã đi đúng

Cái gì giúp phát hiện được, giới hạn được thiệt hại. Giữ lại và nhân rộng.
