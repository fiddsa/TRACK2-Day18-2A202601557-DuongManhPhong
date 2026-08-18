# Reflection: Top Lakehouse Anti-Patterns & Production Realities

Trong 5 anti-pattern của Data Lakehouse, hệ thống dữ liệu của team tôi dễ vướng nhất vào **"Unmanaged Small-File Ingestion & Incomplete Cleanup"** (Ghi micro-batches liên tục nhưng thiếu quy trình dọn dẹp hạ tầng tự động).

### Lý do & Nguy cơ Production
Do đặc thù streaming/CDC, ứng dụng liên tục flush các file Parquet nhỏ (vài KB) xuống đĩa. Việc này không chỉ làm giảm $10\times$ hiệu năng truy vấn mà còn tăng vọt chi phí S3 API request.

### Phát hiện đo đạc thực tế từ Lab
Qua thực nghiệm tại NB6 và NB7, team phát hiện 2 bẫy vận hành thường bị hiểu sai:
1. **`VACUUM` không dọn file mồ côi (Uncommitted Orphans):** Lệnh `VACUUM` trong Delta chỉ thu hồi các file đã bị *tombstone* trong log. Các file do job crash giữa chừng chưa từng vào log sẽ hoàn toàn "vô hình" với `VACUUM`, gây tích tụ rác ngầm billed trên S3.
2. **Iceberg `expire_snapshots` chỉ sửa Metadata:** Lệnh này giảm snapshot từ 20 xuống 3 nhưng **0 file avro/parquet bị xoá thực sự**. Thao tác expiry nếu không chạy kèm orphan sweep là lý do hóa đơn storage không bao giờ giảm.

### Giải pháp
Team cần bắt buộc áp dụng chuỗi 4 Job bảo trì định kỳ: Compaction $\rightarrow$ Clustering $\rightarrow$ Snapshot Expiry $\rightarrow$ Explicit Orphan Sweeping.
