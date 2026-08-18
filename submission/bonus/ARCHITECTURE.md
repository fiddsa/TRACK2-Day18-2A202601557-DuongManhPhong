# Architecture Brief: LLM Observability at 1B Requests/Day Scale

> **Author:** Data Platform Architect Team  
> **Topic A:** High-Scale LLM Observability & Cost/Latency Governance  
> **Deliverable:** Architecture Brief for Senior Design Review  

---

## 1. Problem Statement

Hệ thống API Gateway cho các mô hình ngôn ngữ lớn (LLM Foundation Models) xử lý **1 tỷ requests/ngày**. Mỗi request tạo ra payload chứa metadata, prompt, completion, latency và token usage với kích thước trung bình **~5 KB/request**.

* **Quy mô dữ liệu raw:** 1B reqs/ngày × 5 KB = **5 TB/ngày raw data** (~150 TB/tháng uncompressed).
* **Ràng buộc nghiệp vụ & SLAs:**
  1. Dashboard báo cáo latency p50/p95, error rate và chi phí token theo từng Tenant phải cập nhật mỗi **5 phút**.
  2. Toàn bộ Prompt/Completion đầy đủ phải được lưu trữ **7 ngày** phục vụ incident review & debugging. Sau 7 ngày, thông tin chi tiết được purged, chỉ giữ lại bảng tổng hợp (Aggregated Gold Metrics) trong **1 năm**.
  3. Dữ liệu nhạy cảm (PII/Secret Keys) phải được **anonymize / tokenized tại tầng Ingestion (Bronze Landing)** trước khi bất kỳ nhân sự hoặc analyst nào có thể truy vấn.
  4. **Giới hạn ngân sách cứng (FinOps Cap):** Tổng chi phí hạ tầng storage + compute không vượt quá **$5,000 / tháng**.

---

## 2. System Architecture Diagram

```
[ LLM Gateway Cluster ] ──(Kafka Topic: llm.events.v1)──► [ Flink Ingestion Engine ]
                                                                   │
    ┌──────────────────────────────────────────────────────────────┴──────────────────────────────┐
    │  Bronze Landing Layer (Streaming Ingestion & Inline PII Tokenization)                        │
    │  - Hash/Redact PII (Regex + Presidio)                                                       │
    │  - Format: Delta Lake (zstd compression)                                                    │
    │  - Path: s3://lakehouse-warehouse/bronze/llm_raw/date=YYYY-MM-DD/                            │
    └──────────────────────────────┬──────────────────────────────────────────────────────────────┘
                                   │ (Stream / Micro-batch Compaction every 15 mins)
                                   ▼
    ┌─────────────────────────────────────────────────────────────────────────────────────────────┐
    │  Silver Layer (Cleaned & Parsed Traces)                                                     │
    │  - Schema: request_id, tenant_id, model, prompt_tokens, completion_tokens, latency_ms, status│
    │  - Partitioning: date=YYYY-MM-DD | Clustering: Z-ORDER BY (tenant_id, model)                │
    │  - Path: s3://lakehouse-warehouse/silver/llm_traces/                                         │
    │  - Retention: TTL = 7 days (Automated Expiry & Physical Vacuum)                             │
    └──────────────────────────────┬──────────────────────────────────────────────────────────────┘
                                   │ (Scheduled Aggregate Job every 5 mins via DuckDB / Spark)
                                   ▼
    ┌─────────────────────────────────────────────────────────────────────────────────────────────┐
    │  Gold Layer (Tenant Metrics Rollup)                                                         │
    │  - Aggregates: (window_5m, tenant_id, model) → p50/p95 latency, token_sum, cost_usd, errors│
    │  - Partitioning: year=YYYY/month=MM | Format: Delta Lake                                    │
    │  - Path: s3://lakehouse-warehouse/gold/tenant_daily_metrics/                                │
    │  - Retention: 365 days                                                                      │
    └──────────────────────────────┬──────────────────────────────────────────────────────────────┘
                                   │
                    ┌──────────────┴──────────────┐
                    ▼                             ▼
         [ Trino / DuckDB Serverless ]   [ BI Dashboards & Alerting ]
         (Fast Query Path p95 < 2s)       (Grafana / Superset)
```

---

## 3. Key Decisions & Rejected Alternatives

### Quyết định 1: Định dạng bảng — Chọn Delta Lake (Rust/PyArrow bindings) làm Core Format
* **Tôi chọn:** **Delta Lake (delta-rs / `deltalake` 1.x)** cho toàn bộ tầng Bronze, Silver, Gold.
* **Tôi loại Apache Iceberg:** Mặc dù Iceberg có Hidden Partitioning tốt, tại quy mô ghi micro-batch 1B events/ngày qua Kafka-Flink, Delta-rs cung cấp hiệu năng ghi commit log bằng Rust rất nhẹ, không tốn tài nguyên JVM footprint, dễ tích hợp trực tiếp với DuckDB cho query engine giá rẻ.
* **Tôi loại Raw Parquet / Hive Format:** Loại bỏ hoàn toàn do thiếu tính năng ACID transaction log, không hỗ trợ `MERGE INTO` phục vụ de-duplication, không hỗ trợ Time Travel và rủi ro đọc phải partial writes khi đang stream ingestion.

### Quyết định 2: Chiến lược Catalog — Chọn Apache Polaris (REST Catalog Standard)
* **Tôi chọn:** **Apache Polaris (Iceberg REST Catalog API compatible)** kết hợp Delta UniForm / REST Protocol đóng vai trò Control Plane tập trung.
* **Tôi loại Databricks Unity Catalog:** Dù rất mạnh mẽ nhưng bị dính vendor lock-in và chi phí bản quyền thương mại không đáp ứng được FinOps cap $5K/tháng.
* **Tôi loại Hive Metastore (HMS):** Thao tác partition listing của HMS dạng `SHOW PARTITIONS` trên S3 vô cùng chậm khi số lượng file lớn và không hỗ trợ metadata-level column renaming hay Field-ID stability.

### Quyết định 3: Partitioning & Co-location — Phân vùng `date` + Z-ORDER theo `(tenant_id, model)` ở Silver
* **Tôi chọn:** Phân vùng vật lý theo `date=YYYY-MM-DD` ở tầng Silver và áp dụng **Z-ORDER BY `(tenant_id, model)`** với target file size là 256 MB.
* **Tôi loại Phân vùng trực tiếp theo `tenant_id`:** Với hàng nghìn tenant, việc partition theo `tenant_id` tạo ra bài toán **Small-file Problem** nghiêm trọng (hàng triệu file rác 10-50 KB), tăng gấp $100\times$ chi phí S3 `PUT`/`GET` API requests.
* **Tôi loại Un-sorted Storage:** Nếu không Z-ORDER theo `tenant_id`, các điểm truy vấn theo tenant phải full-scan toàn bộ 5 TB dữ liệu mỗi ngày, làm sụp đổ SLA truy vấn < 2 giây.

### Quyết định 4: Bảo vệ PII — Tokenization & Anonymization tại tầng Ingestion (Bronze Landing)
* **Tôi chọn:** Thực thi Regex Scrubbing & SHA-256 HMAC Salted Tokenization đối với các trường nhạy cảm (Email, API Keys, Credit Card, IP Address) ngay trong bộ nhớ của Flink Stream Processor trước khi ghi file vào Bronze.
* **Tôi loại PII Redaction ở tầng BI / Query Time:** Nếu ghi raw PII xuống S3 rồi mới mask ở tầng SQL Query (View level), bất kỳ ai có quyền truy cập trực tiếp S3 bucket đều có thể đọc lén PII, vi phạm nghiêm trọng GDPR và Nghị định 13.
* **Tôi loại Reversible Encryption cho mọi trường:** Lưu trữ khóa giải mã ngay trong cùng pipeline tạo ra điểm sụp đổ an ninh (single point of failure). Chỉ áp dụng SHA-256 HMAC với chìa khóa lưu ở AWS KMS.

### Quyết định 5: Lifecycle & FinOps Storage Tiering — Hard Purge sau 7 ngày cho Silver
* **Tôi chọn:** Đặt quy trình tự động **Snapshot Expiry (Retention = 168 giờ / 7 ngày)** kết hợp **Physical `VACUUM`** cho tầng Silver. Chuyển tầng Gold sau 90 ngày sang lưu trữ S3 Glacier Instant Retrieval.
* **Tôi loại việc giữ Silver Data 30–90 ngày trên S3 Standard:** Lưu 5 TB/ngày trong 90 ngày = 450 TB storage $\rightarrow$ riêng tiền S3 Storage là $450 \times \$23 = \$10,350/\text{tháng}$, vượt gấp đôi tổng ngân sách toàn hệ thống ($5K/tháng).

---

## 4. Failure Modes & Incident Response (3-AM Scenarios)

### Failure Mode 1: Small-File Explosion do Kafka Ingestion Spike (3 AM Incident)
* **Kịch bản:** Lúc 3h sáng, một đợt DDoS hoặc spike lưu lượng khiến Flink job ghi ra 500,000 file Parquet nhỏ (~20 KB/file) trong 2 giờ. Metadata log phình to, query trên Silver bị timeout (p95 latency > 40s).
* **Phát hiện (Detection):** Alert từ Prometheus / Grafana giám sát chỉ số `delta_table_file_count > 10,000` trên phân vùng hiện tại hoặc S3 `GET` latency tăng vọt.
* **Xử lý & Rollback (Remediation):**
  1. Trigger khẩn cấp job **Compaction & Z-ORDER**: `dt.optimize.compact(target_size=256*1024*1024)` + `dt.optimize.z_order(["tenant_id"])`.
  2. Điều chỉnh Flink `checkpoint.interval` từ 10 giây lên 5 phút để gom batch lớn hơn trước khi flush đĩa.

### Failure Mode 2: Transient Schema Corruption (Schema Drift từ Upstream App)
* **Kịch bản:** Developer upstream cập nhật ứng dụng, đổi trường `usage.prompt_tokens` từ `INTEGER` thành `STRING` (`"150 tokens"`). Nếu không kiểm soát, pipeline ghi dữ liệu Silver sẽ bị lỗi hỏng schema hoặc ngưng trệ.
* **Phát hiện (Detection):** Delta Lake **Schema Enforcement** chặn giao dịch ghi lỗi, bắn alert `SchemaMismatchException` về PagerDuty.
* **Xử lý & Rollback (Remediation):**
  1. Dữ liệu lỗi tự động đẩy vào thư mục **Dead Letter Queue (DLQ)** dạng `s3://.../bronze_dlq/`.
  2. Pipeline Silver tiếp tục vận hành bình thường với các message hợp lệ.
  3. Sau khi upstream sửa code, chạy job replay DLQ bằng `schema_mode="merge"` hoặc parse lại chuỗi số.

### Failure Mode 3: Right-to-Erasure (GDPR / Decree 13) Conflict với Delta Time Travel
* **Kịch bản:** Khách hàng yêu cầu xoá toàn bộ lịch sử dữ liệu của `tenant_999`. Team thực thi lệnh `dt.delete("tenant_id = 'tenant_999'")`. Tuy nhiên, kiểm toán viên phát hiện dữ liệu của `tenant_999` vẫn có thể đọc được thông qua Time Travel (`versionAsOf`).
* **Phát hiện (Detection):** Đơn vị kiểm toán an toàn thông tin quét thấy dữ liệu cá nhân tồn tại trong các file Parquet bị tombstone ở commit log cũ.
* **Xử lý & Rollback (Remediation):**
  1. Thực hiện quy trình **Forced Retention Expiry**: Chạy `VACUUM RETAIN 0 HOURS` (sau khi đã dừng tất cả concurrent readers) để xoá vĩnh viễn các file Parquet bị tombstone ra khỏi S3 bucket.
  2. Ghi nhận nhật ký kiểm toán (Audit Trail Log) chứng minh phiên bản bảng hiện tại và các tệp đĩa đã bị loại bỏ hoàn toàn.

---

## 5. Back-of-Envelope Cost Estimation (FinOps Verification)

Ngân sách cap cứng: **$5,000 / tháng**.

### A. Chi phí Storage (S3)
1. **Bronze Raw (Lưu 3 ngày):** $5\text{ TB/ngày} \times 3\text{ ngày} = 15\text{ TB}$.
   * $15\text{ TB} \times \$0.023/\text{GB} = \mathbf{\$345 / \text{tháng}}$.
2. **Silver Traces (Lưu 7 ngày, nén zstd 30%):** $5\text{ TB} \times 0.7 \times 7\text{ ngày} = 24.5\text{ TB}$.
   * $24.5\text{ TB} \times \$0.023/\text{GB} = \mathbf{\$563.5 / \text{tháng}}$.
3. **Gold Aggregates (Lưu 365 ngày, nén cao):** Dung lượng cực nhẹ (~5 GB/ngày) $\times 365 = 1.8\text{ TB}$.
   * $1.8\text{ TB} \times \$0.023/\text{GB} = \mathbf{\$41.4 / \text{tháng}}$.
4. **S3 API Requests (PUT/GET):** Với Compaction giữ số file $\sim 20,000$ files/ngày:
   * API Requests = $\mathbf{\$150 / \text{tháng}}$.
* **Subtotal Storage:** $\approx \mathbf{\$1,100 / \text{tháng}}$.

### B. Chi phí Compute (Ingestion, Compaction & Query Engine)
1. **Flink Ingestion & Compaction Workers (Spot Instances):** 4 nodes `c6i.xlarge` (4 vCPU, 8GB RAM) spot.
   * $4 \times \$0.06/\text{giờ} \times 730\text{ giờ} = \mathbf{\$1,752 / \text{tháng}}$.
2. **DuckDB / Trino Serverless Query Layer:** Chạy serverless trên Auto-scaling Spot Clusters phục vụ Dashboard & Ad-hoc Queries.
   * Ước tính compute: $\mathbf{\$1,200 / \text{tháng}}$.

### 📊 Tổng chi phí dự toán (Total Estimated Cost)
$$\text{Total Cost} = \$1,100 \text{ (Storage)} + \$1,752 \text{ (Ingestion/Compaction)} + \$1,200 \text{ (Compute Queries)} = \mathbf{\$4,052 / \text{tháng}}$$

$\Rightarrow$ **Nằm an toàn trong hạn mức $5,000/tháng (Tiết kiệm ~19%).**

---

## 6. One-Week MVP Slice Implementation Plan

Để chứng minh kiến trúc hoạt động thành công trong 1 tuần (1-week MVP), team sẽ thực hiện một Vertical Spike tập trung vào các mắt xích rủi ro nhất:

* **Ngày 1–2:** Xây dựng script giả lập sinh 10 triệu events LLM log với PII nhạy cảm và ghi vào Delta Bronze bằng Rust/Python `deltalake`.
* **Ngày 3:** Cấu hình Pipeline Bronze $\rightarrow$ Silver: Thực thi PII Inline Scrubbing, ghi Silver partition theo `date` và Z-ORDER theo `tenant_id`.
* **Ngày 4:** Xây dựng job tổng hợp Gold 5-phút bằng DuckDB SQL (`QUANTILE_CONT` cho p50/p95 latency và tính toán token cost).
* **Ngày 5:** Đánh giá benchmark: Đo đạc tốc độ truy vấn p95 < 2s khi filter theo Tenant, thử nghiệm `VACUUM` xoá dữ liệu 7 ngày và đo lường hóa đơn FinOps thực tế.
