#  HƯỚNG DẪN VẬN HÀNH HỆ THỐNG SENTIMENT ANALYSIS 
> Tài liệu chi tiết hướng dẫn chạy trọn gói các Pipeline: Batch, Streaming, API và Airflow.

---

##  BƯỚC 1: Cấu hình Tài nguyên (Chỉ làm 1 lần)
Để hệ thống chạy mượt mà trên máy 8GB-16GB RAM:
1. Nhấn `Win + R`, gõ `%UserProfile%`, nhấn Enter.
2. Tạo/Mở file `.wslconfig` và dán nội dung:
   ```ini
   [wsl2]
   memory=6GB   # Cấp cho Docker 6GB RAM
   processors=2 # Giới hạn 2 nhân CPU
   ```
3. Lưu file và chạy lệnh `wsl --shutdown` trong PowerShell (Admin).

---

##  BƯỚC 2: Khởi động Hệ thống
Mở Terminal tại thư mục `BM-Rua` và chạy:
```powershell
# 1. Tắt các container cũ (nếu có)
docker-compose down

# 2. Khởi động toàn bộ dịch vụ
docker-compose up -d
```
> **Lưu ý quan trọng:** Đợi khoảng **2 phút** để Spark tự động cài đặt các thư viện cần thiết (`pymongo`, `kafka-python`).

---

##  BƯỚC 3: Chuẩn bị Dữ liệu (Ingestion)
Đưa dữ liệu vào "Hồ dữ liệu" HDFS để xử lý phân tán:
```powershell
# 1. Tải dataset (Nếu chưa có)
bash scripts/download_data.sh

# 2. Đẩy dữ liệu lên HDFS
bash scripts/hadoop/upload_hdfs.sh
```

---

##  BƯỚC 4: Chạy Pipeline BATCH (Xử lý Dữ liệu Lớn)
Quy trình huấn luyện mô hình Machine Learning:
```powershell
# 1. Làm sạch dữ liệu
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /data/scripts/spark/preprocessing.py

# 2. Trích xuất đặc trưng
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /data/scripts/spark/feature_extraction.py
"docker exec -it spark-master pip install numpy
docker exec -it spark-worker pip install numpy"
# 3. Huấn luyện mô hình & Lưu kết quả vào MongoDB
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /data/scripts/spark/ml_training.py
```
*Sau bước này, truy cập API [http://localhost:8000/metrics](http://localhost:8000/metrics) để xem kết quả.*

---

##  BƯỚC 5: Chạy Pipeline STREAMING (Xử lý Thời gian thực)
Phần demo đồ án:

1. **Terminal 1 (Nguồn dữ liệu):**
   ```powershell
   docker exec -it spark-master python3 /data/scripts/kafka/producer.py
   ```
   *(Để terminal này chạy để đẩy dữ liệu liên tục).*

2. **Terminal 2 (Bộ xử lý Spark):**
   ```powershell
   docker exec -it spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /data/scripts/spark/streaming_sentiment.py
   ```
   *(Đợi Spark khởi động và bắt đầu phân tích cảm xúc).*

---

##  BƯỚC 6: Kiểm tra Kết quả & Monitoring

| Dịch vụ | Địa chỉ truy cập | Công dụng |
|---------|-----------------|-----------|
| **API Docs** | [http://localhost:8000/docs](http://localhost:8000/docs) | **Demo chính**: Xem kết quả trả về JSON |
| **Airflow** | [http://localhost:8081](http://localhost:8081) | Quản lý quy trình tự động (admin/admin) |
| **Spark Master** | [http://localhost:8080](http://localhost:8080) | Theo dõi các job đang chạy |
| **HDFS UI** | [http://localhost:9870](http://localhost:9870) | Quản lý file trên cụm Hadoop |

---
# Run dashboard

python dashboard/app.py


## Positive Sentiment
I love this product!
I am very happy with the results.
## Negative Sentiment
This app crashes all the time.
Customer support is awful.
Completely useless system.
The food tastes bad.
## 🛠 XỬ LÝ LỖI THƯỜNG GẶP

1. **Lỗi `ImportError: No module named pymongo`**: Do bạn chưa đợi Spark cài thư viện xong hoặc chưa chạy `docker-compose down && docker-compose up -d`.
2. **HDFS Safe Mode**: Chạy `docker exec namenode hdfs dfsadmin -safemode leave`.
3. **API không có dữ liệu**: Đảm bảo đã chạy xong Bước 4 và đang chạy Bước 5.
4. **Kafka Producer không chạy**: Đảm bảo chạy lệnh `docker exec -it spark-master...` (chạy từ spark-master vì đã có sẵn thư viện).

---
