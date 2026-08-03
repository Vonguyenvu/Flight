# Flight Airspace Density Tracker

Pipeline streaming theo dõi mật độ máy bay trong không phận khu vực Hà Nội theo thời gian thực, sử dụng **Kafka** làm hàng đợi đệm và **Apache Spark Structured Streaming** để xử lý và tổng hợp dữ liệu theo Kiến trúc Medallion (Bronze → Silver → Gold).

---

## 1. Mô tả dự án

### Vấn đề

Dữ liệu vị trí máy bay theo thời gian thực (state vectors) chỉ cho biết **1 lát cắt tại 1 thời điểm** — không có sẵn lịch sử. Để trả lời câu hỏi "mật độ không phận thay đổi ra sao theo thời gian, theo độ cao?", cần liên tục thu thập (poll) và tích lũy dữ liệu theo dòng thời gian.

Dự án xây dựng pipeline thu thập dữ liệu này liên tục, xử lý theo thời gian thực, và tổng hợp thành các chỉ số:

- Số lượng máy bay (unique) xuất hiện trong khu vực theo từng khung giờ
- Số lượng máy bay phân theo dải độ cao

### Phạm vi & giới hạn

- Đây là **proof-of-concept**, không phải hệ thống vận hành thật. Dữ liệu từ OpenSky là crowd-sourced (không phải nguồn chính thức của cơ quan không lưu), độ phủ phụ thuộc vào mật độ trạm thu ADS-B tình nguyện trong khu vực.
- Trường `on_ground` gần như không ghi nhận được giá trị `True` trong khu vực Việt Nam do thiếu trạm thu gần mặt đất sân bay — vì vậy pipeline **không** dùng để phát hiện sự kiện cất/hạ cánh, chỉ tập trung vào mật độ máy bay đang bay (airborne).
- OpenSky giới hạn credit truy vấn API/ngày (tùy theo tier tài khoản) — bounding box được thu hẹp quanh khu vực Hà Nội để tối ưu chi phí truy vấn.

---

## 2. Nguồn dữ liệu

**[OpenSky Network API](https://opensky-network.org/)** — endpoint `All State Vectors`, xác thực qua OAuth2 (client credentials).

Trường dữ liệu sử dụng:

| Trường | Mô tả |
|---|---|
| `icao24` | Mã định danh máy bay (24-bit ICAO address) |
| `callsign` | Hô hiệu chuyến bay |
| `origin_country` | Quốc gia đăng ký máy bay |
| `longitude`, `latitude` | Tọa độ |
| `altitude` | Độ cao khí áp (mét) |
| `on_ground` | Cờ máy bay đang ở mặt đất |
| `velocity` | Vận tốc (m/s) |
| `event_time` | Thời điểm snapshot được thu thập (do producer tự gắn) |

Bounding box mặc định: khu vực Hà Nội (`lamin=20.5, lomin=105.3, lamax=21.5, lomax=106.3`).

---

## 3. Kiến trúc & luồng dữ liệu

```
OpenSky API (state vectors, OAuth2)
        │  poll mỗi 30s
        ▼
  producer.py ──────────► Kafka (Confluent Cloud, topic riêng)
                                  │
                                  ▼
                        main_streaming.py
                    (Spark Structured Streaming,
                     trigger 30s, foreachBatch)
                                  │
                                  ▼
                  PostgreSQL — bronze_flights
                  (raw, append-only, có event_time)
                                  │
                                  ▼
                        transform_silver.py
                (dedup, lọc data lỗi/thiếu, chuẩn hóa)
                                  │
                                  ▼
                  PostgreSQL — silver_transform
                                  │
                                  ▼
                         analysis_gold.py
                (windowing theo giờ, bucket theo độ cao)
                                  │
                                  ▼
              PostgreSQL — gold_density_by_hour
                          gold_density_by_altitude
```

### Các tầng dữ liệu (Medallion Architecture)

- **Bronze** (`bronze_flights`): dữ liệu thô, append-only, giữ nguyên mọi snapshot thu thập được — nguồn sự thật duy nhất, có thể replay lại các tầng sau bất cứ lúc nào.
- **Silver** (`silver_transform`): loại bỏ trùng lặp (`icao24` + `event_time`), lọc bản ghi thiếu/không hợp lệ (tọa độ ngoài phạm vi, độ cao/vận tốc âm, `callsign` rỗng).
- **Gold**:
  - `gold_density_by_hour`: số lượng máy bay unique theo từng khung **1 giờ**
  - `gold_density_by_altitude`: số lượng máy bay unique theo dải độ cao **10.000m** (chỉ tính máy bay đang bay, loại `on_ground=True`)

---

## 4. Công nghệ sử dụng

| Thành phần | Công nghệ |
|---|---|
| Ngôn ngữ | Python 3.12 |
| Message queue | Apache Kafka (Confluent Cloud, managed) |
| Xử lý stream | Apache Spark 3.5.3 (PySpark, Structured Streaming) |
| Lưu trữ | PostgreSQL |
| Kết nối Spark ↔ Postgres | JDBC (`org.postgresql:postgresql`) |
| Xác thực nguồn dữ liệu | OAuth2 client credentials (OpenSky) |
| Quản lý secrets | biến môi trường (`.env`, `python-dotenv`) |

---

## 5. Cấu trúc thư mục

```
Flight/
├── .env                  # secrets thật — KHÔNG commit lên git
├── .env.example           # template, an toàn để commit
├── .gitignore
├── requirements.txt
├── schema.sql              # DDL tạo bảng bronze_flights
├── config.py                # đọc .env, cấu hình Kafka/Postgres/OpenSky
├── producer.py               # lấy data OpenSky, gửi vào Kafka
├── main_streaming.py          # Spark Structured Streaming: Kafka → Bronze
├── transform_silver.py         # Bronze → Silver (làm sạch)
├── analysis_gold.py             # Silver → Gold (tổng hợp mật độ)
└── checkpoints/                  # Spark streaming checkpoint (tự tạo khi chạy)
```

---

## 6. Hướng dẫn cài đặt

### 6.1. Yêu cầu hệ thống

- Python 3.12
- Java 17 (bắt buộc cho PySpark)
- PostgreSQL (native, cài qua package manager của hệ điều hành)

```bash
sudo apt update
sudo apt install openjdk-17-jdk postgresql -y
java -version
```

### 6.2. Cài Python packages

```bash
python -m venv env
source env/bin/activate
pip install -r requirements.txt
```

### 6.3. Tạo Kafka cluster (Confluent Cloud)

1. Tạo tài khoản tại [confluent.cloud](https://confluent.cloud) (free tier)
2. Tạo 1 Kafka cluster (Basic)
3. Tạo 1 topic (ví dụ `flights_data`)
4. Tạo API Key/Secret cho cluster, lấy Bootstrap server URL

### 6.4. Đăng ký OpenSky OAuth2 client credentials

Theo hướng dẫn tại [OpenSky Network API docs](https://openskynetwork.github.io/opensky-api/) để lấy `client_id`/`client_secret` (tăng hạn mức truy vấn so với tài khoản ẩn danh).

### 6.5. Tạo database & bảng PostgreSQL

```bash
sudo -u postgres psql -c "CREATE DATABASE flight_db;"
sudo -u postgres psql -d flight_db -f schema.sql
```

### 6.6. Cấu hình `.env`

```bash
cp .env.example .env
```

Điền đầy đủ:

```
OPENSKY_CLIENT_ID=...
OPENSKY_CLIENT_SECRET=...

KAFKA_BOOTSTRAP_SERVERS=...
KAFKA_API_KEY=...
KAFKA_API_SECRET=...
KAFKA_TOPIC=flights_data

POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=flight_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=...
```

Kiểm tra config:

```bash
python config.py
```

---

## 7. Vận hành

### 7.1. Chạy pipeline lần đầu / hàng ngày

Mở **2 terminal riêng biệt**, chạy song song và để chạy liên tục:

```bash
# Terminal 1 — thu thập dữ liệu liên tục, mỗi 30 giây
python producer.py
```

```bash
# Terminal 2 — xử lý stream, ghi vào Bronze, mỗi 30 giây
python main_streaming.py
```

Dừng bằng `Ctrl+C` ở cả 2 terminal khi cần tạm nghỉ. Dữ liệu đã ghi được giữ nguyên; chạy lại vào lần sau sẽ **tiếp tục tích lũy** (append), không mất dữ liệu cũ — miễn không xóa thư mục `checkpoints/`.

### 7.2. Chạy tầng Silver và Gold (theo yêu cầu, không tự động)

Sau khi đã tích lũy đủ dữ liệu (khuyến nghị để `producer.py`/`main_streaming.py` chạy tối thiểu vài giờ để có đủ nhiều khung giờ khác nhau):

```bash
python transform_silver.py
python analysis_gold.py
```

Mỗi lần chạy, Silver và Gold sẽ được **tính lại toàn bộ** (`mode="overwrite"`) dựa trên toàn bộ dữ liệu Bronze hiện có.

### 7.3. Kiểm tra dữ liệu

```bash
sudo -u postgres psql -d flight_db -c "SELECT COUNT(*) FROM bronze_flights;"
sudo -u postgres psql -d flight_db -c "SELECT * FROM gold_density_by_hour ORDER BY window_start;"
sudo -u postgres psql -d flight_db -c "SELECT * FROM gold_density_by_altitude ORDER BY altitude_bucket;"
```

### 7.4. Reset dữ liệu để chạy lại từ đầu

```bash
sudo -u postgres psql -d flight_db -c "TRUNCATE TABLE bronze_flights RESTART IDENTITY;"
rm -rf checkpoints/
```

---

## 8. Hướng phát triển tiếp theo

- Thêm dashboard trực quan (Metabase/Power BI hoặc chart Python) đọc từ 2 bảng Gold
- Airflow orchestration để tự động hóa lịch chạy Silver/Gold
- Mở rộng bounding box hoặc thêm nhiều khu vực theo dõi song song
- dbt để quản lý transform Silver/Gold thay cho PySpark script thuần