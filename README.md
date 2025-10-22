# 🌀 UTS Aggregator

Sistem Event Aggregator berbasis FastAPI dengan dukungan deduplication store (SQLite), consumer asynchronous (asyncio), serta pengujian performa dan unit test otomatis.

---

## 🚀 Langkah Instalasi & Menjalankan Aplikasi
### 1. Clone Repository
```
git clone https://github.com/Wiraproject/uts-aggregator.git
cd uts-aggregator
```

### 2. Buat Virtual Environment
```
python -m venv venv
```

### 3. Install Dependencies
```
pip install -r requirements.txt
```

### 4. Jalankan Server Lokal
```
uvicorn src.main:app --reload

Secara default, aplikasi akan berjalan di:
🔗 http://127.0.0.1:8000
```

### 5. Lihat Dokumentasi API Lokal
```
http://127.0.0.1:8000/docs
```

---

## 🐳 Build dan Run Menggunakan Docker
### 1. Build Image
```
docker build -t uts-aggregator .
```

### 2. Jalankan Container
```
docker run -d -p 8080:8080 --name aggregator uts-aggregator
```

### 3. Akses Dokumentasi API di Container
```
http://localhost:8080/docs
```

---

## 🧪 Testing
### 1. Unit Test
```
pytest -v
```

## 2. Performance Test (Skala Uji)
Untuk menguji performa sistem dengan lebih dari 5000
```
python performance_test.py
```
Tes ini akan mengukur kecepatan ingest dan deduplikasi data menggunakan endpoint /publish dan /stats.

---

## 🧠 Asumsi Desain Sistem
### 1. Event Aggregation
Sistem menerima event dalam format JSON, baik tunggal maupun batch, melalui endpoint /publish.

### 2. Deduplication Logic
Event dengan kombinasi (topic, event_id) yang sama dianggap duplikat dan tidak akan disimpan ulang.

### 3. Penyimpanan Data
Menggunakan SQLite sebagai basis data sederhana (data.db).
Mode WAL (Write-Ahead Logging) diaktifkan untuk performa dan concurrency yang lebih baik.

### 4. Consumer
Proses asynchronous (asyncio.Queue) yang mengkonsumsi event dari antrian dan memperbarui statistik.

### 5. Statistik Real-time
Endpoint /stats digunakan untuk memantau jumlah event diterima, diproses unik, dan duplikat yang ditolak.

---

## 📡 Endpoint API
| Metode   | Endpoint                | Deskripsi                                                                 | Contoh Response                                                                                                  |
| :------- | :---------------------- | :------------------------------------------------------------------------ | :--------------------------------------------------------------------------------------------------------------- |
| **POST** | `/publish`              | Menerima 1 atau banyak event untuk diproses dan dimasukkan ke dedup store | `{ "enqueued": 5, "duplicates_rejected": 0 }`                                                                    |
| **GET**  | `/events`               | Mengambil semua event yang tersimpan                                      | `{ "events": [ { "topic": "t1", "event_id": "id-1", ... } ] }`                                                   |
| **GET**  | `/events?topic={topic}` | Mengambil event berdasarkan topic tertentu                                | `{ "events": [ ... ] }`                                                                                          |
| **GET**  | `/stats`                | Menampilkan statistik sistem saat ini                                     | `{ "received": 10, "unique_processed": 8, "duplicate_dropped": 2, "uptime_seconds": 25, "topics": ["t1","t2"] }` |

---

# 👤 Author
#### Wiranto (Wira)
#### 📍 Institut Teknologi Kalimantan
#### 📧 github.com/Wiraproject