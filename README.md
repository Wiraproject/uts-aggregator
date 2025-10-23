# 🌀 UTS Aggregator

Sistem Event Aggregator berbasis FastAPI dengan dukungan deduplication store (SQLite), consumer asynchronous (asyncio), serta pengujian performa dan unit test otomatis.

---

## 🚀 Quick Start
### 1. Clone Repository
```
git clone https://github.com/Wiraproject/uts-aggregator.git
cd uts-aggregator
```

---

## 📦 Cara Build & Run
### 1. Build semua services
```
docker-compose build
```

### 2. Jalankan semua services
```
docker-compose up -d  
```

### 3. Akses Dokumentasi API 
```
http://localhost:8080/docs
```

---

## 🧪 Testing
### 1. Unit Test
```
pytest -v
```

### 2. Performance Test (Skala Uji)
Untuk menguji performa sistem dengan lebih dari 5000
```
python performance_test.py
```
Tes ini akan mengukur kecepatan ingest dan deduplikasi data menggunakan endpoint /publish dan /stats.

---

## 🧠 Asumsi Desain Sistem
### 1. Deduplication Strategy
#### Key: Composite key (topic, event_id) digunakan untuk identify unique events
#### Storage: SQLite dengan PRIMARY KEY constraint untuk atomic duplicate detection
#### Rationale: Simple, reliable, dan tidak memerlukan external dependencies

### 2. At-Least-Once → Exactly-Once
#### Problem: Distributed systems sering menggunakan at-least-once delivery (event bisa dikirim berulang kali)
#### Solution: Idempotent processing dengan deduplication store
#### Implementation: Setiap event di-check di database sebelum processing

### 3. Persistence
#### Database: SQLite
#### Volume: Docker volume untuk persist data across restarts
#### Benefit: Simple deployment, no external database required

### 4. Async Processing
#### Queue: asyncio.Queue untuk decoupling ingestion dan processing
#### Consumer: Background task yang consume events dari queue
#### Benefit: High throughput, non-blocking operations

### 5. Multi-Service Architecture
#### Separation: Publisher dan Aggregator sebagai 2 services terpisah
#### Communication: Internal Docker network (service discovery via DNS)
#### Rationale: Mensimulasikan microservices architecture

---

## 📡 Endpoint API

| Method | Endpoint | Description | Request Body | Response |
|--------|----------|-------------|--------------|----------|
| **POST** | `/publish` | Menerima event (single atau batch) | Event JSON / Array | `{"accepted": int, "duplicates_rejected": int}` |
| **GET** | `/events` | Mengambil semua events yang tersimpan | - | `{"events": [Event]}` |
| **GET** | `/events?topic={topic}` | Filter events berdasarkan topic | - | `{"events": [Event]}` |
| **GET** | `/stats` | Statistik sistem real-time | - | Stats JSON |

---

# 👤 Author
#### Wiranto (Wira)
#### 📍 Institut Teknologi Kalimantan
#### 📧 github.com/Wiraproject