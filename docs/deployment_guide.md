# Deployment Guide – Distributed Synchronization System

**NIM:** 11231060 | Tugas 3 – SISTER ITK

---

## Daftar Isi

1. [Prasyarat](#1-prasyarat)
2. [Cara Cepat – Docker Compose](#2-cara-cepat--docker-compose)
3. [Cara Manual – Lokal (Tanpa Docker)](#3-cara-manual--lokal-tanpa-docker)
4. [Menjalankan Tests](#4-menjalankan-tests)
5. [Load Testing dengan Locust](#5-load-testing-dengan-locust)
6. [Referensi Endpoint API](#6-referensi-endpoint-api)
7. [Konfigurasi Environment](#7-konfigurasi-environment)
8. [Troubleshooting](#8-troubleshooting)

---

## 1. Prasyarat

### Software yang Dibutuhkan

| Software | Versi | Keterangan |
|----------|-------|------------|
| Python | 3.11+ | Wajib |
| pip | 23+ | Wajib |
| Docker Desktop | 24+ | Untuk mode Docker |
| Docker Compose | v2 (bundled) | Untuk mode Docker |
| curl / PowerShell | Built-in | Untuk verifikasi |

### Cek Prasyarat

```powershell
python --version        # harus >= 3.11
docker --version        # harus >= 24.x
docker compose version  # harus v2.x
```

---

## 2. Cara Cepat – Docker Compose

Cara paling mudah menjalankan sistem lengkap (9 node + Redis) dalam satu perintah.

### Langkah 2.1 – Masuk ke folder proyek

```powershell
cd distributed-sync-system
```

### Langkah 2.2 – Siapkan file konfigurasi

```powershell
copy .env.example .env
```

> File `.env` sudah berisi nilai default yang langsung bisa dipakai.

### Langkah 2.3 – Build dan jalankan semua services

```powershell
# Menggunakan run script (rekomendasi)
.\run.ps1 docker

# Atau manual
docker compose -f docker/docker-compose.yml up --build -d
```

Output yang diharapkan:
```
[+] Running 10/10
 ✔ Container dss-redis      Started
 ✔ Container lock-node-1    Started
 ✔ Container lock-node-2    Started
 ✔ Container lock-node-3    Started
 ✔ Container queue-node-1   Started
 ✔ Container queue-node-2   Started
 ✔ Container queue-node-3   Started
 ✔ Container cache-node-1   Started
 ✔ Container cache-node-2   Started
 ✔ Container cache-node-3   Started
```

### Langkah 2.4 – Tunggu 10–15 detik lalu verifikasi

```powershell
# Cek status semua node sekaligus
.\run.ps1 status

# Atau manual
curl http://localhost:8100/health
curl http://localhost:8200/health
curl http://localhost:8300/health
```

Response yang diharapkan:
```json
{"node_id": "lock-node-1", "status": "ok", "uptime": 12.5}
```

### Langkah 2.5 – Jalankan demo lengkap

```powershell
.\run.ps1 demo
# atau demo interaktif lebih lengkap:
.\scripts\demo_all.ps1
```

### Langkah 2.6 – Cek logs (opsional)

```powershell
# Semua node
.\run.ps1 logs

# Satu node saja
docker compose -f docker/docker-compose.yml logs -f lock-node-1
```

### Langkah 2.7 – Stop semua

```powershell
.\run.ps1 stop
# atau
docker compose -f docker/docker-compose.yml down
```

### Scaling dinamis

```powershell
# Scale queue node menjadi 5 instance
docker compose -f docker/docker-compose.yml up --scale queue-node-1=5 -d
```

---

## 3. Cara Manual – Lokal (Tanpa Docker)

Gunakan ini jika ingin debugging lebih mudah atau melihat output tiap node di terminal terpisah.

### Langkah 3.1 – Install Python dependencies

```powershell
cd distributed-sync-system

# Buat dan aktifkan virtual environment
python -m venv venv
.\venv\Scripts\Activate.ps1

# Install semua dependencies
pip install -r requirements.txt
```

### Langkah 3.2 – Salin konfigurasi

```powershell
copy .env.example .env
```

### Langkah 3.3 – Jalankan Redis via Docker

```powershell
docker run -d --name dss-redis -p 6379:6379 redis:7-alpine
```

Verifikasi Redis:
```powershell
docker exec dss-redis redis-cli ping   # harus PONG
```

### Langkah 3.4 – Jalankan Lock Manager (3 terminal)

Buka **3 PowerShell window** secara bersamaan.

**Terminal 1 – lock-node-1:**
```powershell
cd distributed-sync-system
.\venv\Scripts\Activate.ps1
$env:NODE_ID="lock-node-1"; $env:NODE_PORT="8100"
$env:CLUSTER_NODES="localhost:8100,localhost:8101,localhost:8102"
$env:REDIS_HOST="localhost"
python -m src.nodes.lock_manager
```

**Terminal 2 – lock-node-2:**
```powershell
cd distributed-sync-system
.\venv\Scripts\Activate.ps1
$env:NODE_ID="lock-node-2"; $env:NODE_PORT="8101"
$env:CLUSTER_NODES="localhost:8100,localhost:8101,localhost:8102"
$env:REDIS_HOST="localhost"
python -m src.nodes.lock_manager
```

**Terminal 3 – lock-node-3:**
```powershell
cd distributed-sync-system
.\venv\Scripts\Activate.ps1
$env:NODE_ID="lock-node-3"; $env:NODE_PORT="8102"
$env:CLUSTER_NODES="localhost:8100,localhost:8101,localhost:8102"
$env:REDIS_HOST="localhost"
python -m src.nodes.lock_manager
```

### Langkah 3.5 – Jalankan Queue Node (3 terminal)

**Terminal 4 – queue-node-1:**
```powershell
$env:NODE_ID="queue-node-1"; $env:NODE_PORT="8200"
$env:CLUSTER_NODES="localhost:8200,localhost:8201,localhost:8202"
$env:REDIS_HOST="localhost"
python -m src.nodes.queue_node
```

**Terminal 5 – queue-node-2:** (port 8201, node-id queue-node-2)

**Terminal 6 – queue-node-3:** (port 8202, node-id queue-node-3)

### Langkah 3.6 – Jalankan Cache Node (3 terminal)

**Terminal 7 – cache-node-1:**
```powershell
$env:NODE_ID="cache-node-1"; $env:NODE_PORT="8300"
$env:CLUSTER_NODES="localhost:8300,localhost:8301,localhost:8302"
python -m src.nodes.cache_node
```

**Terminal 8 – cache-node-2:** (port 8301)
**Terminal 9 – cache-node-3:** (port 8302)

### Alternatif: Jalankan otomatis (buka 9 terminal sekaligus)

```powershell
.\run.ps1 local
```

Perintah ini akan membuka 9 PowerShell window secara otomatis.

---

## 4. Menjalankan Tests

### Setup (jika belum)

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### Unit Tests (tidak perlu node berjalan)

```powershell
# Jalankan semua unit test
python -m pytest tests/unit/ -v

# Jalankan satu file saja
python -m pytest tests/unit/test_raft.py -v
python -m pytest tests/unit/test_lock_manager.py -v
python -m pytest tests/unit/test_cache.py -v
python -m pytest tests/unit/test_queue.py -v
```

### Performance Benchmark (tidak perlu node berjalan)

```powershell
# Benchmark in-memory (LRU, Queue ring, Raft log)
python -m pytest tests/performance/ -v -s
```

Target performa:
| Operasi | Target |
|---------|--------|
| LRU cache write | > 50.000 ops/s |
| LRU cache read | > 100.000 ops/s |
| Queue enqueue | > 10.000 ops/s |
| Raft log append | > 100.000 ops/s |

### Integration Tests (butuh semua node berjalan)

```powershell
# Jalankan Docker atau local terlebih dahulu
.\run.ps1 docker

# Kemudian jalankan integration tests
python -m pytest tests/integration/ -v
```

### Coverage Report

```powershell
# Coverage ke terminal
python -m pytest --cov=src --cov-report=term-missing tests/unit/

# Coverage ke HTML
python -m pytest --cov=src --cov-report=html tests/unit/
# Buka htmlcov/index.html di browser
```

### Gunakan run script

```powershell
.\run.ps1 test      # unit + performance
.\run.ps1 inttest   # integration
```

---

## 5. Load Testing dengan Locust

Pastikan node sudah berjalan (mode docker atau local).

### Web UI (interaktif)

```powershell
# Buka http://localhost:8089 untuk konfigurasi GUI
locust -f benchmarks/load_test_scenarios.py --host http://localhost:8100
```

### Headless Mode (untuk report otomatis)

```powershell
# Lock Manager – 50 users, 60 detik
locust -f benchmarks/load_test_scenarios.py LockUser `
  --host http://localhost:8100 `
  --users 50 --spawn-rate 5 --run-time 60s --headless `
  --html benchmarks/report_lock.html

# Queue Node – 50 users, 60 detik
locust -f benchmarks/load_test_scenarios.py QueueUser `
  --host http://localhost:8200 `
  --users 50 --spawn-rate 5 --run-time 60s --headless `
  --html benchmarks/report_queue.html

# Cache Node – 50 users, 60 detik
locust -f benchmarks/load_test_scenarios.py CacheUser `
  --host http://localhost:8300 `
  --users 50 --spawn-rate 5 --run-time 60s --headless `
  --html benchmarks/report_cache.html
```

### Gunakan run script

```powershell
.\run.ps1 bench
```

Laporan HTML tersimpan di folder `benchmarks/`.

---

## 6. Referensi Endpoint API

### Common (semua node)

| Method | Endpoint | Deskripsi |
|--------|----------|-----------|
| GET | `/health` | Status dan uptime node |
| GET | `/metrics` | Metrics snapshot (counter, gauge, histogram) |

### Lock Manager (port 8100–8102)

| Method | Endpoint | Body / Params |
|--------|----------|---------------|
| POST | `/lock/acquire` | `{"resource":"r1","client_id":"c1","lock_type":"exclusive"}` |
| POST | `/lock/release` | `{"resource":"r1","client_id":"c1"}` |
| POST | `/lock/heartbeat` | `{"resource":"r1","client_id":"c1"}` |
| GET | `/lock/status/{resource}` | – |
| GET | `/lock/deadlocks` | – |

`lock_type` bisa `"exclusive"` atau `"shared"`.

### Queue Node (port 8200–8202)

| Method | Endpoint | Body |
|--------|----------|------|
| POST | `/queue/enqueue` | `{"queue":"orders","payload":{...},"producer_id":"p1"}` |
| POST | `/queue/dequeue` | `{"queue":"orders","consumer_id":"c1"}` |
| POST | `/queue/ack` | `{"msg_id":"..."}` |
| GET | `/queue/stats/{queue}` | – |

### Cache Node (port 8300–8302)

| Method | Endpoint | Body |
|--------|----------|------|
| GET | `/cache/{key}` | – |
| PUT | `/cache/{key}` | `{"value": ...}` |
| DELETE | `/cache/{key}` | – |
| GET | `/cache` | – (list keys) |
| GET | `/cache/stats` | – |

---

## 7. Konfigurasi Environment

Semua konfigurasi disimpan di file `.env`:

```bash
# ---- Node Identity ----
NODE_ID=lock-node-1          # unik per node
NODE_HOST=0.0.0.0
NODE_PORT=8100

# ---- Cluster ----
CLUSTER_NODES=localhost:8100,localhost:8101,localhost:8102

# ---- Raft ----
ELECTION_TIMEOUT_MIN=1.5     # detik
ELECTION_TIMEOUT_MAX=3.0
HEARTBEAT_INTERVAL=0.5

# ---- Redis ----
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
REDIS_PASSWORD=              # kosong = tanpa password

# ---- Queue ----
QUEUE_REPLICAS=3
QUEUE_MAX_SIZE=10000
QUEUE_ACK_TIMEOUT=30.0

# ---- Cache ----
CACHE_SIZE=1000
CACHE_POLICY=LRU             # LRU | LFU

# ---- Lock ----
LOCK_LEASE_DURATION=30.0
LOCK_RETRY_INTERVAL=0.1
DEADLOCK_DETECTION_INTERVAL=5.0

# ---- Failure Detector ----
FAILURE_TIMEOUT=5.0
PING_INTERVAL=1.0

# ---- Logging ----
LOG_LEVEL=INFO               # DEBUG | INFO | WARNING | ERROR
```

---

## 8. Troubleshooting

| Masalah | Kemungkinan Penyebab | Solusi |
|---------|---------------------|--------|
| `/health` tidak merespon | Node belum selesai startup | Tunggu 10–15 detik setelah `docker compose up` |
| Raft leader tidak terpilih | Kurang dari 2 node aktif | Pastikan minimal 2 dari 3 lock node berjalan |
| `Connection refused` di port 8100 | Node crash atau tidak dijalankan | Cek `docker compose ps` atau log |
| Lock tidak dirilis setelah crash | Normal – tunggu lease expiry | Default 30 detik, bisa dikurangi via `LOCK_LEASE_DURATION` |
| Queue message hilang | Replica tidak tersimpan | Pastikan `QUEUE_REPLICAS=3` dan semua queue node aktif |
| Cache data stale | Invalidation gagal dikirim | Cek log `cache_invalidations_sent` di `/metrics` |
| Redis connection error | Redis tidak berjalan | `docker run -d -p 6379:6379 redis:7-alpine` |
| Import error saat `python -m` | Jalankan dari root folder | Pastikan cwd = folder `distributed-sync-system` |
| `uvloop` error di Windows | uvloop tidak support Windows | Normal – sistem berjalan tanpa uvloop (asyncio default) |

### Cek log Docker

```powershell
# Semua node
docker compose -f docker/docker-compose.yml logs

# Node tertentu
docker compose -f docker/docker-compose.yml logs lock-node-1

# Live streaming
docker compose -f docker/docker-compose.yml logs -f
```

### Reset ulang (bersih)

```powershell
# Hentikan dan hapus semua container + volume
docker compose -f docker/docker-compose.yml down -v

# Build ulang dari awal
docker compose -f docker/docker-compose.yml up --build -d
```
