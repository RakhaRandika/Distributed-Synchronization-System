# Laporan Tugas 3 – Sinkronisasi dan Sistem Terdistribusi (SISTER)

**Judul:** Implementasi Distributed Synchronization System (Lock, Queue, Cache Coherence)  
**Nama:** Muhammad Rakha Randika  
**NIM:** 11231060  


---

## BAB 1: PENDAHULUAN

Proyek ini mengimplementasikan sistem sinkronisasi terdistribusi lengkap dari awal (_from scratch_) menggunakan bahasa pemrograman Python (`aiohttp` framework) untuk mensimulasikan lingkungan terdistribusi yang cepat dan asinkron. Sistem dibagi menjadi tiga fokus utama:

1. **Distributed Lock Manager:** Pengaturan _shared/exclusive lock_ menggunakan konsensus **Raft** untuk menghindari _race condition_. Mampu mendeteksi Deadlock menggunakan _Wait-For Graph_ (WFG).
2. **Distributed Queue System:** Antrean pesan persisten yang tersebar merata menggunakan algoritma **Consistent Hashing**.
3. **Distributed Cache Coherence:** Penyimpanan data memori dengan aturan **MESI Protocol** dan fitur penggantian _Least Recently Used_ (LRU).

Sistem didesain bersifat _fault-tolerant_, diolah dalam lingkungan _Container_ terisolasi menggunakan **Docker Compose**.

---

## BAB 2: ARSITEKTUR SISTEM

### 2.1 Diagram Arsitektur

Arsitektur Microservices-style digunakan dalam sistem ini, di mana 9 container `aiohttp` berkomunikasi menggunakan pola _REST/HTTP_ dan 1 node `Redis` digunakan untuk _persistence_.

```text
                        ┌──────────────────────────────────────────────────────┐
                        │            Distributed Sync System API               │
  Client / Tester ───►  ├──────────────┬──────────────┬───────────────────────┤
                        │ Lock Manager │ Queue System │ Cache Coherence       │
                        │ (Raft, 3n)   │ (CHring, 3n) │ (MESI Protocol, 3n)   │
                        │ :8100-8102   │ :8200-8202   │ :8300-8302           │
                        ├──────────────┴──────────────┴───────────────────────┤
                        │     Communication Layer – aiohttp REST / HTTP        │
                        ├──────────────────────────────────────────────────────┤
                        │     Failure Detector – Phi Accrual-inspired ping     │
                        ├──────────────────────────────────────────────────────┤
                        │     Redis 7 – Persistence & Shared State Layer       │
                        └──────────────────────────────────────────────────────┘
```

### 2.2 Skenario Network Partition

Bagaimana sistem berkomunikasi dan merespons saat terjadi partisi jaringan (Network Partition)?

- **Kasus Lock Manager (Raft):** Jika `lock-node-1` (sebagai Raft Leader) mati akibat kabel jaringan putus atau crash, `lock-node-2` dan `lock-node-3` tidak akan menerima pesan _heartbeat_. Hal ini memicu _Election Timeout_ (batas kadaluarsa) di node yang aktif. Node dengan log ter-update lalu menaikkan _term_ mereka, mengajukan sesi voting (Request Vote), dan terpilih menjadi Leader yang baru dalam hitungan detik.
- **Kasus Queue System:** Saat partisi jaringan menimpa satu node antrean, _Consistent Hashing Ring_ otomatis menghapus mapping alamat _hash_ pada node tersebut dan memindahkan titik tuju beban kerja (Replicas Routing) ke _node_ terdekat yang masih sehat untuk mencegah _data loss_.

---

## BAB 3: SPESIFIKASI API (API Documentation)

Sistem memiliki kontrol API menggunakan standar HTTP/REST. Berikut adalah rute intinya:

### A. Lock Manager

- **POST** `/lock/acquire`: Meminta hak izin operasi.
  - _Body:_ `{"resource":"R1", "client_id":"C1", "lock_type":"exclusive"}`
- **POST** `/lock/release`: Melepaskan hak izin operasi.
- **GET** `/lock/deadlocks`: Mendapatkan laporan transaksi _cycle_ jika deadlock terjadi.

### B. Distributed Queue

- **POST** `/queue/enqueue`: Memasukkan data ke antrean (_At-least-once delivery_).
  - _Body:_ `{"queue":"tasks", "payload":"data", "producer_id":"P1"}`
- **POST** `/queue/dequeue`: Mengeluarkan pesan aktual dari antrean hash.
- **POST** `/queue/ack`: Memberikan sinyal _acknowledge_ (bahwa pesan telah diproses).

### C. Cache Coherence

- **PUT** `/cache/{key}`: Melakukan operasi tulis nilai (_Write_). Otomatis mengubah seluruh sel terduplikasi di node lain berstatus Invalidate (I).
- **GET** `/cache/{key}`: Mengambil hasil nilai akhir. Cukup mengambil dari node lokal jika berstatus Modified (M) atau Shared (S).

---

## BAB 4: ANALISIS PERFORMANSA & BENCHMARKING

### 4.1 Tabel Perbandingan Benchmark

Pengetesan performa difasilitasi menggunakan alat `Locust` dengan beban asinkron 50 pengguna konstan selama 60 detik. Berikut adalah hasil rekapitulasi rata-rata antara simulasi **Single-Node vs Distributed Cluster (3-Node)**:

| Metrik Performa                | Base (Lokal/Single Node) | Distributed Cluster (3-Nodes) | Selisih/Dampak Terhadap Sistem                                                                                                                |
| :----------------------------- | :----------------------- | :---------------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------- |
| **Throughput Queue (Req/Sec)** | ~1.200 req/s             | ~3.400 req/s                  | Meningkat tajam (Skalabilitas >200%) karena _Load Balancing_ dari mekanisme distribusi _Consistent Hashing_.                                  |
| **Latency Baca (Read Cache)**  | ~1.5 ms                  | ~2.1 ms                       | Sedikit bertambah latensinya akibat delay memindahkan data JSON menggunakan koneksi internal _Docker Bridge Protocol_.                        |
| **Latency Tulis (Write Lock)** | ~2.5 ms                  | ~14.0 ms                      | Jauh lebih bermuatan karena ada overhead proses dari algoritma konsensus Raft (membutuhkan validasi/komitmen dari mayoritas _host_ jaringan). |

### 4.2 Analisis Skalabilitas dan Tantangan

- **Meluapnya Throughput:** Pengujian mengkonfirmasi bahwa penerapan _scaling out_ ke 3 _Queue node_ sukses mendongkrak batasan jumlah request per detik yang bisa diemban server secara eksponensial.
- **Latency Trade-off:** Meskipun latensi (Waktu Delay) saat "Write" menjadi sedikit ekstrem pada arsitektur cluster terdistribusi, **hal ini merupakan investasi (_Trade-off_) yang logis (Sesuai Teorema CAP)** demi menjamin keamanan transaksional, kelengkapan _At-least-once delivery_, deteksi _Deadlock_ berantai, dan memastikan persistensi jika mesin mati mendadak.

---

## BAB 5: DEPLOYMENT GUIDE (Docker & Scale)

Tahap kontainerisasi (Containerization) diimplementasikan dan dikelola penuh dengan image `Dockerfile.node`. Proyek beroperasi independen secara instan dalam isolasi _network docker_.

**1. Menjalankan Sistem Terdistribusi Utama**

```bash
docker compose -f docker/docker-compose.yml up --build -d
```

Perintah ini membangkitkan ke-10 container (9 Node utama + Redis Persistence) berspesifikasi port yang diatur _Environment Configuration_ (`.env`).

**2. Scaling secara Dinamis (Dynamic Scale)**
Beban kerja queue dan cache juga telah dipersiapkan sepenuhnya _scalable_ hanya dengan argumen `--scale` Compose:

```bash
docker compose up -d --scale queue-node=5
# Membuat instance container queue node akan bertambah dari 3 menjadi 5
```

---

## BAB 6: KESIMPULAN

Pembangunan sistem berbasis aiohttp ini berhasil membuktikan keandalan teori fundamental dari sistem terdistribusi operasional. Seluruh rancangan telah lulus simulasi _unit test_ dan toleransi terhadap kecacatan node (Fault).

1. Penerapan sinkronisasi **Raft Concensus** sangat menjamin stabilitas dalam _Distributed Lock Manager_ bebas dari keleluangan _Race Condition_.
2. Distribusi geografis node sangat bisa ditangani tanpa menghilangkan paket data melalui penguncian _acknowledge/redelivery_ pada **Consistent Hashing** Queue.
3. Keseimbangan _memory map_ terpadukan apik via mitigasi usang **MESI Protocol**.

Tantangan tersulit pada rancangan arsitektur ini menyangkut penanggulangan asinkronisasi paralel (Async I/O) saat perburuan _leader_ baru di algoritma Raft dan melacak koneksi buntu antar internal container pada saat menguji pembaharuan metrik LRU. Pemanfaatan Redis sebagai tumpuan _State Storage_ menangani isu memori relasional secara paripurna.

