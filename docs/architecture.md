# Arsitektur Sistem - Distributed Synchronization System

## Overview

Sistem ini mengimplementasikan tiga komponen utama distributed systems yang berjalan secara terpisah namun dapat dikomposisikan:

```
┌──────────────────────────────────────────────────────────────┐
│                   Distributed Sync System                    │
├─────────────────┬──────────────────┬─────────────────────────┤
│  Lock Manager   │  Queue System    │  Cache Coherence        │
│  (Raft Consensus│  (Consistent     │  (MESI Protocol)        │
│   3 nodes)      │   Hashing, 3n)   │   3 nodes)               │
├─────────────────┴──────────────────┴─────────────────────────┤
│              Communication Layer (aiohttp / HTTP)            │
├─────────────────────────────────────────────────────────────-┤
│              Failure Detector (Phi Accrual-inspired)         │
├──────────────────────────────────────────────────────────────┤
│              Redis (Persistence & Shared State)              │
└──────────────────────────────────────────────────────────────┘
```

---

## 1. Distributed Lock Manager

### Algoritma: Raft Consensus

Raft dipilih karena understandable (mudah dipahami) dan telah terbukti digunakan di sistem produksi (etcd, CockroachDB).

**Fase Raft:**
```
[Follower] ──election timeout──► [Candidate] ──majority votes──► [Leader]
    ▲                                                                  │
    └──────────── step down (higher term seen) ◄───────────────────────┘
```

**Lock State Machine:**
```
acquire(resource, client, EXCLUSIVE) → LockState.exclusive_holder = client
acquire(resource, client, SHARED)    → LockState.shared_holders.add(client)
release(resource, client)            → clear holder, process wait queue
```

**Deadlock Detection - Wait-For Graph (WFG):**
```
Client A ──waits for──► Client B ──waits for──► Client A  (CYCLE → deadlock)
```
Deteksi menggunakan DFS. Resolusi dengan mengaborsi transaksi termuda dalam siklus.

---

## 2. Distributed Queue System

### Algoritma: Consistent Hashing

```
Hash Ring (360°):
     node-1
    /      \
node-3    node-2
    \      /
     (ring)

Queue "orders" hashes to → node-2 (+ 2 replicas: node-3, node-1)
```

**Delivery Guarantee: At-Least-Once**
```
Producer → enqueue(msg)
         → stored on 3 replica nodes
Consumer → dequeue(msg)  ← returned with ack_deadline
         → process(msg)
         → ack(msg_id)   ← removes from pending_acks

If ack not received within deadline → re-queued automatically
```

---

## 3. Distributed Cache Coherence

### Protokol: MESI

| State | Singkatan | Deskripsi |
|-------|-----------|-----------|
| M | Modified | Cache ini memiliki copy terbaru (dirty), tidak ada di cache lain |
| E | Exclusive | Cache ini memiliki copy bersih, tidak ada di cache lain |
| S | Shared | Mungkin ada di beberapa cache, semua bersih |
| I | Invalid | Data stale, harus fetch ulang |

**Transisi State:**
```
Write hit (state=E atau M) → tetap M
Write (ada sharer)         → broadcast INVALIDATE ke semua → state=M
Read miss                  → fetch, jika ada sharer: S, tidak ada: E
Receive INVALIDATE         → state=I
```

---

## 4. Communication Layer

Setiap node mengekspos REST API via aiohttp. Komunikasi inter-node menggunakan HTTP/1.1.

**Pola komunikasi:**
- Fire-and-forget (heartbeats, invalidations)
- Request-reply (vote requests, lock proposals)
- Broadcast (invalidation, replication)

---

## 5. Failure Detector

Menggunakan ping-based detection dengan sliding window untuk menghitung mean interval antar ping sukses. Node dinyatakan gagal jika tidak merespon selama `FAILURE_TIMEOUT` detik.

---

## Deployment Topology

```
Internet/Client
      │
      ▼
[Load Balancer]
  /    |    \
n1    n2    n3    ← Raft/MESI/Queue nodes
  \   |   /
  [Redis Cluster]
```

---

## File Structure

```
distributed-sync-system/
├── src/
│   ├── consensus/raft.py          ← Raft consensus core
│   ├── consensus/pbft.py          ← PBFT (bonus)
│   ├── nodes/lock_manager.py      ← Distributed locks
│   ├── nodes/queue_node.py        ← Distributed queue
│   ├── nodes/cache_node.py        ← MESI cache
│   ├── communication/             ← Message bus & failure detector
│   └── utils/                     ← Config & metrics
├── tests/
├── docker/
├── benchmarks/
└── docs/
```
