

import asyncio
import json
import os
import time
import statistics
from pathlib import Path
from typing import List

import pytest
import pytest_asyncio

from src.nodes.cache_node import CacheLine, DistributedCacheNode, MESIState
from src.nodes.queue_node import DistributedQueueNode, QueueMessage
from src.consensus.raft import RaftConsensus, NodeState


RESULTS_FILE = Path(__file__).parent.parent.parent / "benchmarks" / "results.json"
N_OPS = 1000  # number of operations per test


def save_result(name: str, data: dict):
    RESULTS_FILE.parent.mkdir(exist_ok=True)
    results = {}
    if RESULTS_FILE.exists():
        results = json.loads(RESULTS_FILE.read_text())
    results[name] = data
    RESULTS_FILE.write_text(json.dumps(results, indent=2))


# ------------------------------------------------------------------
# Cache throughput
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_lru_cache_write_throughput():
    node = DistributedCacheNode.__new__(DistributedCacheNode)
    from src.nodes.cache_node import LRUCache
    node._cache = LRUCache(capacity=N_OPS + 100)

    start = time.perf_counter()
    for i in range(N_OPS):
        line = CacheLine(key=f"k{i}", value=i, state=MESIState.EXCLUSIVE)
        node._cache.put(line)
    elapsed = time.perf_counter() - start

    throughput = N_OPS / elapsed
    print(f"\n[LRU Write] {N_OPS} ops in {elapsed:.3f}s → {throughput:.0f} ops/s")
    save_result("lru_write", {"ops": N_OPS, "elapsed_s": elapsed, "ops_per_s": throughput})

    assert throughput > 50_000  # should be very fast (in-memory)


@pytest.mark.asyncio
async def test_lru_cache_read_throughput():
    from src.nodes.cache_node import LRUCache
    cache = LRUCache(capacity=N_OPS + 100)
    for i in range(N_OPS):
        cache.put(CacheLine(key=f"k{i}", value=i))

    latencies: List[float] = []
    start = time.perf_counter()
    for i in range(N_OPS):
        t0 = time.perf_counter()
        cache.get(f"k{i}")
        latencies.append((time.perf_counter() - t0) * 1000)
    elapsed = time.perf_counter() - start

    throughput = N_OPS / elapsed
    p99 = sorted(latencies)[int(N_OPS * 0.99)]
    print(f"\n[LRU Read] {N_OPS} ops in {elapsed:.3f}s → {throughput:.0f} ops/s, p99={p99:.4f}ms")
    save_result("lru_read", {
        "ops": N_OPS, "elapsed_s": elapsed, "ops_per_s": throughput,
        "p50_ms": statistics.median(latencies),
        "p99_ms": p99,
    })
    assert throughput > 100_000


# ------------------------------------------------------------------
# Queue throughput
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_queue_enqueue_throughput():
    node = DistributedQueueNode.__new__(DistributedQueueNode)
    node._local_queues = {}
    node._redis = None
    node.metrics = type('M', (), {'increment': lambda self, *a: None})()

    messages = [
        QueueMessage(queue_name="perf-queue", payload={"i": i}, producer_id="perf")
        for i in range(N_OPS)
    ]

    start = time.perf_counter()
    for msg in messages:
        await node._store_message(msg)
    elapsed = time.perf_counter() - start

    throughput = N_OPS / elapsed
    print(f"\n[Queue Enqueue] {N_OPS} ops in {elapsed:.3f}s → {throughput:.0f} ops/s")
    save_result("queue_enqueue", {"ops": N_OPS, "elapsed_s": elapsed, "ops_per_s": throughput})
    assert throughput > 10_000


@pytest.mark.asyncio
async def test_queue_dequeue_throughput():
    node = DistributedQueueNode.__new__(DistributedQueueNode)
    node._local_queues = {}
    node._redis = None
    node.metrics = type('M', (), {'increment': lambda self, *a: None})()

    for i in range(N_OPS):
        msg = QueueMessage(queue_name="dq-queue", payload={"i": i})
        await node._store_message(msg)

    latencies = []
    start = time.perf_counter()
    for _ in range(N_OPS):
        t0 = time.perf_counter()
        await node._fetch_message("dq-queue")
        latencies.append((time.perf_counter() - t0) * 1000)
    elapsed = time.perf_counter() - start

    throughput = N_OPS / elapsed
    p99 = sorted(latencies)[int(N_OPS * 0.99)]
    print(f"\n[Queue Dequeue] {N_OPS} ops in {elapsed:.3f}s → {throughput:.0f} ops/s, p99={p99:.4f}ms")
    save_result("queue_dequeue", {"ops": N_OPS, "elapsed_s": elapsed, "ops_per_s": throughput, "p99_ms": p99})
    assert throughput > 10_000


# ------------------------------------------------------------------
# Raft log throughput (single-node, no network)
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_raft_log_append_throughput():
    applied = []

    async def state_machine(cmd):
        applied.append(cmd)

    raft = RaftConsensus(
        node_id="perf-node",
        peers=[],
        state_machine_callback=state_machine,
    )
    await raft.start()
    # Force leader role for direct log append
    raft.role = NodeState.LEADER
    raft.state.current_term = 1

    from src.consensus.raft import LogEntry
    start = time.perf_counter()
    for i in range(N_OPS):
        entry = LogEntry(term=1, index=len(raft.state.log), command={"i": i})
        raft.state.log.append(entry)
        raft.state.commit_index = entry.index
    elapsed = time.perf_counter() - start

    await raft.stop()

    throughput = N_OPS / elapsed
    print(f"\n[Raft Log Append] {N_OPS} entries in {elapsed:.3f}s → {throughput:.0f} entries/s")
    save_result("raft_append", {"ops": N_OPS, "elapsed_s": elapsed, "ops_per_s": throughput})
    assert throughput > 100_000
