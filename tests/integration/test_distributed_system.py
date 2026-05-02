"""
Integration tests – spins up 3 real nodes in-process and verifies
end-to-end behaviour across Raft, lock manager, queue, and cache.

Run with:  pytest tests/integration/ -v
Requires:  Python 3.11+, aiohttp, pytest-asyncio
"""

import asyncio
import pytest
import pytest_asyncio

from src.nodes.lock_manager import DistributedLockManager, LockType
from src.nodes.queue_node import DistributedQueueNode
from src.nodes.cache_node import DistributedCacheNode

PORTS = {
    'lock': [9100, 9101, 9102],
    'queue': [9200, 9201, 9202],
    'cache': [9300, 9301, 9302],
}


def peers_for(ports: list, own_port: int) -> list[str]:
    return [f"localhost:{p}" for p in ports if p != own_port]


# ------------------------------------------------------------------
# Lock Manager cluster
# ------------------------------------------------------------------

@pytest_asyncio.fixture
async def lock_cluster():
    nodes = []
    for i, port in enumerate(PORTS['lock']):
        node = DistributedLockManager(
            node_id=f"lock-{i}",
            host="127.0.0.1",
            port=port,
            peers=peers_for(PORTS['lock'], port),
        )
        nodes.append(node)

    await asyncio.gather(*[n.start() for n in nodes])
    await asyncio.sleep(2.0)  # wait for Raft leader election
    yield nodes
    await asyncio.gather(*[n.stop() for n in nodes])


@pytest.mark.asyncio
async def test_lock_cluster_elects_leader(lock_cluster):
    from src.consensus.raft import NodeState
    leaders = [n for n in lock_cluster if n._raft.role == NodeState.LEADER]
    assert len(leaders) == 1, f"Expected exactly 1 leader, got {len(leaders)}"


@pytest.mark.asyncio
async def test_lock_state_machine_apply(lock_cluster):
    from src.consensus.raft import NodeState
    leader = next(n for n in lock_cluster if n._raft.role == NodeState.LEADER)
    leader._apply_acquire("shared_resource", "client_test", LockType.EXCLUSIVE)
    lock = leader._locks.get("shared_resource")
    assert lock is not None
    assert lock.exclusive_holder == "client_test"


# ------------------------------------------------------------------
# Queue cluster
# ------------------------------------------------------------------

@pytest_asyncio.fixture
async def queue_cluster():
    nodes = []
    for i, port in enumerate(PORTS['queue']):
        node = DistributedQueueNode(
            node_id=f"queue-{i}",
            host="127.0.0.1",
            port=port,
            peers=peers_for(PORTS['queue'], port),
        )
        nodes.append(node)

    await asyncio.gather(*[n.start() for n in nodes])
    await asyncio.sleep(0.5)
    yield nodes
    await asyncio.gather(*[n.stop() for n in nodes])


@pytest.mark.asyncio
async def test_queue_store_and_fetch(queue_cluster):
    from src.nodes.queue_node import QueueMessage
    node = queue_cluster[0]
    msg = QueueMessage(queue_name="test-queue", payload={"data": "hello"}, producer_id="test")
    await node._store_message(msg)
    fetched = await node._fetch_message("test-queue")
    assert fetched is not None
    assert fetched["payload"] == {"data": "hello"}


@pytest.mark.asyncio
async def test_queue_at_least_once_redelivery(queue_cluster):
    from src.nodes.queue_node import QueueMessage
    import time

    node = queue_cluster[0]
    msg = QueueMessage(queue_name="rlv-queue", payload={"id": 1}, producer_id="p1", ack_deadline=0.5)
    await node._store_message(msg)

    fetched_raw = await node._fetch_message("rlv-queue")
    assert fetched_raw is not None
    fetched = QueueMessage.from_dict(fetched_raw)
    fetched.delivery_count += 1
    deadline = time.time() + 0.5
    node._pending_acks[fetched.msg_id] = (fetched, deadline)

    await asyncio.sleep(1.0)  # let redelivery fire

    # Message should be back in the queue
    requeued = await node._fetch_message("rlv-queue")
    assert requeued is not None


# ------------------------------------------------------------------
# Cache cluster
# ------------------------------------------------------------------

@pytest_asyncio.fixture
async def cache_cluster():
    nodes = []
    for i, port in enumerate(PORTS['cache']):
        node = DistributedCacheNode(
            node_id=f"cache-{i}",
            host="127.0.0.1",
            port=port,
            peers=peers_for(PORTS['cache'], port),
        )
        nodes.append(node)

    await asyncio.gather(*[n.start() for n in nodes])
    await asyncio.sleep(0.3)
    yield nodes
    await asyncio.gather(*[n.stop() for n in nodes])


@pytest.mark.asyncio
async def test_cache_put_and_get(cache_cluster):
    from src.nodes.cache_node import CacheLine, MESIState

    node = cache_cluster[0]
    line = CacheLine(key="mykey", value="myvalue", state=MESIState.EXCLUSIVE)
    node._cache.put(line)

    retrieved = node._cache.get("mykey")
    assert retrieved is not None
    assert retrieved.value == "myvalue"


@pytest.mark.asyncio
async def test_cache_invalidation_marks_invalid(cache_cluster):
    from src.nodes.cache_node import CacheLine, MESIState

    node = cache_cluster[0]
    line = CacheLine(key="stale_key", value="old", state=MESIState.EXCLUSIVE)
    node._cache.put(line)
    node._cache.invalidate("stale_key")

    retrieved = node._cache.get("stale_key")
    assert retrieved.state == MESIState.INVALID
