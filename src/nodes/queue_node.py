import asyncio
import hashlib
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import redis.asyncio as aioredis
from aiohttp import web

from ..utils.config import Config
from ..utils.metrics import MetricsCollector
from .base_node import BaseNode

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Consistent Hashing Ring
# ------------------------------------------------------------------

class ConsistentHashRing:
    def __init__(self, nodes: List[str], virtual_nodes: int = 150):
        self._ring: Dict[int, str] = {}
        self._sorted_keys: List[int] = []
        self.virtual_nodes = virtual_nodes
        for node in nodes:
            self.add_node(node)

    def _hash(self, key: str) -> int:
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def add_node(self, node: str):
        for i in range(self.virtual_nodes):
            h = self._hash(f"{node}:{i}")
            self._ring[h] = node
        self._sorted_keys = sorted(self._ring.keys())

    def remove_node(self, node: str):
        for i in range(self.virtual_nodes):
            h = self._hash(f"{node}:{i}")
            self._ring.pop(h, None)
        self._sorted_keys = sorted(self._ring.keys())

    def get_node(self, key: str) -> Optional[str]:
        if not self._ring:
            return None
        h = self._hash(key)
        for k in self._sorted_keys:
            if h <= k:
                return self._ring[k]
        return self._ring[self._sorted_keys[0]]

    def get_replica_nodes(self, key: str, replicas: int) -> List[str]:
        if not self._ring:
            return []
        h = self._hash(key)
        result: List[str] = []
        seen: set = set()

        start_idx = 0
        for i, k in enumerate(self._sorted_keys):
            if h <= k:
                start_idx = i
                break

        for i in range(len(self._sorted_keys)):
            idx = (start_idx + i) % len(self._sorted_keys)
            node = self._ring[self._sorted_keys[idx]]
            if node not in seen:
                seen.add(node)
                result.append(node)
            if len(result) >= replicas:
                break

        return result


# ------------------------------------------------------------------
# Message dataclass
# ------------------------------------------------------------------

@dataclass
class QueueMessage:
    queue_name: str
    payload: Any
    msg_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    producer_id: str = ""
    created_at: float = field(default_factory=time.time)
    delivery_count: int = 0
    ack_deadline: float = Config.QUEUE_ACK_TIMEOUT

    def to_dict(self) -> dict:
        return {
            'queue_name': self.queue_name,
            'payload': self.payload,
            'msg_id': self.msg_id,
            'producer_id': self.producer_id,
            'created_at': self.created_at,
            'delivery_count': self.delivery_count,
            'ack_deadline': self.ack_deadline,
        }

    @classmethod
    def from_dict(cls, d: dict) -> 'QueueMessage':
        return cls(**d)


# ------------------------------------------------------------------
# Queue Node
# ------------------------------------------------------------------

class DistributedQueueNode(BaseNode):
    def __init__(self, node_id: str, host: str, port: int, peers: List[str]):
        super().__init__(node_id, host, port, peers)

        self._ring = ConsistentHashRing([node_id] + peers)
        self._redis: Optional[aioredis.Redis] = None

        # In-memory fallback when Redis unavailable
        self._local_queues: Dict[str, List[dict]] = {}
        # pending acks: msg_id -> (msg, deadline)
        self._pending_acks: Dict[str, tuple] = {}

        self._redelivery_task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # Routes
    # ------------------------------------------------------------------

    def _setup_routes(self):
        self._app.router.add_post('/queue/enqueue', self._http_enqueue)
        self._app.router.add_post('/queue/dequeue', self._http_dequeue)
        self._app.router.add_post('/queue/ack', self._http_ack)
        self._app.router.add_get('/queue/stats/{queue}', self._http_queue_stats)
        self._app.router.add_post('/queue/internal/store', self._http_internal_store)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def _on_start(self):
        try:
            self._redis = aioredis.from_url(
                f"redis://{Config.REDIS_HOST}:{Config.REDIS_PORT}/{Config.REDIS_DB}",
                password=Config.REDIS_PASSWORD,
                encoding='utf-8',
                decode_responses=True,
            )
            await self._redis.ping()
            logger.info("Redis connected on %s", self.node_id)
        except Exception as exc:
            logger.warning("Redis unavailable, using in-memory fallback: %s", exc)
            self._redis = None

        self._redelivery_task = asyncio.create_task(self._redelivery_loop())

    async def _on_stop(self):
        if self._redelivery_task:
            self._redelivery_task.cancel()
        if self._redis:
            await self._redis.close()

    # ------------------------------------------------------------------
    # HTTP handlers
    # ------------------------------------------------------------------

    async def _http_enqueue(self, request: web.Request) -> web.Response:
        data = await request.json()
        msg = QueueMessage(
            queue_name=data['queue'],
            payload=data['payload'],
            producer_id=data.get('producer_id', 'unknown'),
        )

        target_nodes = self._ring.get_replica_nodes(msg.queue_name, Config.QUEUE_REPLICAS)
        stored = 0

        for node in target_nodes:
            if node == self.node_id:
                await self._store_message(msg)
                stored += 1
            else:
                ok = await self._forward_to_node(node, msg)
                if ok:
                    stored += 1

        self.metrics.increment('messages_enqueued')
        return web.json_response({'msg_id': msg.msg_id, 'stored_on': stored})

    async def _http_dequeue(self, request: web.Request) -> web.Response:
        data = await request.json()
        queue_name = data['queue']
        consumer_id = data.get('consumer_id', 'unknown')

        msg_dict = await self._fetch_message(queue_name)
        if not msg_dict:
            return web.json_response({'msg': None})

        msg = QueueMessage.from_dict(msg_dict)
        msg.delivery_count += 1
        deadline = time.time() + msg.ack_deadline
        self._pending_acks[msg.msg_id] = (msg, deadline)

        self.metrics.increment('messages_dequeued')
        return web.json_response({'msg': msg.to_dict()})

    async def _http_ack(self, request: web.Request) -> web.Response:
        data = await request.json()
        msg_id = data['msg_id']
        self._pending_acks.pop(msg_id, None)
        self.metrics.increment('messages_acked')
        return web.json_response({'status': 'acked'})

    async def _http_queue_stats(self, request: web.Request) -> web.Response:
        queue = request.match_info['queue']
        length = await self._queue_length(queue)
        return web.json_response({
            'queue': queue,
            'length': length,
            'pending_acks': len(self._pending_acks),
            'node': self.node_id,
        })

    async def _http_internal_store(self, request: web.Request) -> web.Response:
        data = await request.json()
        msg = QueueMessage.from_dict(data)
        await self._store_message(msg)
        return web.json_response({'status': 'stored'})

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------

    async def _store_message(self, msg: QueueMessage):
        key = f"queue:{msg.queue_name}"
        serialized = json.dumps(msg.to_dict())
        if self._redis:
            await self._redis.rpush(key, serialized)
        else:
            self._local_queues.setdefault(key, []).append(msg.to_dict())

    async def _fetch_message(self, queue_name: str) -> Optional[dict]:
        key = f"queue:{queue_name}"
        if self._redis:
            raw = await self._redis.lpop(key)
            return json.loads(raw) if raw else None
        local = self._local_queues.get(key, [])
        return local.pop(0) if local else None

    async def _queue_length(self, queue_name: str) -> int:
        key = f"queue:{queue_name}"
        if self._redis:
            return await self._redis.llen(key)
        return len(self._local_queues.get(key, []))

    async def _forward_to_node(self, node: str, msg: QueueMessage) -> bool:
        import aiohttp
        try:
            timeout = aiohttp.ClientTimeout(total=3.0)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    f'http://{node}/queue/internal/store', json=msg.to_dict()
                ) as resp:
                    return resp.status == 200
        except Exception as exc:
            logger.warning("Forward to %s failed: %s", node, exc)
            return False

    # ------------------------------------------------------------------
    # At-least-once redelivery
    # ------------------------------------------------------------------

    async def _redelivery_loop(self):
        while True:
            await asyncio.sleep(5.0)
            now = time.time()
            overdue = [
                (msg_id, info)
                for msg_id, info in list(self._pending_acks.items())
                if now > info[1]
            ]
            for msg_id, (msg, _) in overdue:
                logger.warning("Re-queueing unacked message %s", msg_id)
                del self._pending_acks[msg_id]
                await self._store_message(msg)
                self.metrics.increment('messages_redelivered')


if __name__ == "__main__":
    import signal
    from ..utils.config import Config

    logging.basicConfig(
        level=getattr(logging, Config.LOG_LEVEL, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    node = DistributedQueueNode(
        node_id=Config.NODE_ID,
        host=Config.NODE_HOST,
        port=Config.NODE_PORT,
        peers=Config.CLUSTER_NODES,
    )

    async def _run():
        await node.start()
        loop = asyncio.get_event_loop()
        stop = loop.create_future()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set_result, None)
        await stop
        await node.stop()

    asyncio.run(_run())
