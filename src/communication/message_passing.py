import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

import aiohttp
from aiohttp import web

logger = logging.getLogger(__name__)


class MessageType(Enum):
    # Raft
    RAFT_REQUEST_VOTE = "raft.request_vote"
    RAFT_APPEND_ENTRIES = "raft.append_entries"

    # Lock Manager
    LOCK_ACQUIRE = "lock.acquire"
    LOCK_RELEASE = "lock.release"
    LOCK_HEARTBEAT = "lock.heartbeat"

    # Queue
    QUEUE_ENQUEUE = "queue.enqueue"
    QUEUE_DEQUEUE = "queue.dequeue"
    QUEUE_ACK = "queue.ack"

    # Cache
    CACHE_INVALIDATE = "cache.invalidate"
    CACHE_UPDATE = "cache.update"
    CACHE_READ_MISS = "cache.read_miss"

    # System
    PING = "system.ping"
    PONG = "system.pong"
    METRICS = "system.metrics"


@dataclass
class Message:
    msg_type: MessageType
    sender: str
    payload: dict
    msg_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: float = field(default_factory=time.time)
    correlation_id: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            'msg_type': self.msg_type.value,
            'sender': self.sender,
            'payload': self.payload,
            'msg_id': self.msg_id,
            'timestamp': self.timestamp,
            'correlation_id': self.correlation_id,
        }

    @classmethod
    def from_dict(cls, d: dict) -> 'Message':
        return cls(
            msg_type=MessageType(d['msg_type']),
            sender=d['sender'],
            payload=d['payload'],
            msg_id=d.get('msg_id', str(uuid.uuid4())),
            timestamp=d.get('timestamp', time.time()),
            correlation_id=d.get('correlation_id'),
        )


class MessageBus:
    """
    Lightweight async message bus backed by aiohttp.
    Supports:
      - send(peer, msg)         – fire-and-forget
      - request(peer, msg)      – wait for reply
      - broadcast(peers, msg)   – parallel fire-and-forget
      - subscribe(msg_type, cb) – register a local handler
    """

    def __init__(self, node_id: str, rpc_timeout: float = 5.0):
        self.node_id = node_id
        self._rpc_timeout = rpc_timeout
        self._handlers: Dict[MessageType, List[Callable]] = {}
        self._pending: Dict[str, asyncio.Future] = {}

        # Metrics
        self.sent_count = 0
        self.received_count = 0
        self.error_count = 0

    # ------------------------------------------------------------------
    # Subscription
    # ------------------------------------------------------------------

    def subscribe(self, msg_type: MessageType, handler: Callable):
        self._handlers.setdefault(msg_type, []).append(handler)

    async def dispatch(self, message: Message) -> Optional[dict]:
        """Deliver an incoming message to registered handlers."""
        self.received_count += 1
        handlers = self._handlers.get(message.msg_type, [])
        result = None
        for handler in handlers:
            try:
                result = await handler(message)
            except Exception as exc:
                logger.error("Handler error for %s: %s", message.msg_type, exc)
        return result

    # ------------------------------------------------------------------
    # Sending
    # ------------------------------------------------------------------

    async def send(self, peer: str, message: Message) -> bool:
        """Fire-and-forget – returns True on HTTP 2xx."""
        try:
            timeout = aiohttp.ClientTimeout(total=self._rpc_timeout)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    f'http://{peer}/message', json=message.to_dict()
                ) as resp:
                    self.sent_count += 1
                    return resp.status < 300
        except Exception as exc:
            self.error_count += 1
            logger.debug("send to %s failed: %s", peer, exc)
            return False

    async def request(self, peer: str, message: Message, timeout: float = 5.0) -> Optional[dict]:
        """Send and wait for a correlated reply."""
        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending[message.msg_id] = future
        try:
            ok = await self.send(peer, message)
            if not ok:
                return None
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("request to %s timed out", peer)
            return None
        finally:
            self._pending.pop(message.msg_id, None)

    def resolve_reply(self, correlation_id: str, payload: dict):
        """Called when a reply arrives matching a pending request."""
        future = self._pending.get(correlation_id)
        if future and not future.done():
            future.set_result(payload)

    async def broadcast(self, peers: List[str], message: Message) -> Dict[str, bool]:
        """Send the same message to all peers concurrently."""
        results = await asyncio.gather(
            *[self.send(p, message) for p in peers], return_exceptions=True
        )
        return {p: (r is True) for p, r in zip(peers, results)}

    # ------------------------------------------------------------------
    # aiohttp handler (mount at /message in the HTTP app)
    # ------------------------------------------------------------------

    async def http_handler(self, request: web.Request) -> web.Response:
        data = await request.json()
        msg = Message.from_dict(data)

        # If this is a reply to a pending request, resolve it
        if msg.correlation_id:
            self.resolve_reply(msg.correlation_id, msg.payload)
            return web.json_response({'status': 'ok'})

        result = await self.dispatch(msg)
        return web.json_response({'status': 'ok', 'result': result})

    def get_stats(self) -> dict:
        return {
            'node_id': self.node_id,
            'sent': self.sent_count,
            'received': self.received_count,
            'errors': self.error_count,
            'pending_requests': len(self._pending),
        }
