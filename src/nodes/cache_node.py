import asyncio
import logging
import time
from collections import OrderedDict, defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

import aiohttp
from aiohttp import web

from ..utils.config import Config
from ..utils.metrics import MetricsCollector
from .base_node import BaseNode

logger = logging.getLogger(__name__)


class MESIState(Enum):
    MODIFIED = "M"
    EXCLUSIVE = "E"
    SHARED = "S"
    INVALID = "I"


@dataclass
class CacheLine:
    key: str
    value: Any
    state: MESIState = MESIState.EXCLUSIVE
    last_access: float = field(default_factory=time.time)
    access_count: int = 0
    version: int = 0

    def touch(self):
        self.last_access = time.time()
        self.access_count += 1


# ------------------------------------------------------------------
# LRU Cache (OrderedDict-based)
# ------------------------------------------------------------------

class LRUCache:
    def __init__(self, capacity: int):
        self.capacity = capacity
        self._store: OrderedDict[str, CacheLine] = OrderedDict()

    def get(self, key: str) -> Optional[CacheLine]:
        if key not in self._store:
            return None
        self._store.move_to_end(key)
        line = self._store[key]
        line.touch()
        return line

    def put(self, line: CacheLine) -> Optional[str]:
        """Returns evicted key if capacity exceeded, else None."""
        evicted = None
        if line.key in self._store:
            self._store.move_to_end(line.key)
        else:
            if len(self._store) >= self.capacity:
                evicted, _ = self._store.popitem(last=False)
            self._store[line.key] = line
        return evicted

    def invalidate(self, key: str):
        if key in self._store:
            self._store[key].state = MESIState.INVALID

    def remove(self, key: str):
        self._store.pop(key, None)

    def all_keys(self) -> List[str]:
        return list(self._store.keys())

    def __len__(self):
        return len(self._store)


# ------------------------------------------------------------------
# LFU Cache (min-heap via frequency tracking)
# ------------------------------------------------------------------

class LFUCache:
    def __init__(self, capacity: int):
        self.capacity = capacity
        self._store: Dict[str, CacheLine] = {}
        self._freq: Dict[str, int] = defaultdict(int)

    def get(self, key: str) -> Optional[CacheLine]:
        line = self._store.get(key)
        if line:
            self._freq[key] += 1
            line.touch()
        return line

    def put(self, line: CacheLine) -> Optional[str]:
        evicted = None
        if line.key not in self._store and len(self._store) >= self.capacity:
            lfu_key = min(self._freq, key=lambda k: self._freq[k])
            del self._store[lfu_key]
            del self._freq[lfu_key]
            evicted = lfu_key
        self._store[line.key] = line
        self._freq[line.key] = line.access_count or 1
        return evicted

    def invalidate(self, key: str):
        if key in self._store:
            self._store[key].state = MESIState.INVALID

    def remove(self, key: str):
        self._store.pop(key, None)
        self._freq.pop(key, None)

    def all_keys(self) -> List[str]:
        return list(self._store.keys())

    def __len__(self):
        return len(self._store)


# ------------------------------------------------------------------
# Cache Node
# ------------------------------------------------------------------

class DistributedCacheNode(BaseNode):
    def __init__(self, node_id: str, host: str, port: int, peers: List[str]):
        super().__init__(node_id, host, port, peers)

        if Config.CACHE_POLICY == 'LFU':
            self._cache: LRUCache | LFUCache = LFUCache(Config.CACHE_SIZE)
        else:
            self._cache = LRUCache(Config.CACHE_SIZE)

        self._rpc_timeout = 2.0

    # ------------------------------------------------------------------
    # Routes
    # ------------------------------------------------------------------

    def _setup_routes(self):
        self._app.router.add_get('/cache/{key}', self._http_get)
        self._app.router.add_put('/cache/{key}', self._http_put)
        self._app.router.add_delete('/cache/{key}', self._http_delete)
        self._app.router.add_get('/cache', self._http_list)
        self._app.router.add_post('/cache/internal/invalidate', self._http_invalidate)
        self._app.router.add_post('/cache/internal/update', self._http_update)
        self._app.router.add_get('/cache/stats', self._http_stats)

    # ------------------------------------------------------------------
    # HTTP handlers
    # ------------------------------------------------------------------

    async def _http_get(self, request: web.Request) -> web.Response:
        key = request.match_info['key']
        line = self._cache.get(key)

        if line is None or line.state == MESIState.INVALID:
            self.metrics.increment('cache_misses')
            return web.json_response({'hit': False, 'key': key, 'value': None})

        self.metrics.increment('cache_hits')
        return web.json_response({
            'hit': True,
            'key': key,
            'value': line.value,
            'state': line.state.value,
            'version': line.version,
        })

    async def _http_put(self, request: web.Request) -> web.Response:
        key = request.match_info['key']
        data = await request.json()
        value = data['value']

        # Check if other caches hold this key (to determine MESI state)
        sharers = await self._get_sharers(key)

        if sharers:
            # Other caches exist → broadcast invalidation, then set MODIFIED
            await self._broadcast_invalidate(key)
            state = MESIState.MODIFIED
        else:
            state = MESIState.EXCLUSIVE

        existing = self._cache.get(key)
        new_version = (existing.version + 1) if existing else 1

        line = CacheLine(key=key, value=value, state=state, version=new_version)
        evicted = self._cache.put(line)

        if evicted:
            self.metrics.increment('cache_evictions')

        self.metrics.increment('cache_writes')
        return web.json_response({'key': key, 'state': state.value, 'version': new_version})

    async def _http_delete(self, request: web.Request) -> web.Response:
        key = request.match_info['key']
        self._cache.remove(key)
        await self._broadcast_invalidate(key)
        self.metrics.increment('cache_deletes')
        return web.json_response({'status': 'deleted', 'key': key})

    async def _http_list(self, _request: web.Request) -> web.Response:
        return web.json_response({'keys': self._cache.all_keys(), 'size': len(self._cache)})

    async def _http_invalidate(self, request: web.Request) -> web.Response:
        data = await request.json()
        key = data['key']
        self._cache.invalidate(key)
        self.metrics.increment('cache_invalidations_received')
        return web.json_response({'status': 'invalidated', 'key': key})

    async def _http_update(self, request: web.Request) -> web.Response:
        """Receives a coherence update – transitions I→S or updates S line."""
        data = await request.json()
        key = data['key']
        value = data['value']
        version = data.get('version', 1)

        line = self._cache.get(key)
        if line:
            line.value = value
            line.state = MESIState.SHARED
            line.version = version
        return web.json_response({'status': 'updated'})

    async def _http_stats(self, _request: web.Request) -> web.Response:
        stats = self.metrics.get_stats()
        stats['cache_size'] = len(self._cache)
        stats['cache_capacity'] = Config.CACHE_SIZE
        stats['policy'] = Config.CACHE_POLICY
        return web.json_response(stats)

    # ------------------------------------------------------------------
    # MESI helpers
    # ------------------------------------------------------------------

    async def _get_sharers(self, key: str) -> List[str]:
        """Query peers to find which ones hold this key."""
        sharers = []
        timeout = aiohttp.ClientTimeout(total=self._rpc_timeout)

        async def check(peer: str):
            try:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(f'http://{peer}/cache/{key}') as resp:
                        if resp.status == 200:
                            d = await resp.json()
                            if d.get('hit'):
                                sharers.append(peer)
            except Exception:
                pass

        await asyncio.gather(*[check(p) for p in self.peers], return_exceptions=True)
        return sharers

    async def _broadcast_invalidate(self, key: str):
        """Send MESI invalidation to all peers (I-state transition)."""
        timeout = aiohttp.ClientTimeout(total=self._rpc_timeout)
        payload = {'key': key, 'invalidated_by': self.node_id}

        async def send(peer: str):
            try:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    await session.post(f'http://{peer}/cache/internal/invalidate', json=payload)
            except Exception as exc:
                logger.debug("Invalidate broadcast to %s failed: %s", peer, exc)

        await asyncio.gather(*[send(p) for p in self.peers], return_exceptions=True)
        self.metrics.increment('cache_invalidations_sent')

    async def _broadcast_update(self, key: str, value: Any, version: int):
        """Propagate updated value to peers (S-state)."""
        timeout = aiohttp.ClientTimeout(total=self._rpc_timeout)
        payload = {'key': key, 'value': value, 'version': version}

        async def send(peer: str):
            try:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    await session.post(f'http://{peer}/cache/internal/update', json=payload)
            except Exception as exc:
                logger.debug("Update broadcast to %s failed: %s", peer, exc)

        await asyncio.gather(*[send(p) for p in self.peers], return_exceptions=True)


if __name__ == "__main__":
    import signal
    from ..utils.config import Config

    logging.basicConfig(
        level=getattr(logging, Config.LOG_LEVEL, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    node = DistributedCacheNode(
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
