import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Set

from aiohttp import web

from ..consensus.raft import RaftConsensus, NodeState
from ..utils.config import Config
from ..utils.metrics import MetricsCollector
from .base_node import BaseNode

logger = logging.getLogger(__name__)


class LockType(Enum):
    SHARED = "shared"
    EXCLUSIVE = "exclusive"


class LockStatus(Enum):
    ACQUIRED = "acquired"
    WAITING = "waiting"
    DENIED = "denied"
    RELEASED = "released"


@dataclass
class LockRequest:
    lock_id: str
    client_id: str
    lock_type: LockType
    acquired_at: float = field(default_factory=time.time)
    lease_duration: float = Config.LOCK_LEASE_DURATION


@dataclass
class LockState:
    resource: str
    shared_holders: Set[str] = field(default_factory=set)   # client_ids
    exclusive_holder: Optional[str] = None
    wait_queue: List[LockRequest] = field(default_factory=list)

    def can_acquire_shared(self, client_id: str) -> bool:
        return self.exclusive_holder is None

    def can_acquire_exclusive(self, client_id: str) -> bool:
        return self.exclusive_holder is None and len(self.shared_holders) == 0

    def is_expired(self, request: LockRequest) -> bool:
        return time.time() - request.acquired_at > request.lease_duration


class DistributedLockManager(BaseNode):
    def __init__(self, node_id: str, host: str, port: int, peers: List[str]):
        super().__init__(node_id, host, port, peers)

        self._locks: Dict[str, LockState] = {}
        self._client_locks: Dict[str, List[str]] = defaultdict(list)  # client_id -> [resource]
        self._wait_for: Dict[str, Set[str]] = defaultdict(set)         # client -> set of clients it waits for

        peer_addresses = [f"{p.split(':')[0]}:{p.split(':')[1]}" for p in peers]
        self._raft = RaftConsensus(
            node_id=node_id,
            peers=peer_addresses,
            state_machine_callback=self._apply_command,
        )

        self._lease_checker_task: Optional[asyncio.Task] = None
        self._deadlock_task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # Routes
    # ------------------------------------------------------------------

    def _setup_routes(self):
        self._app.router.add_post('/lock/acquire', self._http_acquire)
        self._app.router.add_post('/lock/release', self._http_release)
        self._app.router.add_post('/lock/heartbeat', self._http_heartbeat)
        self._app.router.add_get('/lock/status/{resource}', self._http_status)
        self._app.router.add_get('/lock/deadlocks', self._http_deadlocks)
        self._app.router.add_post('/raft/request_vote', self._http_raft_vote)
        self._app.router.add_post('/raft/append_entries', self._http_raft_ae)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def _on_start(self):
        await self._raft.start()
        self._lease_checker_task = asyncio.create_task(self._lease_expiry_loop())
        self._deadlock_task = asyncio.create_task(self._deadlock_detection_loop())

    async def _on_stop(self):
        await self._raft.stop()
        for t in [self._lease_checker_task, self._deadlock_task]:
            if t:
                t.cancel()

    # ------------------------------------------------------------------
    # HTTP handlers
    # ------------------------------------------------------------------

    async def _http_acquire(self, request: web.Request) -> web.Response:
        data = await request.json()
        resource = data['resource']
        client_id = data['client_id']
        lock_type = LockType(data.get('lock_type', 'exclusive'))

        if self._raft.role != NodeState.LEADER:
            return web.json_response({'status': 'not_leader', 'leader': self._raft.leader_id}, status=307)

        result = await self._propose_acquire(resource, client_id, lock_type)
        self.metrics.increment(f'lock_acquire_{result}')
        return web.json_response({'status': result})

    async def _http_release(self, request: web.Request) -> web.Response:
        data = await request.json()
        resource = data['resource']
        client_id = data['client_id']

        if self._raft.role != NodeState.LEADER:
            return web.json_response({'status': 'not_leader'}, status=307)

        await self._propose_release(resource, client_id)
        self.metrics.increment('lock_releases')
        return web.json_response({'status': 'released'})

    async def _http_heartbeat(self, request: web.Request) -> web.Response:
        data = await request.json()
        resource = data['resource']
        client_id = data['client_id']
        lock = self._locks.get(resource)
        if lock:
            if lock.exclusive_holder == client_id or client_id in lock.shared_holders:
                # Refresh lease timestamps in wait_queue
                for req in lock.wait_queue:
                    if req.client_id == client_id:
                        req.acquired_at = time.time()
        return web.json_response({'status': 'ok'})

    async def _http_status(self, request: web.Request) -> web.Response:
        resource = request.match_info['resource']
        lock = self._locks.get(resource)
        if not lock:
            return web.json_response({'resource': resource, 'status': 'free'})
        return web.json_response({
            'resource': resource,
            'exclusive_holder': lock.exclusive_holder,
            'shared_holders': list(lock.shared_holders),
            'queue_length': len(lock.wait_queue),
        })

    async def _http_deadlocks(self, _request: web.Request) -> web.Response:
        cycles = self._detect_cycles()
        return web.json_response({'deadlocks': cycles})

    async def _http_raft_vote(self, request: web.Request) -> web.Response:
        data = await request.json()
        result = await self._raft.handle_request_vote(data)
        return web.json_response(result)

    async def _http_raft_ae(self, request: web.Request) -> web.Response:
        data = await request.json()
        result = await self._raft.handle_append_entries(data)
        return web.json_response(result)

    # ------------------------------------------------------------------
    # Raft proposals
    # ------------------------------------------------------------------

    async def _propose_acquire(self, resource: str, client_id: str, lock_type: LockType) -> str:
        committed = await self._raft.propose({
            'op': 'acquire',
            'resource': resource,
            'client_id': client_id,
            'lock_type': lock_type.value,
            'timestamp': time.time(),
        })
        if not committed:
            return 'timeout'

        lock = self._locks.get(resource)
        if lock:
            if lock_type == LockType.EXCLUSIVE and lock.exclusive_holder == client_id:
                return 'acquired'
            if lock_type == LockType.SHARED and client_id in lock.shared_holders:
                return 'acquired'
        return 'waiting'

    async def _propose_release(self, resource: str, client_id: str):
        await self._raft.propose({
            'op': 'release',
            'resource': resource,
            'client_id': client_id,
        })

    # ------------------------------------------------------------------
    # State machine (applied after Raft commit)
    # ------------------------------------------------------------------

    async def _apply_command(self, command: dict):
        op = command['op']
        resource = command['resource']
        client_id = command['client_id']

        if op == 'acquire':
            self._apply_acquire(resource, client_id, LockType(command['lock_type']))
        elif op == 'release':
            self._apply_release(resource, client_id)

    def _apply_acquire(self, resource: str, client_id: str, lock_type: LockType):
        if resource not in self._locks:
            self._locks[resource] = LockState(resource=resource)

        lock = self._locks[resource]
        req = LockRequest(lock_id=f"{resource}:{client_id}", client_id=client_id, lock_type=lock_type)

        if lock_type == LockType.SHARED and lock.can_acquire_shared(client_id):
            lock.shared_holders.add(client_id)
            self._client_locks[client_id].append(resource)
            logger.debug("SHARED lock granted: %s -> %s", client_id, resource)
        elif lock_type == LockType.EXCLUSIVE and lock.can_acquire_exclusive(client_id):
            lock.exclusive_holder = client_id
            self._client_locks[client_id].append(resource)
            logger.debug("EXCLUSIVE lock granted: %s -> %s", client_id, resource)
        else:
            lock.wait_queue.append(req)
            # Update wait-for graph
            if lock.exclusive_holder:
                self._wait_for[client_id].add(lock.exclusive_holder)
            for holder in lock.shared_holders:
                self._wait_for[client_id].add(holder)
            logger.debug("Lock QUEUED: %s -> %s", client_id, resource)

    def _apply_release(self, resource: str, client_id: str):
        lock = self._locks.get(resource)
        if not lock:
            return

        lock.shared_holders.discard(client_id)
        if lock.exclusive_holder == client_id:
            lock.exclusive_holder = None

        self._client_locks[client_id] = [r for r in self._client_locks[client_id] if r != resource]

        # Remove from wait-for graph
        for waiter in list(self._wait_for.keys()):
            self._wait_for[waiter].discard(client_id)

        # Grant pending requests from queue
        self._process_queue(resource)

    def _process_queue(self, resource: str):
        lock = self._locks.get(resource)
        if not lock or not lock.wait_queue:
            return

        # Try to grant the next waiter
        granted: List[int] = []
        for i, req in enumerate(lock.wait_queue):
            if req.lock_type == LockType.EXCLUSIVE:
                if lock.can_acquire_exclusive(req.client_id):
                    lock.exclusive_holder = req.client_id
                    self._client_locks[req.client_id].append(resource)
                    granted.append(i)
                break  # exclusive blocks all following
            elif req.lock_type == LockType.SHARED:
                if lock.can_acquire_shared(req.client_id):
                    lock.shared_holders.add(req.client_id)
                    self._client_locks[req.client_id].append(resource)
                    granted.append(i)

        for i in reversed(granted):
            lock.wait_queue.pop(i)

    # ------------------------------------------------------------------
    # Deadlock detection (Wait-For Graph cycle detection via DFS)
    # ------------------------------------------------------------------

    def _detect_cycles(self) -> List[List[str]]:
        visited: Set[str] = set()
        rec_stack: Set[str] = set()
        cycles: List[List[str]] = []

        def dfs(node: str, path: List[str]):
            visited.add(node)
            rec_stack.add(node)
            path.append(node)
            for neighbor in self._wait_for.get(node, set()):
                if neighbor not in visited:
                    dfs(neighbor, path)
                elif neighbor in rec_stack:
                    cycle_start = path.index(neighbor)
                    cycles.append(path[cycle_start:])
            path.pop()
            rec_stack.discard(node)

        for node in list(self._wait_for.keys()):
            if node not in visited:
                dfs(node, [])

        return cycles

    async def _deadlock_detection_loop(self):
        while True:
            await asyncio.sleep(Config.DEADLOCK_DETECTION_INTERVAL)
            cycles = self._detect_cycles()
            if cycles:
                logger.warning("Deadlock detected! Cycles: %s", cycles)
                self.metrics.increment('deadlocks_detected', len(cycles))
                # Resolve: abort the youngest transaction in each cycle
                for cycle in cycles:
                    victim = cycle[-1]
                    logger.warning("Aborting victim: %s", victim)
                    for resource in list(self._client_locks.get(victim, [])):
                        self._apply_release(resource, victim)

    # ------------------------------------------------------------------
    # Lease expiry
    # ------------------------------------------------------------------

    async def _lease_expiry_loop(self):
        while True:
            await asyncio.sleep(5.0)
            now = time.time()
            for resource, lock in list(self._locks.items()):
                expired = [r for r in lock.wait_queue if lock.is_expired(r)]
                for req in expired:
                    lock.wait_queue.remove(req)
                    logger.info("Expired waiting lock request: %s on %s", req.client_id, resource)


if __name__ == "__main__":
    import signal
    from ..utils.config import Config

    logging.basicConfig(
        level=getattr(logging, Config.LOG_LEVEL, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    node = DistributedLockManager(
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
