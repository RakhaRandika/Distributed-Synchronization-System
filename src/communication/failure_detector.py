import asyncio
import logging
import time
from collections import deque
from typing import Callable, Dict, List, Optional, Set

import aiohttp

logger = logging.getLogger(__name__)


class PeerStatus:
    def __init__(self, peer_id: str, window: int = 10):
        self.peer_id = peer_id
        self.last_seen: float = time.monotonic()
        self.is_alive: bool = True
        self._intervals: deque = deque(maxlen=window)
        self._prev_ping: Optional[float] = None

    def record_success(self):
        now = time.monotonic()
        self.last_seen = now
        self.is_alive = True
        if self._prev_ping is not None:
            self._intervals.append(now - self._prev_ping)
        self._prev_ping = now

    def record_failure(self):
        self.is_alive = False

    @property
    def mean_interval(self) -> float:
        if not self._intervals:
            return 1.0
        return sum(self._intervals) / len(self._intervals)


class FailureDetector:
    """
    Ping-based failure detector.
    Calls on_failure(peer_id) when a peer misses enough pings.
    Calls on_recovery(peer_id) when it comes back.
    """

    def __init__(
        self,
        node_id: str,
        peers: List[str],
        ping_interval: float = 1.0,
        failure_timeout: float = 5.0,
        rpc_timeout: float = 1.0,
        on_failure: Optional[Callable] = None,
        on_recovery: Optional[Callable] = None,
    ):
        self.node_id = node_id
        self.peers = peers
        self._ping_interval = ping_interval
        self._failure_timeout = failure_timeout
        self._rpc_timeout = rpc_timeout
        self.on_failure = on_failure
        self.on_recovery = on_recovery

        self._status: Dict[str, PeerStatus] = {p: PeerStatus(p) for p in peers}
        self._confirmed_dead: Set[str] = set()
        self._running = False
        self._task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._ping_loop(), name="failure-detector")
        logger.info("FailureDetector started for node %s, watching %d peers", self.node_id, len(self.peers))

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _ping_loop(self):
        while self._running:
            await asyncio.gather(
                *[self._ping(peer) for peer in self.peers], return_exceptions=True
            )
            self._check_timeouts()
            await asyncio.sleep(self._ping_interval)

    async def _ping(self, peer: str):
        try:
            timeout = aiohttp.ClientTimeout(total=self._rpc_timeout)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(f'http://{peer}/health') as resp:
                    if resp.status == 200:
                        status = self._status[peer]
                        was_dead = peer in self._confirmed_dead

                        status.record_success()

                        if was_dead:
                            self._confirmed_dead.discard(peer)
                            logger.info("Peer %s recovered", peer)
                            if self.on_recovery:
                                asyncio.create_task(self.on_recovery(peer))
                        return
        except Exception:
            pass

        self._status[peer].record_failure()

    def _check_timeouts(self):
        now = time.monotonic()
        for peer, status in self._status.items():
            if peer in self._confirmed_dead:
                continue
            if not status.is_alive and (now - status.last_seen) > self._failure_timeout:
                self._confirmed_dead.add(peer)
                logger.warning("Peer %s declared FAILED (last seen %.1fs ago)", peer, now - status.last_seen)
                if self.on_failure:
                    asyncio.create_task(self.on_failure(peer))

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def is_alive(self, peer: str) -> bool:
        return peer not in self._confirmed_dead

    def alive_peers(self) -> List[str]:
        return [p for p in self.peers if self.is_alive(p)]

    def dead_peers(self) -> List[str]:
        return list(self._confirmed_dead)

    def get_status(self) -> dict:
        return {
            'node_id': self.node_id,
            'peers': {
                peer: {
                    'alive': self.is_alive(peer),
                    'last_seen_ago': round(time.monotonic() - s.last_seen, 2),
                    'mean_ping_interval': round(s.mean_interval, 3),
                }
                for peer, s in self._status.items()
            },
        }
