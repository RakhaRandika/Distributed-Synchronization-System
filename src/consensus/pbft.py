import asyncio
import hashlib
import json
import logging
import time
from collections import defaultdict
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

import aiohttp

logger = logging.getLogger(__name__)


class PBFTPhase(Enum):
    PRE_PREPARE = "pre-prepare"
    PREPARE = "prepare"
    COMMIT = "commit"
    REPLY = "reply"


class PBFTMessage:
    def __init__(self, phase: PBFTPhase, view: int, seq: int, digest: str, node_id: str, data: Any = None):
        self.phase = phase
        self.view = view
        self.seq = seq
        self.digest = digest
        self.node_id = node_id
        self.data = data
        self.timestamp = time.time()

    def to_dict(self) -> dict:
        return {
            'phase': self.phase.value,
            'view': self.view,
            'seq': self.seq,
            'digest': self.digest,
            'node_id': self.node_id,
            'data': self.data,
            'timestamp': self.timestamp,
        }

    @classmethod
    def from_dict(cls, d: dict) -> 'PBFTMessage':
        return cls(
            phase=PBFTPhase(d['phase']),
            view=d['view'],
            seq=d['seq'],
            digest=d['digest'],
            node_id=d['node_id'],
            data=d.get('data'),
        )


class PBFTNode:
    """
    Simplified PBFT implementation for educational purposes.
    Handles the three-phase protocol: Pre-Prepare → Prepare → Commit.
    """

    def __init__(
        self,
        node_id: str,
        all_nodes: List[str],
        execute_callback: Callable,
        rpc_timeout: float = 2.0,
    ):
        self.node_id = node_id
        self.all_nodes = all_nodes
        self.execute_callback = execute_callback
        self._rpc_timeout = rpc_timeout

        self.n = len(all_nodes)
        self.f = (self.n - 1) // 3  # max tolerated Byzantine faulty nodes

        self.view = 0
        self.seq = 0
        self.primary_id = all_nodes[0]

        # Logs keyed by sequence number
        self.pre_prepare_log: Dict[int, PBFTMessage] = {}
        self.prepare_log: Dict[int, List[PBFTMessage]] = defaultdict(list)
        self.commit_log: Dict[int, List[PBFTMessage]] = defaultdict(list)
        self.executed: set = set()

        self._running = False
        self.requests_committed = 0

    @property
    def is_primary(self) -> bool:
        return self.node_id == self.all_nodes[self.view % self.n]

    def _digest(self, data: Any) -> str:
        return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()

    # ------------------------------------------------------------------
    # Client request entry-point (primary only)
    # ------------------------------------------------------------------

    async def submit_request(self, request: dict) -> bool:
        if not self.is_primary:
            logger.warning("Submit called on non-primary node %s", self.node_id)
            return False

        self.seq += 1
        digest = self._digest(request)
        msg = PBFTMessage(PBFTPhase.PRE_PREPARE, self.view, self.seq, digest, self.node_id, request)
        self.pre_prepare_log[self.seq] = msg

        await self._broadcast('/pbft/pre_prepare', msg.to_dict())
        return True

    # ------------------------------------------------------------------
    # Phase handlers (called by HTTP layer)
    # ------------------------------------------------------------------

    async def handle_pre_prepare(self, data: dict):
        msg = PBFTMessage.from_dict(data)

        if msg.view != self.view:
            return
        if msg.seq in self.pre_prepare_log:
            return
        expected_digest = self._digest(msg.data)
        if expected_digest != msg.digest:
            logger.warning("Digest mismatch in pre-prepare seq=%d", msg.seq)
            return

        self.pre_prepare_log[msg.seq] = msg

        prepare = PBFTMessage(PBFTPhase.PREPARE, self.view, msg.seq, msg.digest, self.node_id)
        await self._broadcast('/pbft/prepare', prepare.to_dict())

    async def handle_prepare(self, data: dict):
        msg = PBFTMessage.from_dict(data)

        if msg.view != self.view:
            return
        self.prepare_log[msg.seq].append(msg)

        # Prepared certificate: pre-prepare + 2f prepare messages
        if (
            msg.seq in self.pre_prepare_log
            and len(self.prepare_log[msg.seq]) >= 2 * self.f
        ):
            commit = PBFTMessage(PBFTPhase.COMMIT, self.view, msg.seq, msg.digest, self.node_id)
            await self._broadcast('/pbft/commit', commit.to_dict())

    async def handle_commit(self, data: dict):
        msg = PBFTMessage.from_dict(data)

        if msg.view != self.view:
            return
        self.commit_log[msg.seq].append(msg)

        # Committed certificate: 2f+1 commit messages
        if (
            msg.seq not in self.executed
            and len(self.commit_log[msg.seq]) >= 2 * self.f + 1
        ):
            self.executed.add(msg.seq)
            if msg.seq in self.pre_prepare_log:
                request = self.pre_prepare_log[msg.seq].data
                await self.execute_callback(request)
                self.requests_committed += 1
                logger.info("PBFT committed seq=%d", msg.seq)

    # ------------------------------------------------------------------
    # Broadcast helper
    # ------------------------------------------------------------------

    async def _broadcast(self, path: str, payload: dict):
        timeout = aiohttp.ClientTimeout(total=self._rpc_timeout)

        async def send(node: str):
            try:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    await session.post(f'http://{node}{path}', json=payload)
            except Exception as exc:
                logger.debug("Broadcast to %s%s failed: %s", node, path, exc)

        await asyncio.gather(*[send(n) for n in self.all_nodes if n != self.node_id], return_exceptions=True)

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def get_status(self) -> dict:
        return {
            'node_id': self.node_id,
            'is_primary': self.is_primary,
            'view': self.view,
            'seq': self.seq,
            'n': self.n,
            'f': self.f,
            'requests_committed': self.requests_committed,
            'executed_seqs': sorted(self.executed),
        }
