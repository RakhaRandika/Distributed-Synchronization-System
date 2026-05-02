import asyncio
import random
import time
import logging
from enum import Enum
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

import aiohttp

logger = logging.getLogger(__name__)


class NodeState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class LogEntry:
    term: int
    index: int
    command: dict


@dataclass
class RaftState:
    # --- Persistent ---
    current_term: int = 0
    voted_for: Optional[str] = None
    log: List[LogEntry] = field(default_factory=list)

    # --- Volatile ---
    commit_index: int = -1
    last_applied: int = -1

    # --- Leader volatile ---
    next_index: Dict[str, int] = field(default_factory=dict)
    match_index: Dict[str, int] = field(default_factory=dict)


class RaftConsensus:
    """
    Raft consensus module.  Communicates with peers over HTTP using aiohttp.
    The host application must expose /raft/request_vote and /raft/append_entries
    and forward requests to handle_request_vote() / handle_append_entries().
    """

    def __init__(
        self,
        node_id: str,
        peers: List[str],
        state_machine_callback: Callable,
        election_timeout_range: tuple[float, float] = (1.5, 3.0),
        heartbeat_interval: float = 0.5,
        rpc_timeout: float = 1.0,
    ):
        self.node_id = node_id
        self.peers = peers
        self.state = RaftState()
        self.role = NodeState.FOLLOWER
        self.leader_id: Optional[str] = None
        self.votes_received: set = set()
        self.state_machine_callback = state_machine_callback

        self._election_timeout_range = election_timeout_range
        self._heartbeat_interval = heartbeat_interval
        self._rpc_timeout = rpc_timeout

        self.election_timeout = self._random_election_timeout()
        self.last_heartbeat = time.monotonic()

        # Metrics
        self.election_count = 0
        self.commits_applied = 0

        self._running = False
        self._tasks: List[asyncio.Task] = []
        self._leader_task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _random_election_timeout(self) -> float:
        lo, hi = self._election_timeout_range
        return random.uniform(lo, hi)

    async def start(self):
        self._running = True
        self._tasks = [
            asyncio.create_task(self._election_timer_loop(), name="election-timer"),
            asyncio.create_task(self._apply_committed_loop(), name="apply-entries"),
        ]
        logger.info("Raft node %s started as %s", self.node_id, self.role.value)

    async def stop(self):
        self._running = False
        all_tasks = list(self._tasks)
        if self._leader_task:
            all_tasks.append(self._leader_task)
        for t in all_tasks:
            t.cancel()
        await asyncio.gather(*all_tasks, return_exceptions=True)
        logger.info("Raft node %s stopped", self.node_id)

    # ------------------------------------------------------------------
    # Election
    # ------------------------------------------------------------------

    async def _election_timer_loop(self):
        while self._running:
            await asyncio.sleep(0.1)
            if self.role != NodeState.LEADER:
                elapsed = time.monotonic() - self.last_heartbeat
                if elapsed > self.election_timeout:
                    await self._start_election()

    async def _start_election(self):
        self.state.current_term += 1
        self.role = NodeState.CANDIDATE
        self.state.voted_for = self.node_id
        self.votes_received = {self.node_id}
        self.election_timeout = self._random_election_timeout()
        self.last_heartbeat = time.monotonic()
        self.election_count += 1

        logger.info(
            "Node %s starting election for term %d",
            self.node_id,
            self.state.current_term,
        )

        last_log_index = len(self.state.log) - 1
        last_log_term = self.state.log[-1].term if self.state.log else 0

        await asyncio.gather(
            *[self._request_vote(p, last_log_index, last_log_term) for p in self.peers],
            return_exceptions=True,
        )

    async def _request_vote(self, peer: str, last_log_index: int, last_log_term: int):
        payload = {
            'term': self.state.current_term,
            'candidate_id': self.node_id,
            'last_log_index': last_log_index,
            'last_log_term': last_log_term,
        }
        try:
            timeout = aiohttp.ClientTimeout(total=self._rpc_timeout)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(f'http://{peer}/raft/request_vote', json=payload) as resp:
                    if resp.status == 200:
                        await self._handle_vote_response(await resp.json())
        except Exception as exc:
            logger.debug("Vote request to %s failed: %s", peer, exc)

    async def _handle_vote_response(self, response: dict):
        if response.get('term', 0) > self.state.current_term:
            self._step_down(response['term'])
            return

        if self.role != NodeState.CANDIDATE:
            return

        if response.get('vote_granted'):
            self.votes_received.add(response.get('voter_id', ''))
            majority = len(self.peers) // 2 + 1
            if len(self.votes_received) >= majority:
                await self._become_leader()

    # ------------------------------------------------------------------
    # Leader duties
    # ------------------------------------------------------------------

    async def _become_leader(self):
        self.role = NodeState.LEADER
        self.leader_id = self.node_id
        logger.info("Node %s is now LEADER for term %d", self.node_id, self.state.current_term)

        next_idx = len(self.state.log)
        for peer in self.peers:
            self.state.next_index[peer] = next_idx
            self.state.match_index[peer] = -1

        if self._leader_task:
            self._leader_task.cancel()
        self._leader_task = asyncio.create_task(self._heartbeat_loop(), name="heartbeat")

    async def _heartbeat_loop(self):
        while self._running and self.role == NodeState.LEADER:
            await self._replicate_to_all()
            await asyncio.sleep(self._heartbeat_interval)

    async def _replicate_to_all(self):
        await asyncio.gather(
            *[self._send_append_entries(p) for p in self.peers],
            return_exceptions=True,
        )

    async def _send_append_entries(self, peer: str):
        prev_idx = self.state.next_index.get(peer, 0) - 1
        prev_term = 0
        if 0 <= prev_idx < len(self.state.log):
            prev_term = self.state.log[prev_idx].term

        entries_to_send = [
            {'term': e.term, 'index': e.index, 'command': e.command}
            for e in self.state.log[self.state.next_index.get(peer, 0):]
        ]

        payload = {
            'term': self.state.current_term,
            'leader_id': self.node_id,
            'prev_log_index': prev_idx,
            'prev_log_term': prev_term,
            'entries': entries_to_send,
            'leader_commit': self.state.commit_index,
        }

        try:
            timeout = aiohttp.ClientTimeout(total=self._rpc_timeout)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(f'http://{peer}/raft/append_entries', json=payload) as resp:
                    if resp.status == 200:
                        await self._handle_ae_response(peer, await resp.json(), len(entries_to_send))
        except Exception as exc:
            logger.debug("AppendEntries to %s failed: %s", peer, exc)

    async def _handle_ae_response(self, peer: str, response: dict, sent: int):
        if response.get('term', 0) > self.state.current_term:
            self._step_down(response['term'])
            return

        if response.get('success'):
            self.state.next_index[peer] = self.state.next_index.get(peer, 0) + sent
            self.state.match_index[peer] = self.state.next_index[peer] - 1
            await self._advance_commit_index()
        else:
            self.state.next_index[peer] = max(0, self.state.next_index.get(peer, 1) - 1)

    async def _advance_commit_index(self):
        for idx in range(len(self.state.log) - 1, self.state.commit_index, -1):
            if self.state.log[idx].term != self.state.current_term:
                continue
            replicated = 1 + sum(
                1 for p in self.peers if self.state.match_index.get(p, -1) >= idx
            )
            if replicated > (len(self.peers) + 1) // 2:
                self.state.commit_index = idx
                break

    # ------------------------------------------------------------------
    # Apply committed entries to state machine
    # ------------------------------------------------------------------

    async def _apply_committed_loop(self):
        while self._running:
            await asyncio.sleep(0.05)
            while self.state.last_applied < self.state.commit_index:
                self.state.last_applied += 1
                entry = self.state.log[self.state.last_applied]
                try:
                    await self.state_machine_callback(entry.command)
                    self.commits_applied += 1
                except Exception as exc:
                    logger.error("State machine error at index %d: %s", self.state.last_applied, exc)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def propose(self, command: dict, timeout: float = 5.0) -> bool:
        """Append a command to the leader's log and wait for it to be committed."""
        if self.role != NodeState.LEADER:
            return False

        entry = LogEntry(
            term=self.state.current_term,
            index=len(self.state.log),
            command=command,
        )
        self.state.log.append(entry)

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.state.commit_index >= entry.index:
                return True
            await asyncio.sleep(0.05)

        return False

    # ------------------------------------------------------------------
    # RPC handlers (called by HTTP layer)
    # ------------------------------------------------------------------

    async def handle_request_vote(self, request: dict) -> dict:
        term = request['term']
        candidate_id = request['candidate_id']
        last_log_index = request['last_log_index']
        last_log_term = request['last_log_term']

        if term > self.state.current_term:
            self._step_down(term)

        vote_granted = False

        if term >= self.state.current_term:
            not_voted = self.state.voted_for in (None, candidate_id)
            my_last_idx = len(self.state.log) - 1
            my_last_term = self.state.log[-1].term if self.state.log else 0
            log_ok = last_log_term > my_last_term or (
                last_log_term == my_last_term and last_log_index >= my_last_idx
            )
            if not_voted and log_ok:
                vote_granted = True
                self.state.voted_for = candidate_id
                self.last_heartbeat = time.monotonic()

        return {
            'term': self.state.current_term,
            'vote_granted': vote_granted,
            'voter_id': self.node_id,
        }

    async def handle_append_entries(self, request: dict) -> dict:
        term = request['term']
        leader_id = request['leader_id']
        prev_log_index = request['prev_log_index']
        prev_log_term = request['prev_log_term']
        entries = request['entries']
        leader_commit = request['leader_commit']

        if term < self.state.current_term:
            return {'term': self.state.current_term, 'success': False}

        self.last_heartbeat = time.monotonic()
        if term > self.state.current_term:
            self._step_down(term)
        self.role = NodeState.FOLLOWER
        self.leader_id = leader_id

        # Consistency check
        if prev_log_index >= 0:
            if prev_log_index >= len(self.state.log):
                return {'term': self.state.current_term, 'success': False}
            if self.state.log[prev_log_index].term != prev_log_term:
                self.state.log = self.state.log[:prev_log_index]
                return {'term': self.state.current_term, 'success': False}

        # Append / overwrite entries
        for entry_data in entries:
            entry = LogEntry(
                term=entry_data['term'],
                index=entry_data['index'],
                command=entry_data['command'],
            )
            idx = entry.index
            if idx < len(self.state.log):
                if self.state.log[idx].term != entry.term:
                    self.state.log = self.state.log[:idx]
                    self.state.log.append(entry)
            else:
                self.state.log.append(entry)

        if leader_commit > self.state.commit_index:
            self.state.commit_index = min(leader_commit, len(self.state.log) - 1)

        return {'term': self.state.current_term, 'success': True}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _step_down(self, new_term: int):
        self.state.current_term = new_term
        self.role = NodeState.FOLLOWER
        self.state.voted_for = None
        if self._leader_task:
            self._leader_task.cancel()
            self._leader_task = None

    def get_status(self) -> dict:
        return {
            'node_id': self.node_id,
            'role': self.role.value,
            'term': self.state.current_term,
            'leader_id': self.leader_id,
            'log_length': len(self.state.log),
            'commit_index': self.state.commit_index,
            'last_applied': self.state.last_applied,
            'election_count': self.election_count,
            'commits_applied': self.commits_applied,
            'peers': self.peers,
        }
