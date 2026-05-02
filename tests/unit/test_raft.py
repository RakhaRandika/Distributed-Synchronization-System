"""Unit tests for Raft consensus module."""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock

from src.consensus.raft import RaftConsensus, NodeState, LogEntry, RaftState


@pytest.fixture
def state_machine():
    return AsyncMock()


@pytest.fixture
def make_raft(state_machine):
    def _make(node_id="n1", peers=None):
        return RaftConsensus(
            node_id=node_id,
            peers=peers or ["n2:8001", "n3:8002"],
            state_machine_callback=state_machine,
            election_timeout_range=(0.2, 0.4),
            heartbeat_interval=0.1,
        )
    return _make


class TestRaftState:
    def test_initial_state(self, make_raft):
        raft = make_raft()
        assert raft.role == NodeState.FOLLOWER
        assert raft.state.current_term == 0
        assert raft.state.voted_for is None
        assert len(raft.state.log) == 0

    def test_step_down_resets_voted_for(self, make_raft):
        raft = make_raft()
        raft.state.current_term = 5
        raft.state.voted_for = "n2:8001"
        raft._step_down(10)
        assert raft.state.current_term == 10
        assert raft.state.voted_for is None
        assert raft.role == NodeState.FOLLOWER


class TestRequestVote:
    @pytest.mark.asyncio
    async def test_grant_vote_when_log_ok(self, make_raft):
        raft = make_raft()
        response = await raft.handle_request_vote({
            'term': 1,
            'candidate_id': 'n2:8001',
            'last_log_index': -1,
            'last_log_term': 0,
        })
        assert response['vote_granted'] is True
        assert raft.state.voted_for == 'n2:8001'

    @pytest.mark.asyncio
    async def test_deny_vote_lower_term(self, make_raft):
        raft = make_raft()
        raft.state.current_term = 5
        response = await raft.handle_request_vote({
            'term': 3,
            'candidate_id': 'n2:8001',
            'last_log_index': -1,
            'last_log_term': 0,
        })
        assert response['vote_granted'] is False

    @pytest.mark.asyncio
    async def test_deny_duplicate_vote(self, make_raft):
        raft = make_raft()
        raft.state.current_term = 1
        raft.state.voted_for = 'n3:8002'
        response = await raft.handle_request_vote({
            'term': 1,
            'candidate_id': 'n2:8001',
            'last_log_index': -1,
            'last_log_term': 0,
        })
        assert response['vote_granted'] is False

    @pytest.mark.asyncio
    async def test_deny_stale_log(self, make_raft):
        raft = make_raft()
        raft.state.log = [LogEntry(term=2, index=0, command={})]
        response = await raft.handle_request_vote({
            'term': 3,
            'candidate_id': 'n2:8001',
            'last_log_index': -1,
            'last_log_term': 0,
        })
        assert response['vote_granted'] is False


class TestAppendEntries:
    @pytest.mark.asyncio
    async def test_accept_heartbeat(self, make_raft):
        raft = make_raft()
        response = await raft.handle_append_entries({
            'term': 1,
            'leader_id': 'n2:8001',
            'prev_log_index': -1,
            'prev_log_term': 0,
            'entries': [],
            'leader_commit': -1,
        })
        assert response['success'] is True
        assert raft.leader_id == 'n2:8001'

    @pytest.mark.asyncio
    async def test_reject_lower_term(self, make_raft):
        raft = make_raft()
        raft.state.current_term = 5
        response = await raft.handle_append_entries({
            'term': 3,
            'leader_id': 'n2:8001',
            'prev_log_index': -1,
            'prev_log_term': 0,
            'entries': [],
            'leader_commit': -1,
        })
        assert response['success'] is False

    @pytest.mark.asyncio
    async def test_append_new_entries(self, make_raft):
        raft = make_raft()
        response = await raft.handle_append_entries({
            'term': 1,
            'leader_id': 'n2:8001',
            'prev_log_index': -1,
            'prev_log_term': 0,
            'entries': [{'term': 1, 'index': 0, 'command': {'op': 'set', 'key': 'x', 'value': 1}}],
            'leader_commit': -1,
        })
        assert response['success'] is True
        assert len(raft.state.log) == 1
        assert raft.state.log[0].command['key'] == 'x'

    @pytest.mark.asyncio
    async def test_reject_inconsistent_prev(self, make_raft):
        raft = make_raft()
        raft.state.log = [LogEntry(term=1, index=0, command={})]
        response = await raft.handle_append_entries({
            'term': 2,
            'leader_id': 'n2:8001',
            'prev_log_index': 0,
            'prev_log_term': 99,  # wrong term
            'entries': [],
            'leader_commit': -1,
        })
        assert response['success'] is False


class TestElectionMetrics:
    @pytest.mark.asyncio
    async def test_election_increments_term(self, make_raft):
        raft = make_raft()
        assert raft.state.current_term == 0
        await raft._start_election()
        assert raft.state.current_term == 1
        assert raft.election_count == 1
        assert raft.role == NodeState.CANDIDATE

    def test_get_status_shape(self, make_raft):
        raft = make_raft()
        status = raft.get_status()
        for key in ['node_id', 'role', 'term', 'leader_id', 'log_length', 'commit_index']:
            assert key in status
