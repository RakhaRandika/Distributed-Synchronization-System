"""Unit tests for Distributed Lock Manager state machine."""

import pytest
from src.nodes.lock_manager import DistributedLockManager, LockState, LockType, LockRequest


def make_manager():
    mgr = DistributedLockManager.__new__(DistributedLockManager)
    mgr._locks = {}
    mgr._client_locks = {}
    from collections import defaultdict
    mgr._client_locks = defaultdict(list)
    mgr._wait_for = defaultdict(set)
    mgr.metrics = type('M', (), {'increment': lambda self, *a: None})()
    return mgr


class TestLockAcquire:
    def test_exclusive_lock_granted_when_free(self):
        mgr = make_manager()
        mgr._apply_acquire('resource_a', 'client_1', LockType.EXCLUSIVE)
        lock = mgr._locks['resource_a']
        assert lock.exclusive_holder == 'client_1'
        assert len(lock.shared_holders) == 0

    def test_shared_lock_granted_when_free(self):
        mgr = make_manager()
        mgr._apply_acquire('resource_a', 'client_1', LockType.SHARED)
        lock = mgr._locks['resource_a']
        assert 'client_1' in lock.shared_holders
        assert lock.exclusive_holder is None

    def test_multiple_shared_locks_allowed(self):
        mgr = make_manager()
        mgr._apply_acquire('resource_a', 'client_1', LockType.SHARED)
        mgr._apply_acquire('resource_a', 'client_2', LockType.SHARED)
        lock = mgr._locks['resource_a']
        assert 'client_1' in lock.shared_holders
        assert 'client_2' in lock.shared_holders

    def test_exclusive_blocked_when_shared_held(self):
        mgr = make_manager()
        mgr._apply_acquire('resource_a', 'client_1', LockType.SHARED)
        mgr._apply_acquire('resource_a', 'client_2', LockType.EXCLUSIVE)
        lock = mgr._locks['resource_a']
        assert lock.exclusive_holder is None
        assert len(lock.wait_queue) == 1
        assert lock.wait_queue[0].client_id == 'client_2'

    def test_shared_blocked_when_exclusive_held(self):
        mgr = make_manager()
        mgr._apply_acquire('resource_a', 'client_1', LockType.EXCLUSIVE)
        mgr._apply_acquire('resource_a', 'client_2', LockType.SHARED)
        lock = mgr._locks['resource_a']
        assert len(lock.shared_holders) == 0
        assert len(lock.wait_queue) == 1


class TestLockRelease:
    def test_release_exclusive_grants_waiting_exclusive(self):
        mgr = make_manager()
        mgr._apply_acquire('res', 'c1', LockType.EXCLUSIVE)
        mgr._apply_acquire('res', 'c2', LockType.EXCLUSIVE)
        mgr._apply_release('res', 'c1')
        lock = mgr._locks['res']
        assert lock.exclusive_holder == 'c2'
        assert len(lock.wait_queue) == 0

    def test_release_shared_grants_waiting_exclusive(self):
        mgr = make_manager()
        mgr._apply_acquire('res', 'c1', LockType.SHARED)
        mgr._apply_acquire('res', 'c2', LockType.EXCLUSIVE)
        mgr._apply_release('res', 'c1')
        lock = mgr._locks['res']
        assert lock.exclusive_holder == 'c2'

    def test_release_updates_client_locks(self):
        mgr = make_manager()
        mgr._apply_acquire('res', 'c1', LockType.EXCLUSIVE)
        assert 'res' in mgr._client_locks['c1']
        mgr._apply_release('res', 'c1')
        assert 'res' not in mgr._client_locks['c1']


class TestDeadlockDetection:
    def test_no_cycle_returns_empty(self):
        mgr = make_manager()
        mgr._wait_for['c1'] = {'c2'}
        mgr._wait_for['c2'] = {'c3'}
        assert mgr._detect_cycles() == []

    def test_direct_cycle_detected(self):
        mgr = make_manager()
        mgr._wait_for['c1'] = {'c2'}
        mgr._wait_for['c2'] = {'c1'}
        cycles = mgr._detect_cycles()
        assert len(cycles) > 0

    def test_three_node_cycle_detected(self):
        mgr = make_manager()
        mgr._wait_for['c1'] = {'c2'}
        mgr._wait_for['c2'] = {'c3'}
        mgr._wait_for['c3'] = {'c1'}
        cycles = mgr._detect_cycles()
        assert len(cycles) > 0
