"""Unit tests for LRU/LFU caches and MESI state transitions."""

import pytest
from src.nodes.cache_node import LRUCache, LFUCache, CacheLine, MESIState


def make_line(key: str, value=None, state=MESIState.EXCLUSIVE) -> CacheLine:
    return CacheLine(key=key, value=value or key, state=state)


class TestLRUCache:
    def test_put_and_get(self):
        cache = LRUCache(capacity=3)
        cache.put(make_line("a"))
        assert cache.get("a") is not None

    def test_miss_returns_none(self):
        cache = LRUCache(capacity=3)
        assert cache.get("missing") is None

    def test_evicts_lru_on_overflow(self):
        cache = LRUCache(capacity=2)
        cache.put(make_line("a"))
        cache.put(make_line("b"))
        evicted = cache.put(make_line("c"))
        assert evicted == "a"
        assert cache.get("a") is None
        assert cache.get("b") is not None

    def test_access_refreshes_lru_order(self):
        cache = LRUCache(capacity=2)
        cache.put(make_line("a"))
        cache.put(make_line("b"))
        cache.get("a")         # 'a' is now most recently used
        evicted = cache.put(make_line("c"))
        assert evicted == "b"  # 'b' is LRU now

    def test_invalidate_sets_state_invalid(self):
        cache = LRUCache(capacity=5)
        cache.put(make_line("x"))
        cache.invalidate("x")
        assert cache.get("x").state == MESIState.INVALID

    def test_remove_deletes_entry(self):
        cache = LRUCache(capacity=5)
        cache.put(make_line("x"))
        cache.remove("x")
        assert cache.get("x") is None

    def test_len(self):
        cache = LRUCache(capacity=10)
        for i in range(5):
            cache.put(make_line(str(i)))
        assert len(cache) == 5


class TestLFUCache:
    def test_put_and_get(self):
        cache = LFUCache(capacity=3)
        cache.put(make_line("a"))
        assert cache.get("a") is not None

    def test_evicts_lfu_on_overflow(self):
        cache = LFUCache(capacity=2)
        cache.put(make_line("a"))
        cache.put(make_line("b"))
        cache.get("a")  # 'a' used twice, 'b' used once
        evicted = cache.put(make_line("c"))
        assert evicted == "b"

    def test_invalidate(self):
        cache = LFUCache(capacity=5)
        cache.put(make_line("k"))
        cache.invalidate("k")
        assert cache.get("k").state == MESIState.INVALID

    def test_remove(self):
        cache = LFUCache(capacity=5)
        cache.put(make_line("k"))
        cache.remove("k")
        assert cache.get("k") is None


class TestMESIStates:
    def test_initial_state_exclusive(self):
        line = make_line("k")
        assert line.state == MESIState.EXCLUSIVE

    def test_state_values(self):
        assert MESIState.MODIFIED.value == "M"
        assert MESIState.EXCLUSIVE.value == "E"
        assert MESIState.SHARED.value == "S"
        assert MESIState.INVALID.value == "I"
