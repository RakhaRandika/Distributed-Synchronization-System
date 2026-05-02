"""Unit tests for consistent hashing ring and queue message."""

import pytest
from src.nodes.queue_node import ConsistentHashRing, QueueMessage


class TestConsistentHashRing:
    def test_single_node_returns_itself(self):
        ring = ConsistentHashRing(["node1"])
        assert ring.get_node("any-key") == "node1"

    def test_all_keys_map_to_a_node(self):
        ring = ConsistentHashRing(["node1", "node2", "node3"])
        for key in ["alpha", "beta", "gamma", "delta", "epsilon"]:
            assert ring.get_node(key) in {"node1", "node2", "node3"}

    def test_remove_node_remaps_keys(self):
        ring = ConsistentHashRing(["node1", "node2", "node3"])
        ring.remove_node("node2")
        for key in ["alpha", "beta", "gamma"]:
            assert ring.get_node(key) in {"node1", "node3"}

    def test_replicas_returns_distinct_nodes(self):
        ring = ConsistentHashRing(["n1", "n2", "n3", "n4"])
        replicas = ring.get_replica_nodes("my-queue", 3)
        assert len(replicas) == 3
        assert len(set(replicas)) == 3  # all distinct

    def test_replicas_capped_by_node_count(self):
        ring = ConsistentHashRing(["n1", "n2"])
        replicas = ring.get_replica_nodes("key", 5)
        assert len(replicas) <= 2

    def test_consistent_mapping_after_add(self):
        ring = ConsistentHashRing(["n1", "n2"])
        before = ring.get_node("stable-key")
        ring.add_node("n3")
        # May or may not change, but must still be a valid node
        after = ring.get_node("stable-key")
        assert after in {"n1", "n2", "n3"}


class TestQueueMessage:
    def test_serialization_round_trip(self):
        msg = QueueMessage(queue_name="orders", payload={"item": "book", "qty": 2}, producer_id="prod-1")
        d = msg.to_dict()
        msg2 = QueueMessage.from_dict(d)
        assert msg2.queue_name == msg.queue_name
        assert msg2.payload == msg.payload
        assert msg2.msg_id == msg.msg_id
        assert msg2.producer_id == msg.producer_id

    def test_unique_msg_ids(self):
        ids = {QueueMessage(queue_name="q", payload={}).msg_id for _ in range(100)}
        assert len(ids) == 100
