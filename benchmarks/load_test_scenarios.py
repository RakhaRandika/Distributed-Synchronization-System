"""
Load test scenarios menggunakan locust.
Jalankan dengan: locust -f benchmarks/load_test_scenarios.py --host http://localhost:8100

Skenario yang diuji:
  - LockUser     : acquire/release locks secara berulang
  - QueueUser    : produce & consume messages
  - CacheUser    : cache reads, writes, dan invalidations
"""

import random
import uuid

from locust import HttpUser, TaskSet, between, task


# ---------------------------------------------------------------------------
# Lock Manager Load Test
# ---------------------------------------------------------------------------

class LockTaskSet(TaskSet):
    def on_start(self):
        self.client_id = f"locust-{uuid.uuid4().hex[:8]}"
        self.resources = [f"resource-{i}" for i in range(10)]

    @task(3)
    def acquire_exclusive(self):
        resource = random.choice(self.resources)
        with self.client.post(
            "/lock/acquire",
            json={
                "resource": resource,
                "client_id": self.client_id,
                "lock_type": "exclusive",
            },
            catch_response=True,
        ) as resp:
            if resp.status_code in (200, 307):
                resp.success()
            else:
                resp.failure(f"Unexpected status {resp.status_code}")

    @task(2)
    def acquire_shared(self):
        resource = random.choice(self.resources)
        self.client.post(
            "/lock/acquire",
            json={
                "resource": resource,
                "client_id": self.client_id,
                "lock_type": "shared",
            },
        )

    @task(5)
    def release_lock(self):
        resource = random.choice(self.resources)
        self.client.post(
            "/lock/release",
            json={"resource": resource, "client_id": self.client_id},
        )

    @task(1)
    def check_status(self):
        resource = random.choice(self.resources)
        self.client.get(f"/lock/status/{resource}")


class LockUser(HttpUser):
    tasks = [LockTaskSet]
    wait_time = between(0.05, 0.3)
    host = "http://localhost:8100"


# ---------------------------------------------------------------------------
# Queue Load Test
# ---------------------------------------------------------------------------

class QueueTaskSet(TaskSet):
    queues = ["orders", "events", "notifications", "analytics"]

    def on_start(self):
        self.producer_id = f"prod-{uuid.uuid4().hex[:6]}"
        self.consumer_id = f"cons-{uuid.uuid4().hex[:6]}"
        self.pending_acks = []

    @task(4)
    def enqueue_message(self):
        queue = random.choice(self.queues)
        resp = self.client.post(
            "/queue/enqueue",
            json={
                "queue": queue,
                "payload": {
                    "event_type": random.choice(["click", "purchase", "view"]),
                    "user_id": random.randint(1, 10000),
                    "value": round(random.uniform(1.0, 500.0), 2),
                },
                "producer_id": self.producer_id,
            },
        )
        if resp.status_code == 200:
            msg_id = resp.json().get("msg_id")
            if msg_id:
                self.pending_acks.append(msg_id)

    @task(3)
    def dequeue_message(self):
        queue = random.choice(self.queues)
        resp = self.client.post(
            "/queue/dequeue",
            json={"queue": queue, "consumer_id": self.consumer_id},
        )
        if resp.status_code == 200:
            msg = resp.json().get("msg")
            if msg:
                self.pending_acks.append(msg["msg_id"])

    @task(2)
    def ack_message(self):
        if not self.pending_acks:
            return
        msg_id = self.pending_acks.pop(0)
        self.client.post("/queue/ack", json={"msg_id": msg_id})

    @task(1)
    def check_queue_stats(self):
        queue = random.choice(self.queues)
        self.client.get(f"/queue/stats/{queue}")


class QueueUser(HttpUser):
    tasks = [QueueTaskSet]
    wait_time = between(0.01, 0.2)
    host = "http://localhost:8200"


# ---------------------------------------------------------------------------
# Cache Load Test
# ---------------------------------------------------------------------------

class CacheTaskSet(TaskSet):
    def on_start(self):
        self.keys = [f"key-{i}" for i in range(50)]

    @task(5)
    def cache_read(self):
        key = random.choice(self.keys)
        self.client.get(f"/cache/{key}")

    @task(3)
    def cache_write(self):
        key = random.choice(self.keys)
        self.client.put(
            f"/cache/{key}",
            json={
                "value": {
                    "score": random.randint(0, 100),
                    "label": random.choice(["A", "B", "C"]),
                }
            },
        )

    @task(1)
    def cache_delete(self):
        key = random.choice(self.keys)
        self.client.delete(f"/cache/{key}")

    @task(1)
    def cache_stats(self):
        self.client.get("/cache/stats")


class CacheUser(HttpUser):
    tasks = [CacheTaskSet]
    wait_time = between(0.01, 0.15)
    host = "http://localhost:8300"
