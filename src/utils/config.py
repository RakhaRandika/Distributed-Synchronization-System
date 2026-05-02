import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    # Node identity
    NODE_ID: str = os.getenv('NODE_ID', 'node-1')
    NODE_HOST: str = os.getenv('NODE_HOST', '0.0.0.0')
    NODE_PORT: int = int(os.getenv('NODE_PORT', '8000'))

    # Cluster peers: comma-separated "host:port" entries
    CLUSTER_NODES: list[str] = os.getenv(
        'CLUSTER_NODES', 'localhost:8000,localhost:8001,localhost:8002'
    ).split(',')

    # Raft tuning
    ELECTION_TIMEOUT_MIN: float = float(os.getenv('ELECTION_TIMEOUT_MIN', '1.5'))
    ELECTION_TIMEOUT_MAX: float = float(os.getenv('ELECTION_TIMEOUT_MAX', '3.0'))
    HEARTBEAT_INTERVAL: float = float(os.getenv('HEARTBEAT_INTERVAL', '0.5'))
    RAFT_RPC_TIMEOUT: float = float(os.getenv('RAFT_RPC_TIMEOUT', '1.0'))

    # Redis (persistence layer)
    REDIS_HOST: str = os.getenv('REDIS_HOST', 'localhost')
    REDIS_PORT: int = int(os.getenv('REDIS_PORT', '6379'))
    REDIS_DB: int = int(os.getenv('REDIS_DB', '0'))
    REDIS_PASSWORD: str | None = os.getenv('REDIS_PASSWORD', None)

    # Distributed queue
    QUEUE_REPLICAS: int = int(os.getenv('QUEUE_REPLICAS', '3'))
    QUEUE_MAX_SIZE: int = int(os.getenv('QUEUE_MAX_SIZE', '10000'))
    QUEUE_ACK_TIMEOUT: float = float(os.getenv('QUEUE_ACK_TIMEOUT', '30.0'))

    # Distributed cache
    CACHE_SIZE: int = int(os.getenv('CACHE_SIZE', '1000'))
    CACHE_POLICY: str = os.getenv('CACHE_POLICY', 'LRU')  # LRU | LFU

    # Lock manager
    LOCK_LEASE_DURATION: float = float(os.getenv('LOCK_LEASE_DURATION', '30.0'))
    LOCK_RETRY_INTERVAL: float = float(os.getenv('LOCK_RETRY_INTERVAL', '0.1'))
    DEADLOCK_DETECTION_INTERVAL: float = float(os.getenv('DEADLOCK_DETECTION_INTERVAL', '5.0'))

    # Observability
    METRICS_PORT: int = int(os.getenv('METRICS_PORT', '9090'))
    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO')

    # Failure detector
    FAILURE_TIMEOUT: float = float(os.getenv('FAILURE_TIMEOUT', '5.0'))
    PING_INTERVAL: float = float(os.getenv('PING_INTERVAL', '1.0'))
