import asyncio
import logging
import time
from typing import List, Optional

from aiohttp import web

from ..communication.failure_detector import FailureDetector
from ..communication.message_passing import MessageBus
from ..utils.config import Config
from ..utils.metrics import MetricsCollector

logger = logging.getLogger(__name__)


class BaseNode:
    def __init__(
        self,
        node_id: str,
        host: str,
        port: int,
        peers: List[str],
    ):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers = peers

        self.metrics = MetricsCollector(node_id)
        self.message_bus = MessageBus(node_id)
        self.failure_detector = FailureDetector(
            node_id=node_id,
            peers=peers,
            ping_interval=Config.PING_INTERVAL,
            failure_timeout=Config.FAILURE_TIMEOUT,
            on_failure=self._on_peer_failure,
            on_recovery=self._on_peer_recovery,
        )

        self._app = web.Application()
        self._runner: Optional[web.AppRunner] = None
        self._start_time = time.time()

    # ------------------------------------------------------------------
    # Common routes (every node exposes these)
    # ------------------------------------------------------------------

    def _register_common_routes(self):
        self._app.router.add_get('/health', self._handle_health)
        self._app.router.add_get('/metrics', self._handle_metrics)
        self._app.router.add_post('/message', self.message_bus.http_handler)

    async def _handle_health(self, request: web.Request) -> web.Response:
        return web.json_response({
            'node_id': self.node_id,
            'status': 'ok',
            'uptime': round(time.time() - self._start_time, 2),
        })

    async def _handle_metrics(self, request: web.Request) -> web.Response:
        return web.json_response(self.metrics.get_stats())

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _setup_routes(self):
        """Override in subclass to register additional routes."""

    async def start(self):
        self._register_common_routes()
        self._setup_routes()

        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.host, self.port)
        await site.start()

        await self.failure_detector.start()
        await self._on_start()

        logger.info("Node %s listening on %s:%d", self.node_id, self.host, self.port)

    async def stop(self):
        await self._on_stop()
        await self.failure_detector.stop()
        if self._runner:
            await self._runner.cleanup()
        logger.info("Node %s stopped", self.node_id)

    async def _on_start(self):
        """Hook for subclass initialization after HTTP server is up."""

    async def _on_stop(self):
        """Hook for subclass cleanup before HTTP server is torn down."""

    # ------------------------------------------------------------------
    # Failure events
    # ------------------------------------------------------------------

    async def _on_peer_failure(self, peer_id: str):
        logger.warning("Node %s: peer %s declared failed", self.node_id, peer_id)
        self.metrics.increment('peer_failures')

    async def _on_peer_recovery(self, peer_id: str):
        logger.info("Node %s: peer %s recovered", self.node_id, peer_id)
        self.metrics.increment('peer_recoveries')
