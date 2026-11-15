"""Detector de suspeitas por ausência de heartbeats recebidos via HTTP."""
import asyncio
import logging
import time
from collections.abc import Callable
from contextlib import suppress

from app.communication.client import NodeClient
from app.config import Settings
from app.state import NodeState

logger = logging.getLogger(__name__)

class Heartbeat:
    def __init__(self, settings: Settings, state: NodeState, client: NodeClient, on_leader_failure: Callable[[], None]):
        self.settings = settings
        self.state = state
        self.client = client
        self.on_leader_failure = on_leader_failure
        
        # Dá um timeout inicial completo a quem ainda não enviou heartbeat.
        now = time.monotonic()
        self.last_received = {peer_id: now for peer_id in settings.peers if peer_id != settings.node_id}
        self.suspected_nodes: set[int] = set()
        self._last_failed_leader: tuple[int, int] | None = None
        self._tasks: list[asyncio.Task] = []

    def start(self) -> None:
        if not self._tasks:
            self._tasks = [
                asyncio.create_task(self.send_heartbeats(), name=f"heartbeat-send-{self.state.node_id}"),
                asyncio.create_task(self.monitor_nodes(), name=f"heartbeat-monitor-{self.state.node_id}")
            ]

    def receive(self, sender_id: int) -> None:
        """O endpoint valida o participante antes de renovar seu prazo local."""
        self.last_received[sender_id] = time.monotonic()
        
        if sender_id in self.suspected_nodes:
            self.suspected_nodes.remove(sender_id)
            
            if self._last_failed_leader is not None and self._last_failed_leader[0] == sender_id:
                self._last_failed_leader = None
            logger.info("HEARTBEAT retomado: node=%s peer=%s", self.state.node_id, sender_id)

    async def send_heartbeats(self) -> None:
        async def send_to_peer(peer_id: int) -> None:
            while True:
                started = time.monotonic()
                await self.client.send_heartbeat(peer_id)
                elapsed = time.monotonic() - started
                await asyncio.sleep(max(0, self.settings.heartbeat_interval - elapsed))

        # Cada destino tem sua própria cadência; um envio lento não atrasa os demais.
        await asyncio.gather(*(send_to_peer(peer_id) for peer_id in self.last_received))

    async def monitor_nodes(self) -> None:
        interval = min(0.1, self.settings.heartbeat_interval, self.settings.failure_timeout / 2)
        while True:
            self.check_failures()
            await asyncio.sleep(interval)

    def check_failures(self) -> None:
        now = time.monotonic()
        for peer_id, last_received in self.last_received.items():
            if now - last_received > self.settings.failure_timeout and peer_id not in self.suspected_nodes:
                self.suspected_nodes.add(peer_id)
                logger.info("SUSPECTED node=%s peer=%s", self.state.node_id, peer_id)

        leader_id = self.state.leader_id
        if leader_id is not None and leader_id in self.suspected_nodes:
            leadership = (leader_id, self.state.epoch)
            if leadership != self._last_failed_leader:
                self._last_failed_leader = leadership
                self.on_leader_failure()

    async def close(self) -> None:
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with suppress(asyncio.CancelledError):
                await task
