"""Transporte HTTP limitado por timeout, sem decisões dos algoritmos."""
import asyncio
import logging

import httpx
from app.config import Settings
from app.models import CoordinatorMessage, ElectionMessage, ElectionOK, HeartbeatMessage

logger = logging.getLogger(__name__)

class NodeClient:
    def __init__(self, settings: Settings, client: httpx.AsyncClient):
        self.settings = settings
        self.client = client

    async def send_heartbeat(self, peer_id: int) -> None:
        message = HeartbeatMessage(sender_id=self.settings.node_id)
        try:
            # Limita também a duração total, além dos timeouts de cada fase HTTP.
            async with asyncio.timeout(self.settings.request_timeout):
                response = await self.client.post(
                    f"{self.settings.peers[peer_id]}/internal/heartbeat",
                    json=message.model_dump(),
                    timeout=self.settings.request_timeout
                )
                response.raise_for_status()
        except (httpx.HTTPError, TimeoutError):
            logger.debug("HEARTBEAT não entregue: node=%s peer=%s", self.settings.node_id, peer_id)

    async def send_election(self, peer_id: int, epoch: int) -> ElectionOK | None:
        
        message = ElectionMessage(sender_id=self.settings.node_id, epoch=epoch)
        try:
            response = await self.client.post(f"{self.settings.peers[peer_id]}/internal/election", json=message.model_dump(), timeout=self.settings.request_timeout)
            response.raise_for_status()
            answer = ElectionOK.model_validate(response.json())
            
            if answer.node_id == peer_id and answer.epoch >= epoch:
                return answer
            
        except (httpx.HTTPError, ValueError):
            logger.debug("ELECTION sem OK válido: node=%s peer=%s", self.settings.node_id, peer_id)
        return None

    async def send_coordinator(self, peer_id: int, epoch: int) -> None:
        message = CoordinatorMessage(leader_id=self.settings.node_id, epoch=epoch)
        
        try:
            response = await self.client.post(f"{self.settings.peers[peer_id]}/internal/coordinator", json=message.model_dump(), timeout=self.settings.request_timeout)
            response.raise_for_status()
        except httpx.HTTPError:
            logger.debug("COORDINATOR não entregue: node=%s peer=%s", self.settings.node_id, peer_id)
