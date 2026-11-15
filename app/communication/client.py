"""Transporte HTTP limitado por timeout, sem decisões dos algoritmos."""
import asyncio
import logging

import httpx
from app.config import Settings
from app.models import (
    CommitMessage, ConfigurationAck, ConfigurationMessage, CoordinatorMessage,
    ElectionMessage, ElectionOK, HeartbeatMessage, Operation, PrepareMessage,
    RecoveryState, ReplicationAck, ReplicationState,
)

logger = logging.getLogger(__name__)


class NodeUnavailable(Exception):
    """O destino não forneceu uma resposta válida no prazo."""


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
            async with asyncio.timeout(self.settings.request_timeout):
                response = await self.client.post(f"{self.settings.peers[peer_id]}/internal/coordinator", json=message.model_dump(), timeout=self.settings.request_timeout)
                response.raise_for_status()
        except (httpx.HTTPError, TimeoutError):
            logger.debug("COORDINATOR não entregue: node=%s peer=%s", self.settings.node_id, peer_id)

    async def replication_state(self, peer_id: int) -> ReplicationState:
        try:
            async with asyncio.timeout(self.settings.request_timeout):
                response = await self.client.get(f"{self.settings.peers[peer_id]}/internal/replication/state", timeout=self.settings.request_timeout)
                response.raise_for_status()
                state = ReplicationState.model_validate(response.json())
            
                if state.node_id != peer_id:
                    raise ValueError("Estado recebido de outro participante")
                return state
            
        except (httpx.HTTPError, ValueError, TimeoutError) as error:
            raise NodeUnavailable(f"Estado de replicação indisponível: node={peer_id}") from error

    async def recovery_state(self, peer_id: int) -> RecoveryState:
        try:
            async with asyncio.timeout(self.settings.request_timeout):
                response = await self.client.get(
                    f"{self.settings.peers[peer_id]}/internal/recovery/state",
                    timeout=self.settings.request_timeout,
                )
                response.raise_for_status()
                state = RecoveryState.model_validate(response.json())
                if state.node_id != peer_id:
                    raise ValueError("Log recebido de outro participante")
                return state
        except (httpx.HTTPError, ValueError, TimeoutError) as error:
            raise NodeUnavailable(f"Log de recuperação indisponível: node={peer_id}") from error

    async def send_configuration(self, peer_id: int, message: ConfigurationMessage,
                                 phase: str) -> ConfigurationAck:
        try:
            async with asyncio.timeout(self.settings.request_timeout):
                response = await self.client.post(
                    f"{self.settings.peers[peer_id]}/internal/recovery/{phase}",
                    json=message.model_dump(mode="json"), timeout=self.settings.request_timeout,
                )
                response.raise_for_status()
                ack = ConfigurationAck.model_validate(response.json())
                expected = "SYNCHRONIZED" if phase == "synchronize" else "CONFIRMED"
                if (ack.node_id != peer_id or ack.configuration != message.configuration
                        or ack.log_digest != message.log_digest or ack.tasks_digest != message.tasks_digest
                        or ack.status != expected):
                    raise ValueError("ACK não corresponde à configuração enviada")
                return ack
        except (httpx.HTTPError, ValueError, TimeoutError) as error:
            raise NodeUnavailable(f"Configuração sem ACK válido: node={peer_id} phase={phase}") from error

    async def _replicate(self, peer_id: int, phase: str, operation: Operation) -> ReplicationAck:
        
        if phase == "prepare":
            message = PrepareMessage(sender_id=self.settings.node_id, operation=operation)
        else:
            message = CommitMessage(sender_id=self.settings.node_id, request_id=operation.request_id, epoch=operation.epoch, sequence_number=operation.sequence_number)
        try:
            async with asyncio.timeout(self.settings.request_timeout):
                response = await self.client.post(f"{self.settings.peers[peer_id]}/internal/replication/{phase}", json=message.model_dump(), timeout=self.settings.request_timeout)
                response.raise_for_status()
                ack = ReplicationAck.model_validate(response.json())
                if (ack.node_id != peer_id or ack.request_id != operation.request_id or ack.epoch != operation.epoch or ack.sequence_number != operation.sequence_number or (phase == "commit" and ack.status != "COMMITTED")):
                    raise ValueError("ACK não corresponde à operação enviada")
                return ack
  
        except (httpx.HTTPError, ValueError, TimeoutError) as error:
            raise NodeUnavailable(f"{phase.upper()} sem ACK válido: node={peer_id}") from error

    async def send_prepare(self, peer_id: int, operation: Operation) -> ReplicationAck:
        return await self._replicate(peer_id, "prepare", operation)

    async def send_commit(self, peer_id: int, operation: Operation) -> ReplicationAck:
        return await self._replicate(peer_id, "commit", operation)
