"""Bully assíncrono para o modelo crash-stop, sem partições de rede."""
import asyncio
import logging
from contextlib import suppress

from app.communication.client import NodeClient
from app.config import Settings
from app.models import CoordinatorMessage, ElectionMessage, ElectionOK
from app.state import NodeState

logger = logging.getLogger(__name__)

class Bully:
    def __init__(self, settings: Settings, state: NodeState, client: NodeClient):
        self.settings = settings
        self.state = state
        self.client = client
        self._task: asyncio.Task | None = None
        self._changed = asyncio.Event()

    def _running(self) -> bool:
        return self._task is not None and not self._task.done()

    def _schedule(self) -> None:
        if not self._running():
            self._task = asyncio.create_task(self._run(), name=f"bully-{self.state.node_id}")

    def start_election(self) -> None:
        """Inicia uma rodada; chamadas simultâneas compartilham a mesma rotina."""
        
        if self._running() and self.state.leader_id is None:
            return
        
        self.state.begin_election(self.state.epoch + 1)
        self._changed.set()
        self._schedule()

    def receive_election(self, message: ElectionMessage) -> ElectionOK:
        
        # O endpoint só encaminha mensagens de participantes com ID menor.
        if message.epoch > self.state.epoch or not self._running():
            self.state.begin_election(max(self.state.epoch, message.epoch))
            self._changed.set()
            
        self._schedule()
        return ElectionOK(node_id=self.state.node_id, epoch=self.state.epoch)

    def receive_coordinator(self, message: CoordinatorMessage) -> bool:
        if message.epoch < self.state.epoch:
            return False
        # Um participante ativo com ID maior contesta o anúncio recebido.
        if message.leader_id < self.state.node_id:
            if message.epoch > self.state.epoch:
                self.state.begin_election(message.epoch)
                self._changed.set()
                self._schedule()
            else:
                self.start_election()
            return False
        
        if (message.epoch == self.state.epoch and self.state.leader_id is not None and message.leader_id < self.state.leader_id):
            return False
        
        self.state.recognize_leader(message.leader_id, message.epoch)
        self._changed.set()
        self.client.metrics.event("LEADER_RECOGNIZED", leader=message.leader_id, epoch=message.epoch)
        logger.info("COORDINATOR node=%s leader=%s epoch=%s", self.state.node_id, message.leader_id, message.epoch)
        
        return True

    async def _run(self) -> None:
        
        while True:
            epoch = self.state.epoch
            self._changed.clear()
            logger.info("ELECTION node=%s epoch=%s", self.state.node_id, epoch)
            self.client.metrics.event("ELECTION_ROUND", epoch=epoch)
            
            answers = await asyncio.gather(
                *(self.client.send_election(peer_id, epoch) for peer_id in self.settings.peers if peer_id > self.state.node_id)
            )
            
            observed_epoch = max([epoch] + [answer.epoch for answer in answers if answer is not None])
            
            if observed_epoch > self.state.epoch:
                self.state.begin_election(observed_epoch)
                
            # Um COORDINATOR pode chegar antes das respostas OK.
            if self.state.leader_id is not None:
                return
            if self.state.epoch != epoch:
                continue

            if not any(answer is not None for answer in answers):
                self.state.recognize_leader(self.state.node_id, epoch)
                logger.info("LEADER_ELECTED node=%s epoch=%s", self.state.node_id, epoch)
                self.client.metrics.event("LEADER_RECOGNIZED", leader=self.state.node_id, epoch=epoch)
                
                await asyncio.gather(
                    *(self.client.send_coordinator(peer_id, epoch) for peer_id in self.settings.peers if peer_id != self.state.node_id)
                )
                
                if self.state.epoch == epoch and self.state.leader_id == self.state.node_id:
                    return
                
                continue

            try:
                await asyncio.wait_for(self._changed.wait(), timeout=self.settings.coordinator_timeout)
            except TimeoutError:
                logger.info("Timeout aguardando COORDINATOR node=%s epoch=%s", self.state.node_id, epoch)
                self.state.begin_election(self.state.epoch + 1)
            
            if self.state.leader_id is not None:
                return

    async def close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
