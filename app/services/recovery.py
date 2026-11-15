"""Recuperação e mudança de configuração para crash-stop sem partições."""
import asyncio
import logging
from collections.abc import Callable
from pathlib import Path

from app.communication.client import NodeClient, NodeUnavailable
from app.config import Settings
from app.errors import ProtocolError
from app.models import ConfigurationAck, ConfigurationMessage, Operation, OperationRecord, RecoveryState, ReplicationConfiguration, SynchronizeMessage
from app.state import NodeState
from app.storage.database import Database

logger = logging.getLogger(__name__)

def recover_operations(snapshots: list[RecoveryState]) -> list[OperationRecord]:
    """Preserva COMMITTED em qualquer sobrevivente; descarta somente PREPARED."""
    records: dict[str, OperationRecord] = {}
    positions: dict[tuple[int, int], str] = {}
    for snapshot in snapshots:
        for record in snapshot.operations:
            previous = records.get(record.request_id)
            position = (record.epoch, record.sequence_number)
            
            if (previous is not None and previous.model_dump(exclude={"status"}) != record.model_dump(exclude={"status"})):
                raise ProtocolError(409, "Logs conflitantes para o mesmo request_id")
            
            if position in positions and positions[position] != record.request_id:
                raise ProtocolError(409, "Logs conflitantes na mesma posição")
            
            positions[position] = record.request_id
           
            if previous is None or record.status == "COMMITTED":
                records[record.request_id] = record
                
    return sorted((record for record in records.values() if record.status == "COMMITTED"), key=lambda record: (record.epoch, record.sequence_number))

class Recovery:
    def __init__(self, settings: Settings, state: NodeState, client: NodeClient,
                 database: Database, suspected_nodes: Callable[[], set[int]]):
        self.settings = settings
        self.state = state
        self.client = client
        self.database = database
        self.suspected_nodes = suspected_nodes

    @property
    def members(self) -> set[int]:
        return set(self.state.configuration.members if self.state.configuration else self.settings.peers)

    def needed(self) -> bool:
        return self.state.role == "LEADER_ELECTED" or (self.state.role == "PRIMARY" and bool(self.members & self.suspected_nodes()))

    def _require_candidate(self, configuration: ReplicationConfiguration) -> None:
        if (self.state.role != "LEADER_ELECTED" or self.state.leader_id != configuration.leader or self.state.epoch != configuration.epoch or set(configuration.members) & self.suspected_nodes()):
            raise ProtocolError(409, "Liderança ou membros mudaram durante a recuperação")

    async def _all(self, calls):
        # Aguarda o fim de todos os envios antes de iniciar outra tentativa.
        answers = await asyncio.gather(*calls, return_exceptions=True)
        for answer in answers:
            if isinstance(answer, BaseException):
                raise answer
        return answers

    async def recover(self) -> None:
        """Executado sob o mesmo lock das escritas e mensagens de replicação."""
        if not self.needed():
            return
        if self.state.role == "PRIMARY":
            # Falha de backup muda a configuração sem executar outra eleição Bully.
            self.state.recognize_leader(self.state.node_id, self.state.epoch + 1)
        
        configuration = ReplicationConfiguration(epoch=self.state.epoch, leader=self.state.node_id, members=tuple(sorted(self.members - self.suspected_nodes())))
        backups = [peer for peer in configuration.members if peer != self.state.node_id]
        
        try:
            # Cerca a liderança anterior também na reconfiguração de backups.
            await self._all(self.client.send_coordinator(peer, configuration.epoch, "FENCE") for peer in backups)
            self._require_candidate(configuration)
            snapshots = await self._all(self.client.recovery_state(peer) for peer in backups)
            
            for snapshot in snapshots:
                if (snapshot.leader_id != configuration.leader or snapshot.epoch != configuration.epoch or snapshot.role != "BACKUP"):
                    raise ProtocolError(409, "Sobrevivente ainda não reconhece o candidato")
           
            local = RecoveryState(
                **self.state.snapshot(), log_digest=self.database.digests()[0],
                tasks_digest=self.database.digests()[1], has_prepared=self.database.has_prepared(),
                configuration=self.state.configuration, synchronized=self.state.synchronized,
                operations=self.database.list_operations()
            )
          
            operations = recover_operations([local, *snapshots])
           
            # Reconstitui tarefas em ordem e valida a compatibilidade dos logs.
            replica = Database(Path(":memory:"))
            try:
                for record in operations:
                    replica.prepare(Operation.model_validate(record.model_dump(exclude={"status"})))
                    replica.commit(record.request_id, record.epoch, record.sequence_number)
                log_digest, tasks_digest = replica.digests()
            finally:
                replica.close()
           
            self._require_candidate(configuration)
            message = SynchronizeMessage(sender_id=self.state.node_id, configuration=configuration, log_digest=log_digest, tasks_digest=tasks_digest, operations=operations)
            await self._all(self.client.send_configuration(peer, message, "synchronize") for peer in backups)
            self._require_candidate(configuration)
            self.database.install_snapshot(operations, configuration, log_digest, tasks_digest)
            self.state.configuration = configuration
            confirmation = ConfigurationMessage(**message.model_dump(exclude={"operations"}))
            
            await self._all(self.client.send_configuration(peer, confirmation, "configure") for peer in backups)
            self._require_candidate(configuration)
            self.database.confirm_configuration(configuration)
            self.state.synchronized = True
            self.state.role = "PRIMARY"
           
            self.client.metrics.event("PRIMARY", epoch=configuration.epoch, members=list(configuration.members), recovered=len(operations))
            logger.info("PRIMARY node=%s epoch=%s members=%s recovered=%s", self.state.node_id, configuration.epoch, configuration.members, len(operations))
        except (NodeUnavailable, ProtocolError) as error:
            logger.debug("Recuperação pendente node=%s: %s", self.state.node_id, error)

    def _require_configuration(self, message: ConfigurationMessage) -> None:
        configuration = message.configuration
        
        erros = self.state.role != "BACKUP" or self.state.leader_id != message.sender_id \
                or configuration.leader != message.sender_id or configuration.epoch != self.state.epoch \
                or self.state.node_id not in configuration.members \
                or not set(configuration.members) <= self.members
        
        if (erros):
            raise ProtocolError(409, "Configuração incompatível com a liderança ou os membros reconhecidos")
        if (self.state.configuration is not None and self.state.configuration.epoch == configuration.epoch
                and self.state.configuration != configuration):
            raise ProtocolError(409, "Outra configuração já instalada neste epoch")

    def synchronize(self, message: SynchronizeMessage) -> ConfigurationAck:
        self._require_configuration(message)
        
        if (self.state.synchronized and self.state.configuration == message.configuration and self.database.digests() == (message.log_digest, message.tasks_digest)):
            return self._ack(message, "SYNCHRONIZED") 
        
        self.database.install_snapshot(message.operations, message.configuration, message.log_digest, message.tasks_digest)
        self.state.configuration = message.configuration
        self.state.synchronized = False
        return self._ack(message, "SYNCHRONIZED")

    def configure(self, message: ConfigurationMessage) -> ConfigurationAck:
        self._require_configuration(message)
        if (self.state.configuration != message.configuration or self.database.digests() != (message.log_digest, message.tasks_digest)):
            raise ProtocolError(409, "Confirmação requer snapshot sincronizado com os mesmos hashes")
        self.database.confirm_configuration(message.configuration)
        self.state.synchronized = True
        return self._ack(message, "CONFIRMED")

    def _ack(self, message: ConfigurationMessage, status: str) -> ConfigurationAck:
        return ConfigurationAck(node_id=self.state.node_id, configuration=message.configuration, log_digest=message.log_digest, tasks_digest=message.tasks_digest, status=status)
