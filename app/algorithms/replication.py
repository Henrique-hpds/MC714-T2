"""Primary-Backup síncrono com recuperação e confirmação de todos os membros."""
import asyncio
import logging
from collections.abc import Callable
from contextlib import suppress
from uuid import uuid4

from app.communication.client import NodeClient, NodeUnavailable
from app.config import Settings
from app.errors import ProtocolError
from app.models import (
    CommitMessage, ConfigurationAck, ConfigurationMessage, Operation, OperationPayload, OperationRecord,
    PrepareMessage, RecoveryState, ReplicationAck, ReplicationState, SynchronizeMessage,
    Task, TaskCreate, TaskUpdate,
)
from app.services.recovery import Recovery
from app.state import NodeState
from app.storage.database import Database

logger = logging.getLogger(__name__)

class PrimaryBackup:
    def __init__(self, settings: Settings, state: NodeState, client: NodeClient, database: Database,
                 suspected_nodes: Callable[[], set[int]]):
        self.settings = settings
        self.state = state
        self.client = client
        self.database = database
        
        self.recovery = Recovery(settings, state, client, database, suspected_nodes)
        self._lock = asyncio.Lock()
        self._task: asyncio.Task | None = None

    @property
    def backups(self) -> list[int]:
        return sorted(self.recovery.members - {self.state.node_id})

    def start(self) -> None:
        self._task = asyncio.create_task(self._activate_when_ready(), name=f"replication-{self.state.node_id}")

    async def close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task

    def snapshot(self) -> ReplicationState:
        log_digest, tasks_digest = self.database.digests()
        return ReplicationState(
            **self.state.snapshot(), log_digest=log_digest, tasks_digest=tasks_digest,
            has_prepared=self.database.has_prepared(), configuration=self.state.configuration,
            synchronized=self.state.synchronized,
        )

    def recovery_snapshot(self) -> RecoveryState:
        return RecoveryState(**self.snapshot().model_dump(), operations=self.database.list_operations())

    async def receive_synchronize(self, message: SynchronizeMessage) -> ConfigurationAck:
        async with self._lock:
            return self.recovery.synchronize(message)

    async def receive_configuration(self, message: ConfigurationMessage) -> ConfigurationAck:
        async with self._lock:
            return self.recovery.configure(message)

    async def _activate_when_ready(self) -> None:
        while True:
            if self.recovery.needed():
                await self.activate()
            await asyncio.sleep(min(0.1, self.settings.heartbeat_interval))

    async def activate(self) -> None:
        async with self._lock:
            await self.recovery.recover()

    def _require_primary(self, epoch: int | None = None) -> None:
        if self.state.leader_id is not None and self.state.leader_id != self.state.node_id:
            raise ProtocolError(409, "Acesse o líder indicado para ler ou escrever tarefas")
        if (self.state.role != "PRIMARY" or not self.state.synchronized
                or self.state.configuration is None or self.state.configuration.epoch != self.state.epoch
                or (epoch is not None and self.state.epoch != epoch)):
            raise ProtocolError(503, "Primário indisponível; eleição ou sincronização pendente")

    async def list_tasks(self) -> list[Task]:
        self._require_primary()
        async with self._lock:
            self._require_primary()
            if self.database.has_prepared():
                raise ProtocolError(503, "Resultado de escrita pendente; repita a requisição com a mesma Idempotency-Key")
            return self.database.list_tasks()

    async def get_operation(self, request_id: str) -> OperationRecord:
        self._require_primary()
        async with self._lock:
            self._require_primary()
            operation = self.database.get_operation(request_id)
            if operation is None:
                raise ProtocolError(404, "Operação não encontrada; a requisição pode ser repetida com a mesma chave")
            return operation

    def _require_leader(self, sender_id: int, epoch: int) -> None:
        configuration = self.state.configuration
        if (configuration is None or not self.state.synchronized
                or sender_id not in configuration.members or self.state.node_id not in configuration.members
                or configuration.leader != sender_id or configuration.epoch != epoch
                or sender_id == self.state.node_id or self.state.role != "BACKUP"
                or self.state.leader_id != sender_id or epoch != self.state.epoch):
            raise ProtocolError(409, "Replicação rejeitada: líder ou epoch não corresponde à liderança reconhecida")

    async def receive_prepare(self, message: PrepareMessage) -> ReplicationAck:
        async with self._lock:
            operation = message.operation
            self._require_leader(message.sender_id, operation.epoch)
            record = self.database.prepare(operation)
            return self._ack(record, record.status)

    async def receive_commit(self, message: CommitMessage) -> ReplicationAck:
        async with self._lock:
            self._require_leader(message.sender_id, message.epoch)
            record = self.database.commit(message.request_id, message.epoch, message.sequence_number)
            return self._ack(record, "COMMITTED")

    def _ack(self, operation: Operation, status: str) -> ReplicationAck:
        return ReplicationAck(node_id=self.state.node_id, request_id=operation.request_id, epoch=operation.epoch, sequence_number=operation.sequence_number, status=status)

    async def _replicate(self, operation: Operation, phase: str) -> None:
        send = self.client.send_prepare if phase == "PREPARE" else self.client.send_commit
        answers = await asyncio.gather(*(send(peer, operation) for peer in self.backups), return_exceptions=True)
        
        for answer in answers:
            if isinstance(answer, NodeUnavailable):
                raise ProtocolError(503, f"{phase} incompleto; resultado desconhecido. Repita a mesma Idempotency-Key")
            if isinstance(answer, BaseException):
                raise answer

        self._require_primary(operation.epoch)

    async def write(self, command: TaskCreate | TaskUpdate, task_id: str | None, request_id: str | None) -> tuple[Task, str]:
        request_id = request_id or str(uuid4())
        
        try:
            self._require_primary()
            async with self._lock:
                self._require_primary()
                operation_type = "CREATE" if task_id is None else "UPDATE"
                changes = command.model_dump(exclude_unset=isinstance(command, TaskUpdate))
                existing = self.database.get_operation(request_id)
                if existing is not None:
                   
                    if (existing.operation_type != operation_type or existing.payload.request != changes or (task_id is not None and existing.payload.task.id != task_id)):
                        raise ProtocolError(409, "Idempotency-Key já usada com outra requisição")
                    if existing.status == "COMMITTED":
                        return existing.payload.task, request_id
                    if existing.epoch != self.state.epoch:
                        raise ProtocolError(503, "Operação pendente de outra liderança requer recuperação")
                   
                    operation = Operation.model_validate(existing.model_dump(exclude={"status"}))
                
                else:
                    
                    if self.database.has_prepared():
                        raise ProtocolError(503, "Conclua a operação pendente repetindo sua Idempotency-Key")
                    if task_id is None:
                        task = Task(id=f"task-{uuid4().hex}", version=1, **changes)
                    else:
                        current = self.database.get_task(task_id)
                        if current is None:
                            raise ProtocolError(404, "Tarefa não encontrada")
                        
                        task = current.model_copy(update={**changes, "version": current.version + 1})
                    
                    operation = Operation(request_id=request_id, epoch=self.state.epoch, sequence_number=self.database.next_sequence(self.state.epoch), operation_type=operation_type, payload=OperationPayload(task=task, request=changes))
                    
                    self.database.prepare(operation)
                    
                logger.info("PREPARE node=%s epoch=%s sequence=%s request_id=%s", self.state.node_id, operation.epoch, operation.sequence_number, request_id)
                await self._replicate(operation, "PREPARE")
                await self._replicate(operation, "COMMIT")
                self.database.commit(request_id, operation.epoch, operation.sequence_number)
                logger.info("COMMIT node=%s epoch=%s sequence=%s request_id=%s", self.state.node_id, operation.epoch, operation.sequence_number, request_id)
                return operation.payload.task, request_id
            
        except ProtocolError as error:
            error.request_id = request_id
            raise
