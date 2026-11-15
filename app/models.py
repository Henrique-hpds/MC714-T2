"""Contratos HTTP de coordenação, tarefas e replicação."""
from typing import Annotated, Literal
from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

class ElectionMessage(BaseModel):
    sender_id: int = Field(strict=True, gt=0)
    epoch: int = Field(strict=True, ge=1)

class ElectionOK(BaseModel):
    type: Literal["OK"] = "OK"
    node_id: int = Field(strict=True, gt=0)
    epoch: int = Field(strict=True, ge=1)

class CoordinatorMessage(BaseModel):
    leader_id: int = Field(strict=True, gt=0)
    epoch: int = Field(strict=True, ge=1)

class HeartbeatMessage(BaseModel):
    sender_id: int = Field(strict=True, gt=0)

TaskStatus = Literal["pending", "in_progress", "completed"]
RequestID = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=128)]
TaskTitle = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=500)]

class TaskCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    title: TaskTitle
    status: TaskStatus = "pending"

class TaskUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    title: TaskTitle | None = None
    status: TaskStatus | None = None

    @model_validator(mode="after")
    def validate_changes(self):
        if not self.model_fields_set or any(getattr(self, field) is None for field in self.model_fields_set):
            raise ValueError("Informe title e/ou status, sem valores nulos")
        return self

class Task(TaskCreate):
    id: str = Field(min_length=1, max_length=128)
    version: int = Field(strict=True, ge=1)

class OperationPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    task: Task
    # Entrada original normalizada, para detectar reutilização indevida da chave.
    request: dict[str, str]

class Operation(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_id: RequestID
    epoch: int = Field(strict=True, ge=1)
    sequence_number: int = Field(strict=True, ge=1)
    operation_type: Literal["CREATE", "UPDATE"]
    payload: OperationPayload

class OperationRecord(Operation):
    status: Literal["PREPARED", "COMMITTED"]

class PrepareMessage(BaseModel):
    sender_id: int = Field(strict=True, gt=0)
    operation: Operation

class CommitMessage(BaseModel):
    sender_id: int = Field(strict=True, gt=0)
    epoch: int = Field(strict=True, ge=1)
    request_id: RequestID
    sequence_number: int = Field(strict=True, ge=1)

class ReplicationAck(BaseModel):
    node_id: int = Field(strict=True, gt=0)
    request_id: RequestID
    epoch: int = Field(strict=True, ge=1)
    sequence_number: int = Field(strict=True, ge=1)
    status: Literal["PREPARED", "COMMITTED"]

class ReplicationConfiguration(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    epoch: int = Field(strict=True, ge=1)
    leader: int = Field(strict=True, gt=0)
    members: tuple[Annotated[int, Field(strict=True, gt=0)], ...]

    @model_validator(mode="after")
    def validate_members(self):
        if self.leader not in self.members or tuple(sorted(set(self.members))) != self.members:
            raise ValueError("members deve ser ordenado, sem duplicatas e incluir o líder")
        return self

class ReplicationState(BaseModel):
    node_id: int
    leader_id: int | None
    epoch: int
    role: str
    log_digest: str
    tasks_digest: str
    has_prepared: bool
    configuration: ReplicationConfiguration | None
    synchronized: bool

class RecoveryState(ReplicationState):
    operations: list[OperationRecord]

class ConfigurationMessage(BaseModel):
    sender_id: int = Field(strict=True, gt=0)
    configuration: ReplicationConfiguration
    log_digest: str
    tasks_digest: str

class SynchronizeMessage(ConfigurationMessage):
    operations: list[OperationRecord]

class ConfigurationAck(BaseModel):
    node_id: int = Field(strict=True, gt=0)
    configuration: ReplicationConfiguration
    log_digest: str
    tasks_digest: str
    status: Literal["SYNCHRONIZED", "CONFIRMED"]
