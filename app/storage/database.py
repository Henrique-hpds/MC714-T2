"""Persistência transacional do log e das tarefas de um único nó."""

import hashlib
import json
import sqlite3
from pathlib import Path

from app.errors import ProtocolError
from app.models import Operation, OperationRecord, Task, TaskCreate, TaskUpdate


class Database:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('pending', 'in_progress', 'completed')),
                version INTEGER NOT NULL CHECK (version > 0)
            );
            CREATE TABLE IF NOT EXISTS operations (
                request_id TEXT PRIMARY KEY,
                epoch INTEGER NOT NULL CHECK (epoch > 0),
                sequence_number INTEGER NOT NULL CHECK (sequence_number > 0),
                operation_type TEXT NOT NULL CHECK (operation_type IN ('CREATE', 'UPDATE')),
                payload TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('PREPARED', 'COMMITTED')),
                UNIQUE (epoch, sequence_number)
            );
        """)

    def close(self) -> None:
        self.connection.close()

    def list_tasks(self) -> list[Task]:
        return [Task.model_validate(dict(row)) for row in self.connection.execute("SELECT * FROM tasks ORDER BY id")]

    def get_task(self, task_id: str) -> Task | None:
        row = self.connection.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
        return Task.model_validate(dict(row)) if row else None

    @staticmethod
    def _record(row: sqlite3.Row) -> OperationRecord:
        values = dict(row)
        values["payload"] = json.loads(values["payload"])
        return OperationRecord.model_validate(values)

    def get_operation(self, request_id: str) -> OperationRecord | None:
        row = self.connection.execute("SELECT * FROM operations WHERE request_id = ?", (request_id,)).fetchone()
        return self._record(row) if row else None

    def list_operations(self) -> list[OperationRecord]:
        return [self._record(row) for row in self.connection.execute(
            "SELECT * FROM operations ORDER BY epoch, sequence_number"
        )]

    def has_prepared(self) -> bool:
        return self.connection.execute("SELECT 1 FROM operations WHERE status = 'PREPARED' LIMIT 1").fetchone() is not None

    def max_epoch(self) -> int:
        return self.connection.execute("SELECT COALESCE(MAX(epoch), 0) FROM operations").fetchone()[0]

    def next_sequence(self, epoch: int) -> int:
        return self.connection.execute(
            "SELECT COALESCE(MAX(sequence_number), 0) + 1 FROM operations WHERE epoch = ?", (epoch,)
        ).fetchone()[0]

    def _validate_task(self, operation: Operation) -> None:
        task = operation.payload.task
        changes = operation.payload.request
        current = self.get_task(task.id)
        try:
            if operation.operation_type == "CREATE":
                command = TaskCreate.model_validate(changes)
                valid = current is None and task.version == 1 and changes == command.model_dump()
                expected = Task(id=task.id, version=1, **command.model_dump())
            else:
                command = TaskUpdate.model_validate(changes)
                valid = current is not None
                expected = current.model_copy(update={**command.model_dump(exclude_unset=True), "version": current.version + 1}) if current else None
        except ValueError as error:
            raise ProtocolError(409, "Payload da operação inválido") from error
        if not valid or task != expected:
            raise ProtocolError(409, "Operação incompatível com o estado ou a versão da tarefa")

    def prepare(self, operation: Operation) -> OperationRecord:
        with self.connection:
            existing = self.get_operation(operation.request_id)
            if existing is not None:
                if existing.model_dump(exclude={"status"}) != operation.model_dump():
                    raise ProtocolError(409, "request_id já usado por outra operação")
                return existing
            if self.has_prepared():
                raise ProtocolError(409, "Há uma operação pendente de confirmação")
            if operation.sequence_number != self.next_sequence(operation.epoch):
                raise ProtocolError(409, "sequence_number fora de ordem")
            self._validate_task(operation)
            self.connection.execute(
                "INSERT INTO operations VALUES (?, ?, ?, ?, ?, 'PREPARED')",
                (operation.request_id, operation.epoch, operation.sequence_number,
                 operation.operation_type, operation.payload.model_dump_json()),
            )
        return OperationRecord(**operation.model_dump(), status="PREPARED")

    def commit(self, request_id: str, epoch: int, sequence_number: int) -> OperationRecord:
        with self.connection:
            operation = self.get_operation(request_id)
            if operation is None or operation.epoch != epoch or operation.sequence_number != sequence_number:
                raise ProtocolError(409, "COMMIT sem PREPARE correspondente")
            if operation.status == "COMMITTED":
                return operation
            self._validate_task(operation)
            task = operation.payload.task
            self.connection.execute(
                "INSERT INTO tasks VALUES (?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET "
                "title=excluded.title, status=excluded.status, version=excluded.version",
                (task.id, task.title, task.status, task.version),
            )
            self.connection.execute("UPDATE operations SET status = 'COMMITTED' WHERE request_id = ?", (request_id,))
        return operation.model_copy(update={"status": "COMMITTED"})

    def digests(self) -> tuple[str, str]:
        def digest(values):
            return hashlib.sha256(json.dumps(values, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

        return (
            digest([operation.model_dump() for operation in self.list_operations()]),
            digest([task.model_dump() for task in self.list_tasks()]),
        )
