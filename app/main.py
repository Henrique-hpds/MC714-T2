"""API de tarefas, coordenação e replicação entre os nós."""

import asyncio
from contextlib import asynccontextmanager
from typing import Annotated

import httpx
from fastapi import FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.config import Settings
from app.algorithms.bully import Bully
from app.algorithms.heartbeat import Heartbeat
from app.algorithms.replication import PrimaryBackup
from app.communication.client import NodeClient
from app.errors import ProtocolError
from app.models import (
    CommitMessage, CoordinatorMessage, ElectionMessage, HeartbeatMessage,
    PrepareMessage, ReplicationAck, ReplicationState, RequestID, Task, TaskCreate, TaskUpdate,
)
from app.state import NodeState
from app.storage.database import Database

class InternalMessage(BaseModel):
    sender_id: int = Field(strict=True, gt=0)
    message: str = Field(min_length=1, max_length=4096)

def create_app(settings: Settings | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.settings = settings if settings is not None else Settings.from_env()
        # Comunicação direta entre nós, sem proxies configurados no ambiente.
        config = app.state.settings
        database = Database(config.db_path)
        app.state.database = database
        try:
            async with httpx.AsyncClient(timeout=config.request_timeout, trust_env=False) as client:
                app.state.client = client
                app.state.node = NodeState(config.node_id, epoch=database.max_epoch())
                node_client = NodeClient(config, client)
                app.state.bully = Bully(config, app.state.node, node_client)
                app.state.heartbeat = Heartbeat(
                    config, app.state.node, node_client, app.state.bully.start_election
                )
                app.state.replication = PrimaryBackup(config, app.state.node, node_client, database)
                app.state.bully.start_election()
                app.state.heartbeat.start()
                app.state.replication.start()
                try:
                    yield
                finally:
                    await app.state.replication.close()
                    await app.state.heartbeat.close()
                    await app.state.bully.close()
        finally:
            database.close()

    app = FastAPI(title="Distributed Task Manager", lifespan=lifespan)

    @app.exception_handler(ProtocolError)
    async def protocol_error(request: Request, error: ProtocolError):
        node = request.app.state.node
        body = {
            "detail": error.detail,
            "leader_id": node.leader_id,
            "leader_url": request.app.state.settings.peers.get(node.leader_id),
        }
        headers = {}
        if error.request_id is not None:
            body["request_id"] = error.request_id
            headers["Idempotency-Key"] = error.request_id
        return JSONResponse(status_code=error.status_code, content=body, headers=headers)

    @app.post("/tasks", response_model=Task, status_code=201)
    async def create_task(command: TaskCreate, request: Request, response: Response, request_id: Annotated[RequestID | None, Header(alias="Idempotency-Key")] = None):
        task, key = await request.app.state.replication.write(command, None, request_id)
        response.headers["Idempotency-Key"] = key
        return task

    @app.get("/tasks", response_model=list[Task])
    async def list_tasks(request: Request):
        return await request.app.state.replication.list_tasks()

    @app.put("/tasks/{task_id}", response_model=Task)
    async def update_task(task_id: str, command: TaskUpdate, request: Request, response: Response, request_id: Annotated[RequestID | None, Header(alias="Idempotency-Key")] = None):
        task, key = await request.app.state.replication.write(command, task_id, request_id)
        response.headers["Idempotency-Key"] = key
        return task

    @app.get("/internal/replication/state", response_model=ReplicationState)
    async def replication_state(request: Request):
        return request.app.state.replication.snapshot()

    @app.post("/internal/replication/prepare", response_model=ReplicationAck)
    async def receive_prepare(message: PrepareMessage, request: Request):
        return await request.app.state.replication.receive_prepare(message)

    @app.post("/internal/replication/commit", response_model=ReplicationAck)
    async def receive_commit(message: CommitMessage, request: Request):
        return await request.app.state.replication.receive_commit(message)

    @app.get("/health")
    async def health(request: Request):
        return {"status": "ok", "node_id": request.app.state.settings.node_id}

    @app.get("/status")
    async def status(request: Request):
        config = request.app.state.settings
        client = request.app.state.client

        async def check_peer(peer_id: int, url: str) -> int | None:
            try:
                response = await client.get(f"{url}/health")
                response.raise_for_status()
                body = response.json()
                if (isinstance(body, dict) and body.get("status") == "ok" and body.get("node_id") == peer_id):
                    return peer_id
            except (httpx.HTTPError, ValueError):
                pass
            return None

        # Um nó indisponível não bloqueia as consultas aos demais participantes.
        responding = await asyncio.gather(*(check_peer(peer_id, url) for peer_id, url in config.peers.items() if peer_id != config.node_id))
        
        return {
            **request.app.state.node.snapshot(),
            "active_nodes": sorted([config.node_id] + [peer_id for peer_id in responding if peer_id is not None]),
            "suspected_nodes": sorted(request.app.state.heartbeat.suspected_nodes),
            "replication_members": sorted(config.peers)
        }

    @app.post("/internal/heartbeat")
    async def receive_heartbeat(message: HeartbeatMessage, request: Request):
        config = request.app.state.settings
        if message.sender_id not in config.peers or message.sender_id == config.node_id:
            raise HTTPException(status_code=400, detail="Heartbeat deve vir de outro participante de PEERS")
        request.app.state.heartbeat.receive(message.sender_id)
        return {"status": "received", "node_id": config.node_id}

    @app.post("/internal/election/start", status_code=202)
    async def start_election(request: Request):
        request.app.state.bully.start_election()
        return request.app.state.node.snapshot()

    @app.post("/internal/election")
    async def receive_election(message: ElectionMessage, request: Request):
        config = request.app.state.settings
        
        if message.sender_id not in config.peers or message.sender_id >= config.node_id:
            raise HTTPException(status_code=400, detail="ELECTION deve vir de participante com ID menor")
        
        return request.app.state.bully.receive_election(message)

    @app.post("/internal/coordinator")
    async def receive_coordinator(message: CoordinatorMessage, request: Request):
        config = request.app.state.settings
        
        if message.leader_id not in config.peers or message.leader_id == config.node_id:
            raise HTTPException(status_code=400, detail="Coordenador deve ser outro participante de PEERS")
        
        accepted = request.app.state.bully.receive_coordinator(message)
        return {"accepted": accepted, **request.app.state.node.snapshot()}

    @app.post("/internal/message")
    async def receive_message(message: InternalMessage, request: Request):
        config = request.app.state.settings
        
        if message.sender_id not in config.peers:
            raise HTTPException(status_code=400, detail="Remetente não pertence a PEERS")
        
        return {
            "status": "received",
            "node_id": config.node_id,
            "sender_id": message.sender_id,
            "message": message.message
        }

    return app

app = create_app()
