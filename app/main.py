"""API inicial para verificar a comunicação entre os nós."""

import asyncio
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

from app.config import Settings

class InternalMessage(BaseModel):
    sender_id: int = Field(strict=True, gt=0)
    message: str = Field(min_length=1, max_length=4096)

def create_app(settings: Settings | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.settings = settings if settings is not None else Settings.from_env()
        # Comunicação direta entre nós, sem proxies configurados no ambiente.
        async with httpx.AsyncClient(timeout=1.0, trust_env=False) as client:
            app.state.client = client
            yield

    app = FastAPI(title="Distributed Task Manager", lifespan=lifespan)

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
        responding = await asyncio.gather(
            *(check_peer(peer_id, url) for peer_id, url in config.peers.items() if peer_id != config.node_id)
        )
        
        return {
            "node_id": config.node_id,
            "role": "UNASSIGNED",
            "leader_id": None,
            "active_nodes": sorted([config.node_id] + [peer_id for peer_id in responding if peer_id is not None]),
            "epoch": 0
        }

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