"""Mensagens HTTP dos protocolos Bully e Heartbeat."""
from typing import Literal
from pydantic import BaseModel, Field

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