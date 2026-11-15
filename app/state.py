"""Estado local; a replicação ativa PRIMARY depois de validar todos os membros."""
from dataclasses import dataclass

from app.models import ReplicationConfiguration

@dataclass
class NodeState:
    node_id: int
    role: str = "UNASSIGNED"
    leader_id: int | None = None
    epoch: int = 0
    configuration: ReplicationConfiguration | None = None
    synchronized: bool = False

    def begin_election(self, epoch: int) -> None:
        self.epoch = epoch
        self.leader_id = None
        self.role = "ELECTING"
        self.synchronized = False

    def recognize_leader(self, leader_id: int, epoch: int) -> None:
        if self.leader_id == leader_id and self.epoch == epoch:
            return
        self.epoch = epoch
        self.leader_id = leader_id
        self.role = "LEADER_ELECTED" if leader_id == self.node_id else "BACKUP"
        self.synchronized = False

    def snapshot(self) -> dict:
        return {
            "node_id": self.node_id,
            "role": self.role,
            "leader_id": self.leader_id,
            "epoch": self.epoch
        }
