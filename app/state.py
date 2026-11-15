"""Estado local de coordenação; a eleição ainda não ativa o primário."""
from dataclasses import dataclass

@dataclass
class NodeState:
    node_id: int
    role: str = "UNASSIGNED"
    leader_id: int | None = None
    epoch: int = 0

    def begin_election(self, epoch: int) -> None:
        self.epoch = epoch
        self.leader_id = None
        self.role = "ELECTING"

    def recognize_leader(self, leader_id: int, epoch: int) -> None:
        self.epoch = epoch
        self.leader_id = leader_id
        self.role = "LEADER_ELECTED" if leader_id == self.node_id else "BACKUP"

    def snapshot(self) -> dict:
        return {
            "node_id": self.node_id,
            "role": self.role,
            "leader_id": self.leader_id,
            "epoch": self.epoch
        }
