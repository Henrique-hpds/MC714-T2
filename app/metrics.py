"""Contadores de mensagens e eventos para os experimentos; não influenciam os algoritmos."""
import time
from collections import Counter


class Metrics:
    def __init__(self, node_id: int):
        self.node_id = node_id
        self.sent: Counter[str] = Counter()    # requisições enviadas, por tipo
        self.acked: Counter[str] = Counter()   # respostas válidas recebidas (OK, ACK, ...)
        self.failed: Counter[str] = Counter()  # envios sem resposta válida no prazo
        self.events: list[dict] = []

    def event(self, name: str, **data) -> None:
        # Relógio de parede: os contêineres compartilham o relógio do kernel do host.
        self.events.append({"t": time.time(), "event": name, **data})

    def snapshot(self, since: float = 0.0) -> dict:
        return {
            "node_id": self.node_id,
            "time": time.time(),
            "sent": dict(self.sent),
            "acked": dict(self.acked),
            "failed": dict(self.failed),
            "events": [event for event in self.events if event["t"] >= since],
        }
