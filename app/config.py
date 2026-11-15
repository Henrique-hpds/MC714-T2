"""Configuração de uma instância a partir das variáveis do Compose."""

import math
import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

@dataclass(frozen=True)
class Settings:
    node_id: int
    peers: dict[int, str]
    db_path: Path
    request_timeout: float = 1.0
    coordinator_timeout: float = 3.0

    def __post_init__(self) -> None:
        for name in ("request_timeout", "coordinator_timeout"):
            value = getattr(self, name)
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} deve ser positivo e finito")

    @classmethod
    def from_env(cls) -> "Settings":
        node_id = int(os.environ["NODE_ID"])
        peers: dict[int, str] = {}

        for entry in os.environ["PEERS"].split(","):
            raw_id, separator, raw_url = entry.strip().partition("=")
            if not separator:
                raise ValueError("PEERS deve usar o formato id=http://host:porta")

            peer_id = int(raw_id)
            url = raw_url.strip().rstrip("/")
            parsed = urlsplit(url)
            if peer_id <= 0 or peer_id in peers:
                raise ValueError("Os IDs em PEERS devem ser positivos e únicos")
            if (parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.path or parsed.query or parsed.fragment):
                raise ValueError(f"Endereço inválido em PEERS: {url}")
            peers[peer_id] = url

        if node_id not in peers:
            raise ValueError("NODE_ID deve estar presente em PEERS")
        if not os.environ["DB_PATH"].strip():
            raise ValueError("DB_PATH não pode ser vazio")

        return cls(
            node_id=node_id,
            peers=peers,
            db_path=Path(os.environ["DB_PATH"]),
            request_timeout=float(os.environ.get("REQUEST_TIMEOUT", "1.0")),
            coordinator_timeout=float(os.environ.get("COORDINATOR_TIMEOUT", "3.0"))
        )
