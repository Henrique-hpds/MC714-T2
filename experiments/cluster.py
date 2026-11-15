"""Cluster Docker Compose isolado com N nós, injeção de falhas e acesso HTTP (somente biblioteca padrão)."""
import http.client
import json
import os
import platform
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
IMAGE = "mc714-task-node:experiments"


def run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=True, capture_output=True, text=True, **kwargs)


def build_image() -> None:
    # Uma única imagem para todos os nós e ensaios (evita reconstruir a cada réplica).
    run(["docker", "build", "-q", "-t", IMAGE, str(ROOT)])


class Cluster:
    def __init__(self, n: int, env: dict, workdir: Path, project: str = "mc714-exp", base_port: int = 18000):
        self.n = n
        self.env = {key: str(value) for key, value in env.items()}
        self.project = project
        self.base_port = base_port
        self.compose_file = workdir / "compose.json"

    def url(self, node: int) -> str:
        return f"http://127.0.0.1:{self.base_port + node}"

    def container(self, node: int) -> str:
        return f"{self.project}-node{node}"

    def compose(self, *args: str) -> subprocess.CompletedProcess:
        return run(["docker", "compose", "-f", str(self.compose_file), "-p", self.project, *args])

    def write_compose(self) -> None:
        """Gera um compose equivalente ao do repositório, com N nós (JSON é YAML válido)."""
        peers = ",".join(f"{i}=http://node{i}:8000" for i in range(1, self.n + 1))
        healthcheck = {
            "test": ["CMD", "python", "-c",
                     "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=1).close()"],
            "interval": "1s", "timeout": "2s", "start_period": "5s", "retries": 30,
        }
        services = {
            f"node{i}": {
                "image": IMAGE,
                "container_name": self.container(i),
                "restart": "no",
                "environment": {"NODE_ID": str(i), "PEERS": peers, "DB_PATH": "/data/tasks.db", **self.env},
                "ports": [f"127.0.0.1:{self.base_port + i}:8000"],
                "healthcheck": healthcheck,
            }
            for i in range(1, self.n + 1)
        }
        self.compose_file.parent.mkdir(parents=True, exist_ok=True)
        self.compose_file.write_text(json.dumps({"name": self.project, "services": services}, indent=2))

    def up(self, timeout: int = 120) -> None:
        # Sem volumes: cada ensaio começa com bancos vazios e é descartado no down.
        self.compose("up", "-d", "--wait", "--wait-timeout", str(timeout))

    def down(self) -> None:
        # Nós pausados precisam ser retomados para que o down os encerre.
        subprocess.run(["docker", "compose", "-f", str(self.compose_file), "-p", self.project, "unpause"],
                       capture_output=True)
        subprocess.run(["docker", "compose", "-f", str(self.compose_file), "-p", self.project,
                        "down", "-v", "--remove-orphans", "-t", "1"], capture_output=True)

    def save_logs(self, path: Path) -> None:
        result = subprocess.run(["docker", "compose", "-f", str(self.compose_file), "-p", self.project,
                                 "logs", "--no-color", "-t"], capture_output=True, text=True)
        path.write_text(result.stdout + result.stderr)

    def fail(self, node: int, fault: str = "kill") -> tuple[float, float]:
        """Injeta a falha crash-stop no nó.

        kill:  SIGKILL; o contêiner sai da rede e as conexões a ele são recusadas na hora.
        pause: congela o processo (cgroup freezer); as conexões ficam sem resposta e os
               envios esperam o REQUEST_TIMEOUT, como numa máquina que parou de responder.
        """
        before = time.time()
        if fault == "pause":
            run(["docker", "pause", self.container(node)])
        else:
            run(["docker", "kill", "-s", "KILL", self.container(node)])
        return before, time.time()

    # ---- HTTP ----
    def get(self, node: int, path: str, timeout: float = 2.0):
        try:
            with urllib.request.urlopen(self.url(node) + path, timeout=timeout) as response:
                return json.loads(response.read())
        except (OSError, ValueError):
            return None

    def statuses(self, nodes) -> dict[int, dict | None]:
        return {node: self.get(node, "/status") for node in nodes}

    def metrics(self, nodes, since: float = 0.0) -> dict[int, dict | None]:
        return {node: self.get(node, f"/metrics?since={since}") for node in nodes}

    def converged(self, alive: list[int], leader: int) -> bool:
        """Todos os vivos reconhecem o mesmo líder/epoch; líder PRIMARY, membros = vivos, sincronizados."""
        statuses = self.statuses(alive)
        if any(status is None for status in statuses.values()):
            return False
        epochs = {status["epoch"] for status in statuses.values()}
        return (len(epochs) == 1
                and all(status["leader_id"] == leader and status["synchronized"]
                        and status["replication_members"] == sorted(alive)
                        and status["role"] == ("PRIMARY" if node == leader else "BACKUP")
                        for node, status in statuses.items()))

    def wait_converged(self, alive: list[int], leader: int, timeout: float, interval: float = 0.05) -> float | None:
        """Retorna o instante (time.time) da convergência observada ou None se exceder o prazo."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.converged(alive, leader):
                return time.time()
            time.sleep(interval)
        return None


class TaskClient:
    """Cliente sequencial com conexão HTTP persistente, para medir latência das escritas."""

    def __init__(self, cluster: Cluster, node: int, timeout: float = 10.0):
        self.host, self.port, self.timeout = "127.0.0.1", cluster.base_port + node, timeout
        self.connection = None

    def request(self, method: str, path: str, body: dict | None = None, key: str | None = None):
        headers = {"Content-Type": "application/json"}
        if key is not None:
            headers["Idempotency-Key"] = key
        payload = json.dumps(body) if body is not None else None
        started = time.perf_counter()
        status, data = 0, b""
        # 2 tentativas: o servidor fecha conexões ociosas (keep-alive de 5 s do uvicorn).
        # Repetir é seguro: escritas usam Idempotency-Key.
        for _ in range(2):
            try:
                if self.connection is None:
                    self.connection = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)
                self.connection.request(method, path, body=payload, headers=headers)
                response = self.connection.getresponse()
                data = response.read()
                status = response.status
                break
            except (OSError, http.client.HTTPException):
                self.connection = None
        latency = time.perf_counter() - started
        try:
            parsed = json.loads(data) if data else None
        except ValueError:
            parsed = None
        return status, latency, parsed

    def create(self, title: str, key: str):
        return self.request("POST", "/tasks", {"title": title}, key)


# ---- Registro do ambiente ----
def _output(cmd: list[str]) -> str | None:
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=20).stdout.strip() or None
    except (OSError, subprocess.SubprocessError):
        return None


def environment() -> dict:
    cpu = platform.processor() or None
    if Path("/proc/cpuinfo").exists():
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
    elif sys.platform == "darwin":
        cpu = _output(["sysctl", "-n", "machdep.cpu.brand_string"]) or cpu
    memory = None
    if hasattr(os, "sysconf") and "SC_PHYS_PAGES" in os.sysconf_names:
        memory = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    return {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "os": platform.platform(),
        "machine": platform.machine(),
        "cpu_model": cpu,
        "cpu_count": os.cpu_count(),
        "memory_bytes": memory,
        "python_host": sys.version.split()[0],
        "docker_server": _output(["docker", "version", "--format", "{{.Server.Version}}"]),
        "docker_server_os": _output(["docker", "info", "--format", "{{.OperatingSystem}} / {{.KernelVersion}} / CPUs={{.NCPU}} / Mem={{.MemTotal}}"]),
        "docker_compose": _output(["docker", "compose", "version", "--short"]),
        "image": IMAGE,
        "image_id": _output(["docker", "image", "inspect", "--format", "{{.Id}}", IMAGE]),
        "image_python": _output(["docker", "run", "--rm", IMAGE, "python", "--version"]),
        "image_packages": _output(["docker", "run", "--rm", IMAGE, "pip", "freeze"]),
        "git_commit": _output(["git", "-C", str(ROOT), "rev-parse", "HEAD"]),
        "git_dirty": bool(_output(["git", "-C", str(ROOT), "status", "--porcelain"])),
    }