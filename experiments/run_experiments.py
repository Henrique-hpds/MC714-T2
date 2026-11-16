"""Malha de experimentos do Trabalho 2: Bully, Primary-Backup e Heartbeat.

Cada réplica sobe um cluster Docker Compose novo (bancos vazios), executa o cenário,
salva métricas brutas em <out>/<exp>/<param>=<nível>_seed<seed>/ e derruba o cluster.
Réplicas com run.json existente são puladas (re-executar só completa o que falta).

Uso: python experiments/run_experiments.py --exp all
"""
import csv
import json
import math
import random
import threading
import time
import traceback
from argparse import ArgumentParser
from pathlib import Path

from cluster import Cluster, TaskClient, build_image, environment

# Parâmetro variado e níveis de cada experimento (guia, seção 8)
EXPERIMENTS = {
    "bully": ("n_nodes", (3, 5, 7)),
    "pb": ("n_nodes", (3, 5, 7)),
    "hb": ("failure_timeout", (1.5, 2.5, 4.0)),
    "backup": ("n_nodes", (3, 5, 7)),
}
SEEDS = tuple(1000 * (i + 1) for i in range(10))
MESSAGE_TYPES = ("HEARTBEAT", "ELECTION", "COORDINATOR", "FENCE", "RECOVERY_STATE",
                 "SYNCHRONIZE", "CONFIGURE", "PREPARE", "COMMIT", "REPLICATION_STATE")


# ---------------------------------------------------------------- utilidades
def summary(values: list[float], prefix: str) -> dict:
    """Tendência, variabilidade e percentis (interpolação linear) de uma amostra."""
    if not values:
        return {f"{prefix}_{k}": None for k in ("n", "mean", "std", "min", "p50", "p95", "p99", "max")}
    ordered = sorted(values)

    def pct(p):
        rank = (len(ordered) - 1) * p / 100
        low, high = math.floor(rank), math.ceil(rank)
        return ordered[low] + (ordered[high] - ordered[low]) * (rank - low)

    mean = sum(ordered) / len(ordered)
    std = (sum((v - mean) ** 2 for v in ordered) / (len(ordered) - 1)) ** 0.5 if len(ordered) > 1 else 0.0
    return {f"{prefix}_n": len(ordered), f"{prefix}_mean": mean, f"{prefix}_std": std,
            f"{prefix}_min": ordered[0], f"{prefix}_p50": pct(50), f"{prefix}_p95": pct(95),
            f"{prefix}_p99": pct(99), f"{prefix}_max": ordered[-1]}


def message_delta(before: dict, after: dict, nodes) -> dict:
    """Mensagens no intervalo, somadas entre os nós: sent (requisições), acked (respostas válidas), failed."""
    return {
        f"{field}_{kind}": sum(after[n][field].get(kind, 0) - before[n][field].get(kind, 0) for n in nodes)
        for field in ("sent", "acked", "failed") for kind in MESSAGE_TYPES
    }


def find_events(snapshots: dict, name: str, since: float = 0.0, until: float = math.inf, **match) -> list[dict]:
    found = []
    for node, snapshot in snapshots.items():
        for event in snapshot["events"]:
            if (event["event"] == name and since <= event["t"] <= until
                    and all(event.get(k) == v for k, v in match.items())):
                found.append({**event, "node": node})
    return sorted(found, key=lambda event: event["t"])


def save_json(path: Path, value) -> None:
    path.write_text(json.dumps(value, indent=2))


def save_snapshots(run_dir: Path, **snapshots) -> None:
    """Guarda os /metrics brutos (contadores e eventos) para análises futuras."""
    for name, snapshot in snapshots.items():
        save_json(run_dir / f"metrics_{name}.json", snapshot)


def node_env(args, failure_timeout: float | None = None) -> dict:
    return {
        "HEARTBEAT_INTERVAL": args.heartbeat_interval,
        "FAILURE_TIMEOUT": failure_timeout if failure_timeout is not None else args.failure_timeout,
        "REQUEST_TIMEOUT": args.request_timeout,
        "COORDINATOR_TIMEOUT": args.coordinator_timeout,
    }


def start_cluster(n: int, env: dict, run_dir: Path, args) -> tuple[Cluster, float]:
    cluster = Cluster(n, env, run_dir, base_port=args.base_port)
    cluster.write_compose()
    cluster.down()
    started = time.time()
    try:
        cluster.up()
        converged = cluster.wait_converged(list(range(1, n + 1)), n, args.timeout)
        if converged is None:
            raise RuntimeError("cluster não convergiu na inicialização")
    except Exception:
        cluster.save_logs(run_dir / "docker.log")
        cluster.down()
        raise
    return cluster, converged - started


def write_tasks(client: TaskClient, count: int, prefix: str, rng: random.Random) -> list[dict]:
    """Escritas sequenciais; cada uma com Idempotency-Key única."""
    rows = []
    for i in range(count):
        title = f"{prefix}-{i}-" + "x" * rng.randint(1, 64)  # payload pseudoaleatório (semente registrada)
        sent_at = time.time()
        status, latency, _ = client.create(title, f"{prefix}-{i}")
        rows.append({"i": i, "sent_at": sent_at, "latency_s": latency, "status": status})
    return rows


def consistency(cluster: Cluster, nodes: list[int]) -> dict:
    states = {node: cluster.get(node, "/internal/replication/state") for node in nodes}
    digests = {(s["log_digest"], s["tasks_digest"]) for s in states.values() if s is not None}
    return {"ok_hashes_equal": None not in states.values() and len(digests) == 1,
            "ok_no_prepared": all(s is not None and not s["has_prepared"] for s in states.values())}


def fail_node(cluster: Cluster, node: int, leader: int, rng: random.Random, args) -> dict:
    """Falha do nó numa fase aleatória do ciclo de heartbeat; aguarda a nova configuração com o líder dado."""
    survivors = [i for i in range(1, cluster.n + 1) if i != node]
    time.sleep(rng.uniform(0, args.heartbeat_interval))
    kill_start, kill_end = cluster.fail(node, args.fault)
    converged = cluster.wait_converged(survivors, leader, args.timeout)
    # Referência da falha = início do comando; kill_cmd_s é a incerteza dessa marca.
    return {"survivors": survivors, "kill_start": kill_start, "kill_end": kill_end,
            "kill_cmd_s": kill_end - kill_start, "converged_at": converged}


def failover_metrics(snapshots: dict, failed: int, new_leader: int, kill: dict) -> dict:
    """Decompõe a troca de líder: detecção (Heartbeat) -> eleição (Bully) -> recuperação (Primary-Backup)."""
    t_kill = kill["kill_start"]
    survivors = kill["survivors"]
    statuses_epoch = None
    primary = find_events(snapshots, "PRIMARY", since=t_kill)
    primary = [event for event in primary if event["node"] == new_leader]
    if primary:
        statuses_epoch = primary[-1]["epoch"]

    detections, silences = [], []
    for node in survivors:
        events = find_events({node: snapshots[node]}, "SUSPECTED", since=t_kill, peer=failed)
        if events:
            detections.append(events[0]["t"] - t_kill)
            silences.append(events[0]["silence"])

    rounds = find_events(snapshots, "ELECTION_ROUND", since=t_kill)
    recognized = []
    for node in survivors:
        events = find_events({node: snapshots[node]}, "LEADER_RECOGNIZED", since=t_kill,
                             leader=new_leader, epoch=statuses_epoch)
        if events:
            recognized.append(events[0]["t"])
    election_start = rounds[0]["t"] if rounds else None
    election_end = max(recognized) if len(recognized) == len(survivors) else None
    primary_at = primary[-1]["t"] if primary else None

    def diff(a, b):
        return a - b if a is not None and b is not None else None

    return {
        "final_epoch": statuses_epoch,
        "detected_by": len(detections),
        "detection_first_s": min(detections) if detections else None,
        "detection_last_s": max(detections) if detections else None,
        "detection_samples": detections,          # um valor por sobrevivente (pooling no analyze)
        "silence_samples": silences,              # medido no próprio nó, desde o último heartbeat
        "election_rounds": len(rounds),
        "election_initiator": rounds[0]["node"] if rounds else None,
        "election_participants": sorted({event["node"] for event in rounds}),
        "election_time_s": diff(election_end, election_start),     # 1º ELECTION -> todos reconhecem o líder
        "recovery_time_s": diff(primary_at, election_end),         # líder reconhecido -> PRIMARY
        "failover_time_s": diff(primary_at, t_kill),               # falha -> PRIMARY (eventos)
        "failover_observed_s": diff(kill["converged_at"], t_kill), # falha -> /status convergido (polling)
    }


# ---------------------------------------------------------------- cenários
def scenario_bully(n: int, seed: int, run_dir: Path, args) -> dict:
    """Falha do líder com N nós: tempo de eleição e mensagens ELECTION/OK/COORDINATOR."""
    rng, kill_rng = random.Random(seed), random.Random(seed + 1)
    cluster, startup = start_cluster(n, node_env(args), run_dir, args)
    try:
        leader = TaskClient(cluster, n)
        pre = write_tasks(leader, args.pre_tasks, f"bully-{n}-{seed}-pre", rng)
        before = cluster.metrics(range(1, n + 1))
        kill = fail_node(cluster, n, n - 1, kill_rng, args)
        survivors = kill["survivors"]
        after = cluster.metrics(survivors)
        save_snapshots(run_dir, before=before, after=after)
        result = {"startup_time_s": startup, "converged": kill["converged_at"] is not None,
                  **{k: v for k, v in kill.items() if k != "survivors"},
                  **failover_metrics(after, n, n - 1, kill), **message_delta(before, after, survivors)}
        result["bully_messages"] = result["sent_ELECTION"] + result["acked_ELECTION"] + result["sent_COORDINATOR"]
        if result["converged"]:
            # Continuidade: tarefas confirmadas antes da falha + uma escrita nova no novo primário.
            post = write_tasks(TaskClient(cluster, n - 1), 1, f"bully-{n}-{seed}-post", rng)
            _, _, tasks = TaskClient(cluster, n - 1).request("GET", "/tasks")
            statuses = cluster.statuses(survivors)
            result.update(consistency(cluster, survivors))
            result["ok_single_leader"] = (
                sum(s["role"] == "PRIMARY" for s in statuses.values()) == 1
                and {s["leader_id"] for s in statuses.values()} == {n - 1})
            ok_writes = sum(r["status"] == 201 for r in pre + post)
            result["ok_tasks_preserved"] = isinstance(tasks, list) and len(tasks) == ok_writes == args.pre_tasks + 1
        result["ok"] = bool(result["converged"] and result.get("ok_single_leader")
                            and result.get("ok_tasks_preserved") and result.get("ok_hashes_equal"))
        return result
    finally:
        cluster.save_logs(run_dir / "docker.log")
        cluster.down()


def scenario_pb(n: int, seed: int, run_dir: Path, args) -> dict:
    """Carga sequencial de criações no primário: latência, vazão e mensagens por operação."""
    rng = random.Random(seed)
    cluster, startup = start_cluster(n, node_env(args), run_dir, args)
    try:
        client = TaskClient(cluster, n)
        warm = write_tasks(client, args.warmup_ops, f"pb-{n}-{seed}-warm", rng)  # descartadas
        before = cluster.metrics(range(1, n + 1))
        started = time.time()
        rows = write_tasks(client, args.ops, f"pb-{n}-{seed}", rng)
        duration = time.time() - started
        after = cluster.metrics(range(1, n + 1))
        save_snapshots(run_dir, before=before, after=after)
        with open(run_dir / "latencies.csv", "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)

        ok_rows = [r for r in rows if r["status"] == 201]
        messages = message_delta(before, after, range(1, n + 1))
        replication = sum(messages[f"{f}_{k}"] for f in ("sent", "acked") for k in ("PREPARE", "COMMIT"))
        _, _, tasks = client.request("GET", "/tasks")
        result = {
            "startup_time_s": startup, "duration_s": duration, "warmup_ops": args.warmup_ops,
            "ops": len(rows), "ok_ops": len(ok_rows), "errors": len(rows) - len(ok_rows),
            "throughput_ops_s": len(ok_rows) / duration,
            **summary([r["latency_s"] * 1000 for r in ok_rows], "latency_ms"),
            "replication_messages": replication,
            "messages_per_op": replication / len(ok_rows) if ok_rows else None,
            "expected_messages_per_op": 4 * (n - 1),  # PREPARE+ACK+COMMIT+ACK por backup
            "heartbeats_per_s": messages["sent_HEARTBEAT"] / duration,
            **messages, **consistency(cluster, list(range(1, n + 1))),
        }
        expected_tasks = sum(r["status"] == 201 for r in warm) + len(ok_rows)
        result["ok_task_count"] = isinstance(tasks, list) and len(tasks) == expected_tasks
        result["ok"] = bool(result["errors"] == 0 and result["ok_task_count"]
                            and result["ok_hashes_equal"] and result["ok_no_prepared"])
        return result
    finally:
        cluster.save_logs(run_dir / "docker.log")
        cluster.down()


def scenario_hb(failure_timeout: float, seed: int, run_dir: Path, args) -> dict:
    """Janela estável sob carga (falsas suspeitas) seguida da falha do líder (tempo de detecção)."""
    rng, kill_rng = random.Random(seed), random.Random(seed + 1)
    n = args.hb_nodes
    cluster, startup = start_cluster(n, node_env(args, failure_timeout), run_dir, args)
    try:
        # Carga normal de escrita durante a janela estável, em uma thread separada.
        stop, rows = threading.Event(), []

        def load():
            # Taxa fixa: carga contínua sem limite gera um log de milhares de operações, cujo
            # replay na recuperação excede o REQUEST_TIMEOUT (limitação documentada).
            client, i, next_at = TaskClient(cluster, n), 0, time.time()
            while not stop.is_set():
                rows.extend(write_tasks(client, 1, f"hb-{seed}-{i}", rng))
                i += 1
                next_at += 1 / args.load_rate
                stop.wait(max(0.0, next_at - time.time()))

        before = cluster.metrics(range(1, n + 1))
        window_start = time.time()
        worker = threading.Thread(target=load)
        worker.start()
        time.sleep(args.window)
        stop.set()
        worker.join()
        window_end = time.time()
        middle = cluster.metrics(range(1, n + 1))

        kill = fail_node(cluster, n, n - 1, kill_rng, args)
        after = cluster.metrics(kill["survivors"])
        save_snapshots(run_dir, before=before, window_end=middle, after=after)
        window = window_end - window_start
        pairs = n * (n - 1)  # cada nó monitora os demais
        stable_false = find_events(middle, "SUSPECTED", since=window_start, until=window_end)
        failover_false = [e for e in find_events(after, "SUSPECTED", since=kill["kill_start"]) if e["peer"] != n]
        messages = message_delta(before, middle, range(1, n + 1))
        result = {
            "startup_time_s": startup, "n_nodes": n, "window_s": window,
            "converged": kill["converged_at"] is not None,
            **{k: v for k, v in kill.items() if k != "survivors"},
            **failover_metrics(after, n, n - 1, kill),
            "false_suspicions_stable": len(stable_false),
            "false_suspicions_failover": len(failover_false),
            "false_suspicions_per_min": len(stable_false) / (window / 60),
            "false_suspicions_per_pair_hour": len(stable_false) / pairs / (window / 3600),
            "heartbeats_per_node_s": messages["sent_HEARTBEAT"] / window / n,
            "heartbeat_failed": messages["failed_HEARTBEAT"],
            "load_ops": len(rows), "load_errors": sum(r["status"] != 201 for r in rows),
            **summary([r["latency_s"] * 1000 for r in rows if r["status"] == 201], "load_latency_ms"),
            **messages,
        }
        # Segurança do detector: só suspeita após FAILURE_TIMEOUT sem heartbeat.
        result["ok_detection_bound"] = all(s > failure_timeout for s in result["silence_samples"])
        result["ok"] = bool(result["converged"] and result["detected_by"] == n - 1 and result["ok_detection_bound"])
        return result
    finally:
        cluster.save_logs(run_dir / "docker.log")
        cluster.down()


def scenario_backup(n: int, seed: int, run_dir: Path, args) -> dict:
    """Falha do backup de menor ID: o líder reconfigura (epoch + 1) sem nova eleição."""
    rng, kill_rng = random.Random(seed), random.Random(seed + 1)
    cluster, startup = start_cluster(n, node_env(args), run_dir, args)
    try:
        leader = TaskClient(cluster, n)
        pre = write_tasks(leader, args.pre_tasks, f"backup-{n}-{seed}-pre", rng)
        epoch_before = cluster.statuses([n])[n]["epoch"]
        before = cluster.metrics(range(1, n + 1))
        kill = fail_node(cluster, 1, n, kill_rng, args)
        survivors = kill["survivors"]
        after = cluster.metrics(survivors)
        save_snapshots(run_dir, before=before, after=after)
        t_kill = kill["kill_start"]

        detected = find_events({n: after[n]}, "SUSPECTED", since=t_kill, peer=1)
        primary = find_events({n: after[n]}, "PRIMARY", since=t_kill)
        detection = detected[0]["t"] if detected else None
        primary_at = primary[-1]["t"] if primary else None
        result = {
            "startup_time_s": startup, "converged": kill["converged_at"] is not None,
            **{k: v for k, v in kill.items() if k != "survivors"},
            "detection_leader_s": detection - t_kill if detection else None,           # falha -> líder suspeita
            "reconfiguration_time_s": primary_at - detection if detection and primary_at else None,  # suspeita -> PRIMARY
            "failover_time_s": primary_at - t_kill if primary_at else None,           # falha -> escritas liberadas
            "failover_observed_s": kill["converged_at"] - t_kill if kill["converged_at"] else None,
            "election_rounds": len(find_events(after, "ELECTION_ROUND", since=t_kill)),
            **message_delta(before, after, survivors),
        }
        result["reconfiguration_messages"] = sum(
            result[f"{f}_{k}"] for f in ("sent", "acked") for k in ("FENCE", "RECOVERY_STATE", "SYNCHRONIZE", "CONFIGURE"))
        if result["converged"]:
            post = write_tasks(TaskClient(cluster, n), 1, f"backup-{n}-{seed}-post", rng)
            _, _, tasks = leader.request("GET", "/tasks")
            statuses = cluster.statuses(survivors)
            result.update(consistency(cluster, survivors))
            result["epoch_after"] = statuses[n]["epoch"]
            result["ok_no_election"] = result["election_rounds"] == 0
            result["ok_leader_kept"] = all(s["leader_id"] == n for s in statuses.values())
            result["ok_epoch_incremented"] = result["epoch_after"] == epoch_before + 1
            ok_writes = sum(r["status"] == 201 for r in pre + post)
            result["ok_tasks_preserved"] = isinstance(tasks, list) and len(tasks) == ok_writes == args.pre_tasks + 1
        result["ok"] = bool(result["converged"] and result.get("ok_no_election") and result.get("ok_leader_kept")
                            and result.get("ok_epoch_incremented") and result.get("ok_tasks_preserved")
                            and result.get("ok_hashes_equal"))
        return result
    finally:
        cluster.save_logs(run_dir / "docker.log")
        cluster.down()


SCENARIOS = {"bully": scenario_bully, "pb": scenario_pb, "hb": scenario_hb, "backup": scenario_backup}


# ---------------------------------------------------------------- malha
def main(args):
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    names = list(EXPERIMENTS) if args.exp == "all" else [args.exp]
    seeds = SEEDS[:args.reps]

    if not args.no_build:
        print("Construindo a imagem...", flush=True)
        build_image()
    env_info = environment()
    (out / f"environment_{time.strftime('%Y%m%d_%H%M%S')}.json").write_text(json.dumps(env_info, indent=2))

    for name in names:
        param, levels = EXPERIMENTS[name]
        levels = [int(v) if param == "n_nodes" else float(v) for v in (args.levels or levels)]
        # Intercala os níveis dentro de cada semente para não confundir nível com deriva temporal.
        jobs = [(level, seed) for seed in seeds for level in levels]
        print(f"\n== {name}: {len(levels)} níveis x {len(seeds)} sementes = {len(jobs)} réplicas", flush=True)
        for i, (level, seed) in enumerate(jobs, 1):
            # Falhas por pause ficam em diretório próprio (ex.: bully_pause); pb não injeta falhas.
            folder = name if args.fault == "kill" or name == "pb" else f"{name}_{args.fault}"
            run_dir = out / folder / f"{param}={level}_seed{seed}"
            label = f"[{i}/{len(jobs)}] {name} {param}={level} seed={seed}"
            if (run_dir / "run.json").exists():
                print(f"{label}: já existe, pulada", flush=True)
                continue
            run_dir.mkdir(parents=True, exist_ok=True)
            (run_dir / "error.txt").unlink(missing_ok=True)
            config = {"experiment": name, "fault": args.fault if name != "pb" else None, "param": param, "level": level, "seed": seed,
                      "heartbeat_interval": args.heartbeat_interval, "failure_timeout": args.failure_timeout,
                      "request_timeout": args.request_timeout, "coordinator_timeout": args.coordinator_timeout,
                      "ops": args.ops, "warmup_ops": args.warmup_ops, "pre_tasks": args.pre_tasks,
                      "window_s": args.window, "load_rate": args.load_rate, "hb_nodes": args.hb_nodes, "image_id": env_info["image_id"],
                      "git_commit": env_info["git_commit"]}
            started = time.time()
            try:
                result = SCENARIOS[name](level, seed, run_dir, args)
            except Exception:
                (run_dir / "error.txt").write_text(traceback.format_exc())
                print(f"{label}: ERRO (ver {run_dir / 'error.txt'})", flush=True)
                continue
            run = {"config": config, "started_at": started, "elapsed_s": time.time() - started, "metrics": result}
            (run_dir / "run.json").write_text(json.dumps(run, indent=2))
            print(f"{label}: {'ok' if result.get('ok') else 'VERIFICAÇÃO FALHOU'} "
                  f"({run['elapsed_s']:.1f}s)", flush=True)


if __name__ == "__main__":
    parser = ArgumentParser(description="Experimentos Bully, Primary-Backup e Heartbeat")
    parser.add_argument("--exp", choices=["all", *EXPERIMENTS], default="all")
    parser.add_argument("--fault", choices=["kill", "pause"], default="kill",
                        help="kill: SIGKILL (conexões recusadas); pause: processo congelado (envios esperam timeout)")
    parser.add_argument("--reps", type=int, default=len(SEEDS), help="Réplicas (sementes) por nível, máx. 10")
    parser.add_argument("--levels", type=float, nargs="+", help="Sobrescreve os níveis do experimento")
    parser.add_argument("--out", default=str(Path(__file__).parent / "results" / "raw"))
    parser.add_argument("--ops", type=int, default=1000, help="PB: escritas medidas por réplica")
    parser.add_argument("--warmup-ops", type=int, default=50, help="PB: escritas de aquecimento descartadas")
    parser.add_argument("--pre-tasks", type=int, default=20, help="Bully: tarefas criadas antes da falha")
    parser.add_argument("--window", type=float, default=60.0, help="HB: janela estável sob carga (s)")
    parser.add_argument("--load-rate", type=float, default=10.0, help="HB: escritas/s durante a janela")
    parser.add_argument("--hb-nodes", type=int, default=3, help="HB: número de nós")
    parser.add_argument("--heartbeat-interval", type=float, default=0.5)
    parser.add_argument("--failure-timeout", type=float, default=3.0, help="Bully/PB: FAILURE_TIMEOUT")
    parser.add_argument("--request-timeout", type=float, default=1.0)
    parser.add_argument("--coordinator-timeout", type=float, default=3.0)
    parser.add_argument("--timeout", type=float, default=90.0, help="Prazo de convergência (s)")
    parser.add_argument("--base-port", type=int, default=18000, help="Nó i publicado em base+i")
    parser.add_argument("--no-build", action="store_true", help="Reutiliza a imagem já construída")
    main(parser.parse_args())