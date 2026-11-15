"""Agrega as réplicas de run_experiments.py: CSVs (por réplica e por nível) e gráficos.

Saídas em <results>/:
  <exp>_runs.csv        uma linha por réplica (todas as métricas escalares)
  <exp>_aggregated.csv  por nível e métrica: n, média, DP, IC95 (t de Student), mín, máx,
                        e percentis agrupados (pooled) de latências/tempos de detecção
  figures/*.pdf|png     gráficos para o relatório

Uso: python experiments/analyze_results.py
"""
import csv
import json
import math
from argparse import ArgumentParser
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# t de Student bicaudal 95% por graus de liberdade (n-1)
T_CRIT = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262,
          10: 2.228, 11: 2.201, 12: 2.179, 13: 2.160, 14: 2.145, 15: 2.131, 19: 2.093, 24: 2.064, 29: 2.045}
COLORS = ("#2a78d6", "#eb6834", "#1baf7a")  # paleta categórica validada (3 primeiras posições)
INK, MUTED, GRID = "#0b0b0b", "#52514e", "#e4e3df"
LABELS = {"n_nodes": "Número de nós (N)", "failure_timeout": "FAILURE_TIMEOUT (s)"}


def t_crit(dof: int) -> float:
    if dof in T_CRIT:
        return T_CRIT[dof]
    smaller = [k for k in T_CRIT if k < dof]
    return T_CRIT[max(smaller)] if dof < 30 else 1.96


def stats(values: list[float]) -> dict:
    values = [v for v in values if v is not None]
    n = len(values)
    if n == 0:
        return {"n": 0, "mean": None, "std": None, "ci95": None, "min": None, "max": None}
    mean = sum(values) / n
    std = (sum((v - mean) ** 2 for v in values) / (n - 1)) ** 0.5 if n > 1 else 0.0
    return {"n": n, "mean": mean, "std": std, "ci95": t_crit(n - 1) * std / math.sqrt(n) if n > 1 else 0.0,
            "min": min(values), "max": max(values)}


def percentile(values: list[float], p: float) -> float | None:
    ordered = sorted(values)
    if not ordered:
        return None
    rank = (len(ordered) - 1) * p / 100
    low, high = math.floor(rank), math.ceil(rank)
    return ordered[low] + (ordered[high] - ordered[low]) * (rank - low)


def load(raw: Path, exp: str) -> list[dict]:
    runs = []
    for path in sorted((raw / exp).glob("*/run.json")):
        run = json.loads(path.read_text())
        run["dir"] = path.parent
        runs.append(run)
    return runs


def scalar_rows(runs: list[dict]) -> list[dict]:
    rows = []
    for run in runs:
        row = {k: v for k, v in run["config"].items()}
        row["elapsed_s"] = run["elapsed_s"]
        for key, value in run["metrics"].items():
            if not isinstance(value, (list, dict)):
                row[key] = value
        rows.append(row)
    return rows


def pooled_samples(run: dict, exp: str) -> dict[str, list[float]]:
    """Amostras individuais para percentis agrupados entre réplicas."""
    samples = {}
    if exp == "pb":
        with open(run["dir"] / "latencies.csv") as f:
            samples["latency_ms"] = [float(r["latency_s"]) * 1000 for r in csv.DictReader(f) if r["status"] == "201"]
    else:
        samples["detection_s"] = run["metrics"].get("detection_samples", [])
        samples["silence_s"] = run["metrics"].get("silence_samples", [])
    return samples


def aggregate(runs: list[dict], rows: list[dict], exp: str) -> dict:
    """{level: {metric: stats}} incluindo taxa de sucesso e percentis agrupados."""
    by_level = {}
    for run, row in zip(runs, rows):
        by_level.setdefault(row["level"], []).append((run, row))
    result = {}
    for level in sorted(by_level):
        pairs = by_level[level]
        metrics = {}
        numeric = [k for k, v in pairs[0][1].items()
                   if isinstance(v, (int, float)) and not isinstance(v, bool) and k not in ("level", "seed")]
        for key in numeric:
            metrics[key] = stats([row.get(key) for _, row in pairs])
        for key in [k for k, v in pairs[0][1].items() if isinstance(v, bool)]:
            metrics[key] = stats([float(row[key]) for _, row in pairs if row.get(key) is not None])
        pooled = {}
        for run, _ in pairs:
            for key, values in pooled_samples(run, exp).items():
                pooled.setdefault(key, []).extend(values)
        for key, values in pooled.items():
            for p in (50, 95, 99):
                metrics[f"pooled_{key}_p{p}"] = {"n": len(values), "mean": percentile(values, p)}
        result[level] = metrics
    return result


def write_csv(path: Path, rows: list[dict]) -> None:
    fields = []
    for row in rows:
        fields += [k for k in row if k not in fields]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------- gráficos
def style(ax, xlabel: str, ylabel: str, levels: list) -> None:
    ax.set_xlabel(xlabel, color=MUTED)
    ax.set_ylabel(ylabel, color=MUTED)
    ax.set_xticks(levels)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=MUTED)
    ax.set_ylim(bottom=0)


def line_chart(path: Path, agg: dict, series: list[tuple[str, str]], xlabel: str, ylabel: str,
               title: str, references=(), band=None) -> None:
    """Média por nível com barras de IC95; rótulo direto no último ponto de cada série."""
    levels = list(agg)
    fig, ax = plt.subplots(figsize=(3.5, 2.6), dpi=200)
    for color, (key, label) in zip(COLORS, series):
        points = [(lvl, agg[lvl][key]) for lvl in levels if agg[lvl].get(key, {}).get("mean") is not None]
        if not points:
            continue
        xs = [p[0] for p in points]
        ys = [p[1]["mean"] for p in points]
        errs = [p[1].get("ci95") or 0 for p in points]
        ax.errorbar(xs, ys, yerr=errs, color=color, linewidth=1.5, marker="o", markersize=5,
                    capsize=3, label=label)
    for (xs, ys, label), dash in zip(references, ("--", ":")):
        ax.plot(xs, ys, color=MUTED, linewidth=1, linestyle=dash, label=label)
    if band is not None:
        xs, low, high, label = band
        ax.fill_between(xs, low, high, color=GRID, alpha=0.7, linewidth=0, label=label)
    style(ax, xlabel, ylabel, levels)
    ax.set_title(title, color=INK, fontsize=9, loc="left")
    if len(series) > 1 or references or band is not None:
        ax.legend(frameon=False, fontsize=7)
    fig.tight_layout()
    for ext in ("pdf", "png"):
        fig.savefig(path.with_suffix(f".{ext}"))
    plt.close(fig)


def plots(exp: str, folder: str, agg: dict, figures: Path, xlabel: str, interval: float) -> None:
    """exp = tipo do cenário; folder = nome da pasta (ex.: bully_pause), usado como prefixo das figuras."""
    levels = list(agg)
    figures = figures / f"{folder}_"  # prefixo: figures/<folder>_<grafico>.pdf
    if exp == "bully":
        line_chart(Path(f"{figures}times"), agg,
                   [("detection_first_s", "Detecção (Heartbeat)"), ("election_time_s", "Eleição (Bully)"),
                    ("recovery_time_s", "Recuperação (sincr.)")],
                   xlabel, "Tempo (s)", "Troca de líder: decomposição do tempo")
        line_chart(Path(f"{figures}messages"), agg,
                   [("sent_ELECTION", "ELECTION"), ("acked_ELECTION", "OK"), ("sent_COORDINATOR", "COORDINATOR")],
                   xlabel, "Mensagens por eleição", "Bully: mensagens por tipo")
        # Total com envios ao nó morto: melhor caso N (N-1 detecta primeiro), pior caso N(N-1) (todos iniciam).
        line_chart(Path(f"{figures}messages_total"), agg, [("bully_messages", "Medido")],
                   xlabel, "Mensagens por eleição", "Bully: total vs. limites teóricos",
                   references=[(levels, [n * (n - 1) for n in levels], "Pior caso N(N-1)"),
                               (levels, levels, "Melhor caso N")])
    elif exp == "pb":
        line_chart(Path(f"{figures}latency"), agg,
                   [("latency_ms_p50", "p50"), ("latency_ms_p95", "p95"), ("latency_ms_p99", "p99")],
                   xlabel, "Latência de escrita (ms)", "Primary-Backup: latência")
        line_chart(Path(f"{figures}throughput"), agg, [("throughput_ops_s", "Vazão")],
                   xlabel, "Escritas confirmadas/s", "Primary-Backup: vazão (1 cliente)")
        line_chart(Path(f"{figures}messages"), agg, [("messages_per_op", "Medido")],
                   xlabel, "Mensagens por escrita", "Primary-Backup: custo de comunicação",
                   references=[(levels, [4 * (n - 1) for n in levels], "4(N-1)")])
    elif exp == "hb":
        # Esperado: o último heartbeat chegou até 1 intervalo antes da falha; verificação a cada 0,1 s.
        line_chart(Path(f"{figures}detection"), agg,
                   [("detection_first_s", "1º sobrevivente"), ("detection_last_s", "Último sobrevivente")],
                   xlabel, "Tempo de detecção (s)", "Heartbeat: falha → suspeita",
                   band=(levels, [t - interval for t in levels], [t + 0.1 for t in levels],
                         "Esperado [T−intervalo, T+0,1]"))
        line_chart(Path(f"{figures}false_suspicions"), agg, [("false_suspicions_per_min", "Falsas suspeitas")],
                   xlabel, "Falsas suspeitas por minuto", "Heartbeat: falsas suspeitas sob carga")
        line_chart(Path(f"{figures}failover"), agg, [("failover_time_s", "Falha → PRIMARY")],
                   xlabel, "Tempo (s)", "Tempo total de recuperação do serviço")
    elif exp == "backup":
        line_chart(Path(f"{figures}times"), agg,
                   [("detection_leader_s", "Detecção pelo líder"), ("reconfiguration_time_s", "Reconfiguração"),
                    ("failover_time_s", "Falha → PRIMARY")],
                   xlabel, "Tempo (s)", "Falha de backup: recuperação sem eleição")
        line_chart(Path(f"{figures}messages"), agg, [("reconfiguration_messages", "Reconfiguração")],
                   xlabel, "Mensagens", "Falha de backup: custo da reconfiguração")


def main(args):
    results, raw = Path(args.results), Path(args.raw)
    figures = results / "figures"
    figures.mkdir(parents=True, exist_ok=True)
    for folder in sorted(p.name for p in raw.iterdir() if p.is_dir()):
        exp = folder.split("_")[0]  # bully_pause -> bully
        runs = load(raw, folder)
        if not runs:
            continue
        rows = scalar_rows(runs)
        write_csv(results / f"{folder}_runs.csv", rows)
        agg = aggregate(runs, rows, exp)
        long_rows = [{"experiment": exp, "param": rows[0]["param"], "level": level, "metric": metric, **values}
                     for level, metrics in agg.items() for metric, values in metrics.items()]
        write_csv(results / f"{folder}_aggregated.csv", long_rows)
        plots(exp, folder, agg, figures, LABELS.get(rows[0]["param"], rows[0]["param"]), rows[0]["heartbeat_interval"])

        print(f"\n== {folder} ({len(runs)} réplicas) — média ± IC95")
        keys = {"bully": ("ok", "detection_first_s", "election_time_s", "recovery_time_s", "failover_time_s",
                          "election_rounds", "bully_messages"),
                "pb": ("ok", "throughput_ops_s", "latency_ms_mean", "latency_ms_p50", "latency_ms_p95",
                       "latency_ms_p99", "messages_per_op"),
                "hb": ("ok", "detection_first_s", "detection_last_s", "false_suspicions_per_min",
                       "heartbeats_per_node_s", "failover_time_s"),
                "backup": ("ok", "detection_leader_s", "reconfiguration_time_s", "failover_time_s",
                           "reconfiguration_messages", "election_rounds")}[exp]
        for level, metrics in agg.items():
            cells = []
            for key in keys:
                s = metrics.get(key, {})
                cells.append(f"{key}={s['mean']:.3f}±{s['ci95']:.3f}" if s.get("mean") is not None else f"{key}=NA")
            print(f"{rows[0]['param']}={level}: (n={metrics['ok']['n']}) " + "  ".join(cells))


if __name__ == "__main__":
    parser = ArgumentParser(description="Agregação e gráficos dos experimentos")
    here = Path(__file__).parent
    parser.add_argument("--raw", default=str(here / "results" / "raw"))
    parser.add_argument("--results", default=str(here / "results"))
    main(parser.parse_args())
