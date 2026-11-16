"""Gera a figura e as tabelas do relatório a partir dos dados brutos (results/raw).

Saídas:
  relatorio/images/bully_mensagens.pdf|png  mensagens por eleição x N, com os limites N e N(N-1)
  relatorio/tabelas/tempos_falha.tex        tempo de eleição (Bully) e de detecção (Heartbeat)
  relatorio/tabelas/primary_backup.tex      latência (média, p50, p95, p99) e vazão do Primary-Backup

Uso: python experiments/report_assets.py
"""
import random
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from analyze_results import aggregate, load, scalar_rows

HERE = Path(__file__).parent
RAW = HERE / "results" / "raw"
REPORT = HERE.parent / "relatorio"


def agg(folder: str) -> tuple[list[dict], dict]:
    exp = folder.split("_")[0]
    runs = load(RAW, folder)
    rows = scalar_rows(runs)
    return rows, aggregate(runs, rows, exp)


def cell(stats: dict | None, digits: int) -> str:
    if not stats or stats.get("mean") is None:
        return "--"
    return f"${stats['mean']:.{digits}f} \\pm {stats['ci95']:.{digits}f}$"


# ---------------------------------------------------------------- figura do Bully
def bully_figure() -> None:
    fig, ax = plt.subplots(figsize=(6, 3.6))
    grid = np.linspace(2.8, 7.2, 200)
    ax.plot(grid, grid * (grid - 1), color="black", linewidth=1.8, label=r"Pior caso: $N(N-1)$")
    ax.plot(grid, grid, color="black", linewidth=1.8, linestyle="--", label=r"Melhor caso: $N$")
    jitter = random.Random(0)
    rows, stats = agg("bully")
    levels = sorted(stats)
    ax.errorbar(levels, [stats[n]["bully_messages"]["mean"] for n in levels],
                yerr=[stats[n]["bully_messages"]["ci95"] for n in levels], color="tab:red", marker="o",
                markersize=6, linestyle="none", capsize=4, label="Experimento (média $\\pm$ IC95)", zorder=3)
    # execuções individuais, à esquerda de cada nível
    xs = [r["level"] - 0.18 + jitter.uniform(-0.04, 0.04) for r in rows]
    ax.scatter(xs, [r["bully_messages"] for r in rows], color="tab:red", alpha=0.45, s=14, linewidths=0,
               label="Experimento (execuções)")
    ax.set_xticks([3, 5, 7])
    ax.set_xlabel("Número de nós ($N$)")
    ax.set_ylabel("Mensagens por eleição")
    ax.set_ylim(0, 70)
    ax.grid(alpha=0.35)
    ax.legend(loc="upper left", fontsize=9)
    fig.tight_layout()
    out = REPORT / "images" / "bully_mensagens"
    for ext in ("pdf", "png"):
        fig.savefig(out.with_suffix(f".{ext}"), dpi=300)
    plt.close(fig)


# ---------------------------------------------------------------- tabela de tempos
def times_table(interval: float = 0.5, check: float = 0.1) -> str:
    """Tempo de eleição (Bully, x N) e de detecção (Heartbeat, x T), com o intervalo esperado da detecção."""
    lines = []
    _, stats = agg("bully")
    for i, n in enumerate(sorted(stats)):
        name = "\\multirow{3}{*}{Eleição (Bully)}" if i == 0 else ""
        lines.append(f"{name} & $N={n}$ & {cell(stats[n]['election_time_s'], 3)} & -- \\\\")
    lines.append("\\hline")
    _, stats = agg("hb")
    for i, t in enumerate(sorted(stats)):
        name = "\\multirow{3}{*}{Detecção (Heartbeat)}" if i == 0 else ""
        lines.append(f"{name} & $T={t}$ & {cell(stats[t]['detection_first_s'], 2)} & "
                     f"$[{t - interval:.1f};\\ {t + check:.1f}]$ \\\\")
    body = "\n".join(lines)
    return rf"""\begin{{table}}[htbp]
\caption{{Tempo de eleição do Bully e tempo de detecção do Heartbeat (média $\pm$ IC95, em segundos)}}
\label{{tab:tempos}}
\centering
\footnotesize
\setlength{{\tabcolsep}}{{3pt}}
\begin{{tabular}}{{l c c c}}
\hline\hline
\textbf{{Métrica}} & \textbf{{Nível}} & \textbf{{Medido}} & \textbf{{Esperado}} \\
\hline
{body}
\hline\hline
\end{{tabular}}
\end{{table}}
"""


# ---------------------------------------------------------------- tabela do Primary-Backup
def pb_table() -> str:
    _, stats = agg("pb")
    lines = []
    for n in sorted(stats):
        m = stats[n]
        cols = [cell(m[k], 1) for k in ("latency_ms_mean", "latency_ms_p50", "latency_ms_p95", "latency_ms_p99",
                                        "throughput_ops_s")]
        lines.append(f"{n} & " + " & ".join(cols) + " \\\\")
    body = "\n".join(lines)
    return rf"""\begin{{table}}[htbp]
\caption{{Latência de escrita (ms) e vazão (escritas/s) do Primary-Backup (média $\pm$ IC95)}}
\label{{tab:pb}}
\centering
\footnotesize
\setlength{{\tabcolsep}}{{3pt}}
\begin{{tabular}}{{c c c c c c}}
\hline\hline
$\mathbf{{N}}$ & \textbf{{Média}} & \textbf{{p50}} & \textbf{{p95}} & \textbf{{p99}} & \textbf{{Vazão}} \\
\hline
{body}
\hline\hline
\end{{tabular}}
\end{{table}}
"""


if __name__ == "__main__":
    (REPORT / "images").mkdir(parents=True, exist_ok=True)
    (REPORT / "tabelas").mkdir(parents=True, exist_ok=True)
    bully_figure()
    (REPORT / "tabelas" / "tempos_falha.tex").write_text(times_table())
    (REPORT / "tabelas" / "primary_backup.tex").write_text(pb_table())
    print("Figura e tabelas geradas em", REPORT)
