# Experimentos — Etapa 7

Scripts que respondem aos itens da seção *Análise Experimental* do enunciado: para cada
algoritmo, métricas definidas, um parâmetro em 3 níveis, 10 réplicas independentes
(sementes registradas), média, desvio-padrão, IC95 e percentis, e registro do ambiente.

| Arquivo | Função |
|---|---|
| `cluster.py` | Gera um compose com N nós, sobe/derruba o cluster, injeta falhas (`docker kill -s KILL`), consulta `/status` e `/metrics`, registra o ambiente |
| `run_experiments.py` | Executa a malha de réplicas e salva os dados brutos |
| `analyze_results.py` | Agrega as réplicas em CSV e gera os gráficos |

Requisitos no host: Docker com Compose v2 e Python ≥ 3.10. `run_experiments.py` usa só a
biblioteca padrão; `analyze_results.py` usa `matplotlib`.

## Executar

```bash
python experiments/run_experiments.py --exp all                  # falha por SIGKILL
python experiments/run_experiments.py --exp bully --fault pause  # opcional: falha silenciosa
python experiments/run_experiments.py --exp backup --fault pause # opcional
python experiments/analyze_results.py
```

`--fault kill` (padrão) usa SIGKILL: o contêiner sai da rede e os envios a ele falham
na hora. `--fault pause` congela o processo: as conexões ficam sem resposta e os envios
esperam o `REQUEST_TIMEOUT`, como numa máquina que parou de responder. As réplicas de
`pause` ficam em pastas próprias (`bully_pause/`, `hb_pause/`, `backup_pause/`).

Opções úteis: `--exp bully|pb|hb`, `--reps 3` (ensaio rápido), `--levels 3 5`,
`--ops 1000`, `--window 60`, `--no-build`. Réplicas com `run.json` existente são puladas,
então uma execução interrompida continua de onde parou. Uma réplica que falha grava
`error.txt` e é refeita na próxima execução. Para refazer tudo, apague `experiments/results/raw/`.

Cada réplica sobe um projeto Compose isolado (`mc714-exp`, portas `127.0.0.1:18001…`), sem
volumes, portanto sempre com bancos vazios. Os níveis são intercalados dentro de cada
semente (3, 5, 7, 3, 5, 7, …), para que uma deriva da máquina ao longo do tempo não se
confunda com o efeito do parâmetro. As réplicas rodam em sequência; clusters em paralelo
competiriam por CPU e distorceriam as medidas.

## Experimentos

**Bully: falha do líder, N ∈ {3, 5, 7}.** Cria 20 tarefas, espera uma fase aleatória do
ciclo de heartbeat (semente), mata o nó N e espera a convergência de todos os
sobreviventes (líder N−1 `PRIMARY`, membros = sobreviventes, `synchronized`).
- `election_time_s`: do primeiro `ELECTION_ROUND` até o último sobrevivente reconhecer o líder (tempo do Bully).
- `detection_first_s`, `recovery_time_s` (líder reconhecido → `PRIMARY`), `failover_time_s` (falha → `PRIMARY`).
- `sent_ELECTION`, `acked_ELECTION` (= mensagens OK), `sent_COORDINATOR`, `bully_messages`, `election_rounds`, `election_initiator`, `election_participants`. Os envios ao nó morto entram na contagem (`failed_*`). Limites teóricos de `bully_messages`: N no melhor caso (N−1 detecta primeiro) e N(N−1) no pior caso (todos os sobreviventes iniciam).
- Correção: `ok_single_leader`, `ok_tasks_preserved` (20 + 1 escrita após a falha), `ok_hashes_equal`.

**Primary-Backup: carga de escrita, N ∈ {3, 5, 7}.** Um cliente sequencial com conexão
persistente faz 50 criações de aquecimento (descartadas) e 1.000 medidas no primário.
- `latency_ms_{mean,std,p50,p95,p99,max}` por réplica; `latencies.csv` guarda cada operação.
- `throughput_ops_s`; `messages_per_op` (PREPARE + ACK + COMMIT + ACK) comparado a `expected_messages_per_op` = 4(N−1).
- Correção: sem erros, número de tarefas igual ao de escritas confirmadas, hashes iguais em todos os nós e nenhuma operação `PREPARED` pendente.

**Falha de backup, N ∈ {3, 5, 7}.** Cria 20 tarefas e mata o Node 1. O líder N deve
reconfigurar (epoch + 1, membros sem o Node 1) sem nova eleição.
- `detection_leader_s` (falha → suspeita no líder), `reconfiguration_time_s` (suspeita → `PRIMARY`), `failover_time_s`.
- `reconfiguration_messages` (FENCE, RECOVERY_STATE, SYNCHRONIZE, CONFIGURE, com respostas).
- Correção: `ok_no_election`, `ok_leader_kept`, `ok_epoch_incremented`, `ok_tasks_preserved`, `ok_hashes_equal`.

**Heartbeat: FAILURE_TIMEOUT ∈ {1,5; 2,5; 4,0} s, N = 3, intervalo 0,5 s.** Janela estável
de 60 s com carga de escrita (falsas suspeitas), seguida da falha do líder.
- `detection_first_s` / `detection_last_s`: falha → primeira/última suspeita entre os sobreviventes; `silence_samples`: tempo sem heartbeat medido no próprio nó. Esperado: entre T − HEARTBEAT_INTERVAL e T + 0,1 s, porque o último heartbeat chega até um intervalo antes da falha e a verificação ocorre a cada 0,1 s.
- `false_suspicions_stable` (suspeitas de nós vivos na janela), `false_suspicions_per_min`, `false_suspicions_per_pair_hour`, `false_suspicions_failover`.
- `heartbeats_per_node_s`, `heartbeat_failed`, `failover_time_s` e a latência da carga.
- Correção: `ok_detection_bound` (só suspeita após FAILURE_TIMEOUT sem heartbeat) e todos os sobreviventes detectam.

## Saídas

```
results/raw/environment_<data>.json       hardware, SO, versões (Docker, Compose, Python, pacotes da imagem), commit
results/raw/<exp>/<param>=<nível>_seed<s>/
    run.json        configuração + todas as métricas da réplica
    metrics_*.json  /metrics brutos de cada nó (contadores e eventos) antes/depois da falha
    compose.json    compose exato usado
    docker.log      logs dos contêineres
    latencies.csv   (pb) uma linha por escrita
results/<exp>_runs.csv         uma linha por réplica
results/<exp>_aggregated.csv   por nível e métrica: n, média, DP, IC95, mín, máx; pooled_*_p50/p95/p99
results/figures/<pasta>_*.pdf|png  gráficos para o relatório
```

O IC95 usa t de Student com n−1 graus de liberdade. Os percentis aparecem de duas formas:
por réplica (média ± IC entre réplicas) e agrupados (`pooled_*`, todas as amostras do nível).

## Instrumentação do app

`app/metrics.py` e `GET /metrics` expõem contadores por tipo de mensagem (`sent`: requisições
enviadas; `acked`: respostas válidas, como OK e ACK; `failed`: sem resposta no prazo) e eventos
com horário (`SUSPECTED`, `UNSUSPECTED`, `ELECTION_ROUND`, `LEADER_RECOGNIZED`, `PRIMARY`).
O total de mensagens de um tipo é `sent + acked`. O `main.py` também passa a configurar o
`logging`, então os logs `ELECTION`/`COORDINATOR`/`PREPARE`/`SUSPECTED` aparecem no `docker.log`. O `COORDINATOR` que a recuperação usa
como cercamento é contado à parte, como `FENCE`. A instrumentação não altera as decisões
dos algoritmos.

Os eventos usam `time.time()`. Os contêineres compartilham o relógio do kernel do host, então
os eventos de nós diferentes e o instante da falha medido pelo script são comparáveis. No
Docker Desktop (macOS/Windows), os contêineres rodam numa VM cujo relógio pode divergir um
pouco do host: os tempos medidos entre eventos (eleição, recuperação, `silence`) não são
afetados, mas os tempos medidos a partir da falha (detecção, failover) podem ser. Execute
no Linux para os resultados finais.

## Limitações a discutir no relatório

- Todos os nós estão numa única máquina: a latência de rede é a de loopback/bridge, e os contêineres disputam CPU e disco (o efeito cresce com N).
- O instante da falha é o início do comando `docker kill`/`pause`; `kill_cmd_s` (duração do comando) é a incerteza dessa marca. O tempo `silence` é medido no próprio nó e não depende dela.
- Com `--fault kill`, o Bully não espera timeouts (conexões recusadas); use `--fault pause` para medir o efeito do `REQUEST_TIMEOUT`.
- Os pontos de convergência via `/status` são amostrados a cada 50 ms (`failover_observed_s`); os tempos por eventos não têm essa limitação.
- As falsas suspeitas são medidas sem injeção de atraso. Com carga leve numa única máquina, espera-se zero ou quase zero; injetar atraso (tc/netem) fica como extensão.
