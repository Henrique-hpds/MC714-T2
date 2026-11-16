# Experimentos

Scripts da análise experimental do relatório: Bully, Primary-Backup e Heartbeat, cada um com
um parâmetro em 3 níveis e 10 execuções independentes por nível.

| Arquivo | Função |
|---|---|
| `cluster.py` | Sobe um cluster Docker Compose com N nós, encerra um nó (`docker kill -s KILL`), consulta os nós e derruba o cluster |
| `run_experiments.py` | Executa os experimentos e grava uma linha por execução em `results/raw/<experimento>.csv` |
| `analyze_results.py` | Gera a figura do relatório (`relatorio/images/bully_mensagens.pdf`) e imprime as médias ± IC95 citadas no texto e nas tabelas |

Requisitos: Docker com Compose v2 e o ambiente Python do projeto (`pip install -r requirements.txt`).

## Executar

```bash
python experiments/run_experiments.py            # os três experimentos, 10 execuções por nível (~58 min)
python experiments/run_experiments.py --exp bully --reps 3   # um experimento, ensaio rápido
python experiments/analyze_results.py
```

O script constrói a imagem uma vez (com cache). Cada execução sobe um cluster novo, sem volumes
e portanto com bancos vazios, aplica a carga e a falha, coleta `/metrics` dos nós e derruba o cluster. As
execuções rodam em sequência, com os níveis intercalados em cada semente (3, 5, 7, 3, 5, 7, ...).
Execuções já registradas no CSV são puladas, então uma execução interrompida continua de onde
parou. Para refazer tudo, apague `experiments/results/raw/`.

As sementes `1000·i` (i = 1, ..., 10) definem o conteúdo das tarefas e o instante da falha,
sorteado entre 0 e `HEARTBEAT_INTERVAL` (0,5 s) para cair em qualquer ponto do ciclo de heartbeat.

## Experimentos

**Bully (`bully.csv`), N ∈ {3, 5, 7}.** Cria 20 tarefas, encerra o líder (nó N) e espera os
sobreviventes reconhecerem o nó N−1 como primário.
- `messages`: mensagens ELECTION, OK e COORDINATOR da eleição, incluindo as enviadas ao nó encerrado.
- `election_time_s`: do início da primeira rodada até o último sobrevivente reconhecer o novo líder.
- `ok`: convergência para o nó N−1, 20 + 1 tarefas no novo primário (uma escrita após a falha), hashes iguais e nenhuma operação pendente.

**Primary-Backup (`pb.csv`), N ∈ {3, 5, 7}, sem falhas.** Um cliente sequencial faz 50
escritas de aquecimento (descartadas) e 1.000 escritas medidas no primário.
- `latency_{mean,p50,p95,p99}_ms`, `throughput_ops_s` e `messages_per_op` (PREPARE, COMMIT e as confirmações).
- `heartbeats_per_s`: heartbeats enviados pelo sistema por segundo.
- `ok`: as 1.000 escritas confirmadas, número de tarefas correto, hashes iguais e nenhuma operação pendente.

**Heartbeat (`hb.csv`), T = FAILURE_TIMEOUT ∈ {1,5; 2,5; 4,0} s, N = 3.** O sistema opera
por 60 s com 10 escritas/s e, em seguida, o líder é encerrado.
- `false_suspicions`: suspeitas de nós ativos durante os 60 s.
- `detection_s`: da falha até a primeira suspeita entre os sobreviventes.
- `silence_{min,max}_s`: tempo sem heartbeats medido pelo próprio nó no momento da suspeita.
- `heartbeats_per_node_s`: heartbeats enviados por nó por segundo.
- `ok`: os dois sobreviventes suspeitam do líder, só depois de T segundos sem heartbeats, e o cluster converge.

Todos os CSVs também têm `level`, `seed`, `startup_s` (até a eleição inicial e a ativação do
primário), `duration_s` (execução completa) e, nos cenários com falha, `kill_s` (duração do
comando que encerra o contêiner, que é a incerteza do instante da falha).

## Instrumentação do app

`GET /metrics` retorna os contadores `sent` (requisições enviadas) e `acked` (respostas recebidas
com sucesso) por tipo de mensagem e os eventos `ELECTION_ROUND`, `LEADER_RECOGNIZED` e `SUSPECTED`,
com o horário `time.time()`. Como os contêineres compartilham o relógio do host, os eventos de nós
diferentes e o instante da falha podem ser comparados. O `COORDINATOR` que a recuperação usa como
cercamento é contado à parte, como `FENCE`.
