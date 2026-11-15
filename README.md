# Sistema Distribuído de Gerenciamento de Tarefas


## Executar

Requer Docker com Compose v2 e as portas `8001`–`8003` disponíveis.
Execute os comandos na raiz do repositório:

```bash
docker compose up --build -d --wait --wait-timeout 90
```

Os nós ficam disponíveis em `http://localhost:8001`, `:8002` e `:8003`.
A documentação da API está em [localhost:8001/docs](http://localhost:8001/docs).

```bash
curl http://localhost:8001/health
curl http://localhost:8001/status
```

Com os três nós disponíveis, o Bully é iniciado automaticamente e elege o Node 3.
Após a convergência, `/status` retorna `active_nodes: [1, 2, 3]`, `leader_id: 3`
e o mesmo `epoch` nos três nós. O vencedor tem papel `LEADER_ELECTED` e os
outros têm papel `BACKUP`. Durante a eleição, o papel é `ELECTING` e não há líder.
O vencedor ainda não assume `PRIMARY`: a ativação depende da sincronização de
dados prevista nas próximas etapas.
O endpoint `POST /internal/message` recebe mensagens com `sender_id` e `message`.

A configuração está em `compose.yaml`: `NODE_ID` identifica o nó, `PEERS` define
os participantes e `DB_PATH` reserva o caminho do SQLite em um volume por nó.
`REQUEST_TIMEOUT` (padrão: `1.0` segundo) limita cada envio HTTP e
`COORDINATOR_TIMEOUT` (padrão: `3.0` segundos) limita a espera pelo coordenador
após receber um `OK`. Ambos devem ser positivos e finitos. Ajuste a espera pelo
coordenador conforme o tamanho do cluster e os atrasos esperados.

## Eleição Bully — Etapa 2

Os nós enviam `ELECTION` somente aos participantes de ID maior, recebem `OK`
na resposta HTTP e anunciam o vencedor com `COORDINATOR` aos demais.
Os envios são concorrentes e possuem timeout. Cada nó mantém uma única rotina
de eleição, inclusive ao receber solicitações simultâneas. Se um participante
responder `OK` mas o anúncio não chegar a tempo, uma nova rodada é iniciada.

| Endpoint | Corpo JSON | Resultado |
|---|---|---|
| `POST /internal/election/start` | Sem corpo | Inicia uma eleição local; HTTP 202 |
| `POST /internal/election` | `{"sender_id": 1, "epoch": 2}` | Retorna `{"type": "OK", "node_id": 2, "epoch": 2}` e participa da eleição |
| `POST /internal/coordinator` | `{"leader_id": 3, "epoch": 2}` | Informa `accepted` e o estado local |

As mensagens do protocolo usam IDs de `PEERS` e epochs inteiros positivos.
`ELECTION` deve vir de um ID menor que o destinatário. Os anúncios de epochs
antigos são ignorados; um nó de ID maior contesta candidatos de ID menor.
O `epoch` identifica a rodada em memória e não é uma configuração persistente
de replicação nesta etapa.

Para testar a substituição do líder:

```bash
docker compose stop node3
curl -X POST http://localhost:8001/internal/election/start
curl http://localhost:8001/status
curl http://localhost:8002/status
```

Aguarde a convergência: os Nodes 1 e 2 devem reconhecer `leader_id: 2`, com
Node 2 em `LEADER_ELECTED`. A detecção automática de falhas será implementada
na etapa de Heartbeat; consultar `/status` apenas observa os nós e não inicia
eleições. O modelo assumido é crash-stop, sem partições de rede e com atrasos
compatíveis com os timeouts. Bully sozinho não oferece consenso ou proteção
contra split-brain fora desse modelo.

Para restaurar os três nós após o ensaio, execute `docker compose up -d --wait`.

## Testes

Com as dependências de `requirements.txt` instaladas no ambiente virtual:

```bash
.venv/bin/python -m unittest discover -s tests -v
```

Os testes usam a API real com transporte HTTP em memória e cobrem eleição
inicial, falhas do líder e de backup, eleições simultâneas, perda e atraso de
`COORDINATOR`, falha após `OK`, validação das mensagens e encerramento das rotinas.

Com os três contêineres em execução, valide também a comunicação pelo DNS do Compose:

```bash
.venv/bin/python tests/check_cluster.py
```

Para um ensaio completo com processos independentes, execute:

```bash
.venv/bin/python tests/check_bully_cluster.py
```

Esse script cria um projeto Compose isolado, sem portas publicadas ou volumes
do ambiente de desenvolvimento. Verifica a eleição inicial, eleições
simultâneas e a eleição dos Nodes 2 e 1 após interromper os líderes anteriores.
As eleições após falhas são acionadas explicitamente. Os contêineres e a rede
do ensaio são removidos ao final.

Para acompanhar os logs, use `docker compose logs -f`.
Para encerrar, use `docker compose down`.
