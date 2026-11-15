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
após receber um `OK`. `HEARTBEAT_INTERVAL` (padrão: `0.5` segundo) controla o
envio periódico e `FAILURE_TIMEOUT` (padrão: `3.0` segundos) define o prazo sem
receber heartbeat antes de suspeitar de um participante. Os quatro parâmetros
devem ser positivos e finitos. Configure o timeout de falha com margem para o
intervalo de envio e os atrasos esperados.

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

## Heartbeat — Etapa 3

Cada nó envia `POST /internal/heartbeat` com `{"sender_id": 1}` aos demais
participantes. O destinatário valida se o remetente é outro nó de `PEERS` e
registra o instante do recebimento com relógio monotônico local. Respostas aos
envios e consultas de saúde não renovam esse prazo. Cada participante recebe
um período inicial de `FAILURE_TIMEOUT` para enviar sua primeira mensagem.

O envio e o monitoramento executam em segundo plano durante a vida da aplicação.
Cada destino tem uma rotina de envio independente, com timeout HTTP e limite
total de `REQUEST_TIMEOUT`, para que um nó lento não atrase os outros destinos.
O monitoramento verifica os prazos a cada 100 ms ou menos, conforme os parâmetros.
As rotinas e os envios pendentes são cancelados no encerramento.

Uma suspeita sobre o líder inicia o Bully automaticamente. A mesma suspeita
não dispara eleições repetidas para a mesma liderança. Uma falha de backup
apenas registra a suspeita e preserva o líder e o epoch. Um heartbeat válido
posterior remove a suspeita; isso não representa uma reintegração ao conjunto
de réplicas, cujo protocolo será implementado nas próximas etapas.

O `/status` inclui `suspected_nodes`, ordenado por ID. O campo `active_nodes`
continua indicando quem respondeu às consultas de `/health`, sem alterar a
configuração de replicação. Os dois campos podem divergir durante o prazo de
detecção. Consultar `/status` não inicia uma eleição.

Para testar a substituição automática do líder:

```bash
docker compose stop node3
# Aguarde o timeout de falha e a convergência da eleição.
curl http://localhost:8001/status
curl http://localhost:8002/status
```

Aguarde a convergência: os Nodes 1 e 2 devem reconhecer `leader_id: 2`, com
Node 2 em `LEADER_ELECTED` e `suspected_nodes: [3]`, sem chamadas manuais de
eleição. O modelo assumido é crash-stop, sem partições de rede e com atrasos
compatíveis com os timeouts. Bully e Heartbeat não oferecem consenso ou proteção
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
Também cobrem a eleição automática após falha do líder, suspeitas de backup sem
nova eleição, expiração e renovação dos heartbeats, isolamento de destinos lentos,
validação da configuração e cancelamento de envios pendentes.

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
As eleições após falhas são acionadas automaticamente pelo Heartbeat, sem usar
`/internal/election/start`. Os contêineres e a rede do ensaio são removidos ao final.

Para acompanhar os logs, use `docker compose logs -f`.
Para encerrar, use `docker compose down`.
