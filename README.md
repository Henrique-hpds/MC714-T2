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
e o mesmo `epoch` nos três nós. O vencedor passa de `LEADER_ELECTED` a `PRIMARY`
depois de recuperar os logs, sincronizar as tarefas dos sobreviventes e receber
as confirmações da configuração de replicação. Os outros têm papel `BACKUP`.
Durante a eleição, o papel é `ELECTING` e não há líder. Aguarde `PRIMARY` antes
de enviar operações de tarefas; o healthcheck só indica que a API está acessível.
O endpoint `POST /internal/message` recebe mensagens com `sender_id` e `message`.

A configuração está em `compose.yaml`: `NODE_ID` identifica o nó, `PEERS` define
os participantes e `DB_PATH` define o caminho do SQLite em um volume por nó.
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
O `epoch` identifica a liderança reconhecida e acompanha as mensagens de
replicação. Na inicialização, o maior epoch do log persistido serve como piso
para a eleição, incluindo o epoch da configuração persistida. `PEERS` define os
endereços conhecidos; a configuração de replicação identifica os membros atuais.

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
preserva o líder; a recuperação estabelece um novo epoch com os sobreviventes,
sem executar outra eleição. Um heartbeat válido
posterior remove a suspeita; isso não representa uma reintegração ao conjunto
de réplicas. A reintegração de nós reiniciados permanece fora do escopo.

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
Node 2 em `PRIMARY`, `replication_members: [1, 2]`, `synchronized: true` e
`suspected_nodes: [3]`, sem chamadas manuais de eleição. O modelo assumido é
crash-stop, sem partições de rede e com atrasos
compatíveis com os timeouts. Bully e Heartbeat não oferecem consenso ou proteção
contra split-brain fora desse modelo.

Cada ensaio considera uma falha de nó e termina com os sobreviventes. Para um
novo ensaio com três nós e bancos vazios, remova os volumes com
`docker compose down --volumes` (isso apaga as tarefas) e inicie o Compose
novamente. Reiniciar apenas o nó excluído não realiza sua reintegração.

## Primary-Backup — Etapa 4

Somente o `PRIMARY` atende leituras e escritas de tarefas. Um backup responde
HTTP 409 com `leader_id` e `leader_url`; esse endereço usa o DNS interno de
`PEERS` (no host, use a porta correspondente, por exemplo `8003` para Node 3).
Durante eleição ou ativação pendente, o candidato responde HTTP 503.

| Endpoint | Corpo | Resultado |
|---|---|---|
| `POST /tasks` | `{"title": "Implementar replicação"}` | Tarefa criada, HTTP 201 |
| `GET /tasks` | Sem corpo | Lista de tarefas confirmadas |
| `PUT /tasks/{id}` | `{"status": "completed"}` | Tarefa atualizada, HTTP 200 |
| `GET /operations/{request_id}` | Sem corpo | Registro da operação, incluindo estado e resultado |

As tarefas têm `id`, `title`, `status` e `version`. Os estados permitidos são
`pending` (padrão), `in_progress` e `completed`. Uma atualização informa título,
status ou ambos; a versão inicia em 1 e aumenta uma vez por atualização.
Campos desconhecidos, títulos vazios e atualizações vazias ou nulas retornam
HTTP 422. Atualizar uma tarefa inexistente retorna HTTP 404.

```bash
curl -i -X POST http://localhost:8003/tasks \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: request-001' \
  -d '{"title": "Implementar Primary-Backup"}'

curl http://localhost:8003/tasks

# Substitua TASK_ID pelo id retornado na criação.
curl -X PUT http://localhost:8003/tasks/TASK_ID \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: request-002' \
  -d '{"status": "completed"}'
```

Use uma `Idempotency-Key` única por operação. Ao repetir a mesma entrada com a
mesma chave, o sistema devolve o resultado original sem reaplicar a operação.
Reutilizar a chave com outra entrada, outro tipo de operação ou outra tarefa
retorna HTTP 409. Se omitida, uma chave é gerada e devolvida no cabeçalho da
resposta; enviá-la explicitamente permite repetir a operação mesmo se a conexão
cair antes de o cliente receber a resposta.

As escritas são serializadas com `asyncio.Lock`. O primário persiste a operação
como `PREPARED` e envia PREPARE concorrentemente a todos os backups. Depois de
receber todos os ACKs, envia COMMIT e aguarda todos os COMMIT ACKs. Só então
confirma localmente e responde ao cliente. Cada nó mantém tabelas `tasks` e
`operations` em SQLite, com unicidade de `request_id` e de `(epoch,
sequence_number)`. Aplicar uma tarefa e marcar sua operação `COMMITTED` ocorre
na mesma transação. PREPARE não altera a tabela de tarefas.

Os envios têm limite total de `REQUEST_TIMEOUT`, e os ACKs são conferidos por
nó, requisição, epoch, sequência e fase. Backups rejeitam mensagens de outro
remetente ou epoch, COMMIT sem PREPARE e operações fora de ordem ou com versão
incompatível. PREPARE e COMMIT repetidos são idempotentes. Essas duas fases são
parte da replicação Primary-Backup; não constituem uma implementação completa
de 2PC.

Se faltar qualquer ACK, a API retorna HTTP 503 com `request_id` e mantém a
operação pendente. O resultado pode ser desconhecido: algum backup pode já ter
aplicado o COMMIT. Repita a mesma entrada e chave para concluir a operação na
mesma liderança. Enquanto houver uma operação pendente, novas escritas e
leituras ficam suspensas, evitando exposição de uma escrita ainda incompleta.

A configuração inicial inclui todos os nós de `PEERS`. Uma falha impede
confirmar escritas nessa configuração até a recuperação estabelecer outra com
os sobreviventes. O conjunto de ACKs exigidos é sempre o da configuração
instalada, apresentado como `replication_members` em `/status`.

Endpoints internos usados pelo protocolo:

| Endpoint | Função |
|---|---|
| `GET /internal/replication/state` | Liderança, hashes dos logs e tarefas e presença de operações pendentes |
| `POST /internal/replication/prepare` | Persistir a operação antes de responder ACK |
| `POST /internal/replication/commit` | Aplicar tarefa e confirmar operação antes de responder COMMIT ACK |

## Integração e recuperação — Etapa 5

O modelo adotado usa HTTP/TCP, comunicação parcialmente síncrona, relógio
monotônico para timeouts, SQLite por nó e uma falha crash-stop por ensaio.
Partições de rede, falsas suspeitas e reintegração de nós reiniciados ficam fora
das garantias deste modelo. Bully e Heartbeat não são um protocolo de consenso.

Quando o líder falha, o Heartbeat aciona o Bully. O vencedor fica em
`LEADER_ELECTED`, com leituras e escritas de tarefas suspensas. O serviço de
recuperação consulta os logs de todos os membros não suspeitos, preserva cada
operação `COMMITTED` encontrada em qualquer sobrevivente e descarta operações
que aparecem somente como `PREPARED`. Reconstrói as tarefas em ordem de epoch e
sequência, validando payloads e versões. Logs conflitantes impedem a ativação.

O candidato envia o snapshot recuperado aos backups. Cada destinatário valida
os hashes e impede o descarte de operações já confirmadas. Log, tarefas e
configuração são instalados na mesma transação SQLite. Uma segunda troca de
mensagens confirma a configuração; o candidato só vira `PRIMARY` após receber
todos os ACKs válidos. Falhas ou ACKs inválidos mantêm o candidato suspenso e
provocam uma nova tentativa. A falha de um backup usa o mesmo procedimento,
preservando o líder e aumentando o epoch. Uma falha HTTP isolada não remove
membros: a exclusão depende das suspeitas do Heartbeat.

O `/status` e o estado de replicação apresentam `configuration`, com `epoch`,
`leader` e `members`, e `synchronized`. Após a falha do Node 3, os Nodes 1 e 2
devem confirmar a mesma configuração com líder 2 e membros `[1, 2]`. Toda nova
escrita exige PREPARE e COMMIT ACK do Node 1, além da persistência local no Node 2.
Mensagens de replicação e recuperação de epochs anteriores são rejeitadas.
`active_nodes` é apenas uma consulta de saúde e não altera os membros instalados.

Uma requisição sem resposta pode ter sido efetivada. Consulte
`GET /operations/{request_id}` no novo primário ou repita a entrada com a mesma
`Idempotency-Key`: operações recuperadas devolvem o resultado original, inclusive
para atualizações, sem incrementar a versão novamente. Operações descartadas
retornam HTTP 404 na consulta e podem ser reenviadas com a mesma chave.

Para demonstrar a continuidade após criar tarefas no Node 3:

```bash
docker compose stop node3
# Aguarde PRIMARY e synchronized: true no Node 2.
curl http://localhost:8002/status
curl -X POST http://localhost:8002/tasks \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: request-after-failure' \
  -d '{"title": "Validar continuidade após falha"}'
curl http://localhost:8002/tasks
```

| Endpoint interno | Função |
|---|---|
| `GET /internal/recovery/state` | Consultar o log completo e a configuração local |
| `POST /internal/recovery/synchronize` | Instalar o snapshot recuperado e a configuração pendente |
| `POST /internal/recovery/configure` | Confirmar a configuração após validar os hashes |

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
Os testes de Primary-Backup usam um SQLite temporário independente por nó e
cobrem persistência, atomicidade, criação e atualização, versões, serialização,
idempotência, perda e falsificação de ACKs, timeouts, cancelamento e mudança de
liderança durante escrita. A integração cobre 100 tarefas antes e 100 depois da
falha do primário, descarte de PREPARED, recuperação de COMMIT parcial, repetição
de atualizações, falha de backup sem nova eleição, rejeição de epochs antigos,
persistência da configuração e bloqueio até todos os ACKs de sincronização.

Com os três contêineres em execução, valide também a comunicação pelo DNS do Compose:

```bash
.venv/bin/python tests/check_cluster.py
```

Para um ensaio completo com processos independentes, execute:

```bash
.venv/bin/python tests/check_bully_cluster.py
```

Esse script cria um projeto Compose isolado, sem portas publicadas ou volumes
do ambiente de desenvolvimento. Verifica a ativação inicial do primário,
criação e atualização de tarefas, idempotência e igualdade dos registros nos
três nós. Também verifica eleições simultâneas, cria 100 tarefas, interrompe o
Node 3, aguarda a ativação do Node 2 com membros `[1, 2]` e cria outras 100
tarefas. Ao final, confere as 200 tarefas e os mesmos hashes nos sobreviventes.
As eleições após falhas são acionadas automaticamente pelo Heartbeat, sem usar
`/internal/election/start`. Os contêineres e a rede do ensaio são removidos ao final.

Para acompanhar os logs, use `docker compose logs -f`.
Para encerrar, use `docker compose down`.
