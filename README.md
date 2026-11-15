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

Com os três nós disponíveis, `/status` deve retornar `active_nodes: [1, 2, 3]`.
Até a implementação do Bully, todos permanecem com papel `UNASSIGNED` e sem líder.
O endpoint `POST /internal/message` recebe mensagens com `sender_id` e `message`.

A configuração está em `compose.yaml`: `NODE_ID` identifica o nó, `PEERS` define
os participantes e `DB_PATH` reserva o caminho do SQLite em um volume por nó.

Para acompanhar os logs, use `docker compose logs -f`.
Para encerrar, use `docker compose down`.
