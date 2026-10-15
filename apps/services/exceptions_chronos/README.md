# Exceptions API — Lançamentos de Paradas

API REST + interface web para registro manual de paradas de equipamentos, justificativa de paradas automáticas (micro-eventos) e gestão de exceções do **Chronos / mcExperienceSuite**.

---

## Stack

| Camada       | Tecnologia                          |
|--------------|-------------------------------------|
| Backend      | Python 3.12 + FastAPI + Uvicorn     |
| Banco        | SQL Server (via `pyodbc` + ODBC 18) |
| Frontend     | HTML/CSS/JS puro (sem framework)    |
| Container    | Docker + Docker Compose             |

---

## Estrutura do projeto

```
exceptions-api/
├── config.py                  # Constantes globais (banco, departamento, célula)
├── database.py                # Classe Database + injeção de dependência
├── models.py                  # Todos os modelos Pydantic
├── main.py                    # Setup do app FastAPI e registro de rotas
├── requirements.txt
├── docker-compose.yml
├── .env                       # Variáveis de ambiente (não versionar)
│
├── routes/                    # Handlers HTTP (sem lógica de negócio)
│   ├── exceptions.py
│   ├── micro_events.py
│   └── reasons.py
│
├── services/                  # Lógica de negócio e acesso ao banco
│   ├── exception_service.py
│   ├── micro_event_service.py
│   └── reason_service.py
│
└── static/                    # Interface web servida pelo FastAPI
    └── index.html
```

---

## Variáveis de ambiente

Crie um arquivo `.env` na raiz com:

```env
APP_USERNAME=admin
APP_PASSWORD=admin

MSSQL_HOST=<ip-do-servidor>
MSSQL_DB=db_mcExperienceSuite_mirror
MSSQL_USER=<usuario>
MSSQL_PASS=<senha>

# Opcional — sobrescreve os padrões do config.py
# APP_DEPARTMENT=Engenharia
# APP_FACILITY=Expedição
```

---

## Como executar

### Pré-requisitos

- Docker + Docker Compose
- Imagem `datastack-fastapi:latest` disponível localmente
- Rede Docker `exceptions-net` criada:
  ```bash
  docker network create exceptions-net
  ```

### Subir o container

```bash
docker-compose up -d
```

O código é montado como volume — qualquer alteração nos arquivos `.py` é recarregada automaticamente pelo Uvicorn (`--reload`).

### Logs em tempo real

```bash
docker logs -f exceptions-api
```

### Parar

```bash
docker-compose down
```

---

## Acesso

| Acesso           | URL                                      |
|------------------|------------------------------------------|
| Interface web    | `http://localhost:8011/`                 |
| Docs interativos | `http://localhost:8011/docs`             |
| Health check     | `http://localhost:8011/health`           |

> Em produção a aplicação é acessada através de um nginx reverso em `/exceptions-api/`.  
> As rotas estão registradas com e sem esse prefixo para suportar os dois cenários.

---

## Endpoints

### Autenticação

Os endpoints de `/exceptions` exigem os headers:

```
X-Username: <APP_USERNAME>
X-Password: <APP_PASSWORD>
```

---

### Micro-eventos (`/micro-events`)

| Método   | Rota                          | Descrição                                      |
|----------|-------------------------------|------------------------------------------------|
| `GET`    | `/micro-events`               | Lista paradas com filtros                      |
| `POST`   | `/micro-events`               | Cria parada manual (`Ind_Manual = 1`)          |
| `DELETE` | `/micro-events/{cod_record}`  | Remove parada manual (somente `Ind_Manual = 1`)|
| `PATCH`  | `/micro-events/classification`| Atualiza classificação de paradas              |
| `GET`    | `/micro-events/equipments`    | Lista equipamentos disponíveis                 |
| `GET`    | `/micro-events/natures`       | Lista naturezas (banco)                        |
| `GET`    | `/micro-events/reason-codes`  | Lista códigos de motivo (filtrável por equip.) |
| `GET`    | `/micro-events/stop-classifications` | Lista classificações de parada          |

**Filtros disponíveis em `GET /micro-events`:**

| Parâmetro       | Tipo     | Descrição                      |
|-----------------|----------|--------------------------------|
| `val_equipment` | string   | Filtra por equipamento         |
| `val_workcenter`| string   | Filtra por linha de produção   |
| `cod_nature`    | int      | Filtra por natureza            |
| `cod_shift`     | string   | Filtra por turno (1, 2 ou 3)  |
| `dtt_start`     | datetime | Data/hora inicial              |
| `dtt_end`       | datetime | Data/hora final                |
| `limit`         | int      | Máximo de registros (padrão: 200, máx: 1000) |

---

### Exceções (`/exceptions`) — requer autenticação

| Método | Rota          | Descrição                       |
|--------|---------------|---------------------------------|
| `POST` | `/exceptions` | Registra uma exceção de parada  |
| `GET`  | `/exceptions` | Lista exceções registradas      |

**Body de `POST /exceptions`:**

```json
{
  "val_equipment":        "P278",
  "val_workcenter":       "LINHA_01",
  "val_department":       "Engenharia",
  "dtt_start":            "2026-06-17T08:00:00",
  "dtt_end":              "2026-06-17T09:00:00",
  "cod_exception_reason": 5,
  "des_comment":          "Manutenção preventiva programada"
}
```

---

### Motivos de exceção (`/exception-reasons`)

| Método | Rota                   | Descrição                       |
|--------|------------------------|---------------------------------|
| `GET`  | `/exception-reasons`   | Lista motivos de exceção ativos |

---

## Interface web

Acessível em `http://localhost:8011/`, a interface permite:

- **Pesquisar** micro-eventos com filtros (equipamento, turno, natureza, período)
- **Justificar paradas** — registra exceção para os eventos selecionados
- **Mudar classificação** — atualiza classificação (UPDT / PDT / QUEBRA) em lote
- **Novo lançamento** — cria uma parada manual diretamente pelo formulário
- Ordenação por coluna, paginação e seleção múltipla

---

## Integração com Grafana

A interface web é exibida dentro do Grafana através de um painel **Apache ECharts** (plugin `volkovlabs-echarts-panel`). O script abaixo é colado no campo _Function_ do painel e injeta um `<iframe>` apontando para a aplicação:

```js
// pagina_grafana.js
const url = 'http://10.113.120.120/exceptions-api/';

const container = context.panel.chart.getDom(); // elemento raiz do painel ECharts
container.innerHTML = '';

const iframe = document.createElement('iframe');
iframe.src = url;
iframe.style.width   = '100%';
iframe.style.height  = '100%';
iframe.style.border  = '0';
iframe.style.display = 'block';
iframe.style.background = '#111217';

container.style.overflow = 'hidden';
container.appendChild(iframe);

return {};
```

**Como funciona:**

1. O Grafana renderiza o painel ECharts e chama a função com o contexto do painel.
2. `context.panel.chart.getDom()` retorna o elemento DOM raiz do canvas do ECharts.
3. O script limpa o conteúdo padrão e injeta um `<iframe>` que carrega a URL da aplicação.
4. O painel passa a exibir a interface de lançamentos de paradas diretamente dentro do Grafana, sem abrir uma nova aba.

> O nginx em `10.113.120.120` roteia `/exceptions-api/` para o container desta aplicação e faz strip do prefixo antes de encaminhar ao FastAPI.

---

## Desenvolvimento

O arquivo `.dockerignore` evita que `__pycache__`, `.env` e arquivos desnecessários sejam copiados para o container.

Para adicionar dependências Python, edite o `requirements.txt` e reinicie o container:

```bash
docker-compose restart
```

As dependências são instaladas automaticamente na inicialização pelo comando do `docker-compose.yml`.
