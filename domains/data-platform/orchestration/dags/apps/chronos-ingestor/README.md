# Chronos Ingestor

Projeto simples para buscar dados do Chronos e salvar em banco SQL.

## Instalação

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
```

## Uso

Buscar um dia:

```bash
python -m chronos_ingestor --date 2026-05-01
```

Buscar um período:

```bash
python -m chronos_ingestor --from 2026-05-01 --to 2026-05-07
```

Buscar módulos específicos:

```bash
python -m chronos_ingestor --date 2026-05-01 --modules 13,14
```

Por padrão, apenas módulos com `activate: true` em `data/modulo.json` são buscados. Os eventos são gravados em `data/chronos.sqlite3` na tabela `chronos_events` com `event_id` único.

## SQL Server

Para gravar direto no SQL Server, configure no `.env`:

```env
DATABASE_BACKEND=sqlserver
SQLALCHEMY_URL=mssql+pyodbc://user:password@server/database?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes
SQLSERVER_SCHEMA=mesAdapter
SQLSERVER_TABLE=tb_chronos_events
```

Instale as dependências no ambiente que vai executar o job:

```bash
pip install -r requirements.txt
```

## Airflow

Se o Airflow já possui uma connection SQL Server e a função `get_sqlalchemy_engine`, use `PythonOperator`:

```python
from pathlib import Path
import sys

from airflow.providers.standard.operators.python import PythonOperator
from commom.connections import get_sqlalchemy_engine

DAG_DIR = Path(__file__).resolve().parent
LOCAL_JOB_APP = DAG_DIR.parents[1] / "apps" / "chronos-ingestor"
sys.path.insert(0, str(LOCAL_JOB_APP))

from chronos_ingestor.config import get_settings
from chronos_ingestor.ingest import ingest_chronos


def ingest_chronos_sqlserver(ds=None, **context):
    settings = get_settings()
    engine = get_sqlalchemy_engine("sql_udi_chronos_mirror")
    return ingest_chronos(
        date_start=context["macros"].ds_add(ds, -2),
        date_end=ds,
        settings=settings,
        sql_engine=engine,
    )


insert_chronos = PythonOperator(
    task_id="insert_chronos_sqlserver",
    python_callable=ingest_chronos_sqlserver,
)
```
