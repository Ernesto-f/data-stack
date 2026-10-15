from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.sdk import TaskGroup


DAG_DIR = Path(__file__).resolve().parent

REMOTE_PROJECT_DIR = r"C:\Users\85161000\Documents\Fabric_Copy_Automation_Airflow\ETL_FABRIC_SAP"
SSH_CONN_ID = "pc_apex_ssh_fabric"
FABRIC_POOL = "fabric_windows_pool"

MDQ_JOBS = [
    ("mara", "01_sv_ecc_mara.yaml"),
    ("makt", "02_sv_ecc_makt.yaml"),
    ("marc", "03_sv_ecc_marc.yaml"),
    ("marm", "04_sv_ecc_marm.yaml"),
    ("ausp", "05_sv_ecc_ausp.yaml"),
    ("cabn", "06_sv_ecc_cabn.yaml"),
    ("cabnt", "07_sv_ecc_cabnt.yaml"),
    ("cawn", "08_sv_ecc_cawn.yaml"),
    ("cawnt", "09_sv_ecc_cawnt.yaml"),
    ("afko", "10_sv_ecc_afko.yaml"),
    ("afpo", "11_sv_ecc_afpo.yaml"),
    ("resb", "12_sv_ecc_resb.yaml"),
    ("mast", "13_sv_ecc_mast.yaml"),
    ("estph", "14_sv_ecc_estph.yaml"),
    ("estpp", "15_sv_ecc_estpp.yaml"),
]


DAG_DOC_MD = """
## MDQ Specification Tables

Esta DAG carrega as tabelas base usadas pelas 44 queries de Master Data Quality.

### Desenho

- Um `TaskGroup` por tabela MDQ.
- Cada tabela tem sua propria copia de YAML, carga e avanco de offset.
- As tabelas rodam de forma independente; falha em uma tabela nao bloqueia as demais.
- O pool `fabric_windows_pool` limita concorrencia contra Windows/Fabric.

### YAML Vivo

Em execucoes agendadas, `dag_run.conf` vem vazio e a DAG nao copia o YAML por padrao.

- Primeira carga/reset manual: usar `{"copiar_yaml": true}`.
- Execucao diaria normal: sem `conf`, preservando o offset do YAML no Windows.

### Pool Obrigatorio

Criar no Airflow UI:

```text
Admin -> Pools -> fabric_windows_pool
Slots sugeridos: 2 ou 3
```

### Offset

Depois de cada carga com sucesso, `advance_mdq_yaml_offset.py` atualiza o YAML no Windows:

```text
novo_offset = offset_atual + incremento_configurado
force_reload = false
```
"""


DOC_DECIDIR_COPIA = """
Decide se o YAML local do Airflow deve sobrescrever o YAML vivo no Windows.

- `copiar_yaml=true`: copia YAML para Windows, usado em primeira carga/reset.
- default `false`: pula copia para preservar o offset ja avancado no Windows.
"""


DOC_COPIAR_YAML = """
Copia o YAML da tabela para o Windows.

Use apenas quando `copiar_yaml=true`, porque sobrescrever o YAML vivo pode resetar o offset.
"""


DOC_COPIAR_LOTE = """
Executa `run.bat` no Windows para carregar um lote da tabela Fabric para PostgreSQL.

Esta task usa `fabric_windows_pool` para limitar concorrencia e proteger Fabric/Windows.
"""


DOC_ATUALIZAR_OFFSET = """
Avanca o offset do YAML vivo no Windows apos carga bem sucedida.

Se a carga falhar, esta task nao roda e o offset permanece inalterado para retry seguro.
"""


DOC_VALIDAR_POOL = """
Valida cedo se o pool `fabric_windows_pool` existe e tem slots.

Isso evita descobrir erro de configuracao somente depois que varias tasks ja foram agendadas.
"""


def escolher_copia_yaml(task_id_copy: str, task_id_skip: str, **context) -> str:
    dag_run = context.get("dag_run")
    conf = dag_run.conf or {} if dag_run else {}
    copiar_yaml = conf.get("copiar_yaml", False)

    if isinstance(copiar_yaml, str):
        copiar_yaml = copiar_yaml.strip().lower() in ("true", "1", "yes", "sim", "s")

    return task_id_copy if copiar_yaml else task_id_skip


with DAG(
    dag_id="insert_lote_mdq_specification_tables",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    schedule="*/55 * * * *",
    default_args={"retries": 0},
    tags=["Bronze Ingest", "postgres", "owner_Sampaio", "UDI", "Fabric", "MDQ", "Specification"],
    doc_md=DAG_DOC_MD,
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="all_done")

    groups = []

    for short_name, yaml_name in MDQ_JOBS:
        local_job_yaml = DAG_DIR / "jobs" / "mdq" / yaml_name
        remote_job_relative_path = rf"jobs\mdq\{yaml_name}"
        remote_job_yaml = rf"{REMOTE_PROJECT_DIR}\{remote_job_relative_path}"

        with TaskGroup(group_id=f"tb_{short_name}") as group:
            decidir_copia_yaml = BranchPythonOperator(
                task_id="decidir_copia_yaml",
                python_callable=escolher_copia_yaml,
                doc_md=DOC_DECIDIR_COPIA,
                op_kwargs={
                    "task_id_copy": f"tb_{short_name}.copiar_yaml_para_windows",
                    "task_id_skip": f"tb_{short_name}.skip_copia_yaml",
                },
            )

            copiar_yaml_para_windows = SFTPOperator(
                task_id="copiar_yaml_para_windows",
                ssh_conn_id=SSH_CONN_ID,
                doc_md=DOC_COPIAR_YAML,
                pool=FABRIC_POOL,
                priority_weight=1,
                operation="put",
                local_filepath=str(local_job_yaml),
                remote_filepath=remote_job_yaml,
                create_intermediate_dirs=True,
            )

            skip_copia_yaml = EmptyOperator(task_id="skip_copia_yaml")

            juntar_fluxo = EmptyOperator(
                task_id="juntar_fluxo",
                trigger_rule="none_failed_min_one_success",
            )

            copiar_lote = SSHOperator(
                task_id="copiar_lote",
                ssh_conn_id=SSH_CONN_ID,
                doc_md=DOC_COPIAR_LOTE,
                pool=FABRIC_POOL,
                priority_weight=10,
                command=rf'cmd /c "{REMOTE_PROJECT_DIR}\run.bat {remote_job_relative_path}"',
                cmd_timeout=60 * 60,
            )

            atualizar_offset = SSHOperator(
                task_id="atualizar_offset",
                ssh_conn_id=SSH_CONN_ID,
                doc_md=DOC_ATUALIZAR_OFFSET,
                pool=FABRIC_POOL,
                priority_weight=5,
                command=rf'cmd /c "cd /d {REMOTE_PROJECT_DIR} && python advance_mdq_yaml_offset.py --yaml {remote_job_relative_path}"',
                cmd_timeout=10 * 60,
            )

            decidir_copia_yaml >> copiar_yaml_para_windows >> juntar_fluxo
            decidir_copia_yaml >> skip_copia_yaml >> juntar_fluxo
            juntar_fluxo >> copiar_lote >> atualizar_offset

        start >> group
        groups.append(group)

    groups >> end
