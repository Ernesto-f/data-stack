from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.ssh.operators.ssh import SSHOperator


# Pasta onde esta DAG fica no servidor Airflow.
# Exemplo: /srv/data-stack/domains/data-platform/orchestration/dags/UDI/FABRIC
DAG_DIR = Path(__file__).resolve().parent

# YAML mantido no Airflow, junto da DAG.
LOCAL_JOB_YAML = DAG_DIR / "jobs/"/"tests"/"copiar_apenas_stpo.yaml"

# Pasta do projeto na maquina Windows acessada via SSH (NÃO ALTERAR!!!).
REMOTE_PROJECT_DIR = r"C:\Users\85161000\Documents\Fabric_Copy_Automation_Airflow\ETL_FABRIC_SAP" 

# Nome remoto para evitar depender do YAML produtivo fixo do Windows.
REMOTE_JOB_NAME = "copiar_apenas_stpo.yaml"
REMOTE_JOB_YAML = rf"{REMOTE_PROJECT_DIR}\jobs\{REMOTE_JOB_NAME}"


with DAG(
    dag_id="Z_insert_lote_wm_specification_exemplo",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["Bronze Ingest", "postgres", "owner_Sampaio", "UDI", "Fabric", "WM Specification","Exemplo"],
) as dag:
    start = EmptyOperator(task_id="start")

    copiar_yaml_para_windows_stpo_exemplo = SFTPOperator(
        task_id="copiar_yaml_para_windows_stpo",
        ssh_conn_id="pc_apex_ssh_fabric",
        operation="put",
        local_filepath=str(LOCAL_JOB_YAML),
        remote_filepath=REMOTE_JOB_YAML,
        create_intermediate_dirs=True,
    )

    rodar_bom_windows_apex_exemplo = SSHOperator(
        task_id="rodar_bom_windows_apex",
        ssh_conn_id="pc_apex_ssh_fabric",
        command=rf'cmd /c "{REMOTE_PROJECT_DIR}\run.bat jobs\{REMOTE_JOB_NAME}"',
        cmd_timeout=60 * 60,
    )

    wait_01 = BashOperator(task_id="wait_01", bash_command="sleep 4")
    
    wait_02 = BashOperator(task_id="wait_02", bash_command="sleep 4")

    end = EmptyOperator(task_id="end")

    start >> wait_01 >> copiar_yaml_para_windows_stpo_exemplo >> rodar_bom_windows_apex_exemplo >> wait_02 >> end
