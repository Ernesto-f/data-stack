from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.ssh.operators.ssh import SSHOperator


DAG_DIR = Path(__file__).resolve().parent

LOCAL_JOB_YAML = DAG_DIR / "jobs" / "leaf" / "LQS_dsp_leaf_lamina_moisture_v3.yaml"
LOCAL_SQL_FILE = DAG_DIR / "jobs" / "queries" / "Dados Fabric LQS SKU PMD v2.sql"

REMOTE_PROJECT_DIR = r"C:\Users\85161000\Documents\Fabric_Copy_Automation_Airflow\ETL_FABRIC_SAP"

REMOTE_JOB_NAME = "LQS_dsp_leaf_lamina_moisture_v3.yaml"
REMOTE_SQL_NAME = "Dados Fabric LQS SKU PMD v2.sql"

REMOTE_JOB_RELATIVE_PATH = rf"jobs\leaf\{REMOTE_JOB_NAME}"

REMOTE_JOB_YAML = rf"{REMOTE_PROJECT_DIR}\jobs\leaf\{REMOTE_JOB_NAME}"
REMOTE_SQL_FILE = rf"{REMOTE_PROJECT_DIR}\jobs\queries\{REMOTE_SQL_NAME}"


def escolher_copia_yaml(**context):
    dag_run = context.get("dag_run")
    copiar_yaml = dag_run.conf.get("copiar_yaml", False) if dag_run and dag_run.conf else False

    if copiar_yaml:
        return "copiar_yaml_para_windows"

    return "skip_copia_yaml"


with DAG(
    dag_id="insert_lote_lqs_leaf_lamina_moisture_v3",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    schedule="0 0 10 * 6",
    default_args={"retries": 0},
    tags=["Bronze Ingest", "postgres", "owner_Sampaio", "UDI", "Fabric", "Moisture", "Leaf"],
) as dag:
    start = EmptyOperator(task_id="start")

    wait_01 = BashOperator(
        task_id="wait_01",
        bash_command="sleep 4",
    )

    decidir_copia_yaml = BranchPythonOperator(
        task_id="decidir_copia_yaml",
        python_callable=escolher_copia_yaml,
    )

    copiar_yaml_para_windows = SFTPOperator(
        task_id="copiar_yaml_para_windows",
        ssh_conn_id="pc_apex_ssh_fabric",
        operation="put",
        local_filepath=str(LOCAL_JOB_YAML),
        remote_filepath=REMOTE_JOB_YAML,
        create_intermediate_dirs=True,
    )

    skip_copia_yaml = EmptyOperator(
        task_id="skip_copia_yaml",
    )

    juntar_fluxo = EmptyOperator(
        task_id="juntar_fluxo",
        trigger_rule="none_failed_min_one_success",
    )

    copiar_sql_para_windows = SFTPOperator(
        task_id="copiar_sql_para_windows",
        ssh_conn_id="pc_apex_ssh_fabric",
        operation="put",
        local_filepath=str(LOCAL_SQL_FILE),
        remote_filepath=REMOTE_SQL_FILE,
        create_intermediate_dirs=True,
    )

    rodar_leaf_windows_apex = SSHOperator(
        task_id="rodar_leaf_windows_apex",
        ssh_conn_id="pc_apex_ssh_fabric",
        command=rf'cmd /c "{REMOTE_PROJECT_DIR}\run.bat {REMOTE_JOB_RELATIVE_PATH}"',
        cmd_timeout=60 * 60,
    )

    atualizar_offset_yaml_windows = SSHOperator(
        task_id="atualizar_offset_yaml_windows",
        ssh_conn_id="pc_apex_ssh_fabric",
        command=rf'cmd /c "cd /d {REMOTE_PROJECT_DIR} && python advance_mdq_yaml_offset.py --yaml {REMOTE_JOB_RELATIVE_PATH}"',
        cmd_timeout=10 * 60,
    )

    wait_02 = BashOperator(
        task_id="wait_02",
        bash_command="sleep 4",
    )

    end = EmptyOperator(task_id="end")

    start >> wait_01 >> decidir_copia_yaml

    decidir_copia_yaml >> copiar_yaml_para_windows >> juntar_fluxo
    decidir_copia_yaml >> skip_copia_yaml >> juntar_fluxo

    (
        juntar_fluxo
        >> copiar_sql_para_windows
        >> rodar_leaf_windows_apex
        >> atualizar_offset_yaml_windows
        >> wait_02
        >> end
    )