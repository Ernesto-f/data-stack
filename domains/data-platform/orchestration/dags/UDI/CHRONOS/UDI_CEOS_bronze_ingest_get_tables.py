from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime

from pathlib import Path

DAG_DIR = Path(__file__).resolve().parent
LOCAL_JOB_APP = DAG_DIR.parents[1] / "apps" / "chronos-ingestor"

with DAG(
    dag_id="ceos_chronos_ingestor_daily",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    max_active_runs=1,
    catchup=True,
    tags=["chronos"],
) as dag:

    ceos_chronos_ingestor_daily = BashOperator(
            task_id="ceos_chronos_ingestor_daily",
           bash_command=(
                f"cd {LOCAL_JOB_APP} && "
                "python -m chronos_ingestor "
                "--from {{ macros.ds_add(ds, -180) }} "
                "--to {{ ds }}"
            ),
        )
    start = EmptyOperator(task_id="start")

    wait_01 = BashOperator(
            task_id="wait_01",
            bash_command="sleep 4",
        )

    wait_02 = BashOperator(
            task_id="wait_02",
            bash_command="sleep 4",
        )

    end = EmptyOperator(task_id="end")

    (
    start 
        >> wait_01 
        >> ceos_chronos_ingestor_daily 
        >> wait_02 >> end
    )