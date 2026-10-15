from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="MAINTENANCE_airflow_db_cleanup",
    description="Limpeza semanal de metadados do Airflow mantendo 30 dias",
    start_date=datetime(2026, 1, 1),
    schedule="0 4 * * 0",  # Domingo às 04h
    catchup=False,
    max_active_runs=1,
    tags=["maintenance", "airflow", "database"],
) as dag:

    db_cleanup = BashOperator(
        task_id="cleanup_old_airflow_metadata",
        bash_command="""
        echo "Iniciando limpeza do Airflow: $(date)"

        airflow db clean \
          --clean-before-timestamp "$(date -u -d '30 days ago' +%F)" \
          --yes

        echo "Finalizada limpeza do Airflow: $(date)"
        """
    )