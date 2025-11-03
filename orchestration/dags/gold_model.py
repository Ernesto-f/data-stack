from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def tarefa_exemplo():
    print("Pipeline funcionando! ✅")


# Definição da DAG
with DAG(
    dag_id="dag_gold",
    description="DAG simples de teste",
    start_date=datetime(2025, 1, 1),
    schedule_interval=timedelta(minutes=5),  # Roda a cada 5 minutos
    catchup=False,
    tags=["teste"],
) as dag:

    tarefa = PythonOperator(
        task_id="tarefa_exemplo",
        python_callable=tarefa_exemplo
    )

    tarefa