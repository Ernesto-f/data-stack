from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from airflow.providers.standard.operators.bash import BashOperator


#LIBS AIRFLOW 3X
from airflow.providers.standard.operators.empty import EmptyOperator


## FUNÇÃO DE COPIAR LOTES
from commom.proc_bronze_ingest_VIEW_refresh_workday import copiar_lote_incremental_refresh_filter
from commom.proc_bronza_ingest_tabelaSemID import copiar_lote_incremental_refresh_filter_tabelaSemID


from functools import wraps
import logging

from airflow.sdk import Variable

from commom.connections import get_sqlalchemy_engine

import math
import os
import re
import boto3
import pandas as pd
import io

# ===============================================
# ENV
# Variaveis de ambiente compartilhadas com apps.
# ===============================================
S3_ENDPOINT = os.getenv("S3_ENDPOINT") or os.getenv("UPLOAD_S3_ENDPOINT") or "http://minio:9000"
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY") or os.getenv("UPLOAD_S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY") or os.getenv("UPLOAD_S3_SECRET_KEY")
BUCKET = "utilidades"
PREFIX = "riedel/"
RECURSIVE = True 
UPLOAD_PG_CONN_STR = os.getenv("UPLOAD_PG_CONN_STR")
CHUNKSIZE = 1000
SQL_CONNECTION = "sql_udi_chronos_mirror"


if not S3_ACCESS_KEY or not S3_SECRET_KEY:
    raise RuntimeError(
        "Credenciais S3 não definidas. Verifique S3_ACCESS_KEY/S3_SECRET_KEY "
        "ou UPLOAD_S3_ACCESS_KEY/UPLOAD_S3_SECRET_KEY."
    )


s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
)

# =============================================================================
# Wrappers por tabelas
# ============================================================================= 




def run_status(python_callable, counter_name: str, every: int = 4):
    @wraps(python_callable)
    def wrapper(*args, **kwargs):
        counter = int(Variable.get(counter_name, default="0"))
        counter += 1

        if counter >= every:
            Variable.set(counter_name, "0")
            logging.info(
                "[INFO] Executando task agora. counter=%s. Resetando counter para 0.",
                counter,
            )
            return python_callable(*args, **kwargs)

        Variable.set(counter_name, str(counter))
        logging.info(
            "[INFO] Task agendada para rodar na execução %s. counter=%s. Pulando task.",
            every,
            counter,
        )

        return None  # task termina com sucesso, mas não executa a lógica real

    return wrapper


def insert_lote_csv_tb_sacu() -> None:
    response = s3.list_objects(Bucket=BUCKET)
    files = pd.DataFrame(data=response['Contents'],columns=["Key","LastModified"])
    files["LastModified"] = pd.to_datetime(files["LastModified"], unit='s')
    if files.size == 0:
       print("Nenhum item válido com name e lastModified")
       return
    files.sort_values(by=['LastModified'],ascending=True)
    last_item_inserted = files.iloc[-1]
    resp = s3.get_object(Bucket=BUCKET, Key=last_item_inserted['Key'])
    df   =  pd.read_csv(io.BytesIO(resp['Body'].read()))
    df = df.convert_dtypes()
    df ['collected_at'] = pd.to_datetime(df['collected_at'])
    if df.size == 0:
        print("CSV sem linhas validas para inserir")
        return

    engine = get_sqlalchemy_engine(SQL_CONNECTION)
    inserted_rows = len(df)
    df.to_sql(
        "tb_riedel_tags",
        engine,
        schema="mesAdapter",
        if_exists="append",
        index=False,
        chunksize=CHUNKSIZE,
    )
    source_file = df["source_file"].dropna().iloc[-1] if "source_file" in df.columns else last_item_inserted["Key"]
    preview_sql = """
        SELECT TOP (5)
            collected_at,
            equipment,
            tag_name,
            value_num,
            quality,
            source_file
        FROM mesAdapter.tb_riedel_tags me WITH(NOLOCK)
        WHERE source_file = ?
        ORDER BY collected_at DESC
    """
    preview = pd.read_sql_query(preview_sql, engine, params=(source_file,))

    print(f"Inseridas {inserted_rows} linhas em mesAdapter.tb_riedel_tags")
    print(f"Arquivo inserido: {source_file}")
    print("Amostra confirmada no banco:")
    print(preview.to_string(index=False))

    return {
        "inserted_rows": inserted_rows,
        "source_file": source_file,
        "preview_rows": len(preview),
    }

    


# =============================================================================
# DAG
# =============================================================================
with DAG(
    dag_id="UDI_SACU_ingest_bronze_incremental_tables",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    max_active_runs=1,
    catchup=False,
    default_args={"retries": 0},
    tags=["Bronze Ingest", "sqlserver", "owner_Sampaio", "UDI", "SACU"],
) as dag_insert_lote_sacu_csv:

    wait_01 = BashOperator(task_id="wait_01", bash_command="sleep 4")

    start = EmptyOperator(task_id="start")

    end = EmptyOperator(task_id="end")



    insert_lote_csv_tb_sacu = PythonOperator(
        task_id="insert_lote_csv_tb_sacu",
        python_callable=insert_lote_csv_tb_sacu,
    )

    
    

    start \
    >> insert_lote_csv_tb_sacu >> wait_01 \
    >> end