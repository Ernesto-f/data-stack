from datetime import datetime
import traceback
import pyodbc

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.mysql.hooks.mysql import MySqlHook


def _odbc_connect_str(conn_id: str) -> str:
    c = BaseHook.get_connection(conn_id)

    odbc_connect = (c.extra_dejson or {}).get("odbc_connect")

    if not odbc_connect:
        raise ValueError(
            f"Connection '{conn_id}' precisa ter Extra JSON com 'odbc_connect'."
        )

    return odbc_connect


def testar_conexao_odbc(conn_id: str):
    odbc_connect = _odbc_connect_str(conn_id)

    try:
        with pyodbc.connect(odbc_connect, timeout=10) as cn:
            cn.autocommit = True

            cur = cn.cursor()

            cur.execute("SELECT 1 AS teste;")
            row = cur.fetchone()
            print(f"[{conn_id}] Conexão ODBC OK! Resultado:", row)

            # SQL Server
            cur.execute("""
                SELECT GETDATE();
            """)
            evid = cur.fetchone()
            print(f"[{conn_id}] Evidências de sessão:", evid)

    except Exception as e:
        print(f"[{conn_id}] ERRO AO TESTAR CONEXÃO VIA ODBC:", e)
        print(traceback.format_exc())
        raise


def testar_conexao_mysql(conn_id: str):
    try:
        hook = MySqlHook(mysql_conn_id=conn_id)

        with hook.get_conn() as cn:
            cur = cn.cursor()

            cur.execute("SELECT 1 AS teste;")
            row = cur.fetchone()
            print(f"[{conn_id}] Conexão MySQL OK! Resultado:", row)

            cur.execute("SELECT NOW();")
            evid = cur.fetchone()
            print(f"[{conn_id}] Evidências de sessão:", evid)

    except Exception as e:
        print(f"[{conn_id}] ERRO AO TESTAR CONEXÃO MYSQL:", e)
        print(traceback.format_exc())
        raise


def testar_conexao_postgres_odbc(conn_id: str):
    odbc_connect = _odbc_connect_str(conn_id)

    try:
        with pyodbc.connect(odbc_connect, timeout=10) as cn:
            cn.autocommit = True

            cur = cn.cursor()

            cur.execute("SELECT 1 AS teste;")
            row = cur.fetchone()
            print(f"[{conn_id}] Conexão PostgreSQL ODBC OK! Resultado:", row)

            # PostgreSQL equivalente ao GETDATE()
            cur.execute("""
                SELECT NOW();
            """)
            evid = cur.fetchone()
            print(f"[{conn_id}] Evidências de sessão:", evid)

            cur.execute("""
                SELECT current_database(), current_user, version();
            """)
            info = cur.fetchone()
            print(f"[{conn_id}] Banco / usuário / versão:", info)

    except Exception as e:
        print(f"[{conn_id}] ERRO AO TESTAR CONEXÃO POSTGRESQL VIA ODBC:", e)
        print(traceback.format_exc())
        raise


# ============ DAG 1: Apex ============
with DAG(
    dag_id="Z_teste_conexao_sqlApex_odbc",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["teste", "sqlserver", "odbc"],
) as dag_apex:

    testar_apex = PythonOperator(
        task_id="testar_conexao_odbc_apex",
        python_callable=testar_conexao_odbc,
        op_kwargs={"conn_id": "sql_udi_apex_azure"},
    )


# ============ DAG 2: Leaf CHRONOS ============
with DAG(
    dag_id="Z_teste_conexao_sqlLeaf_odbc",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["teste", "sqlserver", "odbc"],
) as dag_leaf:

    testar_leaf = PythonOperator(
        task_id="testar_conexao_odbc_leaf",
        python_callable=testar_conexao_odbc,
        op_kwargs={"conn_id": "sql_leaf_chronos"},
    )


# ============ DAG 3: UDI CHRONOS ============
with DAG(
    dag_id="Z_teste_conexao_sql_udi_chronos_odbc",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["teste", "sqlserver", "odbc", "UDI"],
) as dag_udi_chronos:

    testar_udi_chronos = PythonOperator(
        task_id="testar_conexao_odbc_udi",
        python_callable=testar_conexao_odbc,
        op_kwargs={"conn_id": "sql_udi_chronos_reader"},
    )


# ============ DAG 4: UDI GUARDIAN ============
with DAG(
    dag_id="z_teste_conexao_sql_udi_guardian_odbc",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["teste", "sqlserver", "odbc", "UDI"],
) as dag_udi_guardian:

    testar_udi_guardian = PythonOperator(
        task_id="teste_conexao_sql_udi_guardian_odbc",
        python_callable=testar_conexao_odbc,
        op_kwargs={"conn_id": "sql_udi_guardian"},
    )


# ============ DAG 5: GMES ============
with DAG(
    dag_id="Z_teste_conexao_sql_udi_gmes_odbc",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["teste", "sqlserver", "odbc", "UDI"],
) as dag_udi_gmes:

    testar_udi_gmes = PythonOperator(
        task_id="teste_conexao_sql_udi_gmes_odbc",
        python_callable=testar_conexao_odbc,
        op_kwargs={"conn_id": "sql_udi_gmes"},
    )


# ============ DAG 6: PMDNET MYSQL ============
with DAG(
    dag_id="Z_testar_conexao_mysql_pmdnet",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["teste", "mysql", "hook"],
) as dag_pmdnet:

    testar_pmdnet = PythonOperator(
        task_id="testar_conexao_mysql_pmdnet",
        python_callable=testar_conexao_mysql,
        op_kwargs={"conn_id": "sql_udi_pmdnet"},
    )


# ============ DAG 7: PG - DW APEX ============
with DAG(
    dag_id="Z_testar_conexao_pg_dw_apex_odbc",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["teste", "postgres", "odbc", "DW", "APEX"],
) as dag_pg_dw_apex:

    testar_pg_dw_apex = PythonOperator(
        task_id="testar_conexao_pg_dw_apex_odbc",
        python_callable=testar_conexao_postgres_odbc,
        op_kwargs={"conn_id": "pg_dw_apex"},
    )

# ============ DAG 8: PG - LEAF - LDS - MACAW ============
with DAG(
    dag_id="Z_testar_conexao_sql_leaf_lds_macaw_odbc",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["teste", "sql", "odbc", "macaw", "LEAF"],
) as dag_sql_leaf_macaw:

    testar_pg_dw_apex = PythonOperator(
        task_id="testar_conexao_sql_leaf_lds_macaw_odbc",
        python_callable=testar_conexao_postgres_odbc,
        op_kwargs={"conn_id": "sql_leaf_macaw"},
    )