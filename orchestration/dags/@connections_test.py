from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


## ================================== ##
## === TESTES DE CONEXAO COM SQL  === ## 
## ================================== ##

## TESTES DE CONEXAO: Servidor SQL Apex
def testar_conexao():
    try:
        hook = MsSqlHook(mssql_conn_id="sql_udi_apex_azure")  
        resultado = hook.get_first("SELECT 1 AS teste")
        print("Conexão OK! Resultado:", resultado)
    except Exception as e:
        import traceback
        print("ERRO AO TESTAR CONEXÃO:", e)
        print(traceback.format_exc())
        # re-levanta o erro pra task aparecer como FAILED no Airflow
        raise


with DAG(
    dag_id="dag_teste_conexao_sqlApex2",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # roda só manualmente
    catchup=False,
    tags=["teste", "sqlserver"],
) as dag:

    testar = PythonOperator(
        task_id="testar_conexao",
        python_callable=testar_conexao,
    )



## TESTES DE CONEXAO: Servidor SQL Leaf
def testar_conexao_leaf():
    try:
        hook = MsSqlHook(mssql_conn_id="sql_leaf_chronos")
        resultado = hook.get_first("SELECT 1 AS teste")
        print("Conexão OK! Resultado:", resultado)
    except Exception as e:
        import traceback
        print("ERRO AO TESTAR CONEXÃO:", e)
        print(traceback.format_exc())
        raise


with DAG(
    dag_id="dag_teste_conexao_sqlLeaf",
    start_date=datetime(2025, 1, 1),
    schedule=None, 
    catchup=False,
    tags=["teste", "sqlserver"],
) as dag:

    testar_ = PythonOperator(
        task_id="testar_conexao_leaf",
        python_callable=testar_conexao_leaf,
    )


## TESTES DE CONEXAO: Servidor SQL UDI
def testar_conexao_udi():
    try:
        hook = MsSqlHook(mssql_conn_id="sql_udi_chronos")

        engine = hook.get_sqlalchemy_engine()
        print("SQLAlchemy URL (sem senha):", engine.url.render_as_string(hide_password=True))
        print("Dialect:", engine.dialect.name)
        print("Driver:", getattr(engine.dialect, "driver", None))

        # Executa via SQLAlchemy (pyodbc), sem passar pelo pymssql
        with engine.connect() as conn:
            row = conn.exec_driver_sql("SELECT 1 AS teste").first()
            print("Conexão OK! Resultado:", row)

            evid = conn.exec_driver_sql("""
                SELECT
                    SUSER_SNAME()    AS suser,
                    ORIGINAL_LOGIN() AS original_login,
                    HOST_NAME()      AS host_name,
                    APP_NAME()       AS app_name,
                    @@SPID           AS spid;
            """).first()
            print("Evidências de sessão:", evid)

    except Exception as e:
        import traceback
        print("ERRO AO TESTAR CONEXÃO:", e)
        print(traceback.format_exc())
        raise

with DAG(
    dag_id="dag_teste_conexao_sql_udi",
    start_date=datetime(2025, 1, 1),
    schedule=None,          # apenas manual
    catchup=False,
    tags=["teste", "sqlserver", "udi"],
) as dag:

    testar = PythonOperator(
        task_id="testar_conexao_udi",
        python_callable=testar_conexao_udi,
    )

