
#=================================================================================================================================
#
#LOGICA DE EXECUÇÃO
#
# INICIO!
# PRIMEIRA TASK = COPIA HISTORICO DO BANCO DE ORIGEM
#  -> Regra: Enquanto tiver registro historico, encerra execução e roda no proximo schedule a PRIMEIRA TASK.
#
# SEGUNDA TASK = ATUALIZA OS DADOS DE HOJE FEITOS PELA TABELA ORIGEM
#   -> Regra: - Deleta os dados de hoje da tabela destino, 
#             - Copia os dados na tabela origem
#             - Inseri no destino
# FIM!
#=================================================================================================================================



# =============================================================================
# Função genérica: copia incremental em lote com fast_executemany
# =============================================================================


# PARAMETROS
# conn_origem_id: nome da connection cadastrada no airflow, que você ira buscar a informação Ex.: "udi_DW_Chronos",
# conn_destino_id: nome da connection cadastrada no airflow, que você ira inserir a informação Ex.: "udi_DW_Chronos",
# tabela_origem: nome da tabela que você ira buscar a informação Ex.: "db_mcExperienceSuite.mesAdapter.tb_ProductionOrder",
# tabela_destino: nome da tabela que você ira inserir a insormação Ex.: "db_mcExperienceSuite.mesAdapter.tb_ProductionOrder",
# colunas_busca: nome das colunas + tratamento , de onde voce ira buscar a informação, EX.: ["Cod_Record", "Val_WorkDay"],
# colunas_insert: nome das colunas que você ira inserir a informação, EX.: ["Cod_Record", "Val_WorkDay"],
#
#   OBS Colunas_busca e insert = a ordem das colunas precisam ser identicas!!,
#                                voce pode fazer tratamento de dados na coluna busca, 
#                                exemplo DATEADD(DAY, -1, dtt_record) AS dtt_record
#
# joins_list: lista de joins que você fara na consulta de busca, se nao colocar nada, vai sem join.
#               Ex.: joins_list=[
#                        "LEFT JOIN dbo.tb_WorkCenter w ON w.WorkCenterId = t.WorkCenterId",
#                        "LEFT JOIN dbo.tb_Linha l ON l.LinhaId = w.LinhaId",
#                        "LEFT JOIN dbo.tb_Plant p ON p.PlantId = l.PlantId"
#                        ],
#                   Obs.: Colunas de busca precisam ter o nome junto ao alias, se nao vai quebrar
#
#
# chave_id: Nome da coluna que é o ID da tabela, Ex.: "Cod_Record",
#
# lote: Quantidades de registros a serem copiados por execução (Recomendo iniciar com 10, dps.. 1000, limite de 1000),
#
# id_inicial: linha que você quer começar a copiar da tabela origem, Ex.: 224641,
#
# period_filter_status: Ex.:"on"/"off" , se on = copia os dados até o periodo que voce determinou, dtt_record < period_value (period_value sera informado abaixo)
#
# refresh_workday_status: Ex.:"on"/"off", se on = apos copiar os dados históricos, começa a deletar e copiar denovo os dados apartir do period_value selecionado Ex.: dtt_record >= period_value
#
# workday_role: ["workday", "getdate"] = Determina se você quer pegar data de workday (regra das 5:20am é dia anterior) ou getdate = data horario real.
#
# period_column: Qual a coluna que determina a data nas tabelas? ex.: Dtt_Record. SE ESTIVER USANDO ALLIAS (LEFTJOIN) , COLOCAR AQUI TAMBEM ex.: t.Dtt_Record
#
# CALCULO DO PERIODO
# period_name: "HOUR", "DAY", "MONTH", "YEAR" (Quanto que sera removido da data atual)
# period_value: o numero a ser reduzido.,
# Ex.: period_name = "DAY", period_value = -2 => DATEADD(DAY, -2, WORKDAY)


from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator

## FUNÇÃO DE COPIAR LOTES
from commom.proc_bronze_ingest_VIEW_refresh_workday import copiar_lote_incremental_refresh_filter



###############################################################################################################
#                             Definição das tarefas de copiar tabelas incrementais
###############################################################################################################

def copiar_lote_TUATABELA_view() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= sql_udi_chronos,
        conn_destino_id= ,
        tabela_origem= ,
        tabela_destino= ,
        colunas_busca= ,
        colunas_insert= ,
        joins_list= ,
        chave_id= ,
        lote= ,
        id_inicial= ,
        period_filter_status= ,
        refresh_workday_status= ,
        workday_role= ,
        period_column= ,
        period_name= ,
        period_value= ,
    )


###############################################################################################################
#                          Definição das tarefas de atualizar tabelas de cadastro
###############################################################################################################
"""
with DAG(
    dag_id="LEAF_CHRONOS_ingest_bronze_incremental_tables",
    start_date=datetime(2025, 1, 1),
    schedule="*/5 * * * *",
    max_active_runs=1,
    catchup=False,
    default_args={"retries": 0},
    tags=["Bronze Ingest", "sqlserver", "owner_FelipeEidt", "LEAF"],
) as dag_get_tables:

    start = EmptyOperator(task_id="start")

    get_tb_TUATABELA =   PythonOperator(
        task_id="copiar_lote_TUATABELA_view",
        python_callable=copiar_lote_TUATABELA_view,
    )

    get_tb_ProductionOrder = PythonOperator(
        task_id="copiar_lote_leaf_productionorder",
        python_callable=copiar_lote_leaf_productionorder,
    )


    end = EmptyOperator(task_id="end")

    wait_01 = BashOperator(task_id="wait_01", bash_command="sleep 4")


    start \
    >> get_tb_ProductionOrderLog >> wait_01 \
    >> end

    """