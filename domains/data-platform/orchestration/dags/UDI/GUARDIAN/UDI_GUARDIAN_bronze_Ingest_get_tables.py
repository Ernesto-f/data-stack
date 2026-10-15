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
# conn_origem_id: nome da connection cadastrada no airflow, que você ira buscar a informação,
# conn_destino_id: nome da connection cadastrada no airflow, que você ira inserir a informação,
# tabela_origem: nome da tabela que você ira buscar a informação,
# tabela_destino: nome da tabela que você ira inserir a insormação,
# colunas_busca: nome das colunas + tratamento , de onde voce ira buscar a informação,
# colunas_insert: nome das colunas que você ira inserir a informação,
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
# chave_id: Nome da coluna que é o ID da tabela, Ex.: Cod_Record,
#
# lote: Quantidades de registros a serem copiados por execução (Recomendo iniciar com 10, dps.. 100, limite de 1000),
#
# id_inicial: linha que você quer começar a copiar da tabela origem,
#
# period_filter_status: ["on"/"off"] , se on = copia os dados até o periodo que voce determinou, dtt_record < period_value (period_value sera informado abaixo)
#
# refresh_workday_status: ["on"/"off"], se on = apos copiar os dados históricos, começa a deletar e copiar denovo os dados apartir do period_value selecionado Ex.: dtt_record >= period_value
#
# workday_role: ["workday", "getdate"] = Determina se você quer pegar data de workday (regra das 5:20am é dia anterior) ou getdate = data horario real.
#
# period_column: Qual a coluna que determina a data nas tabelas? ex.: Dtt_Record. SE ESTIVER USANDO ALLIAS (LEFTJOIN) , COLOCAR AQUI TAMBEM ex.: t.Dtt_Record
#
# CALCULO DO PERIODO
# period_name: HOUR, DAY, MONTH, YEAR (Quanto que sera removido da data atual)
# period_value: o numero a ser reduzido.,
# Ex.: period_name = DAY, period_value = -2 => DATEADD(DAY, -2, WORKDAY)



from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from commom.proc_bronze_ingest_VIEW_refresh_workday import copiar_lote_incremental_refresh_filter
from airflow.providers.standard.operators.bash import BashOperator

#LIBS AIRFLOW 3X
from airflow.providers.standard.operators.empty import EmptyOperator
###############################################################################################################
#                             Definição das tarefas de copiar tabelas incrementais
###############################################################################################################

def copiar_lote_vwdatasetrelatorio_pg() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_udi_guardian",
        conn_destino_id="pg_dw_apex" ,
        tabela_origem= "GUARDIAN_BAT.dbo.VWDATASETRELATORIO R",
        tabela_destino="apex_dw_raw.udi_guardian.tb_datasetrelatorio" ,
        colunas_busca = [
            "R.TICKET_SEQUENCIAL",
            "R.TICKET_PLACA_CARRETA",
            "R.TICKET_ESTADO",
            "R.TICKET_OBSERVACAO",
            "R.TICKET_DATA",
            "R.TICKET_MANUAL",
            "R.INICIAL_OPERACAO_DATA",
            "R.INICIAL_OPERACAO_PESO",
            "R.FINAL_OPERACAO_DATA",
            "R.FINAL_OPERACAO_PESO",
            "R.OPERACAO_PESO_TARA",
            "R.OPERACAO_PESO_BRUTO",
            "R.OPERACAO_PESO_LIQUIDO",
            "R.TRANSPORTADORA_CNPJ",
            "R.TRANSPORTADORA_RAZAO_SOCIAL",
            "I.ITM_CODIGO",
            "I.ITM_NOME",
            "R.CAMPO_ADICIONAL_1",
            "R.CAMPO_ADICIONAL_2",
            "R.CAMPO_ADICIONAL_3",
            "R.CAMPO_ADICIONAL_4",
            "R.CAMPO_ADICIONAL_5",
            "R.CAMPO_ADICIONAL_6",
            "R.CAMPO_ADICIONAL_7",
            "R.CAMPO_ADICIONAL_8",
            "R.CAMPO_ADICIONAL_9",
            "R.CAMPO_ADICIONAL_10"
        ],
        colunas_insert = [
            "ticket_sequencial",
            "ticket_placa_carreta",
            "ticket_estado",
            "ticket_observacao",
            "ticket_data",
            "ticket_manual",
            "inicial_operacao_data",
            "inicial_operacao_peso",
            "final_operacao_data",
            "final_operacao_peso",
            "operacao_peso_tara",
            "operacao_peso_bruto",
            "operacao_peso_liquido",
            "transportadora_cnpj",
            "transportadora_razao_social",
            "itm_codigo",
            "itm_nome",
            "campo_adicional_1",
            "campo_adicional_2",
            "campo_adicional_3",
            "campo_adicional_4",
            "campo_adicional_5",
            "campo_adicional_6",
            "campo_adicional_7",
            "campo_adicional_8",
            "campo_adicional_9",
            "campo_adicional_10"
        ],
        joins_list=["LEFT JOIN GUARDIAN_BAT.dbo.TBITENSTICKET ITK ON R.TICKET_SEQUENCIAL= ITK.TCK_SEQUENCIAL",
                    "LEFT JOIN GUARDIAN_BAT.dbo.TBITEM I ON ITK.ITM_SEQUENCIAL = I.ITM_SEQUENCIAL"] ,
        chave_id="R.ticket_sequencial" ,
        lote=10000 ,
        id_inicial=1,
        period_filter_status="on" ,
        refresh_workday_status="on" ,
        workday_role="getdate" ,
        period_column="R.ticket_data" ,
        period_name="DAY" ,
        period_value= -14 ,
    )



# =============================================================================
# DAG
# =============================================================================

with DAG(
    dag_id="UDI_GUARDIAN_ingest_bronze_incremental_tables",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    max_active_runs=1,
    catchup=False,
    default_args={
        "retries": 0,
        "depends_on_past": False
    },
    tags=["Bronze Ingest", "SQL Server", "owner_FelipeEidt", "UDI", "GUARDIAN"],
) as dag_get_tables_guardian:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    get_tb_VWDATASETRELATORIO_pg = PythonOperator(
        task_id="Copiar_tabela_VWDATASETRELATORIO_pg",
        python_callable=copiar_lote_vwdatasetrelatorio_pg,
    )

    start \
    >> get_tb_VWDATASETRELATORIO_pg \
    >> end

    