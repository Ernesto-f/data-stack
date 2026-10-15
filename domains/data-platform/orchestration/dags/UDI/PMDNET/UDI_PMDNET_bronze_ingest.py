
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
def copiar_lote_rastreabilidade_caixas_view() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_udi_pmdnet",
        conn_destino_id="pg_dw_apex" ,
        tabela_origem= "pmdnet.rastreabilidade_caixa R",
        tabela_destino="apex_dw_raw.udi_pmdnet.tb_container_read" ,
        colunas_busca= ["R.ID",
                        "R.NUMERO_ORDEM_PRODUCAO",
                        "R.NUMERO_OPERACAO",
                        "R.NUMERO_MISTURA",
                        "R.CODIGO_BARRAS_CAIXA",
                        "L.NOME_LINHA",
                        "R.DATA_LANCAMENTO",
                        "U.NOME_USINA",
                        "R.ANO_SAFRA"],
        colunas_insert= ["id",
                        "numero_ordem_producao",
                        "numero_operacao",
                        "numero_mistura",
                        "codigo_barras_caixa",
                        "nome_linha",
                        "data_lancamento",
                        "nome_usina",
                        "ano_safra",] ,
        joins_list=["LEFT JOIN pmdnet.cadastro_linha L ON R.ID_LINHA=L.ID",
                    "LEFT JOIN pmdnet.cadastro_usina U ON R.CODIGO_USINA = U.CODIGO_USINA"] ,
        chave_id="R.id" ,
        lote=100 ,
        id_inicial=1109525 ,
        period_filter_status="off" ,
        refresh_workday_status="off" ,
        workday_role=None ,
        period_column=None ,
        period_name=None ,
        period_value=None ,
    )

def copiar_lote_registro_acompanhamento_processo() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_udi_pmdnet",
        conn_destino_id="pg_dw_apex" ,
        tabela_origem= "pmdnet.registro_acompanhamento_processo",
        tabela_destino="apex_dw_raw.udi_pmdnet.tb_registro_acompanhamento_processo" ,
        colunas_busca= ["ID",
                        "DATA_HORA_EVENTO",
                        "CONCAT(YEAR(DATA_HORA_EVENTO),LPAD(MONTH(DATA_HORA_EVENTO), 2, '0')) AS ANO_MES",
                        "ID_PONTO_COLETA",
                        "NUMERO_OPERACAO",
                        "NUMERO_MISTURA",
                        "NUMERO_ORDEM_PRODUCAO",
                        "LIC",
                        "SP",
                        "LSC",
                        "RESULTADO",
                        "NUMERO_OPERADOR",
                        "DATA_LANCAMENTO",
                        ],
        colunas_insert= ["id",
                        "data_hora_evento",
                        "ano_mes",
                        "id_ponto_coleta",
                        "numero_operacao",
                        "numero_mistura",
                        "numero_ordem_producao",
                        "lic",
                        "sp",
                        "lsc",
                        "resultado",
                        "numero_operador",
                        "data_lancamento",
                        ],
        joins_list= None,
        chave_id="id" ,
        lote=10000 ,
        id_inicial=161808241 ,
        period_filter_status="off" ,
        refresh_workday_status="off" ,
        workday_role=None ,
        period_column=None ,
        period_name=None ,
        period_value=None ,
    )

def copiar_lote_programacao_lamina() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_udi_pmdnet",
        conn_destino_id="pg_dw_apex" ,
        tabela_origem= "pmdnet.programacao_lamina",
        tabela_destino="apex_dw_raw.udi_pmdnet.tb_programacao_lamina" ,
        colunas_busca= ["ID",
                        "DATA",
                        "SEQUENCIA",
                        "ANO_MES",
                        "OPERACAO",
                        "ID_MISTURA",
                        "IDENTIFICADOR",
                        "ID_ORDEM_PRODUCAO",
                        "NUMERO_ORDEM_PRODUCAO",
                        "CASE WHEN SINGLE_LINE = 1 THEN 'true' ELSE 'false' END AS DINGLE_LINE",
                        "CASE WHEN ANTECIPA_NORMAL = 1 THEN 'true' ELSE 'false' END AS ANTECIPA_NORMAL",
                        "CASE WHEN ANTECIPA_TORREFACAO = 1 THEN 'true' ELSE 'false' END AS ANTECIPA_TORREFACAO",
                        "DATA_CRIACAO",
                        ],
        colunas_insert= ["id",
                        "data",
                        "sequencia",
                        "ano_mes",
                        "operacao",
                        "id_mistura",
                        "identificador",
                        "id_ordem_producao",
                        "numero_ordem_producao",
                        "single_line",
                        "antecipa_normal",
                        "antecipa_torrefacao",
                        "data_criacao",
                        ],
        joins_list= None,
        chave_id="id" ,
        lote=10000 ,
        id_inicial=53278 ,
        period_filter_status="on",
        refresh_workday_status="on",
        workday_role="getdate",
        period_column='data' ,
        period_name="DAY",
        period_value=-1,
    )


def copiar_lote_programacao_sts() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_udi_pmdnet",
        conn_destino_id="pg_dw_apex" ,
        tabela_origem= "pmdnet.programacao_sts",
        tabela_destino="apex_dw_raw.udi_pmdnet.tb_programacao_sts" ,
        colunas_busca= ["ID",
                        "DATA",
                        "SEQUENCIA",
                        "ANO_MES",
                        "OPERACAO",
                        "ID_MISTURA",
                        "IDENTIFICADOR",
                        "ID_ORDEM_PRODUCAO",
                        "NUMERO_ORDEM_PRODUCAO",
                        "DATA_CRIACAO",
                        "DATA_HORA_ABASTECIMENTO",
                        "DATA_INICIAL",
                        "HORA_INICIAL",
                        "DATA_FINAL",
                        "HORA_FINAL",
                        "INTERVALO_ABASTECIMENTO",
                        "NUMERO_SILO",
                        "PESO_EXTRACAO_RAT_TAIL",
                        "PESO_PESADO_TORRE",
                        "MATRICULA_OPERADOR",
                        "DATA_HORA_PRODUCAO",
                        "DATA_INICIAL_PRODUCAO",
                        "HORA_INICIAL_PRODUCAO",
                        "DATA_FINAL_PRODUCAO",
                        "HORA_FINAL_PRODUCAO",
                        "INTERVALO_PRODUCAO",
                        "NUMERO_SILO_PRODUCAO",
                        "PESO_BALANCA",
                        "MATRICULA_OPERADOR_PRODUCAO",
                        ],
        colunas_insert= ["id",
                        "data",
                        "sequencia",
                        "ano_mes",
                        "operacao",
                        "id_mistura",
                        "identificador",
                        "id_ordem_producao",
                        "numero_ordem_producao",
                        "data_criacao",
                        "data_hora_abastecimento",
                        "data_inicial",
                        "hora_inicial",
                        "data_final",
                        "hora_final",
                        "intervalo_abastecimento",
                        "numero_silo",
                        "peso_extracao_rat_tail",
                        "peso_pesado_torre",
                        "matricula_operador",
                        "data_hora_producao",
                        "data_inicial_producao",
                        "hora_inicial_producao",
                        "data_final_producao",
                        "hora_final_producao",
                        "intervalo_producao",
                        "numero_silo_producao",
                        "peso_balanca",
                        "matricula_operador_producao",
                        ],
        joins_list= None,
        chave_id="id" ,
        lote=10000 ,
        id_inicial=36652 ,
        period_filter_status="on",
        refresh_workday_status="on",
        workday_role="getdate",
        period_column='data' ,
        period_name="DAY",
        period_value=-1,
    )

def copiar_lote_programacao_normal() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_udi_pmdnet",
        conn_destino_id="pg_dw_apex" ,
        tabela_origem= "pmdnet.programacao_normal",
        tabela_destino="apex_dw_raw.udi_pmdnet.tb_programacao_normal" ,
        colunas_busca = [
            "ID",
            "DATA_PROGRAMACAO_LINHA",
            "ID_PROGRAMACAO",
            "DATA_HORA_LINHA_NORMAL",
            "DATA_INICIAL_LINHA_NORMAL",
            "TIME_FORMAT(HORA_INICIAL_LINHA_NORMAL, '%%H:%%i:%%s') AS HORA_INICIAL_LINHA_NORMAL",
            "DATA_FINAL_LINHA_NORMAL",
            "TIME_FORMAT(HORA_FINAL_LINHA_NORMAL, '%%H:%%i:%%s') AS HORA_FINAL_LINHA_NORMAL",
            "NUMERO_SILO_TORREFACAO_LINHA_NORMAL",
            "NUMERO_SILO_MISTURA_LINHA_NORMAL",
            "NUMERO_TACHO_MELADO_NORMAL",
            "VOLUME_INICIAL_MELADO_LINHA_NORMAL",
            "VOLUME_CONSUMIDO_MELADO_LINHA_NORMAL",
            "NUMERO_FARDOS_CONTADOS_LINHA_NORMAL",
            "PESO_REMESSA_LINHA_NORMAL",
            "CODIGO_GPC",
            "NUMERO_LOTE_GPC",
            "MATRICULA_OPERADOR_LINHA_NORMAL",
            "DATA_LANCAMENTO"
            ],
        colunas_insert = [
            "id",
            "data_programacao_linha",
            "id_programacao",
            "data_hora_linha_normal",
            "data_inicial_linha_normal",
            "hora_inicial_linha_normal",
            "data_final_linha_normal",
            "hora_final_linha_normal",
            "numero_silo_torrefacao_linha_normal",
            "numero_silo_mistura_linha_normal",
            "numero_tacho_melado_normal",
            "volume_inicial_melado_linha_normal",
            "volume_consumido_melado_linha_normal",
            "numero_fardos_contados_linha_normal",
            "peso_remessa_linha_normal",
            "codigo_gpc",
            "numero_lote_gpc",
            "matricula_operador_linha_normal",
            "data_lancamento"
            ],
        joins_list= None,
        chave_id="id" ,
        lote=10000 ,
        id_inicial=50828 ,
        period_filter_status="on",
        refresh_workday_status="on",
        workday_role="getdate",
        period_column='data_programacao_linha' ,
        period_name="DAY",
        period_value=-1,
    )

def copiar_sugestao_umidade_dr() -> None:
    # leia acima pra saber o que é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id="sql_udi_pmdnet",
        conn_destino_id="pg_dw_apex",
        tabela_origem="pmdnet.sugestao_umidade_dr",
        tabela_destino="apex_dw_raw.udi_pmdnet.tb_sugestao_umidade_dr",
        colunas_busca=[
            "ID",
            "DATA",
            "NUMERO_OPERACAO",
            "NUMERO_MISTURA",
            "TIME_FORMAT(HORA_INICIO, '%%H:%%i:%%s') AS HORA_INICIO",
            "UMIDADE",
            "DATA_CRIACAO",
        ],
        colunas_insert=[
            "id",
            "data",
            "numero_operacao",
            "numero_mistura",
            "hora_inicio",
            "umidade",
            "data_criacao",
        ],
        joins_list=None,
        chave_id="id",
        lote=5000,
        id_inicial=1,
        period_filter_status="on",
        refresh_workday_status="on",
        workday_role="getdate",
        period_column="data",
        period_name="DAY",
        period_value=-1,
    )


def copiar_lote_producao_linha_corte() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_udi_pmdnet",
        conn_destino_id="pg_dw_apex" ,
        tabela_origem= "pmdnet.producao_linha_corte",
        tabela_destino="apex_dw_raw.udi_pmdnet.tb_producao_linha_corte" ,
        colunas_busca = [
                        "ID",
                        "DATA_PRODUCAO",
                        "NUMERO_OPERACAO",
                        "NUMERO_MISTURA",
                        "HORA_INICIO_REMESSA",
                        "HORA_FIM_REMESSA",
                        "TEMPO_DURACAO_REMESSA",
                        "PESO_LAMINA",
                        "PESO_LAMINA_LIC",
                        "PESO_LAMINA_LSC",
                        "PESO_STS",
                        "PESO_SDS",
                        "PESO_DEER",
                        "PESO_DIET",
                        "PESO_FINES",
                        "PESO_RESIDUO",
                        "TOTAL_PESO_REMESSA",
                        "TOTAL_PESO_REMESSA_DIA",
                        "SP_FLUXO",
                        "PV_FLUXO",
                        "FLUXO_HORA",
                        "MEDIA_FLUXO",
                        "UMIDADE_REMESSA",
                        "DATA_LANCAMENTO",
                        ],
        colunas_insert = [
                        "id",
                        "data_producao",
                        "numero_operacao",
                        "numero_mistura",
                        "hora_inicio_remessa",
                        "hora_fim_remessa",
                        "tempo_duracao_remessa",
                        "peso_lamina",
                        "peso_lamina_lic",
                        "peso_lamina_lsc",
                        "peso_sts",
                        "peso_sds",
                        "peso_deer",
                        "peso_diet",
                        "peso_fines",
                        "peso_residuo",
                        "total_peso_remessa",
                        "total_peso_remessa_dia",
                        "sp_fluxo",
                        "pv_fluxo",
                        "fluxo_hora",
                        "media_fluxo",
                        "umidade_remessa",
                        "data_lancamento",
                        ],
        joins_list= None,
        chave_id="id" ,
        lote=10000 ,
        id_inicial=48317 ,
        period_filter_status="off",
        refresh_workday_status="off",
        workday_role=None,
        period_column=None,
        period_name=None,
        period_value=None,
    )

###############################################################################################################
#                          Definição das tarefas de atualizar tabelas de cadastro
###############################################################################################################
'''def atualizar_tabela_grandeza() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_udi_pmdnet",
        conn_destino_id="sql_udi_apex_azure" ,
        tabela_origem= "pmdnet.programacao_sts",
        tabela_destino="udi_DW_PMDNET.dbo.tb_programacao_sts" ,
        colunas_busca= ["ID",
                        "DESCRICAO_GRANDEZA",
                        "REGISTRO_ATIVO",
                        ],
        colunas_insert= ["ID",
                        "DESCRICAO_GRANDEZA",
                        "REGISTRO_ATIVO",
                        ],
        joins_list= None,
        chave_id="ID" ,
        lote=1500 ,
        id_inicial=1 ,
        period_filter_status="off" ,
        refresh_workday_status="on" ,
        workday_role="getdate" ,
        period_column=None ,
        period_name=None ,
        period_value=None ,
    )
'''



# =============================================================================
# DAG
# =============================================================================



with DAG(
    dag_id="UDI_PMDNET_ingest_bronze_incremental_tables",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    max_active_runs=1,
    catchup=False,
    default_args={
        "retries": 0,
        "depends_on_past": False
    },
    tags=["Bronze Ingest", "MySQL", "owner_GuiRabuske", "UDI", "PMDNET"],
) as dag_get_tables_pmdnet:


    wait_01 = BashOperator(task_id="wait_01", bash_command="sleep 4")
    wait_02 = BashOperator(task_id="wait_02", bash_command="sleep 4")
    wait_03 = BashOperator(task_id="wait_03", bash_command="sleep 4")
    wait_04 = BashOperator(task_id="wait_04", bash_command="sleep 4")
    wait_05 = BashOperator(task_id="wait_05", bash_command="sleep 4")
    wait_06 = BashOperator(task_id="wait_06", bash_command="sleep 4")

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    get_tb_rastreabilidade_caixas = PythonOperator(
        task_id="Copiar_tabela_rastreabilidade_caixas",
        python_callable=copiar_lote_rastreabilidade_caixas_view,
    )
    get_tb_acompanhamento_processo = PythonOperator(
        task_id="Copiar_tabela_acompanhamento_processo",
        python_callable=copiar_lote_registro_acompanhamento_processo,
    )
    get_tb_programacao_lamina = PythonOperator(
        task_id="Copiar_tabela_programacao_lamina",
        python_callable=copiar_lote_programacao_lamina,
    )
    get_tb_programacao_sts = PythonOperator(
        task_id="Copiar_tabela_programacao_sts",
        python_callable=copiar_lote_programacao_sts,
    )
    get_tb_programacao_normal = PythonOperator(
        task_id="Copiar_tabela_programacao_normal",
        python_callable=copiar_lote_programacao_normal,
    )
    get_tb_sugestao_umidade_dr = PythonOperator(
        task_id="copiar_sugestao_umidade_dr",
        python_callable=copiar_sugestao_umidade_dr,
    )
    get_tb_producao_linha_corte = PythonOperator(
        task_id="copiar_lote_producao_linha_corte",
        python_callable=copiar_lote_producao_linha_corte,
    )

    start \
    >> get_tb_rastreabilidade_caixas >> wait_01 \
    >> get_tb_acompanhamento_processo >> wait_02 \
    >> get_tb_programacao_lamina >> wait_03 \
    >> get_tb_programacao_sts >> wait_04 \
    >> get_tb_programacao_normal >> wait_05 \
    >> get_tb_sugestao_umidade_dr >> wait_06 \
    >> get_tb_producao_linha_corte \
    >> end

    