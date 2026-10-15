
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
# period_name: HOUR, DAY, MONTH, YEAR (Quanto que sera removido da data atual)
# period_value: o numero a ser reduzido.,
# Ex.: period_name = DAY, period_value = -2 => DATEADD(DAY, -2, WORKDAY)



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

def copiar_lote_tb_ProductionOrder() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_udi_chronos_reader",
        conn_destino_id= "pg_dw_apex",
        tabela_origem= "db_mcExperienceSuite.mesAdapter.tb_ProductionOrder",
        tabela_destino= "apex_dw_raw.udi_chronos_mcexperiencesuite.tb_productionorder",
        colunas_busca= [
            "Cod_Record","Cod_ProductDesign","Val_ProductionOrderNumber",
            "Val_OperationSequenceNo","Val_ScheduledStartUTC","Val_ScheduledEndUTC","Dtt_Start","Dtt_Stop",
            "Val_Identifier","Val_Facility","Val_Department","Val_WorkCenter","Val_ReqQuantity","Val_UOM",
            "Val_CompleteQuantity","Val_ConfirmedQuantityGMES","Val_RatioQuantityGMES","Val_CasePacker","Ind_Status",
            "Ind_Test","Des_Comment","Usr_Record","Dtt_Record","Dtt_Update","Ind_ManualInsert","Val_BarCode",
            "Cod_ProductionOrderClassification","Val_Equipment","Flg_DWProcessed","Stt_Record","Dtt_Disable"
                        ],
        colunas_insert= [
            "cod_record","cod_productdesign","val_productionordernumber",
            "val_operationsequenceno","val_scheduledstartutc","val_scheduledendutc","dtt_start","dtt_stop",
            "val_identifier","val_facility","val_department","val_workcenter","val_reqquantity","val_uom",
            "val_completequantity","val_confirmedquantitygmes","val_ratioquantitygmes","val_casepacker","ind_status",
            "ind_test","des_comment","usr_record","dtt_record","dtt_update","ind_manualinsert","val_barcode",
            "cod_productionorderclassification","val_equipment","flg_dwprocessed","stt_record","dtt_disable"
                        ],
        joins_list= None,
        chave_id= "cod_record",
        lote= 1000,
        id_inicial= 219841,
        period_filter_status= "on",
        refresh_workday_status= "on",
        workday_role= "workday",
        period_column= "dtt_record",
        period_name= "DAY",
        period_value= -4,
    )

def copiar_lote_tb_ProductionOrderLog() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_udi_chronos_reader",
        conn_destino_id= "pg_dw_apex",
        tabela_origem= "db_mcExperienceSuite.mesAdapter.tb_ProductionOrderLog",
        tabela_destino= "apex_dw_raw.udi_chronos_mcexperiencesuite.tb_productionorderlog",
        colunas_busca= [
            "Cod_Record","Cod_ProductionOrder","Cod_Shift","Val_Facility","Val_Department","Val_WorkCenter",
            "Val_Equipment","Val_EquipmentType","Val_CompleteQuantity","Val_WorkDay","Dtt_Collect",
            "Dtt_Record","Ind_Manual","Val_UOM","Val_UOMConverted","Val_CompleteQuantityConverted","Ind_Type"
                        ],
        colunas_insert= [
            "cod_record","cod_productionorder","cod_shift","val_facility","val_department","val_workcenter",
            "val_equipment","val_equipmenttype","val_completequantity","val_workday","dtt_collect",
            "dtt_record","ind_manual","val_uom","val_uomconverted","val_completequantityconverted","ind_type"
                        ],
        joins_list= None,
        chave_id= "cod_record",
        lote= 5000,
        id_inicial= 101375073,
        period_filter_status= "off",
        refresh_workday_status= "off",
        workday_role= None,
        period_column= None,
        period_name= None,
        period_value= None,
    )

def copiar_lote_tb_ProductionOrderBOM() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_udi_chronos_reader",
        conn_destino_id= "pg_dw_apex",
        tabela_origem= "db_mcExperienceSuite.mesAdapter.tb_ProductionOrderBOM",
        tabela_destino= "apex_dw_raw.udi_chronos_mcexperiencesuite.tb_productionorderbom",
        colunas_busca= [
            "Cod_Record","Cod_ProductionOrder","Val_MaterialGroup","Val_Material",
            "Val_MaterialDescription","Val_ReqQuantity","Val_ReqUnitOfMeasure","Val_OprSequenceNo","Typ_Component",
            "Dtt_Record"
                        ],
        colunas_insert= [
            "cod_record","cod_productionorder","val_materialgroup","val_material",
            "val_materialdescription","val_reqquantity","val_requnitofmeasure","val_oprsequenceno","typ_component",
            "dtt_record"
                        ],
        joins_list= None,
        chave_id= "cod_record",
        lote= 5000,
        id_inicial= 2701806,
        period_filter_status= "on",
        refresh_workday_status= "on",
        workday_role= "workday",
        period_column= "dtt_record",
        period_name= "DAY",
        period_value= -4,
    )

def copiar_lote_tb_ProductDesign() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_udi_chronos_reader",
        conn_destino_id= "pg_dw_apex",
        tabela_origem= "db_mcExperienceSuite.mesAdapter.tb_ProductDesign",
        tabela_destino= "apex_dw_raw.udi_chronos_mcexperiencesuite.tb_productdesign",
        colunas_busca= [
            "Cod_Record","Cod_Product","1 AS Nam_ProductDesign", "Dtt_Record","Dtt_Update"
                        ],
        colunas_insert= [
            "cod_record", "cod_product", "nam_productdesign", "dtt_record","dtt_update"
                        ],
        joins_list= None,
        chave_id= "cod_record",
        lote= 500,
        id_inicial= 0,
        period_filter_status= "off",
        refresh_workday_status= "off",
        workday_role= None,
        period_column= None,
        period_name= None,
        period_value= None,
    )

def copiar_lote_tb_Product() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_udi_chronos_reader",
        conn_destino_id= "pg_dw_apex",
        tabela_origem= "db_mcExperienceSuite.mesAdapter.tb_Product",
        tabela_destino= "apex_dw_raw.udi_chronos_mcexperiencesuite.tb_product",
        colunas_busca= [
            "Cod_Record","Num_ProductNo","Des_ProductDescription","Ind_Premium","Dtt_Record","Dtt_Update","Dtt_Disable"

                        ],
        colunas_insert= [
            "cod_record","num_productno","des_productdescription","ind_premium","dtt_record","dtt_update","dtt_disable"
                        ],
        joins_list= None,
        chave_id= "cod_record",
        lote= 50,
        id_inicial= 0,
        period_filter_status= "off",
        refresh_workday_status= "off",
        workday_role= None,
        period_column= None,
        period_name= None,
        period_value= None,
    )


def copiar_lote_tb_MicroEvents() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_udi_chronos_reader",
        conn_destino_id= "pg_dw_apex",
        tabela_origem= "db_mcExperienceSuite.mesAdapter.tb_MicroEvents",
        tabela_destino= "apex_dw_raw.udi_chronos_mcexperiencesuite.tb_microevents",
        colunas_busca = [
            "Cod_Record", "Cod_ProductionOrder", "Cod_ReasonCode", "Cod_Cause", "Cod_Nature", "Cod_Shift",
            "Dtt_Start", "Dtt_End", "Ind_Type", "Val_Facility", "Val_Department", "Val_WorkCenter",
            "Val_Equipment", "Val_EquipmentType", "Val_Quantity", "Val_UOM", "Val_CompleteQuantityConverted",
            "Val_UOMConverted", "Ind_Authorized", "Ind_Manual", "Dtt_Record", "Usr_Record", "Dtt_WorkDay",
            "Dtt_Update", "Val_StandartRate", "Usr_Update", "Cod_StopClassification", "Stt_Record",
            "Dtt_Disable", "Cod_ExceptionReason", "Ind_TypeCalc", "Cod_PTA", "Cod_PTASubGroup",
            "Cod_PTASubGroupComponents", "Cod_ExceptionEntryDate", "Cod_RefBreak"
        ],
        colunas_insert= [
            "cod_record", "cod_productionorder", "cod_reasoncode", "cod_cause", "cod_nature", "cod_shift",
            "dtt_start", "dtt_end", "ind_type", "val_facility", "val_department", "val_workcenter",
            "val_equipment", "val_equipmenttype", "val_quantity", "val_uom", "val_completequantityconverted",
            "val_uomconverted", "ind_authorized", "ind_manual", "dtt_record", "usr_record", "dtt_workday",
            "dtt_update", "val_standartrate", "usr_update", "cod_stopclassification", "stt_record",
            "dtt_disable", "cod_exceptionreason", "ind_typecalc", "cod_pta", "cod_ptasubgroup",
            "cod_ptasubgroupcomponents", "cod_exceptionentrydate", "cod_refbreak"
                        ],
        joins_list= None,
        chave_id= "cod_record",
        lote= 1000,
        id_inicial= 281146785,
        period_filter_status= "off",
        refresh_workday_status= "off",
        workday_role= None,
        period_column= None,
        period_name= None,
        period_value= None,
    )


def copiar_lote_tb_CauseMicroEvents() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_udi_chronos_reader",
        conn_destino_id= "pg_dw_apex",
        tabela_origem= "db_mcExperienceSuite.mesAdapter.tb_CauseMicroEvents",
        tabela_destino= "apex_dw_raw.udi_chronos_mcexperiencesuite.tb_causemicroevents",
        colunas_busca = [
            "Cod_Record",
            "Nam_Cause",
            "Des_Cause",
            "Val_Equipment",
            "Ind_Type",
            "Dtt_Record",
            "Dtt_Update",
            "Stt_Record",
            "Dtt_Disable"
            ],
        colunas_insert= [
            "cod_record",
            "nam_cause",
            "des_cause",
            "val_equipment",
            "ind_type",
            "dtt_record",
            "dtt_update",
            "stt_record",
            "dtt_disable"    
        ],
        joins_list= None,
        chave_id= "cod_record",
        lote= 1000,
        id_inicial= 26507,
        period_filter_status= "off",
        refresh_workday_status= "off",
        workday_role= None,
        period_column= None,
        period_name= None,
        period_value= None,
    )

def copiar_lote_tb_NatureMicroEvents() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_udi_chronos_reader",
        conn_destino_id= "pg_dw_apex",
        tabela_origem= "db_mcExperienceSuite.mesAdapter.tb_NatureMicroEvents",
        tabela_destino= "apex_dw_raw.udi_chronos_mcexperiencesuite.tb_naturemicroevents",
        colunas_busca = [
            "Cod_Record",
            "Nam_Nature",
            "Des_Nature",
            "Ind_Type",
            "Dtt_Record",
            "Stt_Record",
            "Dtt_Update",
            "Dtt_Disable"
        ],
        colunas_insert= [
            "cod_record",
            "nam_nature",
            "des_nature",
            "ind_type",
            "dtt_record",
            "stt_record",
            "dtt_update",
            "dtt_disable"
        ],
        joins_list= None,
        chave_id= "cod_record",
        lote= 1000,
        id_inicial= 1,
        period_filter_status= "off",
        refresh_workday_status= "off",
        workday_role= None,
        period_column= None,
        period_name= None,
        period_value= None,
    )

def copiar_lote_tb_ReasonCodeMicroEvents() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_udi_chronos_reader",
        conn_destino_id= "pg_dw_apex",
        tabela_origem= "db_mcExperienceSuite.mesAdapter.tb_ReasonCodeMicroEvents",
        tabela_destino= "apex_dw_raw.udi_chronos_mcexperiencesuite.tb_reasoncodemicroevents",
        colunas_busca = [
            "Cod_Record",
            "Val_Equipment",
            "Val_ReasonCode",
            "Des_ReasonCode",
            "Ind_Type",
            "Ind_Manual",
            "Dtt_Record",
            "Typ_Sensor",
            "Cod_Nature",
            "Dtt_Update",
            "Stt_Record",
            "Dtt_Disable",
            "Cod_PTA"
        ],
        colunas_insert= [
            "cod_record",
            "val_equipment",
            "val_reasoncode",
            "des_reasoncode",
            "ind_type",
            "ind_manual",
            "dtt_record",
            "typ_sensor",
            "cod_nature",
            "dtt_update",
            "stt_record",
            "dtt_disable",
            "cod_pta"
        ],
        joins_list= None,
        chave_id= "cod_record",
        lote= 1000,
        id_inicial= 122353,
        period_filter_status= "off",
        refresh_workday_status= "off",
        workday_role= None,
        period_column= None,
        period_name= None,
        period_value= None,
    )


def copiar_lote_tb_ExceptionEntry() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_udi_chronos_reader",
        conn_destino_id= "pg_dw_apex",
        tabela_origem= "db_mcExperienceSuite.mesAdapter.tb_ExceptionEntry",
        tabela_destino= "apex_dw_raw.udi_chronos_mcexperiencesuite.tb_exceptionentry",
        colunas_busca = [
            "Cod_Record",
            "Cod_ExceptionReason",
            "Val_Department",
            "Val_WorkCenter",
            "Val_Equipment",
            "Des_Exception",
            "Stt_Record",
            "Dtt_Record",
            "Dtt_Update",
            "Dtt_Disable",
            "Val_Status"
        ],
        colunas_insert= [
            "cod_record",
            "cod_exceptionreason",
            "val_department",
            "val_workcenter",
            "val_equipment",
            "des_exception",
            "stt_record",
            "dtt_record",
            "dtt_update",
            "dtt_disable",
            "val_status"
        ],
        joins_list= None,
        chave_id= "cod_record",
        lote= 1000,
        id_inicial= 3,
        period_filter_status= "off",
        refresh_workday_status= "off",
        workday_role= None,
        period_column= None,
        period_name= None,
        period_value= None,
    )

def copiar_lote_tb_ExceptionEntryDateApprover() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_udi_chronos_reader",
        conn_destino_id= "pg_dw_apex",
        tabela_origem= "db_mcExperienceSuite.mesAdapter.tb_ExceptionEntryDateApprover",
        tabela_destino= "apex_dw_raw.udi_chronos_mcexperiencesuite.tb_exceptionentrydateapprover",
        colunas_busca = [
            "Cod_Record",
            "Cod_ExceptionEntryDate",
            "Typ_Approver",
            "Val_Status",
            "Des_Comment",
            "Dtt_Record"
        ],
        colunas_insert= [
            "cod_record",
            "cod_exceptionentrydate",
            "typ_approver",
            "val_status",
            "des_comment",
            "dtt_record"
        ],
        joins_list= None,
        chave_id= "cod_record",
        lote= 1000,
        id_inicial= 170785,
        period_filter_status= "off",
        refresh_workday_status= "off",
        workday_role= None,
        period_column= None,
        period_name= None,
        period_value= None,
    )

def copiar_lote_tb_ExceptionEntryDate() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_udi_chronos_reader",
        conn_destino_id= "pg_dw_apex",
        tabela_origem= "db_mcExperienceSuite.mesAdapter.tb_ExceptionEntryDate",
        tabela_destino= "apex_dw_raw.udi_chronos_mcexperiencesuite.tb_exceptionentrydate",
        colunas_busca= [
                "Cod_Record", "Cod_ExceptionEntry", "Dtt_Start", "Dtt_End",
                "Hrr_Start", "Hrr_End", "Cod_Shift", "Dtt_Record",
                "Dtt_Update", "Stt_Record", "Dtt_Disable", "Typ_Request",
                "Val_Status"
            ],
        colunas_insert= [
                "cod_record", "cod_exceptionentry", "dtt_start", "dtt_end",
                "hrr_start", "hrr_end", "cod_shift", "dtt_record",
                "dtt_update", "stt_record", "dtt_disable", "typ_request",
                "val_status"
            ],
        joins_list= None,
        chave_id= "cod_record",
        lote= 1000,
        id_inicial= 40,
        period_filter_status= "off",
        refresh_workday_status= "off",
        workday_role= None,
        period_column= None,
        period_name= None,
        period_value= None,
    )

def copiar_lote_tb_ExceptionReasonApprover() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_udi_chronos_reader",
        conn_destino_id= "pg_dw_apex",
        tabela_origem= "db_mcExperienceSuite.mesAdapter.tb_ExceptionReasonApprover",
        tabela_destino= "apex_dw_raw.udi_chronos_mcexperiencesuite.tb_exceptionreasonapprover",
        colunas_busca= [
                "Cod_Record", "Cod_ExceptionReason", "Cod_User", "Typ_Approver",
                "Typ_Request", "Ind_Default", "Dtt_Record", "Usr_Record",
                "Dtt_Update", "Usr_Update", "Stt_Record"
            ],
        colunas_insert= [
                "cod_record", "cod_exceptionreason", "cod_user", "typ_approver",
                "typ_request", "ind_default", "dtt_record", "usr_record",
                "dtt_update", "usr_update", "stt_record"
            ],
        joins_list= None,
        chave_id= "cod_record",
        lote= 1000,
        id_inicial= 0,
        period_filter_status= "off",
        refresh_workday_status= "off",
        workday_role= None,
        period_column= None,
        period_name= None,
        period_value= None,
    )

def copiar_lote_tb_ExceptionEntryAttachments() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_udi_chronos_reader",
        conn_destino_id= "pg_dw_apex",
        tabela_origem= "db_mcExperienceSuite.mesAdapter.tb_ExceptionEntryAttachments",
        tabela_destino= "apex_dw_raw.udi_chronos_mcexperiencesuite.tb_exceptionentryattachments",
        colunas_busca= [
                "Cod_Record", "Cod_ExceptionEntry", "Des_FileName", "Val_FileData",
                "Dtt_Record", "Dtt_Update", "Stt_Record", "Dtt_Disable"
            ],
        colunas_insert= [
                "cod_record", "cod_exceptionentry", "des_filename", "val_filedata",
                "dtt_record", "dtt_update", "stt_record", "dtt_disable"
            ],
        joins_list= None,
        chave_id= "cod_record",
        lote= 50,
        id_inicial= 0,
        period_filter_status= "off",
        refresh_workday_status= "off",
        workday_role= None,
        period_column= None,
        period_name= None,
        period_value= None,
    )

def copiar_lote_tb_ExceptionCalendar() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_udi_chronos_reader",
        conn_destino_id= "pg_dw_apex",
        tabela_origem= "db_mcExperienceSuite.mesAdapter.tb_ExceptionCalendar",
        tabela_destino= "apex_dw_raw.udi_chronos_mcexperiencesuite.tb_exceptioncalendar",
        colunas_busca= [
                "Cod_Record", "Cod_ExceptionReason", "Val_Department",
                "Val_WorkCenter", "Val_Equipment", "Des_Exception",
                "Stt_Record", "Dtt_Record",
                "Dtt_Update", "Dtt_Disable"
            ],
        colunas_insert= [
                "cod_record", "cod_exceptionreason", "val_department",
                "val_workcenter", "val_equipment", "des_exception",
                "stt_record", "dtt_record",
                "dtt_update", "dtt_disable"
            ],
        joins_list= None,
        chave_id= "cod_record",
        lote= 1000,
        id_inicial= 0,
        period_filter_status= "off",
        refresh_workday_status= "off",
        workday_role= None,
        period_column= None,
        period_name= None,
        period_value= None,
    )

def copiar_lote_tb_ExceptionCalendarDate() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_udi_chronos_reader",
        conn_destino_id= "pg_dw_apex",
        tabela_origem= "db_mcExperienceSuite.mesAdapter.tb_ExceptionCalendarDate",
        tabela_destino= "apex_dw_raw.udi_chronos_mcexperiencesuite.tb_exceptioncalendardate",
        colunas_busca= [
                "Cod_Record", "Cod_ExceptionCalendar", "Dtt_Exception",
                "Cod_Shift", "Dtt_Record", "Dtt_Update",
                "Stt_Record", "Dtt_Disable"
            ],
        colunas_insert= [
                "cod_record", "cod_exceptioncalendar", "dtt_exception",
                "cod_shift", "dtt_record", "dtt_update",
                "stt_record", "dtt_disable"
            ],
        joins_list= None,
        chave_id= "cod_record",
        lote= 1000,
        id_inicial= 0,
        period_filter_status= "off",
        refresh_workday_status= "off",
        workday_role= None,
        period_column= None,
        period_name= None,
        period_value= None,
    )

def copiar_lote_tb_Characteristic() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_udi_chronos_reader",
        conn_destino_id= "pg_dw_apex",
        tabela_origem= "db_mcExperienceSuite.mesAdapter.tb_Characteristic",
        tabela_destino= "apex_dw_raw.udi_chronos_mcexperiencesuite.tb_characteristic",
        colunas_busca= [
                "Cod_Record", "Nam_Characteristic", "Des_Characteristic",
                "Val_UnitOfMeasure", "Ind_Q2Type", "Val_ExternalCodeCharacteristic",
                "Val_ExternalCodeSetPointEng", "Val_ExternalCodeLIEng",
                "Val_ExternalCodeLSEng", "Ind_ProcessingType", "Val_LimitRule",
                "Val_RagRule", "Val_DecimalRule", "Dtt_Record",
                "Val_ExternalCodeLocalMes", "Nam_Reference",
                "Val_ExternalCodeCharacteristic2", "Val_ExternalCodeCharacteristic3",
                "Flg_DWProcessed", "Dtt_Update", "Stt_Record", "Dtt_Disable"
            ],
        colunas_insert= [
                "cod_record", "nam_characteristic", "des_characteristic",
                "val_unitofmeasure", "ind_q2type", "val_externalcodecharacteristic",
                "val_externalcodesetpointeng", "val_externalcodelieng",
                "val_externalcodelseng", "ind_processingtype", "val_limitrule",
                "val_ragrule", "val_decimalrule", "dtt_record",
                "val_externalcodelocalmes", "nam_reference",
                "val_externalcodecharacteristic2", "val_externalcodecharacteristic3",
                "flg_dwprocessed", "dtt_update", "stt_record", "dtt_disable"
            ],
        joins_list= None,
        chave_id= "cod_record",
        lote= 10,
        id_inicial= 0,
        period_filter_status= "off",
        refresh_workday_status= "off",
        workday_role= None,
        period_column= None,
        period_name= None,
        period_value= None,
    )

def copiar_lote_tb_QualitySample() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_udi_chronos_reader",
        conn_destino_id= "pg_dw_apex",
        tabela_origem= "db_mcExperienceSuite.mesAdapter.tb_QualitySample",
        tabela_destino= "apex_dw_raw.udi_chronos_mcexperiencesuite.tb_qualitysample",
        colunas_busca= [
                "Cod_Record", "Cod_ProductionOrder", "Val_SampleID", "Val_Facility",
                "Val_Department", "Val_WorkCenter", "Val_Equipment", "Val_EquipmentType",
                "Val_QualityEquipmentID", "Val_NPIFlag", "Cod_Shift", "Val_Material",
                "Val_POShiftStartDateTime", "Val_SFGMaterial", "Val_SFGSpecValidFrom",
                "Val_WorkDay", "Dtt_Record", "Ind_Manual", "Des_Comment",
                "Cod_ImportFile", "Val_Source", "Dtt_Update", "Usr_Update",
                "Ind_Reprocess", "Dtt_Reprocess", "Ind_Planned", "Stt_Record",
                "Dtt_Disable"
            ],
        colunas_insert= [
                "cod_record", "cod_productionorder", "val_sampleid", "val_facility",
                "val_department", "val_workcenter", "val_equipment", "val_equipmenttype",
                "val_qualityequipmentid", "val_npiflag", "cod_shift", "val_material",
                "val_poshiftstartdatetime", "val_sfgmaterial", "val_sfgspecvalidfrom",
                "val_workday", "dtt_record", "ind_manual", "des_comment",
                "cod_importfile", "val_source", "dtt_update", "usr_update",
                "ind_reprocess", "dtt_reprocess", "ind_planned", "stt_record",
                "dtt_disable"
            ],
        joins_list= None,
        chave_id= "cod_record",
        lote= 5000,
        id_inicial= 12432894,
        period_filter_status= "off",
        refresh_workday_status= "off",
        workday_role= None,
        period_column= None,
        period_name= None,
        period_value= None,
    )

def copiar_lote_tb_QualitySampleResult() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_udi_chronos_reader",
        conn_destino_id= "pg_dw_apex",
        tabela_origem= "db_mcExperienceSuite.mesAdapter.tb_QualitySampleResult",
        tabela_destino= "apex_dw_raw.udi_chronos_mcexperiencesuite.tb_qualitysampleresult",
        colunas_busca= [
                "Cod_Record", "Cod_ResultGroup", "Cod_QualitySample",
                "Cod_Characteristic", "Val_Result", "Des_DeleteReason",
                "Ind_Delete", "Dtt_Record", "Dtt_Result",
                "Dtt_Update", "Val_Identifier", "Flg_DWProcessed",
                "Stt_Record", "Dtt_Disable"
            ],
        colunas_insert= [
                "cod_record", "cod_resultgroup", "cod_qualitysample",
                "cod_characteristic", "val_result", "des_deletereason",
                "ind_delete", "dtt_record", "dtt_result",
                "dtt_update", "val_identifier", "flg_dwprocessed",
                "stt_record", "dtt_disable"
            ],
        joins_list= None,
        chave_id= "cod_record",
        lote= 25000,
        id_inicial= 161120675,
        period_filter_status= "off",
        refresh_workday_status= "off",
        workday_role= None,
        period_column= None,
        period_name= None,
        period_value= None,
    )

def copiar_lote_tb_DadosLossTree_PowerBI() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_udi_chronos_reader",
        conn_destino_id= "pg_dw_apex",
        tabela_origem= "db_mcChronosDW.consolidation.tb_DadosLossTree_PowerBI",
        tabela_destino= "apex_dw_raw.udi_chronos_mcchronosdw.tb_dadoslosstree_powerbi",
        colunas_busca= [
            "Cod_Record", "Dtt_Workday", "Cod_Shift", "Val_Celula", "Val_Linha", "Val_WorkCenter",
            "Val_ScheduleTime", "Val_Producao", "Val_ProducaoTeorica", "Val_Updt_Mtbf",
            "Val_Tempo_Updt_Mtbf", "Val_Tempo_Uptime_Mtbf", "Val_Tempo_Exception",
            "Val_Total_Paradas", "Val_PDT", "Val_Tempo_PDT", "Val_UPDT", "Val_Tempo_UPDT",
            "Val_Downtime", "Val_Uptime", "Val_Tempo_Updt_Cigarette", "Val_Cigarette_Quebra",
            "Val_Cigarette_Process_Failure", "Val_Cigarette_Small_Stops", "Val_Tempo_Updt_Wallet",
            "Val_Wallet_Quebra", "Val_Wallet_Process_Failure", "Val_Wallet_Small_Stops",
            "Val_Tempo_Updt_Bundle", "Val_Bundle_Quebra", "Val_Bundle_Process_Failure",
            "Val_Bundle_Small_Stops", "Dtt_Record", "Ind_Type", "Qtd_Cigarette_Quebra",
            "Qtd_Cigarette_Process_Failure", "Qtd_Cigarette_Small_Stops", "Qtd_Wallet_Quebra",
            "Qtd_Wallet_Process_Failure", "Qtd_Wallet_Small_Stops", "Qtd_Bundle_Quebra",
            "Qtd_Bundle_Process_Failure", "Qtd_Bundle_Small_Stops", "Mtbf_Maker_Updt",
            "Mtbf_Maker_Tempo_Updt", "Mtbf_Maker_Tempo_Uptime", "Mtbf_Bundle_Updt",
            "Mtbf_Bundle_Tempo_Updt", "Mtbf_Bundle_Tempo_Uptime", "Maker_Producao",
            "Maker_Rejeicao", "Packer_Producao", "Packer_Rejeicao", "Val_Tecnologia",
            "Val_Tempo_Updt_Case", "Val_Case_Quebra", "Val_Case_Process_Failure",
            "Val_Case_Small_Stops", "Val_Tempo_Updt_Wrapper", "Val_Wrapper_Quebra",
            "Val_Wrapper_Process_Failure", "Val_Wrapper_Small_Stops", "Val_Planejada_CIL",
            "Val_Planejada_Outros", "Dtt_Update", "Stt_Record", "Dtt_Disable"
        ],
        colunas_insert= [
            "cod_record", "dtt_workday", "cod_shift", "val_celula", "val_linha", "val_workcenter",
            "val_scheduletime", "val_producao", "val_producaoteorica", "val_updt_mtbf",
            "val_tempo_updt_mtbf", "val_tempo_uptime_mtbf", "val_tempo_exception",
            "val_total_paradas", "val_pdt", "val_tempo_pdt", "val_updt", "val_tempo_updt",
            "val_downtime", "val_uptime", "val_tempo_updt_cigarette", "val_cigarette_quebra",
            "val_cigarette_process_failure", "val_cigarette_small_stops", "val_tempo_updt_wallet",
            "val_wallet_quebra", "val_wallet_process_failure", "val_wallet_small_stops",
            "val_tempo_updt_bundle", "val_bundle_quebra", "val_bundle_process_failure",
            "val_bundle_small_stops", "dtt_record", "ind_type", "qtd_cigarette_quebra",
            "qtd_cigarette_process_failure", "qtd_cigarette_small_stops", "qtd_wallet_quebra",
            "qtd_wallet_process_failure", "qtd_wallet_small_stops", "qtd_bundle_quebra",
            "qtd_bundle_process_failure", "qtd_bundle_small_stops", "mtbf_maker_updt",
            "mtbf_maker_tempo_updt", "mtbf_maker_tempo_uptime", "mtbf_bundle_updt",
            "mtbf_bundle_tempo_updt", "mtbf_bundle_tempo_uptime", "maker_producao",
            "maker_rejeicao", "packer_producao", "packer_rejeicao", "val_tecnologia",
            "val_tempo_updt_case", "val_case_quebra", "val_case_process_failure",
            "val_case_small_stops", "val_tempo_updt_wrapper", "val_wrapper_quebra",
            "val_wrapper_process_failure", "val_wrapper_small_stops", "val_planejada_cil",
            "val_planejada_outros", "dtt_update", "stt_record", "dtt_disable"
        ],
        joins_list= None,
        chave_id= "cod_record",
        lote= 1000,
        id_inicial= 2149268,
        period_filter_status= "off",
        refresh_workday_status= "off",
        workday_role= None,
        period_column= None,
        period_name= None,
        period_value= None,
    )
    
def copiar_lote_tb_LT_Tratada() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_udi_chronos_reader",
        conn_destino_id= "pg_dw_apex",
        tabela_origem= "db_mcChronosDW.consolidation.tb_LT_Tratada",
        tabela_destino= "apex_dw_raw.udi_chronos_mcchronosdw.tb_lt_tratada",
        colunas_busca= [
                "Cod_Record", "Id", "Planta", "Departamento", "Celula", "Linha",
                "Modulo", "Maquina", "Equipamento", "Equipment_Type", "Grupo_PTA",
                "Sub_Grupo", "Componente", "Ind_Type", "Code_Exc", "Workday",
                "Shift", "Data_Hora_Inicio_Stop", "Data_Hora_Final_Stop", "Motivo",
                "Motivo_Ingles", "Natureza", "Classificacao", "Reclassificacao",
                "Tipo", "Referencia", "Comentario_Chronos", "Downtime", "Uptime",
                "Scheduletime", "Std_Rate", "Producao_Teorica", "Producao_Total",
                "SKU", "Cod_Process", "Des_Process", "Cod_RefBreak", "Dtt_Record",
                "Usr_Record", "Dtt_Update", "Stt_Record", "Dtt_Disable"
            ],
        colunas_insert= [
                "cod_record", "id", "planta", "departamento", "celula", "linha",
                "modulo", "maquina", "equipamento", "equipment_type", "grupo_pta",
                "sub_grupo", "componente", "ind_type", "code_exc", "workday",
                "shift", "data_hora_inicio_stop", "data_hora_final_stop", "motivo",
                "motivo_ingles", "natureza", "classificacao", "reclassificacao",
                "tipo", "referencia", "comentario_chronos", "downtime", "uptime",
                "scheduletime", "std_rate", "producao_teorica", "producao_total",
                "sku", "cod_process", "des_process", "cod_refbreak", "dtt_record",
                "usr_record", "dtt_update", "stt_record", "dtt_disable"
            ],
        joins_list= None,
        chave_id= "cod_record",
        lote= 1000,
        id_inicial= 0,
        period_filter_status= "on",
        refresh_workday_status= "on",
        workday_role= "workday",
        period_column= "dtt_record",
        period_name= "DAY",
        period_value= -1,
    )

###############################################################################################################
#                          Definição das tarefas de atualizar tabelas de cadastro
###############################################################################################################

with DAG(
    dag_id="UDI_CHRONOS_ingest_bronze_incremental_tables",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    max_active_runs=1,
    catchup=False,
    default_args={"retries": 0},
    tags=["Bronze Ingest", "postgres", "owner_JoaoLucas", "UDI","CHRONOS", "owner_Janatan"],
) as dag_get_tables:

    start = EmptyOperator(task_id="start")

    get_tb_ProductionOder =   PythonOperator(
        task_id="copiar_lote_tb_ProductionOrder",
        python_callable=copiar_lote_tb_ProductionOrder,
    )
    
    get_tb_ProductionOderLog =   PythonOperator(
        task_id="copiar_lote_tb_ProductionOrderLog",
        python_callable=copiar_lote_tb_ProductionOrderLog,
    )

    get_tb_ProductionOderBOM =   PythonOperator(
        task_id="copiar_lote_tb_ProductionOrderBOM",
        python_callable=copiar_lote_tb_ProductionOrderBOM,
    )

    get_tb_ProductDesign =   PythonOperator(
        task_id="copiar_lote_tb_ProductDesign",
        python_callable=copiar_lote_tb_ProductDesign,
    )

    get_tb_Product =   PythonOperator(
        task_id="copiar_lote_tb_Product",
        python_callable=copiar_lote_tb_Product,
    )

    get_tb_MicroEvents =   PythonOperator(
        task_id="copiar_lote_tb_MicroEvents",
        python_callable=copiar_lote_tb_MicroEvents,
    )

    get_tb_CauseMicroEvents =   PythonOperator(
        task_id="copiar_lote_tb_CauseMicroEvents",
        python_callable=copiar_lote_tb_CauseMicroEvents,
    )

    get_tb_NatureMicroEvents =   PythonOperator(
        task_id="copiar_lote_tb_NatureMicroEvents",
        python_callable=copiar_lote_tb_NatureMicroEvents,
    )

    get_tb_ReasonCodeMicroEvents =   PythonOperator(
        task_id="copiar_lote_tb_ReasonCodeMicroEvents",
        python_callable=copiar_lote_tb_ReasonCodeMicroEvents,
    )

    get_tb_ExceptionEntry =   PythonOperator(
        task_id="copiar_lote_tb_ExceptionEntry",
        python_callable=copiar_lote_tb_ExceptionEntry,
    )

    get_tb_ExceptionEntryDateApprover =   PythonOperator(
        task_id="copiar_lote_tb_ExceptionEntryDateApprover",
        python_callable=copiar_lote_tb_ExceptionEntryDateApprover,
    )

    get_tb_ExceptionEntryDate =   PythonOperator(
        task_id="copiar_lote_tb_ExceptionEntryDate",
        python_callable=copiar_lote_tb_ExceptionEntryDate,
    )

    get_tb_ExceptionReasonApprover =   PythonOperator(
        task_id="copiar_lote_tb_ExceptionReasonApprover",
        python_callable=copiar_lote_tb_ExceptionReasonApprover,
    )

    get_tb_ExceptionEntryAttachments =   PythonOperator(
        task_id="copiar_lote_tb_ExceptionEntryAttachments",
        python_callable=copiar_lote_tb_ExceptionEntryAttachments,
    )

    get_tb_ExceptionCalendar =   PythonOperator(
        task_id="copiar_lote_tb_ExceptionCalendar",
        python_callable=copiar_lote_tb_ExceptionCalendar,
    )

    get_tb_ExceptionCalendarDate =   PythonOperator(
        task_id="copiar_lote_tb_ExceptionCalendarDate",
        python_callable=copiar_lote_tb_ExceptionCalendarDate,
    )

    get_tb_Characteristic =   PythonOperator(
        task_id="copiar_lote_tb_Characteristic",
        python_callable=copiar_lote_tb_Characteristic,
    )
    
    get_tb_QualitySample =   PythonOperator(
        task_id="copiar_lote_tb_QualitySample",
        python_callable=copiar_lote_tb_QualitySample,
    )
    
    get_tb_QualitySampleResult =   PythonOperator(
        task_id="copiar_lote_tb_QualitySampleResult",
        python_callable=copiar_lote_tb_QualitySampleResult,
    )
    
    get_tb_DadosLossTree_PowerBI =   PythonOperator(
        task_id="copiar_lote_tb_DadosLossTree_PowerBI",
        python_callable=copiar_lote_tb_DadosLossTree_PowerBI,
    )
    
    get_tb_LT_Tratada =   PythonOperator(
        task_id="copiar_lote_tb_LT_Tratada",
        python_callable=copiar_lote_tb_LT_Tratada,
    )

    end = EmptyOperator(task_id="end")

    wait_01 = BashOperator(task_id="wait_01", bash_command="sleep 4")
    wait_02 = BashOperator(task_id="wait_02", bash_command="sleep 4")
    wait_03 = BashOperator(task_id="wait_03", bash_command="sleep 4")
    wait_04 = BashOperator(task_id="wait_04", bash_command="sleep 4")
    wait_05 = BashOperator(task_id="wait_05", bash_command="sleep 4")
    wait_06 = BashOperator(task_id="wait_06", bash_command="sleep 4")
    wait_07 = BashOperator(task_id="wait_07", bash_command="sleep 4")
    wait_08 = BashOperator(task_id="wait_08", bash_command="sleep 4")
    wait_09 = BashOperator(task_id="wait_09", bash_command="sleep 4")
    wait_10 = BashOperator(task_id="wait_10", bash_command="sleep 4")
    wait_11 = BashOperator(task_id="wait_11", bash_command="sleep 4")
    wait_12 = BashOperator(task_id="wait_12", bash_command="sleep 4")
    wait_13 = BashOperator(task_id="wait_13", bash_command="sleep 4")
    wait_14 = BashOperator(task_id="wait_14", bash_command="sleep 4")
    wait_15 = BashOperator(task_id="wait_15", bash_command="sleep 4")
    wait_16 = BashOperator(task_id="wait_16", bash_command="sleep 4")
    wait_17 = BashOperator(task_id="wait_17", bash_command="sleep 4")
    wait_18 = BashOperator(task_id="wait_18", bash_command="sleep 4")
    wait_19 = BashOperator(task_id="wait_19", bash_command="sleep 4")
    wait_20 = BashOperator(task_id="wait_20", bash_command="sleep 4")


    start \
    >> get_tb_ProductionOder >> wait_01 \
    >> get_tb_ProductionOderLog >> wait_02 \
    >> get_tb_ProductDesign >> wait_03 \
    >> get_tb_Product >> wait_04 \
    >> get_tb_ProductionOderBOM >> wait_05 \
    >> get_tb_MicroEvents >> wait_06 \
    >> get_tb_CauseMicroEvents >> wait_07 \
    >> get_tb_ReasonCodeMicroEvents >> wait_08 \
    >> get_tb_NatureMicroEvents >> wait_09 \
    >> get_tb_ExceptionEntry >> wait_10 \
    >> get_tb_ExceptionEntryDateApprover >> wait_11 \
    >> get_tb_ExceptionEntryDate >> wait_12 \
    >> get_tb_ExceptionReasonApprover >> wait_13 \
    >> get_tb_ExceptionEntryAttachments >> wait_14 \
    >> get_tb_ExceptionCalendar >> wait_15 \
    >> get_tb_ExceptionCalendarDate >> wait_16 \
    >> get_tb_Characteristic >> wait_17 \
    >> get_tb_QualitySample >> wait_18 \
    >> get_tb_QualitySampleResult >> wait_19 \
    >> get_tb_DadosLossTree_PowerBI >> wait_20 \
    >> get_tb_LT_Tratada \
    >> end

    