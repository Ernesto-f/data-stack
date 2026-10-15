from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator

## FUNÇÃO DE COPIAR LOTES
##from commom.proc_bronze_ingest_tables import copiar_lote_incremental_odbc
from commom.proc_bronze_ingest_VIEW_refresh_workday import copiar_lote_incremental_refresh_filter


# =====================================================================
# Wrappers por tabela
# =====================================================================



## migrado para o postgres
def copiar_lote_leaf_productionorderlog() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_leaf_chronos",
        conn_destino_id= "pg_dw_apex",
        tabela_origem= "db_mcExperienceSuite.mesAdapter.tb_ProductionOrderLog",
        tabela_destino= "apex_dw_raw.leaf_chronos_mcexperiencesuite.tb_productionorderlog",
        colunas_busca= [
            "Cod_Record","Cod_ProductionOrder","Cod_Shift","Val_Facility","Val_Department",
            "Val_WorkCenter","Val_Equipment","Val_EquipmentType","Val_CompleteQuantity",
            "Val_WorkDay","Dtt_Collect","Des_Comment","Dtt_Record","Ind_Manual","Usr_Record",
            "Val_UOM","Val_UOMConverted","Val_CompleteQuantityConverted","Ind_Type",
            "Cod_Container","Val_Material","Des_Reason","Val_QualityStatus","Val_Experiment",
            "Val_QualitySample","Dtt_StatusUpdate","Val_TypeStatus","Usr_RejectionStatus",
            "Val_StatusChangeReason","Val_RejectionComment","Ind_SapNotification",
            "Dtt_Update","Dtt_Disable","Stt_Record","Val_ErpReferenceCode","Des_ErpMessage",
            "Ind_ErpResult","Dtt_ErpExecution","Stt_ErpReversal","Ind_QualityStatusERP"
                        ],
        colunas_insert= [
            "cod_record","cod_productionorder","cod_shift","val_facility","val_department",
            "val_workcenter","val_equipment","val_equipmenttype","val_completequantity",
            "val_workday","dtt_collect","des_comment","dtt_record","ind_manual","usr_record",
            "val_uom","val_uomconverted","val_completequantityconverted","ind_type",
            "cod_container","val_material","des_reason","val_qualitystatus","val_experiment",
            "val_qualitysample","dtt_statusupdate","val_typestatus","usr_rejectionstatus",
            "val_statuschangereason","val_rejectioncomment","ind_sapnotification",
            "dtt_update","dtt_disable","stt_record","val_erpreferencecode","des_erpmessage",
            "ind_erpresult","dtt_erpexecution","stt_erpreversal","ind_qualitystatuserp",
        ],
        joins_list= None,
        chave_id= "cod_record",
        lote= 5000,
        id_inicial= 1308146,
        period_filter_status= "off",
        refresh_workday_status= "off",
        workday_role= None,
        period_column= None,
        period_name= None,
        period_value= None,
    )


## migrado pro postgres
def copiar_lote_leaf_productionorder() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_leaf_chronos",
        conn_destino_id= "pg_dw_apex",
        tabela_origem= "db_mcExperienceSuite.mesAdapter.tb_ProductionOrder",
        tabela_destino= "apex_dw_raw.leaf_chronos_mcexperiencesuite.tb_productionorder",
    colunas_busca=[
        "Cod_Record","Cod_ProductDesign","Val_ProductionOrderNumber","Val_OperationSequenceNo",
        "Val_ScheduledStartUTC","Val_ScheduledEndUTC","Dtt_Start","Dtt_Stop","Val_Identifier",
        "Val_Facility","Val_Department","Val_WorkCenter","Val_ReqQuantity","Val_UOM",
        "Val_ConfirmedQuantity","Val_RatioQuantity","Ind_Status","Ind_Test","Usr_Record",
        "Dtt_Record","Dtt_Update","Ind_ManualInsert","Val_BarCode","Cod_ProductionOrderClassification",
        "Val_Equipment","Dtt_Disable","Stt_Record",
    ],
        colunas_insert=[
            "cod_record","cod_productdesign","val_productionordernumber","val_operationsequenceno",
            "val_scheduledstartutc","val_scheduledendutc","dtt_start","dtt_stop","val_identifier",
            "val_facility","val_department","val_workcenter","val_reqquantity","val_uom",
            "val_confirmedquantity","val_ratioquantity","ind_status","ind_test","usr_record",
            "dtt_record","dtt_update","ind_manualinsert","val_barcode","cod_productionorderclassification",
            "val_equipment","dtt_disable","stt_record",
        ],
        joins_list= None,
        chave_id= "cod_record",
        lote= 5000,
        id_inicial= 228371,
        period_filter_status= "on",
        refresh_workday_status= "on",
        workday_role= "workday",
        period_column= "dtt_record",
        period_name= "DAY",
        period_value= -7,
    )


##def copiar_lote_leaf_container():
##    copiar_lote_incremental_odbc(
##        conn_origem_id="sql_leaf_chronos",
##        conn_destino_id="sql_udi_apex_azure",
##        tabela_origem="db_mcExperienceSuite.mesAdapter.tb_Container",
##        tabela_destino="leaf_db_mcExperienceSuite.mesAdapter.tb_Container",
##        chave_id="Cod_Record",
##        lote=5000,
##        id_inicial=5716936,
##        colunas_busca=[
##            "Cod_Record","Dtt_Record","Usr_Record","Typ_Container","Typ_UOM","Qtd_Capacity",
##            "Des_Position","Jsn_Metadata","Des_Status","Cod_Container","Cod_LotManegement",
##            "Des_ContainerID","Qtd_Tare","Qtd_Container","Qtd_Consumed","Des_LabelStatus",
##            "Ind_Quality","Des_Inactivation","Nam_Inactivation","Dtt_LastMoviment",
##            "Des_ContainerOld","Num_ContainerSequence","Dtt_LastAppLoad","Val_CodeBar",
##            "Num_Position","Sts_SAPBlock","Dtt_Update","Dtt_Disable","Stt_Record","Des_ErpLotType",
##        ],
##        colunas_insert=[
##            "Cod_Record","Dtt_Record","Usr_Record","Typ_Container","Typ_UOM","Qtd_Capacity",
##            "Des_Position","Jsn_Metadata","Des_Status","Cod_Container","Cod_LotManegement",
##            "Des_ContainerID","Qtd_Tare","Qtd_Container","Qtd_Consumed","Des_LabelStatus",
##            "Ind_Quality","Des_Inactivation","Nam_Inactivation","Dtt_LastMoviment",
##            "Des_ContainerOld","Num_ContainerSequence","Dtt_LastAppLoad","Val_CodeBar",
##            "Num_Position","Sts_SAPBlock","Dtt_Update","Dtt_Disable","Stt_Record","Des_ErpLotType",
##        ],
##    )

## migrado para o postgres
def copiar_lote_leaf_container() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_leaf_chronos",
        conn_destino_id= "pg_dw_apex",
        tabela_origem= "db_mcExperienceSuite.mesAdapter.tb_Container",
        tabela_destino= "apex_dw_raw.leaf_chronos_mcexperiencesuite.tb_container",
        colunas_busca=[
            "Cod_Record","Dtt_Record","Usr_Record","Typ_Container","Typ_UOM","Qtd_Capacity",
            "Des_Position","Jsn_Metadata","Des_Status","Cod_Container","Cod_LotManegement",
            "Des_ContainerID","Qtd_Tare","Qtd_Container","Qtd_Consumed","Des_LabelStatus",
            "Ind_Quality","Des_Inactivation","Nam_Inactivation","Dtt_LastMoviment",
            "Des_ContainerOld","Num_ContainerSequence","Dtt_LastAppLoad","Val_CodeBar",
            "Num_Position","Sts_SAPBlock","Dtt_Update","Dtt_Disable","Stt_Record","Des_ErpLotType",
        ],
        colunas_insert=[
            "cod_record","dtt_record","usr_record","typ_container","typ_uom","qtd_capacity",
            "des_position","jsn_metadata","des_status","cod_container","cod_lotmanegement",
            "des_containerid","qtd_tare","qtd_container","qtd_consumed","des_labelstatus",
            "ind_quality","des_inactivation","nam_inactivation","dtt_lastmoviment",
            "des_containerold","num_containersequence","dtt_lastappload","val_codebar",
            "num_position","sts_sapblock","dtt_update","dtt_disable","stt_record","des_erplottype",
        ],
        joins_list= None,
        chave_id= "cod_record",
        lote= 10000,
        id_inicial= 5716936,
        period_filter_status= "on",
        refresh_workday_status= "on",
        workday_role= "workday",
        period_column= "dtt_record",
        period_name= "DAY",
        period_value= -7,
    )

##def copiar_lote_leaf_qualitysample():
##    copiar_lote_incremental_odbc(
##        conn_origem_id="sql_leaf_chronos",
##        conn_destino_id="sql_udi_apex_azure",
##        tabela_origem="db_mcExperienceSuite.mesAdapter.tb_QualitySample",
##        tabela_destino="leaf_db_mcExperienceSuite.mesAdapter.tb_QualitySample",
##        chave_id="Cod_Record",
##        lote=10000,
##        id_inicial=3585561,
##        colunas_busca=[
##            "Cod_Record","Cod_ProductionOrder","Val_SampleID","Val_Facility","Val_Department",
##            "Val_WorkCenter","Val_Equipment","Val_EquipmentType","Val_QualityEquipmentID",
##            "Val_NPIFlag","Cod_Shift","Val_Material","Val_POShiftStartDateTime","Val_SFGMaterial",
##            "Val_SFGSpecValidFrom","Val_WorkDay","Dtt_Record","Ind_Manual","Usr_Record","Des_Comment",
##            "Cod_ImportFile","Val_Source","Val_Status","Val_ContainerIDIni","Val_ContainerIDFim",
##            "Dtt_SampleReference","Dtt_Update","Usr_Update","Ind_Reprocess","Dtt_Reprocess",
##            "Ind_Planned","Ind_Delete","Des_DeleteReason","Dtt_Disable","Stt_Record",
##        ],
##        colunas_insert=[
##            "Cod_Record","Cod_ProductionOrder","Val_SampleID","Val_Facility","Val_Department",
##            "Val_WorkCenter","Val_Equipment","Val_EquipmentType","Val_QualityEquipmentID",
##            "Val_NPIFlag","Cod_Shift","Val_Material","Val_POShiftStartDateTime","Val_SFGMaterial",
##            "Val_SFGSpecValidFrom","Val_WorkDay","Dtt_Record","Ind_Manual","Usr_Record","Des_Comment",
##            "Cod_ImportFile","Val_Source","Val_Status","Val_ContainerIDIni","Val_ContainerIDFim",
##            "Dtt_SampleReference","Dtt_Update","Usr_Update","Ind_Reprocess","Dtt_Reprocess",
##            "Ind_Planned","Ind_Delete","Des_DeleteReason","Dtt_Disable","Stt_Record",
##        ],
##    )

## migrado para postgres
def copiar_lote_leaf_qualitysample() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_leaf_chronos",
        conn_destino_id= "pg_dw_apex",
        tabela_origem= "db_mcExperienceSuite.mesAdapter.tb_QualitySample",
        tabela_destino= "apex_dw_raw.leaf_chronos_mcexperiencesuite.tb_qualitysample",
        colunas_busca=[
            "Cod_Record","Cod_ProductionOrder","Val_SampleID","Val_Facility","Val_Department",
            "Val_WorkCenter","Val_Equipment","Val_EquipmentType","Val_QualityEquipmentID",
            "Val_NPIFlag","Cod_Shift","Val_Material","Val_POShiftStartDateTime","Val_SFGMaterial",
            "Val_SFGSpecValidFrom","Val_WorkDay","Dtt_Record","Ind_Manual","Usr_Record","Des_Comment",
            "Cod_ImportFile","Val_Source","Val_Status","Val_ContainerIDIni","Val_ContainerIDFim",
            "Dtt_SampleReference","Dtt_Update","Usr_Update","Ind_Reprocess","Dtt_Reprocess",
            "Ind_Planned","Ind_Delete","Des_DeleteReason","Dtt_Disable","Stt_Record",
        ],
        colunas_insert=[
            "cod_record","cod_productionorder","val_sampleid","val_facility","val_department",
            "val_workcenter","val_equipment","val_equipmenttype","val_qualityequipmentid",
            "val_npiflag","cod_shift","val_material","val_poshiftstartdatetime","val_sfgmaterial",
            "val_sfgspecvalidfrom","val_workday","dtt_record","ind_manual","usr_record","des_comment",
            "cod_importfile","val_source","val_status","val_containeridini","val_containeridfim",
            "dtt_samplereference","dtt_update","usr_update","ind_reprocess","dtt_reprocess",
            "ind_planned","ind_delete","des_deletereason","dtt_disable","stt_record",
        ],
        joins_list= None,
        chave_id= "cod_record",
        lote= 10000,
        id_inicial= 3585561,
        period_filter_status= "on",
        refresh_workday_status= "on",
        workday_role= "workday",
        period_column= "dtt_record",
        period_name= "DAY",
        period_value= -7,
    )



## migrado para postgres
def copiar_lote_leaf_qualitysampleresult() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_leaf_chronos",
        conn_destino_id= "pg_dw_apex",
        tabela_origem= "db_mcExperienceSuite.mesAdapter.tb_QualitySampleResult",
        tabela_destino= "apex_dw_raw.leaf_chronos_mcexperiencesuite.tb_qualitysampleresult",
        colunas_busca=[
             "Cod_Record","Cod_ResultGroup","Cod_QualitySample","Cod_Characteristic","Val_Result",
             "Des_DeleteReason","Ind_Delete","Dtt_Record","Usr_Record","Dtt_Result","Dtt_Update",
             "Val_Identifier","Val_Status","Val_ContainerIDIni","Val_ContainerIDFim","Ind_ManualRead",
             "Des_ManualReadReason","Dtt_Disable","Stt_Record",
         ],
        colunas_insert=[
             "cod_record","cod_resultgroup","cod_qualitysample","cod_characteristic","val_result",
             "des_deletereason","ind_delete","dtt_record","usr_record","dtt_result","dtt_update",
             "val_identifier","val_status","val_containeridini","val_containeridfim","ind_manualread",
             "des_manualreadreason","dtt_disable","stt_record",
         ],
        joins_list= None,
        chave_id= "cod_record",
        lote= 5000,
        id_inicial= 22897667,
        period_filter_status= "off",
        refresh_workday_status= "off",
        workday_role= None,
        period_column= None,
        period_name= None,
        period_value= None,
    )

## migrado para postgres
def copiar_lote_leaf_experiment() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_leaf_chronos",
        conn_destino_id= "pg_dw_apex",
        tabela_origem= "db_mcExperienceSuite.mesAdapter.tb_Experiment",
        tabela_destino= "apex_dw_raw.leaf_chronos_mcexperiencesuite.tb_experiment",
        colunas_busca=[
            "Cod_Record","Nam_Experiment","Des_Experiment","Ind_Composit","Ind_Setup","Stt_Record",
            "Val_PageSource","Val_TagColor","Ind_DiffSKU","Ind_Container","Usr_Record",
            "Dtt_Record","Dtt_Update","Dtt_Disable",
        ],
        colunas_insert=[
            "cod_record","nam_experiment","des_experiment","ind_composit","ind_setup","stt_record",
            "val_pagesource","val_tagcolor","ind_diffsku","ind_container","usr_record",
            "dtt_record","dtt_update","dtt_disable",
        ],
        joins_list= None,
        chave_id= "cod_record",
        lote= 200,
        id_inicial= 0,
        period_filter_status= "off",
        refresh_workday_status= "off",
        workday_role= None,
        period_column= None,
        period_name= None,
        period_value= None,
    )



## migrado para postgres
def copiar_lote_leaf_characteristic() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_leaf_chronos",
        conn_destino_id= "pg_dw_apex",
        tabela_origem= "db_mcExperienceSuite.mesAdapter.tb_Characteristic",
        tabela_destino= "apex_dw_raw.leaf_chronos_mcexperiencesuite.tb_characteristic",
        colunas_busca=[
            "Cod_Record","Nam_Characteristic","Des_Characteristic","Val_UnitOfMeasure","Ind_Q2Type",
            "Val_ExternalCodeCharacteristic","Val_ExternalCodeSetPointEng","Val_ExternalCodeLIEng",
            "Val_ExternalCodeLSEng","Ind_ProcessingType","Val_LimitRule","Val_RagRule",
            "Val_DecimalRule","Usr_Record","Dtt_Record","Val_ExternalCodeLocalMes","Nam_Reference",
            "Val_ExternalCodeCharacteristic2","Val_ExternalCodeCharacteristic3","Flg_DWProcessed",
            "Cod_ExperimentFK","Val_ToleranceRule","Dtt_Update","Dtt_Disable","Stt_Record",
        ],
        colunas_insert=[
            "cod_record","nam_characteristic","des_characteristic","val_unitofmeasure","ind_q2type",
            "val_externalcodecharacteristic","val_externalcodesetpointeng","val_externalcodelieng",
            "val_externalcodelseng","ind_processingtype","val_limitrule","val_ragrule",
            "val_decimalrule","usr_record","dtt_record","val_externalcodelocalmes","nam_reference",
            "val_externalcodecharacteristic2","val_externalcodecharacteristic3","flg_dwprocessed",
            "cod_experimentfk","val_tolerancerule","dtt_update","dtt_disable","stt_record",
        ],
        joins_list= None,
        chave_id= "cod_record",
        lote= 200,
        id_inicial= 0,
        period_filter_status= "off",
        refresh_workday_status= "off",
        workday_role= None,
        period_column= None,
        period_name= None,
        period_value= None,
    )



## migrado para postgres
def copiar_lote_leaf_containerqualitystatushistory() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_leaf_chronos",
        conn_destino_id= "pg_dw_apex",
        tabela_origem= "db_mcExperienceSuite.mesAdapter.tb_ContainerQualityStatusHistory",
        tabela_destino= "apex_dw_raw.leaf_chronos_mcexperiencesuite.tb_containerqualitystatushistory",
        colunas_busca=[
             "Cod_Record","Cod_ProductionOrder","Cod_Container","Val_QualityStatus","Val_TypeUpdate",
             "Usr_Update","Dtt_Record","Cod_QualitySample","Cod_Experiment","Val_Document",
             "Dtt_Update","Dtt_Disable","Stt_Record",
         ],
        colunas_insert=[
             "cod_record","cod_productionorder","cod_container","val_qualitystatus","val_typeupdate",
             "usr_update","dtt_record","cod_qualitysample","cod_experiment","val_document",
             "dtt_update","dtt_disable","stt_record",
         ],
        joins_list= None,
        chave_id= "cod_record",
        lote= 5000,
        id_inicial= 1513446,
        period_filter_status= "off",
        refresh_workday_status= "off",
        workday_role= None,
        period_column= None,
        period_name= None,
        period_value= None,
    )

def copiar_lote_leaf_microevents() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_leaf_chronos",
        conn_destino_id= "pg_dw_apex",
        tabela_origem= "db_mcExperienceSuite.mesAdapter.tb_MicroEvents",
        tabela_destino= "apex_dw_raw.leaf_chronos_mcexperiencesuite.tb_microevents",
        colunas_busca = [
            "Cod_Record", "Cod_ProductionOrder", "Cod_ReasonCode", "Cod_Cause", "Cod_Nature", "Cod_Shift",
            "Dtt_Start", "Dtt_End", "Ind_Type", "Val_Facility", "Val_Department",
            "Val_WorkCenter", "Val_Equipment", "Val_EquipmentType", "Val_Quantity", "Val_UOM",
            "Val_CompleteQuantityConverted", "Val_UOMConverted", "Ind_Authorized", "Ind_Manual",
            "Dtt_Record", "Usr_Record", "Dtt_WorkDay", "Dtt_Update", "Val_StandartRate",
            "Des_EquipmentID", "Dtt_Disable", "Stt_Record"
                        ],
        colunas_insert= [
            "cod_record", "cod_productionorder", "cod_reasoncode", "cod_cause", "cod_nature", "cod_shift",
            "dtt_start", "dtt_end", "ind_type", "val_facility", "val_department",
            "val_workcenter", "val_equipment", "val_equipmenttype", "val_quantity", "val_uom",
            "val_completequantityconverted", "val_uomconverted", "ind_authorized", "ind_manual",
            "dtt_record", "usr_record", "dtt_workday", "dtt_update", "val_standartrate",
            "des_equipmentid", "dtt_disable", "stt_record"
                        ],
        joins_list= None,
        chave_id= "cod_record",
        lote= 1000,
        id_inicial= 264926,
        period_filter_status= "off",
        refresh_workday_status= "off",
        workday_role= None,
        period_column= None,
        period_name= None,
        period_value= None,
    )

def copiar_lote_leaf_causemicroevents() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_leaf_chronos",
        conn_destino_id= "pg_dw_apex",
        tabela_origem= "db_mcExperienceSuite.mesAdapter.tb_CauseMicroEvents",
        tabela_destino= "apex_dw_raw.leaf_chronos_mcexperiencesuite.tb_causemicroevents",
        colunas_busca = [
            "Cod_Record", "Nam_Cause", "Des_Cause", "Val_Equipment", "Ind_Type",
            "Dtt_Record", "Dtt_Update", "Dtt_Disable", "Stt_Record"
                        ],
        colunas_insert= [
            "cod_record", "nam_cause", "des_cause", "val_equipment", "ind_type",
            "dtt_record", "dtt_update", "dtt_disable", "stt_record"
                        ],
        joins_list= None,
        chave_id= "cod_record",
        lote= 500,
        id_inicial= 1,
        period_filter_status= "off",
        refresh_workday_status= "off",
        workday_role= None,
        period_column= None,
        period_name= None,
        period_value= None,
    )

def copiar_lote_leaf_naturemicroevents() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_leaf_chronos",
        conn_destino_id= "pg_dw_apex",
        tabela_origem= "db_mcExperienceSuite.mesAdapter.tb_NatureMicroEvents",
        tabela_destino= "apex_dw_raw.leaf_chronos_mcexperiencesuite.tb_naturemicroevents",
        colunas_busca = [
            "Cod_Record", "Nam_Nature", "Des_Nature", "Ind_Type", "Dtt_Record",
            "Dtt_Update", "Dtt_Disable", "Stt_Record"
                        ],
        colunas_insert= [
            "cod_record", "nam_nature", "des_nature", "ind_type", "dtt_record",
            "dtt_update", "dtt_disable", "stt_record"
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

def copiar_lote_leaf_reasoncodemicroevents() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_leaf_chronos",
        conn_destino_id= "pg_dw_apex",
        tabela_origem= "db_mcExperienceSuite.mesAdapter.tb_ReasonCodeMicroEvents",
        tabela_destino= "apex_dw_raw.leaf_chronos_mcexperiencesuite.tb_reasoncodemicroevents",
        colunas_busca = [
            "Cod_Record", "Val_Equipment", "Val_ReasonCode", "Des_ReasonCode", "Ind_Type",
            "Ind_Manual", "Dtt_Record", "Dtt_Update", "Dtt_Disable", "Stt_Record"
                        ],
        colunas_insert= [
            "cod_record", "val_equipment", "val_reasoncode", "des_reasoncode", "ind_type",
            "ind_manual", "dtt_record", "dtt_update", "dtt_disable", "stt_record"
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

def copiar_lote_leaf_productdesign() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_leaf_chronos",
        conn_destino_id= "pg_dw_apex",
        tabela_origem= "db_mcExperienceSuite.mesAdapter.tb_ProductDesign",
        tabela_destino= "apex_dw_raw.leaf_chronos_mcexperiencesuite.tb_productdesign",
        colunas_busca = [
            "Cod_Record", "Cod_Product", "Nam_ProductDesign", "Des_ProductDesign",
            "Dtt_Record", "Des_Identifier", "Dtt_Update",
            "Dtt_Disable", "Stt_Record"
            ],
        colunas_insert= [
            "cod_record", "cod_product", "nam_productdesign", "des_productdesign",
            "dtt_record", "des_identifier", "dtt_update",
            "dtt_disable", "stt_record"
            ],
        joins_list= None,
        chave_id= "cod_record",
        lote= 250,
        id_inicial= 2067,
        period_filter_status= "off",
        refresh_workday_status= "off",
        workday_role= None,
        period_column= None,
        period_name= None,
        period_value= None,
    )

def copiar_lote_leaf_product() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_leaf_chronos",
        conn_destino_id= "pg_dw_apex",
        tabela_origem= "db_mcExperienceSuite.mesAdapter.tb_Product",
        tabela_destino= "apex_dw_raw.leaf_chronos_mcexperiencesuite.tb_product",
        colunas_busca = [
            "Cod_Record", "Num_ProductNo", "Des_ProductDescription", "Ind_Premium",
            "Dtt_Record", "Dtt_Update", "Dtt_Disable", "Stt_Record"
            ],
        colunas_insert= [
            "cod_record", "num_productno", "des_productdescription", "ind_premium",
            "dtt_record", "dtt_update", "dtt_disable", "stt_record"
        ],
        joins_list= None,
        chave_id= "cod_record",
        lote= 500,
        id_inicial= 558,
        period_filter_status= "off",
        refresh_workday_status= "off",
        workday_role= None,
        period_column= None,
        period_name= None,
        period_value= None,
    )

def copiar_lote_leaf_productionorderbom() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id= "sql_leaf_chronos",
        conn_destino_id= "pg_dw_apex",
        tabela_origem= "db_mcExperienceSuite.mesAdapter.tb_ProductionOrderBOM",
        tabela_destino= "apex_dw_raw.leaf_chronos_mcexperiencesuite.tb_productionorderbom",
        colunas_busca = [
            "Cod_Record", "Cod_ProductionOrder", "Val_MaterialGroup",
            "Val_MaterialGroupDescription", "Val_Material", "Val_MaterialDescription",
            "Val_ReqQuantity", "Val_ReqUnitOfMeasure", "Val_OprSequenceNo",
            "Typ_Component", "Dtt_Record", "Val_ReqQuantitySAP",
            "Dtt_Update", "Dtt_Disable", "Stt_Record"
                        ],
        colunas_insert= [
            "cod_record", "cod_productionorder", "val_materialgroup",
            "val_materialgroupdescription", "val_material", "val_materialdescription",
            "val_reqquantity", "val_requnitofmeasure", "val_oprsequenceno",
            "typ_component", "dtt_record", "val_reqquantitysap",
            "dtt_update", "dtt_disable", "stt_record"
                        ],
        joins_list= None,
        chave_id= "cod_record",
        lote= 1000,
        id_inicial= 1315377,
        period_filter_status= "on",
        refresh_workday_status= "on",
        workday_role= "workday",
        period_column= "dtt_record",
        period_name= "DAY",
        period_value= -7,
    )


# =====================================================================
# DAGs (um por tabela)
# =====================================================================


with DAG(
    dag_id="LEAF_CHRONOS_ingest_bronze_incremental_tables",
    start_date=datetime(2025, 1, 1),
    schedule="*/5 * * * *",
    max_active_runs=1,
    catchup=False,
    default_args={"retries": 0},
    tags=["Bronze Ingest", "postgres", "owner_FelipeEidt", "LEAF", "owner_JoaoLucas"],
) as dag_get_tables:

    start = EmptyOperator(task_id="start")

    get_tb_ProductionOrderLog =   PythonOperator(
        task_id="copiar_lote_leaf_productionorderlog",
        python_callable=copiar_lote_leaf_productionorderlog,
    )

    get_tb_ProductionOrder = PythonOperator(
        task_id="copiar_lote_leaf_productionorder",
        python_callable=copiar_lote_leaf_productionorder,
    )

    get_tb_container = PythonOperator(
        task_id="copiar_lote_leaf_container",
        python_callable=copiar_lote_leaf_container,
    )

    get_tb_qualitysample = PythonOperator(
        task_id="copiar_lote_leaf_qualitysample",
        python_callable=copiar_lote_leaf_qualitysample,
    )

    get_tb_qualitysampleresult =   PythonOperator(
        task_id="copiar_lote_leaf_qualitysampleresult",
        python_callable=copiar_lote_leaf_qualitysampleresult,
    )

    get_tb_experiment =  PythonOperator(
        task_id="copiar_lote_leaf_experiment",
        python_callable=copiar_lote_leaf_experiment,
    )

    get_tb_characteristic = PythonOperator(
        task_id="copiar_lote_leaf_characteristic",
        python_callable=copiar_lote_leaf_characteristic,
    )

    get_tb_containerqualitystatushistory = PythonOperator(
        task_id="copiar_lote_leaf_containerqualitystatushistory",
        python_callable=copiar_lote_leaf_containerqualitystatushistory,
    )
    
    get_tb_microevents = PythonOperator(
        task_id="copiar_lote_leaf_microevents",
        python_callable=copiar_lote_leaf_microevents,
    )

    get_tb_causemicroevents = PythonOperator(
        task_id="copiar_lote_leaf_causemicroevents",
        python_callable=copiar_lote_leaf_causemicroevents,
    )

    get_tb_naturemicroevents = PythonOperator(
        task_id="copiar_lote_leaf_naturemicroevents",
        python_callable=copiar_lote_leaf_naturemicroevents,
    )

    get_tb_reasoncodemicroevents = PythonOperator(
        task_id="copiar_lote_leaf_reasoncodemicroevents",
        python_callable=copiar_lote_leaf_reasoncodemicroevents,
    )

    get_tb_productdesign = PythonOperator(
        task_id="copiar_lote_leaf_productdesign",
        python_callable=copiar_lote_leaf_productdesign,
    )

    get_tb_product = PythonOperator(
        task_id="copiar_lote_leaf_product",
        python_callable=copiar_lote_leaf_product,
    )

    get_tb_productionorderbom = PythonOperator(
        task_id="copiar_lote_leaf_productionorderbom",
        python_callable=copiar_lote_leaf_productionorderbom,
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
    wait_14 = BashOperator(task_id="wait_14", bash_command="sleep 4")
    wait_15 = BashOperator(task_id="wait_15", bash_command="sleep 4")

    start \
    >> get_tb_ProductionOrderLog >> wait_01 \
    >> get_tb_ProductionOrder >> wait_02 \
    >> get_tb_container >> wait_03 \
    >> get_tb_experiment >> wait_04 \
    >> get_tb_characteristic >> wait_05 \
    >> get_tb_containerqualitystatushistory >> wait_06 \
    >> get_tb_qualitysample >> wait_07 \
    >> get_tb_qualitysampleresult >> wait_08 \
    >> get_tb_microevents >> wait_09 \
    >> get_tb_causemicroevents >> wait_10 \
    >> get_tb_naturemicroevents >> wait_11 \
    >> get_tb_reasoncodemicroevents >> wait_12 \
    >> get_tb_productdesign >> wait_14 \
    >> get_tb_product >> wait_15 \
    >> get_tb_productionorderbom \
    >> end
  