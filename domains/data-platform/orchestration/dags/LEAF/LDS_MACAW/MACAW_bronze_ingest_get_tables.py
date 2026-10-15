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
        conn_origem_id= "",
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
