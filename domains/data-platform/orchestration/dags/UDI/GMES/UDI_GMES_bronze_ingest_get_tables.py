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

# =============================================================================
# Wrappers por tabelas
# ============================================================================= 




##def check_counter_group(counter_name: str, every: int = 4):
##    counter = int(Variable.get(counter_name, default="0"))
##    counter += 1
##
##    if counter >= every:
##        Variable.set(counter_name, "0")
##        print(f"[INFO] Liberando execução do grupo. counter={counter}")
##        return True
##
##    Variable.set(counter_name, str(counter))
##    print(
##        f"[INFO] Pulando grupo. Task agendada para rodar na execução {every}. "
##        f"counter={counter}"
##    )
##    return False
##


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


def copiar_lote_GMES_tb_ContainerHistory() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id="sql_udi_gmes",
        conn_destino_id="pg_dw_apex",
        tabela_origem="BRZGMESApriso2024.dbo.AT_CONTAINER_HISTORY",
        tabela_destino="apex_dw_raw.udi_gmes.at_container_history",

        colunas_busca=[
            "ID","TransactionContext","TransactionCode","HostMovementCode",
            "Container","InContainer","Container2","ContainerClassID","InContainerClassID",
            "SrcProductID","DstProductID","SrcProductNo","DstProductNo",
            "OldQuantity","NewQuantity","QuantityAdjusted","OldQtyAlloc","NewQtyAlloc","QuantityAllocated",
            "UomCode","SrcInventoryStatus","DstInventoryStatus",
            "SrcWarehouseLocationID","DstWarehouseLocationID","SrcWarehouseLocation","DstWarehouseLocation",
            "SrcWarehouse","DstWarehouse","SrcLotNo","DstLotNo","SrcPartnerLotNo","DstPartnerLotNo",
            "SAPBatchManaged","SrcPartnerID","DstPartnerID","SrcGradeID","DstGradeID",
            "OrderNo","OrderLineNo","OrderType","WipOrderNo","WipOrderType","ReferenceID",
            "LastUpdateOn","LastUpdatedBy",
            "CAST(createdOn AT TIME ZONE 'UTC' AT TIME ZONE 'E. South America Standard Time' AS DATETIME) AS CreatedOn",
            "CreatedBy","Active",
            "LastDeleteOn","LastDeletedBy","LastReactivateOn","LastReactivatedBy",
            "ArchiveID","LastArchiveOn","LastArchivedBy","LastRestoreOn","LastRestoredBy","RowVersionStamp",
        ], 

        colunas_insert = [
            "id","transactioncontext","transactioncode","hostmovementcode",
            "container","incontainer","container2","containerclassid","incontainerclassid",
            "srcproductid","dstproductid","srcproductno","dstproductno",
            "oldquantity","newquantity","quantityadjusted","oldqtyalloc","newqtyalloc","quantityallocated",
            "uomcode","srcinventorystatus","dstinventorystatus",
            "srcwarehouselocationid","dstwarehouselocationid","srcwarehouselocation","dstwarehouselocation",
            "srcwarehouse","dstwarehouse","srclotno","dstlotno","srcpartnerlotno","dstpartnerlotno",
            "sapbatchmanaged","srcpartnerid","dstpartnerid","srcgradeid","dstgradeid",
            "orderno","orderlineno","ordertype","wiporderno","wipordertype","referenceid",
            "lastupdateon","lastupdatedby","createdon","createdby","active",
            "lastdeleteon","lastdeletedby","lastreactivateon","lastreactivatedby",
            "archiveid","lastarchiveon","lastarchivedby","lastrestoreon","lastrestoredby","rowversionstamp",
        ],
        joins_list= None,
        chave_id= "id",
        lote= 1000,
        id_inicial= 127836784,
        period_filter_status= "off",
        refresh_workday_status= "off",
        workday_role=None ,
        period_column=None,
        period_name=None ,
        period_value=None ,
    )


def copiar_lote_GMES_tb_Inventory() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id="sql_udi_gmes",
        conn_destino_id="pg_dw_apex",
        tabela_origem="BRZGMESApriso2024.dbo.INVENTORY",
        tabela_destino="apex_dw_raw.udi_gmes.inventory",
        colunas_busca = [
            "ID", "ProductID", "UomCode", "InventoryClassID", "InventoryStatus",
            "WarehouseLocationID", "Facility", "Warehouse", "LotNo", "RotationKey",
            "InventoryTag", "ReservationTag", "GradeID", "QuantityOnHand",
            "QuantityToPick", "QuantityToPut", "QuantityToPickReleased",
            "QuantityToPutReleased", "QuantityAllocated", "QuantityException",
            "MinFifoDate", "MaxFifoDate", "ExpirationDate", "ProductInventoryType",
            "PartnerID", "QuantityInActive", "TextID", "ReferenceID", "LastUpdateOn",
            "LastUpdatedBy", "CreatedOn", "CreatedBy", "Active", "LastDeleteOn",
            "LastDeletedBy", "LastReactivateOn", "LastReactivatedBy", "ArchiveID",
            "LastArchiveOn", "LastArchivedBy", "LastRestoreOn", "LastRestoredBy",
            "RowVersionStamp"
        ],

        colunas_insert = [
            "id", "productid", "uomcode", "inventoryclassid", "inventorystatus",
            "warehouselocationid", "facility", "warehouse", "lotno", "rotationkey",
            "inventorytag", "reservationtag", "gradeid", "quantityonhand",
            "quantitytopick", "quantitytoput", "quantitytopickreleased",
            "quantitytoputreleased", "quantityallocated", "quantityexception",
            "minfifodate", "maxfifodate", "expirationdate", "productinventorytype",
            "partnerid", "quantityinactive", "textid", "referenceid", "lastupdateon",
            "lastupdatedby", "createdon", "createdby", "active", "lastdeleteon",
            "lastdeletedby", "lastreactivateon", "lastreactivatedby", "archiveid",
            "lastarchiveon", "lastarchivedby", "lastrestoreon", "lastrestoredby",
            "rowversionstamp"
        ],
       
        joins_list= None,
        chave_id="id",
        lote= 5000,
        id_inicial= 2860356,
        period_filter_status= "off",
        refresh_workday_status= "off",
        workday_role=None ,
        period_column=None,
        period_name=None ,
        period_value=None ,
    )


def copiar_lote_GMES_tb_Inventory_container() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id="sql_udi_gmes",
        conn_destino_id="pg_dw_apex",
        tabela_origem="BRZGMESApriso2024.dbo.INVENTORY_CONTAINER",
        tabela_destino="apex_dw_raw.udi_gmes.inventory_container",
        colunas_busca = [
            "Container", "InventoryID", "QuantityOnHand", "QuantityToPick", "QuantityToPut",
            "QuantityToPickReleased", "QuantityToPutReleased", "QuantityAllocated", "QuantityException",
            "LastProductID", "LastInventoryStatus", "LastFacility", "LastWarehouse", "LastWarehouseLocation",
            "LastGradeID", "LastPartnerID", "QuantityInActive", "ReferenceID", "LastUpdateOn",
            "LastUpdatedBy", "CreatedOn", "CreatedBy", "Active", "LastDeleteOn", "LastDeletedBy",
            "LastReactivateOn", "LastReactivatedBy", "ArchiveID", "LastArchiveOn", "LastArchivedBy",
            "LastRestoreOn", "LastRestoredBy", "RowVersionStamp",
            "CAST(createdOn AT TIME ZONE 'UTC' AT TIME ZONE 'E. South America Standard Time' AS DATETIME) AS CreatedOn_local"
        ],

        colunas_insert = [
            "container", "inventoryid", "quantityonhand", "quantitytopick", "quantitytoput",
            "quantitytopickreleased", "quantitytoputreleased", "quantityallocated", "quantityexception",
            "lastproductid", "lastinventorystatus", "lastfacility", "lastwarehouse", "lastwarehouselocation",
            "lastgradeid", "lastpartnerid", "quantityinactive", "referenceid", "lastupdateon",
            "lastupdatedby", "createdon", "createdby", "active", "lastdeleteon", "lastdeletedby",
            "lastreactivateon", "lastreactivatedby", "archiveid", "lastarchiveon", "lastarchivedby",
            "lastrestoreon", "lastrestoredby", "rowversionstamp", "createdon_local"
        ],
       
        joins_list= None,
        chave_id="inventoryid",
        lote= 5000,
        id_inicial=2839154,
        period_filter_status= "off",
        refresh_workday_status= "off",
        workday_role=None ,
        period_column=None,
        period_name=None ,
        period_value=None ,
    )


def copiar_lote_GMES_tb_warehouse_location() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id="sql_udi_gmes",
        conn_destino_id="pg_dw_apex",
        tabela_origem="BRZGMESApriso2024.dbo.WAREHOUSE_LOCATION",
        tabela_destino="apex_dw_raw.udi_gmes.warehouse_location",
        colunas_busca = [
            "ID", "Location", "Facility", "Warehouse", "WarehouseLocationType", "Zone", "StorageBin",
            "WalkPath", "AllocationPath", "FillPath", "XCoordinate1", "YCoordinate1", "ZCoordinate1",
            "UomCodeXCoordinate1", "UomCodeYCoordinate1", "UomCodeZCoordinate1", "MapID1",
            "XCoordinate2", "YCoordinate2", "ZCoordinate2", "UomCodeXCoordinate2", "UomCodeYCoordinate2",
            "UomCodeZCoordinate2", "MapID2", "OALocatorID", "TextID", "Department", "CalendarID",
            "MobilityID", "OwnershipID", "UnitID", "SpecificationID", "Aisle", "Row_", "Tier",
            "WorkCenter", "WarehouseLocationClassID", "CurrentWeightUtilization", "CurrentSpaceUtilization",
            "ReferenceID", "LastUpdateOn", "LastUpdatedBy", "CreatedOn", "CreatedBy", "Active",
            "LastDeleteOn", "LastDeletedBy", "LastReactivateOn", "LastReactivatedBy", "ArchiveID",
            "LastArchiveOn", "LastArchivedBy", "LastRestoreOn", "LastRestoredBy", "RowVersionStamp",
            "ControlNo", "DSID", "DSName",
            "CAST(createdOn AT TIME ZONE 'UTC' AT TIME ZONE 'E. South America Standard Time' AS DATETIME) AS CreatedOn_local"
        ],

        colunas_insert = [
            "id", "location", "facility", "warehouse", "warehouselocationtype", "zone", "storagebin",
            "walkpath", "allocationpath", "fillpath", "xcoordinate1", "ycoordinate1", "zcoordinate1",
            "uomcodexcoordinate1", "uomcodeycoordinate1", "uomcodezcoordinate1", "mapid1",
            "xcoordinate2", "ycoordinate2", "zcoordinate2", "uomcodexcoordinate2", "uomcodeycoordinate2",
            "uomcodezcoordinate2", "mapid2", "oalocatorid", "textid", "department", "calendarid",
            "mobilityid", "ownershipid", "unitid", "specificationid", "aisle", "row_", "tier",
            "workcenter", "warehouselocationclassid", "currentweightutilization", "currentspaceutilization",
            "referenceid", "lastupdateon", "lastupdatedby", "createdon", "createdby", "active",
            "lastdeleteon", "lastdeletedby", "lastreactivateon", "lastreactivatedby", "archiveid",
            "lastarchiveon", "lastarchivedby", "lastrestoreon", "lastrestoredby", "rowversionstamp",
            "controlno", "dsid", "dsname",
            "createdon_local",
        ],
       
        joins_list= None,
        chave_id="id",
        lote= 5000,
        id_inicial=10297,
        period_filter_status= "off",
        refresh_workday_status= "off",
        workday_role=None ,
        period_column=None,
        period_name=None ,
        period_value=None ,
    )


def copiar_lote_GMES_tb_product() -> None:
    #leia acima pra saber oque é cada uma das funções
    copiar_lote_incremental_refresh_filter(
        conn_origem_id="sql_udi_gmes",
        conn_destino_id="pg_dw_apex",
        tabela_origem="BRZGMESApriso2024.dbo.PRODUCT",
        tabela_destino="apex_dw_raw.udi_gmes.product",
        colunas_busca = [
            "ID", "ProductNo", "FUID", "Facility", "TextID", "UnitID", "RotationType", "RotationDays",
            "LotTrackingCode", "SerialTrackingCode", "FractionAllowed", "RevisionControlFlag", "ProductRevision",
            "ProductRevisionCreateDate", "ProductRevisionEffectiveDate", "ProductRevisionDiscontinueDate",
            "ProductRevisionECOID", "DefaultProductRevision", "DefaultUomCode", "DefaultWarehouse",
            "DefaultWarehouseLocationID", "AlternateUomCode", "KanbanFlag", "SKUCode", "CommodityCode",
            "UNNACode", "HarmonizedTariffCode", "ShelfLifeDays", "InspectionRequired",
            "DefaultOverReceivePercentage", "CycleCountFlag", "ABCFlag", "NextCycleCountDate",
            "LastCycleCountDate", "CrossFacilityProdStatus", "CrossFacilityProdStatusValid",
            "CrossDistribProdStatus", "CrossDistribProdStatusValid", "OAProductID", "OwnerFacility",
            "SpecificationID", "CodeSystemType", "FormatType", "DomainManagerID", "ObjectClass",
            "ProductInventoryType", "ProductABCClassID", "ReferenceID", "LastUpdateOn", "LastUpdatedBy",
            "CreatedOn", "CreatedBy", "Active", "LastDeleteOn", "LastDeletedBy", "LastReactivateOn",
            "LastReactivatedBy", "ArchiveID", "LastArchiveOn", "LastArchivedBy", "LastRestoreOn",
            "LastRestoredBy", "RowVersionStamp", "PLMEntityID", "DSID", "DSName", "ClassificationID",
            "CAST(createdOn AT TIME ZONE 'UTC' AT TIME ZONE 'E. South America Standard Time' AS DATETIME) AS CreatedOn_local"
        ],

        colunas_insert = [
            "id", "productno", "fuid", "facility", "textid", "unitid", "rotationtype", "rotationdays",
            "lottrackingcode", "serialtrackingcode", "fractionallowed", "revisioncontrolflag", "productrevision",
            "productrevisioncreatedate", "productrevisioneffectivedate", "productrevisiondiscontinuedate",
            "productrevisionecoid", "defaultproductrevision", "defaultuomcode", "defaultwarehouse",
            "defaultwarehouselocationid", "alternateuomcode", "kanbanflag", "skucode", "commoditycode",
            "unnacode", "harmonizedtariffcode", "shelflifedays", "inspectionrequired",
            "defaultoverreceivepercentage", "cyclecountflag", "abcflag", "nextcyclecountdate",
            "lastcyclecountdate", "crossfacilityprodstatus", "crossfacilityprodstatusvalid",
            "crossdistribprodstatus", "crossdistribprodstatusvalid", "oaproductid", "ownerfacility",
            "specificationid", "codesystemtype", "formattype", "domainmanagerid", "objectclass",
            "productinventorytype", "productabcclassid", "referenceid", "lastupdateon", "lastupdatedby",
            "createdon", "createdby", "active", "lastdeleteon", "lastdeletedby", "lastreactivateon",
            "lastreactivatedby", "archiveid", "lastarchiveon", "lastarchivedby", "lastrestoreon",
            "lastrestoredby", "rowversionstamp", "plmentityid", "dsid", "dsname", "classificationid",
            "createdon_local"
        ],
       
        joins_list= None,
        chave_id="id",
        lote= 5000,
        id_inicial=10000,
        period_filter_status= "off",
        refresh_workday_status= "off",
        workday_role=None ,
        period_column=None,
        period_name=None ,
        period_value=None ,
    )


def copiar_lote_GMES_tb_resources() -> None:
   #leia acima pra saber oque é cada uma das funções
   copiar_lote_incremental_refresh_filter(
       conn_origem_id="sql_udi_gmes",
       conn_destino_id="pg_dw_apex",
       tabela_origem="BRZGMESApriso2024.dbo.RESOURCE_",
       tabela_destino="apex_dw_raw.udi_gmes.resource_",
       colunas_busca = ["ID", "FUID", "Name", "ResourceName", "ResourceType", "ResourceClassID", "TextID",
            "TrackingUomCode", "MobilityFlag", "Department", "Facility", "WorkCenter",
            "AutoCharge", "StdRate", "ExcludeFromPlanning", "LastMaintenanceDate",
            "NextMaintenanceDate", "ProductionLine", "TrackAttendanceFlag", "TrackLaborFlag",
            "TrackResourceLaborFlag", "CapacityID", "WorkLoadID", "CostID", "ProcurementID",
            "MobilityID", "AvailabilityID", "OwnershipID", "UnitID", "SpecificationID",
            "LifeCycle", "AutoAdjustState", "CoupleState", "CalendarID", "PayRule",
            "WarehouseLocationID", "WorkCenterPayPoint", "DepartmentPayPoint", "FacilityPayPoint",
            "WorkCenterConstraint", "DepartmentConstraint", "ProductionLinePayPoint",
            "ProductionLineConstraint", "ProductBOMFacility", "ProductBOMNumber", "MasterResource",
            "ProductID", "ReferenceID", "LastUpdateOn", "LastUpdatedBy",
            "CreatedOn",
            "CreatedBy", "Active", "LastDeleteOn", "LastDeletedBy", "LastReactivateOn",
            "LastReactivatedBy", "ArchiveID", "LastArchiveOn", "LastArchivedBy",
            "LastRestoreOn", "LastRestoredBy", "RowVersionStamp", "ReceiptLocationID",
            "PutAwayLocationID", "MaintenanceCalendarID", "OperatingRange", "DSID", "DSName",
            "ClassificationID", "SlowSpeedPercentageThreshold", "FastSpeedPercentageThreshold",
            "CAST(createdOn AT TIME ZONE 'UTC' AT TIME ZONE 'E. South America Standard Time' AS DATETIME) AS CreatedOn_local"
         ],      

       colunas_insert = ["id", "fuid", "name", "resourcename", "resourcetype", "resourceclassid", "textid",
           "trackinguomcode", "mobilityflag", "department", "facility", "workcenter",
           "autocharge", "stdrate", "excludefromplanning", "lastmaintenancedate",
           "nextmaintenancedate", "productionline", "trackattendanceflag", "tracklaborflag",
           "trackresourcelaborflag", "capacityid", "workloadid", "costid", "procurementid",
           "mobilityid", "availabilityid", "ownershipid", "unitid", "specificationid",
           "lifecycle", "autoadjuststate", "couplestate", "calendarid", "payrule",
           "warehouselocationid", "workcenterpaypoint", "departmentpaypoint", "facilitypaypoint",
           "workcenterconstraint", "departmentconstraint", "productionlinepaypoint",
           "productionlineconstraint", "productbomfacility", "productbomnumber", "masterresource",
           "productid", "referenceid", "lastupdateon", "lastupdatedby", "createdon",
           "createdby", "active", "lastdeleteon", "lastdeletedby", "lastreactivateon",
           "lastreactivatedby", "archiveid", "lastarchiveon", "lastarchivedby",
           "lastrestoreon", "lastrestoredby", "rowversionstamp", "receiptlocationid",
           "putawaylocationid", "maintenancecalendarid", "operatingrange", "dsid", "dsname",
           "classificationid", "slowspeedpercentagethreshold", "fastspeedpercentagethreshold",
           "createdon_local"
         ],

       joins_list= None,
       chave_id="id",
       lote= 1000,
       id_inicial=10000,
       period_filter_status= "off",
       refresh_workday_status= "off",
       workday_role=None ,
       period_column=None,
       period_name=None ,
       period_value=None ,
 )


def copiar_lote_GMES_tb_resource_class() -> None:
 #leia acima pra saber oque é cada uma das funções
   copiar_lote_incremental_refresh_filter(
       conn_origem_id="sql_udi_gmes",
       conn_destino_id="pg_dw_apex",
       tabela_origem="BRZGMESApriso2024.dbo.RESOURCE_CLASS",
       tabela_destino="apex_dw_raw.udi_gmes.resource_class",
       colunas_busca = ["ID", "Name", "ResourceType", "TextID", "UnitID", "SpecificationID",
           "PlanningUomCode", "TrackingUomCode", "CostingUomCode", "CostAccrualMethod",
           "MobilityFlag", "TrackAttendanceFlag", "TrackLaborFlag", "TrackResourceLaborFlag",
           "LocationHistoryFrequency", "DomainManagerID", "ReferenceID", "LastUpdateOn",
           "LastUpdatedBy", "CreatedOn",
           "CreatedBy", "Active", "LastDeleteOn", "LastDeletedBy", "LastReactivateOn",
           "LastReactivatedBy", "ArchiveID", "LastArchiveOn", "LastArchivedBy",
           "LastRestoreOn", "LastRestoredBy", "RowVersionStamp", "DSID", "DSName",
           "ClassificationID",
           "CAST(createdOn AT TIME ZONE 'UTC' AT TIME ZONE 'E. South America Standard Time' AS DATETIME) AS CreatedOn_local"
           ],      
     
       colunas_insert = ["id", "name", "resourcetype", "textid", "unitid", "specificationid",
            "planninguomcode", "trackinguomcode", "costinguomcode", "costaccrualmethod",
            "mobilityflag", "trackattendanceflag", "tracklaborflag", "trackresourcelaborflag",
            "locationhistoryfrequency", "domainmanagerid", "referenceid", "lastupdateon",
            "lastupdatedby", "createdon", "createdby", "active", "lastdeleteon",
            "lastdeletedby", "lastreactivateon", "lastreactivatedby", "archiveid",
            "lastarchiveon", "lastarchivedby", "lastrestoreon", "lastrestoredby",
            "rowversionstamp", "dsid", "dsname", "classificationid", "createdon_local"
           ],
    
       joins_list= None,
       chave_id="id",
       lote= 5000,
       id_inicial=10000,
       period_filter_status= "off",
       refresh_workday_status= "off",
       workday_role=None ,
       period_column=None,
       period_name=None ,
       period_value=None ,
 )      


def copiar_lote_GMES_tb_wip_req_resource() -> None:
 #leia acima pra saber oque é cada uma das funções
 copiar_lote_incremental_refresh_filter(
       conn_origem_id="sql_udi_gmes",
       conn_destino_id="pg_dw_apex",
       tabela_origem="BRZGMESApriso2024.dbo.WIP_REQ_RESOURCE",
       tabela_destino="apex_dw_raw.udi_gmes.wip_req_resource",
       colunas_busca = ["ID", "WipOrderNo", "WipOrderType", "OprSequenceNo", "StepSequenceNo",
           "ResourceClassID", "ResourceType", "ResourceName", "EffectiveDate",
           "DiscontinueDate", "RoleID", "SkillID", "SkillExperienceLevel",
           "StdTotalRunEffort", "DurationRemainingSeconds", "StdEffortEarned",
           "WorkLoadID", "ExternalSource", "UnitID", "ResourceID",
           "ReferenceID", "LastUpdateOn", "LastUpdatedBy",
           "CreatedOn",
           "CreatedBy", "Active", "LastDeleteOn", "LastDeletedBy",
           "LastReactivateOn", "LastReactivatedBy", "ArchiveID",
           "LastArchiveOn", "LastArchivedBy", "LastRestoreOn",
           "LastRestoredBy", "RowVersionStamp", "UsageClassID",
           "ParentWipReqResourceID", "NoteID", "ResourceQuantity",
           "DSInstanceID", "DSInstanceName", "ResourceSet", "Priority",
           "PrimaryWipReqResourceID", "ResourceAllocationType",
           "CertificationID",
           "CAST(createdOn AT TIME ZONE 'UTC' AT TIME ZONE 'E. South America Standard Time' AS DATETIME) AS CreatedOn_local",
            ],      
     
       colunas_insert = ["id", "wiporderno", "wipordertype", "oprsequenceno", "stepsequenceno",
           "resourceclassid", "resourcetype", "resourcename", "effectivedate",
           "discontinuedate", "roleid", "skillid", "skillexperiencelevel",
           "stdtotalruneffort", "durationremainingseconds", "stdeffortearned",
           "workloadid", "externalsource", "unitid", "resourceid",
           "referenceid", "lastupdateon", "lastupdatedby", "createdon",
           "createdby", "active", "lastdeleteon", "lastdeletedby",
           "lastreactivateon", "lastreactivatedby", "archiveid",
           "lastarchiveon", "lastarchivedby", "lastrestoreon",
           "lastrestoredby", "rowversionstamp", "usageclassid",
           "parentwipreqresourceid", "noteid", "resourcequantity",
           "dsinstanceid", "dsinstancename", "resourceset", "priority",
           "primarywipreqresourceid", "resourceallocationtype",
           "certificationid",
           "createdon_local"
         ],
    
       joins_list= None,
       chave_id="id",
       lote= 5000,
       id_inicial= 100211529,
       period_filter_status= "on",
       refresh_workday_status= "on",
       workday_role= "getdate",
       period_column= "createdon",
       period_name= "DAY",
       period_value= -4,
 )


def copiar_lote_GMES_tb_resource_collection() -> None:
 #leia acima pra saber oque é cada uma das funções
   copiar_lote_incremental_refresh_filter(
       conn_origem_id="sql_udi_gmes",
       conn_destino_id="pg_dw_apex",
       tabela_origem="BRZGMESApriso2024.dbo.RESOURCE_COLLECTION",
       tabela_destino="apex_dw_raw.udi_gmes.resource_collection",
       colunas_busca = ["ID", "ParentResourceName", "ParentResourceType", "ResourceName",
           "ResourceType", "EffectiveDate", "DiscontinueDate", "TextID",
           "ReferenceID", "LastUpdateOn", "LastUpdatedBy",
           "CreatedOn",
           "CreatedBy", "Active", "LastDeleteOn", "LastDeletedBy",
           "LastReactivateOn", "LastReactivatedBy", "ArchiveID",
           "LastArchiveOn", "LastArchivedBy", "LastRestoreOn",
           "LastRestoredBy", "RowVersionStamp",
           "CAST(createdOn AT TIME ZONE 'UTC' AT TIME ZONE 'E. South America Standard Time' AS DATETIME) AS CreatedOn_local",
           ],      
     
       colunas_insert = ["id", "parentresourcename", "parentresourcetype", "resourcename",
           "resourcetype", "effectivedate", "discontinuedate", "textid",
           "referenceid", "lastupdateon", "lastupdatedby", "createdon",
           "createdby", "active", "lastdeleteon", "lastdeletedby",
           "lastreactivateon", "lastreactivatedby", "archiveid",
           "lastarchiveon", "lastarchivedby", "lastrestoreon",
           "lastrestoredby", "rowversionstamp", "createdon_local"
            ],
    
       joins_list= None,
       chave_id="id",
       lote= 5000,
       id_inicial=0,
       period_filter_status= "on",
       refresh_workday_status= "on",
       workday_role= "getdate",
       period_column= "createdon",
       period_name= "DAY",
       period_value= -60,
 )      
 

def copiar_lote_GMES_tb_resource_link() -> None:
 #leia acima pra saber oque é cada uma das funções
   copiar_lote_incremental_refresh_filter(
       conn_origem_id="sql_udi_gmes",
       conn_destino_id="pg_dw_apex",
       tabela_origem="BRZGMESApriso2024.dbo.RESOURCE_RESOURCE_LINK",
       tabela_destino="apex_dw_raw.udi_gmes.resource_resource_link",
       colunas_busca = ["ID", "ParentResourceName", "ParentResourceType", "ResourceName",
           "ResourceType", "AssignedOn", "DurationSeconds", "ResourceLinkClassID",
           "ReleasedOn", "ReferenceID", "LastUpdateOn", "LastUpdatedBy",
           "CreatedOn",
           "CreatedBy", "Active", "LastDeleteOn", "LastDeletedBy",
           "LastReactivateOn", "LastReactivatedBy", "ArchiveID",
           "LastArchiveOn", "LastArchivedBy", "LastRestoreOn",
           "LastRestoredBy", "RowVersionStamp",
           "CAST(createdOn AT TIME ZONE 'UTC' AT TIME ZONE 'E. South America Standard Time' AS DATETIME) AS CreatedOn"
           ],      
     
       colunas_insert = ["id", "parentresourcename", "parentresourcetype", "resourcename",
           "resourcetype", "assignedon", "durationseconds", "resourcelinkclassid",
           "releasedon", "referenceid", "lastupdateon", "lastupdatedby",
           "createdon", "createdby", "active", "lastdeleteon",
           "lastdeletedby", "lastreactivateon", "lastreactivatedby",
           "archiveid", "lastarchiveon", "lastarchivedby",
           "lastrestoreon", "lastrestoredby", "rowversionstamp", "createdon_local"
           ],
    
       joins_list= None,
       chave_id="id",
       lote= 5000,
       id_inicial=10000,
       period_filter_status= "on",
       refresh_workday_status= "on",
       workday_role= "getdate",
       period_column= "createdon",
       period_name= "DAY",
       period_value= -180,
 )      
 

def copiar_lote_GMES_tb_equipment() -> None:
 #leia acima pra saber oque é cada uma das funções
   copiar_lote_incremental_refresh_filter(
        conn_origem_id="sql_udi_gmes",
        conn_destino_id="pg_dw_apex",
        tabela_origem="BRZGMESApriso2024.dbo.EQUIPMENT",
        tabela_destino="apex_dw_raw.udi_gmes.equipment",
        colunas_busca = ["ID","Equipment","Facility","EquipmentType","TextID","CurrentEmployeeID","LastMaintenanceDate","NextMaintenanceDate","TransportEquipment","LogisticsID",
                "MobilityID","OwnershipID","MaximumLogins","ResourceID","FUID","ReferenceID","LastUpdateOn","LastUpdatedBy","CreatedOn","CreatedBy","Active","LastDeleteOn","LastDeletedBy",
                "LastReactivateOn","LastReactivatedBy","ArchiveID","LastArchiveOn","LastArchivedBy","LastRestoreOn","LastRestoredBy","RowVersionStamp","EquipmentClassID"
            ],      
            
        colunas_insert = ["id","equipment","facility","equipmenttype","textid","currentemployeeid","lastmaintenancedate","nextmaintenancedate","transportequipment","logisticsid",
                "mobilityid","ownershipid","maximumlogins","resourceid","fuid","referenceid","lastupdateon","lastupdatedby","createdon","createdby","active","lastdeleteon","lastdeletedby",
                "lastreactivateon","lastreactivatedby","archiveid","lastarchiveon","lastarchivedby","lastrestoreon","lastrestoredby","rowversionstamp","equipmentclassid"
            ],

        joins_list= None,
        chave_id="id",
        lote= 5000,
        id_inicial=0,
        period_filter_status= "off",
        refresh_workday_status= "off",
        workday_role=None ,
        period_column=None,
        period_name=None ,
        period_value=None ,
 )

    
def copiar_lote_GMES_tb_wip_operation() -> None:
    #leia acima pra saber oque é cada uma das funções
   copiar_lote_incremental_refresh_filter(
       conn_origem_id="sql_udi_gmes",
       conn_destino_id="pg_dw_apex",
       tabela_origem="BRZGMESApriso2024.dbo.WIP_OPERATION",
       tabela_destino="apex_dw_raw.udi_gmes.wip_operation",
       colunas_busca = ["WipOrderNo", "WipOrderType", "OprSequenceNo", "PreviousSequenceNo",
           "NextSequenceNo", "OperationID", "OperationType", "WorkCenter",
           "OperationStatus", "OperationCode", "InsideProcessFlag", "ExpectedStartDate",
           "ScheduledDurationSeconds", "ScheduledStartDate", "ActualStartDate",
           "ActualCompletionDate", "WBSCode", "KanbanCards", "KanbanLotSizes",
           "MinKanbanCards", "ValueAdd", "DeterminationStrategyUsed", "WorkflowStatusID",
           "SamplePlanID", "SpecificationID", "LinkToProcessID", "ProcessID",
           "Process", "ProductRoutingID", "ResourceRoutingID", "EffectiveDate",
           "DiscontinueDate", "EcoID", "StdRate", "StdTotalRunEffort",
           "DurationRemainingSeconds", "StdEffortEarned", "ScheduledCompletionDate",
           "SAPControlKey", "TextID", "WorkLoadID", "ProgressStatus", "WorkingTime",
           "UnitID", "OccupationFacility", "Occupation", "LaborProrateType",
           "TaskStrategyType", "Milestone", "ExpectedCompletionDate",
           "ActualDurationSeconds", "ExpectedDurationSeconds", "ProcessOperationID",
           "PercentageCompleted", "Parameters", "OverrideWI", "ExecutionTemplate",
           "ReferenceID", "LastUpdateOn", "LastUpdatedBy",
           "CreatedOn",
           "CreatedBy", "Active", "LastDeleteOn", "LastDeletedBy", "LastReactivateOn",
           "LastReactivatedBy", "ArchiveID", "LastArchiveOn", "LastArchivedBy",
           "LastRestoreOn", "LastRestoredBy", "RowVersionStamp", "DSID", "DSName",
           "DSInstanceID", "DSInstanceName", "OperationHeaderID",
           "OperationActivityType", "ClassificationID",
           "CAST(createdOn AT TIME ZONE 'UTC' AT TIME ZONE 'E. South America Standard Time' AS DATETIME) AS CreatedOn_local"
        ],      
     
       colunas_insert = ["wiporderno", "wipordertype", "oprsequenceno", "previoussequenceno",
           "nextsequenceno", "operationid", "operationtype", "workcenter",
           "operationstatus", "operationcode", "insideprocessflag", "expectedstartdate",
           "scheduleddurationseconds", "scheduledstartdate", "actualstartdate",
           "actualcompletiondate", "wbscode", "kanbancards", "kanbanlotsizes",
           "minkanbancards", "valueadd", "determinationstrategyused", "workflowstatusid",
           "sampleplanid", "specificationid", "linktoprocessid", "processid",
           "process", "productroutingid", "resourceroutingid", "effectivedate",
           "discontinuedate", "ecoid", "stdrate", "stdtotalruneffort",
           "durationremainingseconds", "stdeffortearned", "scheduledcompletiondate",
           "sapcontrolkey", "textid", "workloadid", "progressstatus", "workingtime",
           "unitid", "occupationfacility", "occupation", "laborproratetype",
           "taskstrategytype", "milestone", "expectedcompletiondate",
           "actualdurationseconds", "expecteddurationseconds", "processoperationid",
           "percentagecompleted", "parameters", "overridewi", "executiontemplate",
           "referenceid", "lastupdateon", "lastupdatedby", "createdon",
           "createdby", "active", "lastdeleteon", "lastdeletedby", "lastreactivateon",
           "lastreactivatedby", "archiveid", "lastarchiveon", "lastarchivedby",
           "lastrestoreon", "lastrestoredby", "rowversionstamp", "dsid", "dsname",
           "dsinstanceid", "dsinstancename", "operationheaderid",
           "operationactivitytype", "classificationid","createdon_local"
        ],
    
       joins_list= None,
       chave_id="workloadid",
       lote= 5000,
       id_inicial=100422409,
       period_filter_status= "on",
       refresh_workday_status= "on",
       workday_role= "getdate",
       period_column= "createdon",
       period_name= "DAY",
       period_value= -4,
 )    


def copiar_lote_GMES_tb_wip_order() -> None:
 #leia acima pra saber oque é cada uma das funções
   copiar_lote_incremental_refresh_filter_tabelaSemID(
       conn_origem_id="sql_udi_gmes",
       conn_destino_id="pg_dw_apex",
       tabela_origem="BRZGMESApriso2024.dbo.WIP_ORDER",
       tabela_destino="apex_dw_raw.udi_gmes.wip_order",
       colunas_busca = ["WipOrderNo","WipOrderType","PutAwayLocationID","ProductID","OrderQuantity","CompletedQuantity","Priority",
            "ExpectedStartDate","ReleaseDate","ScheduledStartDate","DueDate","ActualStartDate","ActualCompletionDate",
            "ScheduledDurationSeconds","UomCode","WorkOrderStatus","ParentWIPOrderNo","ParentWIPOrderType","ParentOprSequenceNo","ProjectCode",
            "TextID","ScheduleName","SchedulePayday","ProductionLineNo","ResourceName","ResourceType","EcoID","OAWipEntityID","SamplePlanID",
            "SpecificationID","ReleasedFacility","ProcurementID","OriginalProcessID","WorkflowStatusID","ProcessID","DeterminationStrategyUsed",
            "UnderTollerancePercent","OverTollerancePercent","UnitID","OrderNo","OrderType","OrderLineNo","DaysEarlyAllowed","DaysLateAllowed",
            "DaysException","ProgressStatus","ExternalSequenceID","ProjectID","TargetQuantity","TargetOrderQuantity","WorkingTime","WipOperationFlowLayout",
            "LaborProrateType","InternalState","RecipeID","BomNumber","OrderCategory","ScheduledCompletionDate","ExpectedCompletionDate","ActualDurationSeconds",
            "ExpectedDurationSeconds","PercentageCompleted","ReceiptLocationID","ReferenceID","LastUpdateOn","LastUpdatedBy",
            "CreatedOn",
            "CreatedBy","Active","LastDeleteOn","LastDeletedBy","LastReactivateOn","LastReactivatedBy","ArchiveID","LastArchiveOn","LastArchivedBy",
            "LastRestoreOn","LastRestoredBy","RowVersionStamp","DSID","DSName","DSInstanceID","DSInstanceName","DSPlatformAlias","DSProcessID",
            "DSProductionPlanID","DSFilter","ClassificationID",
            "CAST(OrderNo AS INT) AS OrderNoInt",
            "CAST(createdOn AT TIME ZONE 'UTC' AT TIME ZONE 'E. South America Standard Time' AS DATETIME) AS CreatedOn_local"
        ],      
    
       colunas_insert = ["wiporderno","wipordertype","putawaylocationid","productid","orderquantity","completedquantity","priority",
            "expectedstartdate","releasedate","scheduledstartdate","duedate","actualstartdate","actualcompletiondate",
            "scheduleddurationseconds","uomcode","workorderstatus","parentwiporderno","parentwipordertype","parentoprsequenceno","projectcode",
            "textid","schedulename","schedulepayday","productionlineno","resourcename","resourcetype","ecoid","oawipentityid","sampleplanid",
            "specificationid","releasedfacility","procurementid","originalprocessid","workflowstatusid","processid","determinationstrategyused",
            "undertollerancepercent","overtollerancepercent","unitid","orderno","ordertype","orderlineno","daysearlyallowed","dayslateallowed",
            "daysexception","progressstatus","externalsequenceid","projectid","targetquantity","targetorderquantity","workingtime","wipoperationflowlayout",
            "laborproratetype","internalstate","recipeid","bomnumber","ordercategory","scheduledcompletiondate","expectedcompletiondate","actualdurationseconds",
            "expecteddurationseconds","percentagecompleted","receiptlocationid","referenceid","lastupdateon","lastupdatedby",
            "createdon","createdby","active","lastdeleteon","lastdeletedby","lastreactivateon","lastreactivatedby","archiveid","lastarchiveon","lastarchivedby",
            "lastrestoreon","lastrestoredby","rowversionstamp","dsid","dsname","dsinstanceid","dsinstancename","dsplatformalias","dsprocessid",
            "dsproductionplanid","dsfilter","classificationid","ordernoint","createdon_local"
        ],
   
       joins_list= None,
       chave_id="wipordertype",
       chave_id_2="wiporderno",
       lote= 5000,
       id_inicial= '23',
       id_inicial_2 = '5851495',
       period_filter_status= "on",
       refresh_workday_status= "on",
       workday_role= "getdate",
       period_column= "createdon",
       period_name= "DAY",
       period_value= -4,
    )     


# =============================================================================
# DAG
# =============================================================================
with DAG(
    dag_id="UDI_GMES_ingest_bronze_incremental_tables",
    start_date=datetime(2026, 1, 1),
    schedule="*/2 * * * *",
    max_active_runs=1,
    catchup=False,
    default_args={"retries": 0},
    tags=["Bronze Ingest", "sqlserver", "owner_FelipeEidt", "UDI", "GMES", "owner_Janatan"],
) as dag_get_tables_gmes:

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

    start = EmptyOperator(task_id="start")

    end = EmptyOperator(task_id="end")


 #  check_counter = ShortCircuitOperator(
 #      task_id="check_counter_selected_group",
 #      python_callable=check_counter_group,
 #      op_kwargs={
 #          "counter_name": "UDI_GMES_selected_group_counter",
 #          "every": 4,
 #      },
 #  )

    get_tb_ContainerHistory = PythonOperator(
        task_id="copiar_lote_GMES_tb_ContainerHistory",
        python_callable=copiar_lote_GMES_tb_ContainerHistory,
    )

    get_tb_inventory = PythonOperator(
       task_id="copiar_lote_GMES_tb_Inventory",
       python_callable=copiar_lote_GMES_tb_Inventory,
    )
    get_tb_inventory_container = PythonOperator(
        task_id="copiar_lote_GMES_tb_Inventory_container",
        python_callable=copiar_lote_GMES_tb_Inventory_container,
    )

    get_tb_warehouse_location = PythonOperator(
        task_id="copiar_lote_GMES_tb_warehouse_location",
        python_callable=copiar_lote_GMES_tb_warehouse_location,
    )

    get_tb_product = PythonOperator(
        task_id="copiar_lote_GMES_tb_product",
        python_callable=copiar_lote_GMES_tb_product,
    )

    get_tb_resources = PythonOperator(
        task_id="copiar_lote_GMES_tb_resources",
        python_callable=copiar_lote_GMES_tb_resources,
    )

    get_tb_resource_class= PythonOperator(
        task_id="copiar_lote_GMES_tb_resource_class",
        python_callable=copiar_lote_GMES_tb_resource_class,
    )

    get_tb_wip_req_resource= PythonOperator(
        task_id="copiar_lote_GMES_tb_wip_req_resource",
        python_callable=copiar_lote_GMES_tb_wip_req_resource,
    )   

    get_tb_resource_collection= PythonOperator(
        task_id="copiar_lote_GMES_tb_resource_collection",
        python_callable=run_status(
            copiar_lote_GMES_tb_resource_collection,
            counter_name="copiar_lote_GMES_tb_resource_collection",
            every=4,
        ),        
    )     

    get_tb_resource_link= PythonOperator(
        task_id="copiar_lote_GMES_tb_resource_link",
        python_callable=run_status(
            copiar_lote_GMES_tb_resource_link,
            counter_name="copiar_lote_GMES_tb_resource_link",
            every=4,
        ),        
    )     

    get_tb_equipment = PythonOperator(
        task_id="copiar_lote_GMES_tb_equipment",
        python_callable=run_status(
            copiar_lote_GMES_tb_equipment,
            counter_name="copiar_lote_GMES_tb_equipment_counter",
            every=4,
        ),
    )

    get_tb_wip_operation= PythonOperator(
        task_id="copiar_lote_GMES_tb_wip_operation",
        python_callable=copiar_lote_GMES_tb_wip_operation,
    )   

    get_tb_wip_order= PythonOperator(
        task_id="copiar_lote_GMES_tb_wip_order",
        python_callable=copiar_lote_GMES_tb_wip_order,
    )   

    

    start \
    >> get_tb_ContainerHistory >> wait_01 \
    >> get_tb_inventory >> wait_02 \
    >> get_tb_equipment >> wait_03 \
    >> get_tb_warehouse_location >> wait_04 \
    >> get_tb_product >> wait_05 \
    >> get_tb_resources >> wait_06 \
    >> get_tb_resource_class >> wait_07 \
    >> get_tb_resource_collection >> wait_08 \
    >> get_tb_resource_link >> wait_09 \
    >> get_tb_inventory_container >> wait_10 \
    >> get_tb_wip_order >> wait_11 \
    >> get_tb_wip_req_resource >> wait_12 \
    >> get_tb_wip_operation \
    >> end