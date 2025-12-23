from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


## ================================== ##
## ===      GET DADOS USINA       === ##
## ================================== ##


# LISTA DE TABELAS COPIADAS
#   tb_ProductionOrderLog               OK
#   tb_ProductionOrder                  OK
#   tb_Container                        OK
#   tb_QualitySample                    OK
#   tb_QualitySampleResults             OK
#   tb_Experiment                       OK
#   tb_Characteristic                   OK
#   tb_ContainerQualityStatusHistory    OK




##/////////////////////////////////##
##      tb_ProductionOrderLog      ##
##/////////////////////////////////##

def copiar_lote_leaf_productionorderlog():

    TABELA_ORIGEM = "db_mcExperienceSuite.mesAdapter.tb_ProductionOrderLog"
    TABELA_DESTINO = "leaf_db_mcExperienceSuite.mesAdapter.tb_ProductionOrderLog"
    CHAVE_ID = "Cod_Record"
    LOTE = 100
    ID_INICIAL = 1308146

    # Lista de colunas na ORDEM EXATA da tabela
    COLUNAS_tb_ProductionOrderLog = [
        "Cod_Record",
        "Cod_ProductionOrder",
        "Cod_Shift",
        "Val_Facility",
        "Val_Department",
        "Val_WorkCenter",
        "Val_Equipment",
        "Val_EquipmentType",
        "Val_CompleteQuantity",
        "Val_WorkDay",
        "Dtt_Collect",
        "Des_Comment",
        "Dtt_Record",
        "Ind_Manual",
        "Usr_Record",
        "Val_UOM",
        "Val_UOMConverted",
        "Val_CompleteQuantityConverted",
        "Ind_Type",
        "Cod_Container",
        "Val_Material",
        "Des_Reason",
        "Val_QualityStatus",
        "Val_Experiment",
        "Val_QualitySample",
        "Dtt_StatusUpdate",
        "Val_TypeStatus",
        "Usr_RejectionStatus",
        "Val_StatusChangeReason",
        "Val_RejectionComment",
        "Ind_SapNotification",
        "Dtt_Update",
        "Dtt_Disable",
        "Stt_Record",
        "Val_ErpReferenceCode",
        "Des_ErpMessage",
        "Ind_ErpResult",
        "Dtt_ErpExecution",
        "Stt_ErpReversal",
        "Ind_QualityStatusERP",
    ]

    # 1) Conexões com SQL Server (Hooks)
    origem = MsSqlHook(mssql_conn_id="sql_leaf_chronos")
    destino = MsSqlHook(mssql_conn_id="sql_udi_apex_azure")

    # 2) Pegar o último ID já copiado no destino (MAX(ID))
    sql_ultimo_id = f"""
        SELECT ISNULL(MAX({CHAVE_ID}), {ID_INICIAL})
        FROM {TABELA_DESTINO}
    """
    resultado = destino.get_first(sql_ultimo_id)
    ultimo_id = int(resultado[0]) if resultado and resultado[0] is not None else ID_INICIAL

    print(f"[INFO] Último ID já copiado no destino = {ultimo_id}")

    # 3) Buscar um lote de linhas na origem acima desse ID
    sql_busca = f"""
        SELECT TOP ({LOTE}) {", ".join(COLUNAS_tb_ProductionOrderLog)}
        FROM {TABELA_ORIGEM} WITH (NOLOCK)
        WHERE {CHAVE_ID} > {ultimo_id}
        ORDER BY {CHAVE_ID} ASC
    """

    linhas = origem.get_records(sql_busca)
    qtde = len(linhas)

    if qtde == 0:
        print("[INFO] Nenhum registro novo para transferir.")
        return

    print(f"[INFO] Encontradas {qtde} linhas para copiar.")

    # 4) Bulk insert no destino - executemany (COM FECHAMENTO SEGURO)
    conn = None
    cursor = None

    try:
        conn = destino.get_conn()
        cursor = conn.cursor()

        # Detecta qual driver está usando
        cursor_mod = type(cursor).__module__.lower()
        usa_pymssql = "pymssql" in cursor_mod

        ph = "%s" if usa_pymssql else "?"  # pymssql -> %s | pyodbc -> ?
        colunas_sql = ", ".join(f"[{c}]" for c in COLUNAS_tb_ProductionOrderLog)
        placeholders = ", ".join(ph for _ in COLUNAS_tb_ProductionOrderLog)

        print(f"[INFO] Driver detectado = {cursor_mod}")
        print(f"[INFO] Placeholder usado = '{ph}'")

        sql_insert = f"""
            INSERT INTO {TABELA_DESTINO} ({colunas_sql})
            VALUES ({placeholders})
        """

        # fast_executemany só existe/ajuda com pyodbc
        if not usa_pymssql:
            try:
                cursor.fast_executemany = True
                print("[INFO] fast_executemany = True")
            except Exception as e:
                print(f"[WARN] Não foi possível ativar fast_executemany: {e}")

        cursor.executemany(sql_insert, linhas)
        conn.commit()

        maior_id_lote = max(r[0] for r in linhas if r and r[0] is not None)
        print(f"[OK] Insert concluído: {qtde} linhas. Maior ID no lote = {maior_id_lote}")

    except Exception as e:
        # Rollback para evitar transação pendurada/locks
        try:
            if conn:
                conn.rollback()
                print("[INFO] Rollback executado devido a erro.")
        except Exception as rb_e:
            print(f"[WARN] Falha ao executar rollback: {rb_e}")

        print(f"[ERRO] Falha ao inserir lote: {e}")
        raise

    finally:
        # Fecha cursor/conn sempre, mesmo em erro
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass

        try:
            if conn:
                conn.close()
        except Exception:
            pass


# CONFIG_DAG Get_leaf_tables
with DAG(
    dag_id="DAG_Get_leaf_tb_ProductionOrderLog",
    start_date=datetime(2025, 1, 1),
    schedule="*/5 * * * *",
    max_active_runs=1,
    catchup=False,
    default_args={
        "retries": 0,
    },
    tags=["copiar dados de tabelas", "sqlserver"],
) as dag:

    tarefa_copiar_lote = PythonOperator(
        task_id="copiar_lote_leaf_productionorderlog",
        python_callable=copiar_lote_leaf_productionorderlog,
    )





##/////////////////////////////////##
##        tb_ProductionOrder       ##
##/////////////////////////////////##

def copiar_lote_leaf_productionorder():

    TABELA_ORIGEM = "db_mcExperienceSuite.mesAdapter.tb_ProductionOrder"
    TABELA_DESTINO = "leaf_db_mcExperienceSuite.mesAdapter.tb_ProductionOrder"
    CHAVE_ID = "Cod_Record"
    LOTE = 1000
    ID_INICIAL = 228371

    # Lista de colunas na ORDEM EXATA da tabela
    COLUNAS_tb_ProductionOrder = [
        "Cod_Record",
        "Cod_ProductDesign",
        "Val_ProductionOrderNumber",
        "Val_OperationSequenceNo",
        "Val_ScheduledStartUTC",
        "Val_ScheduledEndUTC",
        "Dtt_Start",
        "Dtt_Stop",
        "Val_Identifier",
        "Val_Facility",
        "Val_Department",
        "Val_WorkCenter",
        "Val_ReqQuantity",
        "Val_UOM",
        "Val_ConfirmedQuantity",
        "Val_RatioQuantity",
        "Ind_Status",
        "Ind_Test",
        "Usr_Record",
        "Dtt_Record",
        "Dtt_Update",
        "Ind_ManualInsert",
        "Val_BarCode",
        "Cod_ProductionOrderClassification",
        "Val_Equipment",
        "Dtt_Disable",
        "Stt_Record",
    ]

    # 1) Conexões com SQL Server (Hooks)
    origem = MsSqlHook(mssql_conn_id="sql_leaf_chronos")
    destino = MsSqlHook(mssql_conn_id="sql_udi_apex_azure")

    # 2) Pegar o último ID já copiado no destino (MAX(ID))
    sql_ultimo_id = f"""
        SELECT ISNULL(MAX({CHAVE_ID}), {ID_INICIAL})
        FROM {TABELA_DESTINO}
    """
    resultado = destino.get_first(sql_ultimo_id)
    ultimo_id = int(resultado[0]) if resultado and resultado[0] is not None else ID_INICIAL

    print(f"[INFO] Último ID já copiado no destino = {ultimo_id}")

    # 3) Buscar um lote de linhas na origem acima desse ID
    sql_busca = f"""
        SELECT TOP ({LOTE}) {", ".join(COLUNAS_tb_ProductionOrder)}
        FROM {TABELA_ORIGEM} WITH (NOLOCK)
        WHERE {CHAVE_ID} > {ultimo_id}
        ORDER BY {CHAVE_ID} ASC
    """

    linhas = origem.get_records(sql_busca)
    qtde = len(linhas)

    if qtde == 0:
        print("[INFO] Nenhum registro novo para transferir.")
        return

    print(f"[INFO] Encontradas {qtde} linhas para copiar.")

    # 4) Bulk insert no destino - executemany
    conn = None
    cursor = None

    try:
        conn = destino.get_conn()
        cursor = conn.cursor()

        # Detecta qual driver está usando
        cursor_mod = type(cursor).__module__.lower()
        usa_pymssql = "pymssql" in cursor_mod

        ph = "%s" if usa_pymssql else "?"  # pymssql -> %s | pyodbc -> ?
        colunas_sql = ", ".join(f"[{c}]" for c in COLUNAS_tb_ProductionOrder)
        placeholders = ", ".join(ph for _ in COLUNAS_tb_ProductionOrder)

        print(f"[INFO] Driver detectado = {cursor_mod}")
        print(f"[INFO] Placeholder usado = '{ph}'")

        sql_insert = f"""
            INSERT INTO {TABELA_DESTINO} ({colunas_sql})
            VALUES ({placeholders})
        """

        # fast_executemany só existe/ajuda com pyodbc
        if not usa_pymssql:
            try:
                cursor.fast_executemany = True
                print("[INFO] fast_executemany = True")
            except Exception as e:
                print(f"[WARN] Não foi possível ativar fast_executemany: {e}")

        cursor.executemany(sql_insert, linhas)
        conn.commit()

        # Log opcional: qual foi o maior ID inserido nesse lote
        maior_id_lote = max(r[0] for r in linhas if r and r[0] is not None)
        print(f"[OK] Insert concluído: {qtde} linhas. Maior ID no lote = {maior_id_lote}")

    except Exception as e:
        # Rollback para evitar transação pendurada/locks
        try:
            if conn:
                conn.rollback()
                print("[INFO] Rollback executado devido a erro.")
        except Exception as rb_e:
            print(f"[WARN] Falha ao executar rollback: {rb_e}")

        print(f"[ERRO] Falha ao inserir lote: {e}")
        raise

    finally:
        # Fecha cursor/conn sempre, mesmo em erro
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass

        try:
            if conn:
                conn.close()
        except Exception:
            pass


# CONFIG_DAG Get_leaf_tables
with DAG(
    dag_id="DAG_Get_leaf_productionorder",
    start_date=datetime(2025, 1, 1),
    schedule="*/5 * * * *",
    max_active_runs=1,
    catchup=False,
    default_args={
        "retries": 0,
    },
    tags=["copiar dados de tabelas", "sqlserver"],
) as dag:

    tarefa_copiar_lote = PythonOperator(
        task_id="copiar_lote_leaf_productionorder",
        python_callable=copiar_lote_leaf_productionorder,
    )





##/////////////////////////////////##
##           tb_Container          ##
##/////////////////////////////////##

def copiar_lote_leaf_container():

    TABELA_ORIGEM = "db_mcExperienceSuite.mesAdapter.tb_Container"
    TABELA_DESTINO = "leaf_db_mcExperienceSuite.mesAdapter.tb_Container"
    CHAVE_ID = "Cod_Record"
    LOTE = 1000
    ID_INICIAL = 5716936

    # Lista de colunas na ORDEM EXATA da tabela
    COLUNAS_tb_Container = [
        "Cod_Record",
        "Dtt_Record",
        "Usr_Record",
        "Typ_Container",
        "Typ_UOM",
        "Qtd_Capacity",
        "Des_Position",
        "Jsn_Metadata",
        "Des_Status",
        "Cod_Container",
        "Cod_LotManegement",
        "Des_ContainerID",
        "Qtd_Tare",
        "Qtd_Container",
        "Qtd_Consumed",
        "Des_LabelStatus",
        "Ind_Quality",
        "Des_Inactivation",
        "Nam_Inactivation",
        "Dtt_LastMoviment",
        "Des_ContainerOld",
        "Num_ContainerSequence",
        "Dtt_LastAppLoad",
        "Val_CodeBar",
        "Num_Position",
        "Sts_SAPBlock",
        "Dtt_Update",
        "Dtt_Disable",
        "Stt_Record",
        "Des_ErpLotType"
    ]

    # 1) Conexões com SQL Server (Hooks)
    origem = MsSqlHook(mssql_conn_id="sql_leaf_chronos")
    destino = MsSqlHook(mssql_conn_id="sql_udi_apex_azure")

    # 2) Pegar o último ID já copiado no destino (MAX(ID))
    sql_ultimo_id = f"""
        SELECT ISNULL(MAX({CHAVE_ID}), {ID_INICIAL})
        FROM {TABELA_DESTINO}
    """
    resultado = destino.get_first(sql_ultimo_id)
    ultimo_id = int(resultado[0]) if resultado and resultado[0] is not None else ID_INICIAL

    print(f"[INFO] Último ID já copiado no destino = {ultimo_id}")

    # 3) Buscar um lote de linhas na origem acima desse ID
    sql_busca = f"""
        SELECT TOP ({LOTE}) {", ".join(COLUNAS_tb_Container)}
        FROM {TABELA_ORIGEM} WITH (NOLOCK)
        WHERE {CHAVE_ID} > {ultimo_id}
        ORDER BY {CHAVE_ID} ASC
    """

    linhas = origem.get_records(sql_busca)
    qtde = len(linhas)

    if qtde == 0:
        print("[INFO] Nenhum registro novo para transferir.")
        return

    print(f"[INFO] Encontradas {qtde} linhas para copiar.")

    # 4) Bulk insert no destino - executemany
    conn = None
    cursor = None

    try:
        conn = destino.get_conn()
        cursor = conn.cursor()

        # Detecta qual driver está usando
        cursor_mod = type(cursor).__module__.lower()
        usa_pymssql = "pymssql" in cursor_mod

        ph = "%s" if usa_pymssql else "?"  # pymssql -> %s | pyodbc -> ?
        colunas_sql = ", ".join(f"[{c}]" for c in COLUNAS_tb_Container)
        placeholders = ", ".join(ph for _ in COLUNAS_tb_Container)

        print(f"[INFO] Driver detectado = {cursor_mod}")
        print(f"[INFO] Placeholder usado = '{ph}'")

        sql_insert = f"""
            INSERT INTO {TABELA_DESTINO} ({colunas_sql})
            VALUES ({placeholders})
        """

        # fast_executemany só existe/ajuda com pyodbc
        if not usa_pymssql:
            try:
                cursor.fast_executemany = True
                print("[INFO] fast_executemany = True")
            except Exception as e:
                print(f"[WARN] Não foi possível ativar fast_executemany: {e}")

        cursor.executemany(sql_insert, linhas)
        conn.commit()

        # Log opcional: qual foi o maior ID inserido nesse lote
        maior_id_lote = max(r[0] for r in linhas if r and r[0] is not None)
        print(f"[OK] Insert concluído: {qtde} linhas. Maior ID no lote = {maior_id_lote}")

    except Exception as e:
        # Rollback para evitar transação pendurada/locks
        try:
            if conn:
                conn.rollback()
                print("[INFO] Rollback executado devido a erro.")
        except Exception as rb_e:
            print(f"[WARN] Falha ao executar rollback: {rb_e}")

        print(f"[ERRO] Falha ao inserir lote: {e}")
        raise

    finally:
        # Fecha cursor/conn sempre, mesmo em erro
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass

        try:
            if conn:
                conn.close()
        except Exception:
            pass


# CONFIG_DAG Get_leaf_tables
with DAG(
    dag_id="copiar_lote_leaf_container",
    start_date=datetime(2025, 1, 1),
    schedule="*/5 * * * *",
    max_active_runs=1,
    catchup=False,
    default_args={
        "retries": 0,
    },
    tags=["copiar dados de tabelas", "sqlserver"],
) as dag:

    tarefa_copiar_lote = PythonOperator(
        task_id="copiar_lote_leaf_container",
        python_callable=copiar_lote_leaf_container,
    )




##/////////////////////////////////##
##        tb_QualitySample         ##
##/////////////////////////////////##


def copiar_lote_leaf_qualitysample():

    TABELA_ORIGEM = "db_mcExperienceSuite.mesAdapter.tb_QualitySample"
    TABELA_DESTINO = "leaf_db_mcExperienceSuite.mesAdapter.tb_QualitySample"
    CHAVE_ID = "Cod_Record"
    LOTE = 1000
    ID_INICIAL = 3585561

    # Lista de colunas na ORDEM EXATA da tabela
    COLUNAS_tb_QualitySample = [
        "Cod_Record",
        "Cod_ProductionOrder",
        "Val_SampleID",
        "Val_Facility",
        "Val_Department",
        "Val_WorkCenter",
        "Val_Equipment",
        "Val_EquipmentType",
        "Val_QualityEquipmentID",
        "Val_NPIFlag",
        "Cod_Shift",
        "Val_Material",
        "Val_POShiftStartDateTime",
        "Val_SFGMaterial",
        "Val_SFGSpecValidFrom",
        "Val_WorkDay",
        "Dtt_Record",
        "Ind_Manual",
        "Usr_Record",
        "Des_Comment",
        "Cod_ImportFile",
        "Val_Source",
        "Val_Status",
        "Val_ContainerIDIni",
        "Val_ContainerIDFim",
        "Dtt_SampleReference",
        "Dtt_Update",
        "Usr_Update",
        "Ind_Reprocess",
        "Dtt_Reprocess",
        "Ind_Planned",
        "Ind_Delete",
        "Des_DeleteReason",
        "Dtt_Disable",
        "Stt_Record"
     ]


    # 1) Conexões com SQL Server (Hooks)
    origem = MsSqlHook(mssql_conn_id="sql_leaf_chronos")
    destino = MsSqlHook(mssql_conn_id="sql_udi_apex_azure")

    # 2) Pegar o último ID já copiado no destino (MAX(ID))
    sql_ultimo_id = f"""
        SELECT ISNULL(MAX({CHAVE_ID}), {ID_INICIAL})
        FROM {TABELA_DESTINO}
    """
    resultado = destino.get_first(sql_ultimo_id)
    ultimo_id = int(resultado[0]) if resultado and resultado[0] is not None else ID_INICIAL

    print(f"[INFO] Último ID já copiado no destino = {ultimo_id}")

    # 3) Buscar um lote de linhas na origem acima desse ID
    sql_busca = f"""
        SELECT TOP ({LOTE}) {", ".join(COLUNAS_tb_QualitySample)}
        FROM {TABELA_ORIGEM} WITH (NOLOCK)
        WHERE {CHAVE_ID} > {ultimo_id}
        ORDER BY {CHAVE_ID} ASC
    """

    linhas = origem.get_records(sql_busca)
    qtde = len(linhas)

    if qtde == 0:
        print("[INFO] Nenhum registro novo para transferir.")
        return

    print(f"[INFO] Encontradas {qtde} linhas para copiar.")

    # 4) Bulk insert no destino - executemany
    conn = None
    cursor = None

    try:
        conn = destino.get_conn()
        cursor = conn.cursor()

        # Detecta qual driver está usando
        cursor_mod = type(cursor).__module__.lower()
        usa_pymssql = "pymssql" in cursor_mod

        ph = "%s" if usa_pymssql else "?"  # pymssql -> %s | pyodbc -> ?
        colunas_sql = ", ".join(f"[{c}]" for c in COLUNAS_tb_QualitySample)
        placeholders = ", ".join(ph for _ in COLUNAS_tb_QualitySample)

        print(f"[INFO] Driver detectado = {cursor_mod}")
        print(f"[INFO] Placeholder usado = '{ph}'")

        sql_insert = f"""
            INSERT INTO {TABELA_DESTINO} ({colunas_sql})
            VALUES ({placeholders})
        """

        # fast_executemany só existe/ajuda com pyodbc
        if not usa_pymssql:
            try:
                cursor.fast_executemany = True
                print("[INFO] fast_executemany = True")
            except Exception as e:
                print(f"[WARN] Não foi possível ativar fast_executemany: {e}")

        cursor.executemany(sql_insert, linhas)
        conn.commit()

        # Log opcional: qual foi o maior ID inserido nesse lote
        maior_id_lote = max(r[0] for r in linhas if r and r[0] is not None)
        print(f"[OK] Insert concluído: {qtde} linhas. Maior ID no lote = {maior_id_lote}")

    except Exception as e:
        # Rollback para evitar transação pendurada/locks
        try:
            if conn:
                conn.rollback()
                print("[INFO] Rollback executado devido a erro.")
        except Exception as rb_e:
            print(f"[WARN] Falha ao executar rollback: {rb_e}")

        print(f"[ERRO] Falha ao inserir lote: {e}")
        raise

    finally:
        # Fecha cursor/conn sempre, mesmo em erro
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass

        try:
            if conn:
                conn.close()
        except Exception:
            pass


# CONFIG_DAG Get_leaf_tables
with DAG(
    dag_id="copiar_lote_leaf_qualitysample",
    start_date=datetime(2025, 1, 1),
    schedule="*/5 * * * *",
    max_active_runs=1,
    catchup=False,
    default_args={
        "retries": 0,
    },
    tags=["copiar dados de tabelas", "sqlserver"],
) as dag:

    tarefa_copiar_lote = PythonOperator(
        task_id="copiar_lote_leaf_qualitysample",
        python_callable=copiar_lote_leaf_qualitysample,
    )




##/////////////////////////////////##
##     tb_QualitySampleResult      ##
##/////////////////////////////////##


def copiar_lote_leaf_qualitysampleresult():

    TABELA_ORIGEM = "db_mcExperienceSuite.mesAdapter.tb_QualitySampleResult"
    TABELA_DESTINO = "leaf_db_mcExperienceSuite.mesAdapter.tb_QualitySampleResult"
    CHAVE_ID = "Cod_Record"
    LOTE = 1000
    ID_INICIAL = 22897667

    # Lista de colunas na ORDEM EXATA da tabela
    COLUNAS_tb_QualitySampleResult = [
    "Cod_Record",
    "Cod_ResultGroup",
    "Cod_QualitySample",
    "Cod_Characteristic",
    "Val_Result",
    "Des_DeleteReason",
    "Ind_Delete",
    "Dtt_Record",
    "Usr_Record",
    "Dtt_Result",
    "Dtt_Update",
    "Val_Identifier",
    "Val_Status",
    "Val_ContainerIDIni",
    "Val_ContainerIDFim",
    "Ind_ManualRead",
    "Des_ManualReadReason",
    "Dtt_Disable",
    "Stt_Record"
    ]


    # 1) Conexões com SQL Server (Hooks)
    origem = MsSqlHook(mssql_conn_id="sql_leaf_chronos")
    destino = MsSqlHook(mssql_conn_id="sql_udi_apex_azure")

    # 2) Pegar o último ID já copiado no destino (MAX(ID))
    sql_ultimo_id = f"""
        SELECT ISNULL(MAX({CHAVE_ID}), {ID_INICIAL})
        FROM {TABELA_DESTINO}
    """
    resultado = destino.get_first(sql_ultimo_id)
    ultimo_id = int(resultado[0]) if resultado and resultado[0] is not None else ID_INICIAL

    print(f"[INFO] Último ID já copiado no destino = {ultimo_id}")

    # 3) Buscar um lote de linhas na origem acima desse ID
    sql_busca = f"""
        SELECT TOP ({LOTE}) {", ".join(COLUNAS_tb_QualitySampleResult)}
        FROM {TABELA_ORIGEM} WITH (NOLOCK)
        WHERE {CHAVE_ID} > {ultimo_id}
        ORDER BY {CHAVE_ID} ASC
    """

    linhas = origem.get_records(sql_busca)
    qtde = len(linhas)

    if qtde == 0:
        print("[INFO] Nenhum registro novo para transferir.")
        return

    print(f"[INFO] Encontradas {qtde} linhas para copiar.")

    # 4) Bulk insert no destino - executemany
    conn = None
    cursor = None

    try:
        conn = destino.get_conn()
        cursor = conn.cursor()

        # Detecta qual driver está usando
        cursor_mod = type(cursor).__module__.lower()
        usa_pymssql = "pymssql" in cursor_mod

        ph = "%s" if usa_pymssql else "?"  # pymssql -> %s | pyodbc -> ?
        colunas_sql = ", ".join(f"[{c}]" for c in COLUNAS_tb_QualitySampleResult)
        placeholders = ", ".join(ph for _ in COLUNAS_tb_QualitySampleResult)

        print(f"[INFO] Driver detectado = {cursor_mod}")
        print(f"[INFO] Placeholder usado = '{ph}'")

        sql_insert = f"""
            INSERT INTO {TABELA_DESTINO} ({colunas_sql})
            VALUES ({placeholders})
        """

        # fast_executemany só existe/ajuda com pyodbc
        if not usa_pymssql:
            try:
                cursor.fast_executemany = True
                print("[INFO] fast_executemany = True")
            except Exception as e:
                print(f"[WARN] Não foi possível ativar fast_executemany: {e}")

        cursor.executemany(sql_insert, linhas)
        conn.commit()

        # Log opcional: qual foi o maior ID inserido nesse lote
        maior_id_lote = max(r[0] for r in linhas if r and r[0] is not None)
        print(f"[OK] Insert concluído: {qtde} linhas. Maior ID no lote = {maior_id_lote}")

    except Exception as e:
        # Rollback para evitar transação pendurada/locks
        try:
            if conn:
                conn.rollback()
                print("[INFO] Rollback executado devido a erro.")
        except Exception as rb_e:
            print(f"[WARN] Falha ao executar rollback: {rb_e}")

        print(f"[ERRO] Falha ao inserir lote: {e}")
        raise

    finally:
        # Fecha cursor/conn sempre, mesmo em erro
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass

        try:
            if conn:
                conn.close()
        except Exception:
            pass


# CONFIG_DAG Get_leaf_tables
with DAG(
    dag_id="copiar_lote_leaf_qualitysampleresult",
    start_date=datetime(2025, 1, 1),
    schedule="*/5 * * * *",
    max_active_runs=1,
    catchup=False,
    default_args={
        "retries": 0,
    },
    tags=["copiar dados de tabelas", "sqlserver"],
) as dag:

    tarefa_copiar_lote = PythonOperator(
        task_id="copiar_lote_leaf_qualitysampleresult",
        python_callable=copiar_lote_leaf_qualitysampleresult,
    )




##/////////////////////////////////##
##         tb_Experiment           ##
##/////////////////////////////////##


def copiar_lote_leaf_experiment():

    TABELA_ORIGEM = "db_mcExperienceSuite.mesAdapter.tb_Experiment"
    TABELA_DESTINO = "leaf_db_mcExperienceSuite.mesAdapter.tb_Experiment"
    CHAVE_ID = "Cod_Record"
    LOTE = 100
    ID_INICIAL = 0

        # Lista de colunas na ORDEM EXATA da tabela
    COLUNAS_tb_Experiment = [
        "Cod_Record",
        "Nam_Experiment",
        "Des_Experiment",
        "Ind_Composit",
        "Ind_Setup",
        "Stt_Record",
        "Val_PageSource",
        "Val_TagColor",
        "Ind_DiffSKU",
        "Ind_Container",
        "Usr_Record",
        "Dtt_Record",
        "Dtt_Update",
        "Dtt_Disable"
    ]



    # 1) Conexões com SQL Server (Hooks)
    origem = MsSqlHook(mssql_conn_id="sql_leaf_chronos")
    destino = MsSqlHook(mssql_conn_id="sql_udi_apex_azure")

    # 2) Pegar o último ID já copiado no destino (MAX(ID))
    sql_ultimo_id = f"""
        SELECT ISNULL(MAX({CHAVE_ID}), {ID_INICIAL})
        FROM {TABELA_DESTINO}
    """
    resultado = destino.get_first(sql_ultimo_id)
    ultimo_id = int(resultado[0]) if resultado and resultado[0] is not None else ID_INICIAL

    print(f"[INFO] Último ID já copiado no destino = {ultimo_id}")

    # 3) Buscar um lote de linhas na origem acima desse ID
    sql_busca = f"""
        SELECT TOP ({LOTE}) {", ".join(COLUNAS_tb_Experiment)}
        FROM {TABELA_ORIGEM} WITH (NOLOCK)
        WHERE {CHAVE_ID} > {ultimo_id}
        ORDER BY {CHAVE_ID} ASC
    """

    linhas = origem.get_records(sql_busca)
    qtde = len(linhas)

    if qtde == 0:
        print("[INFO] Nenhum registro novo para transferir.")
        return

    print(f"[INFO] Encontradas {qtde} linhas para copiar.")

    # 4) Bulk insert no destino - executemany
    conn = None
    cursor = None

    try:
        conn = destino.get_conn()
        cursor = conn.cursor()

        # Detecta qual driver está usando
        cursor_mod = type(cursor).__module__.lower()
        usa_pymssql = "pymssql" in cursor_mod

        ph = "%s" if usa_pymssql else "?"  # pymssql -> %s | pyodbc -> ?
        colunas_sql = ", ".join(f"[{c}]" for c in COLUNAS_tb_Experiment)
        placeholders = ", ".join(ph for _ in COLUNAS_tb_Experiment)

        print(f"[INFO] Driver detectado = {cursor_mod}")
        print(f"[INFO] Placeholder usado = '{ph}'")

        sql_insert = f"""
            INSERT INTO {TABELA_DESTINO} ({colunas_sql})
            VALUES ({placeholders})
        """

        # fast_executemany só existe/ajuda com pyodbc
        if not usa_pymssql:
            try:
                cursor.fast_executemany = True
                print("[INFO] fast_executemany = True")
            except Exception as e:
                print(f"[WARN] Não foi possível ativar fast_executemany: {e}")

        cursor.executemany(sql_insert, linhas)
        conn.commit()

        # Log opcional: qual foi o maior ID inserido nesse lote
        maior_id_lote = max(r[0] for r in linhas if r and r[0] is not None)
        print(f"[OK] Insert concluído: {qtde} linhas. Maior ID no lote = {maior_id_lote}")

    except Exception as e:
        # Rollback para evitar transação pendurada/locks
        try:
            if conn:
                conn.rollback()
                print("[INFO] Rollback executado devido a erro.")
        except Exception as rb_e:
            print(f"[WARN] Falha ao executar rollback: {rb_e}")

        print(f"[ERRO] Falha ao inserir lote: {e}")
        raise

    finally:
        # Fecha cursor/conn sempre, mesmo em erro
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass

        try:
            if conn:
                conn.close()
        except Exception:
            pass


# CONFIG_DAG Get_leaf_tables
with DAG(
    dag_id="copiar_lote_leaf_experiment",
    start_date=datetime(2025, 1, 1),
    schedule="*/5 * * * *",
    max_active_runs=1,
    catchup=False,
    default_args={
        "retries": 0,
    },
    tags=["copiar dados de tabelas", "sqlserver"],
) as dag:

    tarefa_copiar_lote = PythonOperator(
        task_id="copiar_lote_leaf_experiment",
        python_callable=copiar_lote_leaf_experiment,
    )


##/////////////////////////////////##
##         tb_Characteristic       ##
##/////////////////////////////////##


def copiar_lote_leaf_characteristic():

    TABELA_ORIGEM = "db_mcExperienceSuite.mesAdapter.tb_Characteristic"
    TABELA_DESTINO = "leaf_db_mcExperienceSuite.mesAdapter.tb_Characteristic"
    CHAVE_ID = "Cod_Record"
    LOTE = 100
    ID_INICIAL = 0

        # Lista de colunas na ORDEM EXATA da tabela
    COLUNAS_tb_Characteristic = [
        "Cod_Record",
        "Nam_Characteristic",
        "Des_Characteristic",
        "Val_UnitOfMeasure",
        "Ind_Q2Type",
        "Val_ExternalCodeCharacteristic",
        "Val_ExternalCodeSetPointEng",
        "Val_ExternalCodeLIEng",
        "Val_ExternalCodeLSEng",
        "Ind_ProcessingType",
        "Val_LimitRule",
        "Val_RagRule",
        "Val_DecimalRule",
        "Usr_Record",
        "Dtt_Record",
        "Val_ExternalCodeLocalMes",
        "Nam_Reference",
        "Val_ExternalCodeCharacteristic2",
        "Val_ExternalCodeCharacteristic3",
        "Flg_DWProcessed",
        "Cod_ExperimentFK",
        "Val_ToleranceRule",
        "Dtt_Update",
        "Dtt_Disable",
        "Stt_Record"
    ]




    # 1) Conexões com SQL Server (Hooks)
    origem = MsSqlHook(mssql_conn_id="sql_leaf_chronos")
    destino = MsSqlHook(mssql_conn_id="sql_udi_apex_azure")

    # 2) Pegar o último ID já copiado no destino (MAX(ID))
    sql_ultimo_id = f"""
        SELECT ISNULL(MAX({CHAVE_ID}), {ID_INICIAL})
        FROM {TABELA_DESTINO}
    """
    resultado = destino.get_first(sql_ultimo_id)
    ultimo_id = int(resultado[0]) if resultado and resultado[0] is not None else ID_INICIAL

    print(f"[INFO] Último ID já copiado no destino = {ultimo_id}")

    # 3) Buscar um lote de linhas na origem acima desse ID
    sql_busca = f"""
        SELECT TOP ({LOTE}) {", ".join(COLUNAS_tb_Characteristic)}
        FROM {TABELA_ORIGEM} WITH (NOLOCK)
        WHERE {CHAVE_ID} > {ultimo_id}
        ORDER BY {CHAVE_ID} ASC
    """

    linhas = origem.get_records(sql_busca)
    qtde = len(linhas)

    if qtde == 0:
        print("[INFO] Nenhum registro novo para transferir.")
        return

    print(f"[INFO] Encontradas {qtde} linhas para copiar.")

    # 4) Bulk insert no destino - executemany
    conn = None
    cursor = None

    try:
        conn = destino.get_conn()
        cursor = conn.cursor()

        # Detecta qual driver está usando
        cursor_mod = type(cursor).__module__.lower()
        usa_pymssql = "pymssql" in cursor_mod

        ph = "%s" if usa_pymssql else "?"  # pymssql -> %s | pyodbc -> ?
        colunas_sql = ", ".join(f"[{c}]" for c in COLUNAS_tb_Characteristic)
        placeholders = ", ".join(ph for _ in COLUNAS_tb_Characteristic)

        print(f"[INFO] Driver detectado = {cursor_mod}")
        print(f"[INFO] Placeholder usado = '{ph}'")

        sql_insert = f"""
            INSERT INTO {TABELA_DESTINO} ({colunas_sql})
            VALUES ({placeholders})
        """

        # fast_executemany só existe/ajuda com pyodbc
        if not usa_pymssql:
            try:
                cursor.fast_executemany = True
                print("[INFO] fast_executemany = True")
            except Exception as e:
                print(f"[WARN] Não foi possível ativar fast_executemany: {e}")

        cursor.executemany(sql_insert, linhas)
        conn.commit()

        # Log opcional: qual foi o maior ID inserido nesse lote
        maior_id_lote = max(r[0] for r in linhas if r and r[0] is not None)
        print(f"[OK] Insert concluído: {qtde} linhas. Maior ID no lote = {maior_id_lote}")

    except Exception as e:
        # Rollback para evitar transação pendurada/locks
        try:
            if conn:
                conn.rollback()
                print("[INFO] Rollback executado devido a erro.")
        except Exception as rb_e:
            print(f"[WARN] Falha ao executar rollback: {rb_e}")

        print(f"[ERRO] Falha ao inserir lote: {e}")
        raise

    finally:
        # Fecha cursor/conn sempre, mesmo em erro
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass

        try:
            if conn:
                conn.close()
        except Exception:
            pass


# CONFIG_DAG Get_leaf_tables
with DAG(
    dag_id="copiar_lote_leaf_characteristic",
    start_date=datetime(2025, 1, 1),
    schedule="*/5 * * * *",
    max_active_runs=1,
    catchup=False,
    default_args={
        "retries": 0,
    },
    tags=["copiar dados de tabelas", "sqlserver"],
) as dag:

    tarefa_copiar_lote = PythonOperator(
        task_id="copiar_lote_leaf_characteristic",
        python_callable=copiar_lote_leaf_characteristic,
    )




##/////////////////////////////////////////##
##     tb_ContainerQualityStatusHistory    ##
##/////////////////////////////////////////##


def copiar_lote_leaf_containerqualitystatushistory():

    TABELA_ORIGEM = "db_mcExperienceSuite.mesAdapter.tb_ContainerQualityStatusHistory"
    TABELA_DESTINO = "leaf_db_mcExperienceSuite.mesAdapter.tb_ContainerQualityStatusHistory"
    CHAVE_ID = "Cod_Record"
    LOTE = 1000
    ID_INICIAL = 1513446

    # Lista de colunas na ORDEM EXATA da tabela
    COLUNAS_tb_ContainerQualityStatusHistory = [
        "Cod_Record",
        "Cod_ProductionOrder",
        "Cod_Container",
        "Val_QualityStatus",
        "Val_TypeUpdate",
        "Usr_Update",
        "Dtt_Record",
        "Cod_QualitySample",
        "Cod_Experiment",
        "Val_Document",
        "Dtt_Update",
        "Dtt_Disable",
        "Stt_Record"
    ]


    # 1) Conexões com SQL Server (Hooks)
    origem = MsSqlHook(mssql_conn_id="sql_leaf_chronos")
    destino = MsSqlHook(mssql_conn_id="sql_udi_apex_azure")

    # 2) Pegar o último ID já copiado no destino (MAX(ID))
    sql_ultimo_id = f"""
        SELECT ISNULL(MAX({CHAVE_ID}), {ID_INICIAL})
        FROM {TABELA_DESTINO}
    """
    resultado = destino.get_first(sql_ultimo_id)
    ultimo_id = int(resultado[0]) if resultado and resultado[0] is not None else ID_INICIAL

    print(f"[INFO] Último ID já copiado no destino = {ultimo_id}")

    # 3) Buscar um lote de linhas na origem acima desse ID
    sql_busca = f"""
        SELECT TOP ({LOTE}) {", ".join(COLUNAS_tb_ContainerQualityStatusHistory)}
        FROM {TABELA_ORIGEM} WITH (NOLOCK)
        WHERE {CHAVE_ID} > {ultimo_id}
        ORDER BY {CHAVE_ID} ASC
    """

    linhas = origem.get_records(sql_busca)
    qtde = len(linhas)

    if qtde == 0:
        print("[INFO] Nenhum registro novo para transferir.")
        return

    print(f"[INFO] Encontradas {qtde} linhas para copiar.")

    # 4) Bulk insert no destino - executemany
    conn = None
    cursor = None

    try:
        conn = destino.get_conn()
        cursor = conn.cursor()

        # Detecta qual driver está usando
        cursor_mod = type(cursor).__module__.lower()
        usa_pymssql = "pymssql" in cursor_mod

        ph = "%s" if usa_pymssql else "?"  # pymssql -> %s | pyodbc -> ?
        colunas_sql = ", ".join(f"[{c}]" for c in COLUNAS_tb_ContainerQualityStatusHistory)
        placeholders = ", ".join(ph for _ in COLUNAS_tb_ContainerQualityStatusHistory)

        print(f"[INFO] Driver detectado = {cursor_mod}")
        print(f"[INFO] Placeholder usado = '{ph}'")

        sql_insert = f"""
            INSERT INTO {TABELA_DESTINO} ({colunas_sql})
            VALUES ({placeholders})
        """

        # fast_executemany só existe/ajuda com pyodbc
        if not usa_pymssql:
            try:
                cursor.fast_executemany = True
                print("[INFO] fast_executemany = True")
            except Exception as e:
                print(f"[WARN] Não foi possível ativar fast_executemany: {e}")

        cursor.executemany(sql_insert, linhas)
        conn.commit()

        # Log opcional: qual foi o maior ID inserido nesse lote
        maior_id_lote = max(r[0] for r in linhas if r and r[0] is not None)
        print(f"[OK] Insert concluído: {qtde} linhas. Maior ID no lote = {maior_id_lote}")

    except Exception as e:
        # Rollback para evitar transação pendurada/locks
        try:
            if conn:
                conn.rollback()
                print("[INFO] Rollback executado devido a erro.")
        except Exception as rb_e:
            print(f"[WARN] Falha ao executar rollback: {rb_e}")

        print(f"[ERRO] Falha ao inserir lote: {e}")
        raise

    finally:
        # Fecha cursor/conn sempre, mesmo em erro
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass

        try:
            if conn:
                conn.close()
        except Exception:
            pass


# CONFIG_DAG Get_leaf_tables
with DAG(
    dag_id="copiar_lote_leaf_containerqualitystatushistory",
    start_date=datetime(2025, 1, 1),
    schedule="*/5 * * * *",
    max_active_runs=1,
    catchup=False,
    default_args={
        "retries": 0,
    },
    tags=["copiar dados de tabelas", "sqlserver"],
) as dag:

    tarefa_copiar_lote = PythonOperator(
        task_id="copiar_lote_leaf_containerqualitystatushistory",
        python_callable=copiar_lote_leaf_containerqualitystatushistory,
    )
