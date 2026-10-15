from __future__ import annotations

from contextlib import closing
import time

from commom.fn_GetWorkday import fn_GetWorkDay
from commom.connections import get_src_conn, get_dst_conn, conn_runtime, insert_rows_bulk
from commom.sql_dialect import get_dialect


# =============================================================================
# Função genérica: copia incremental em lote com fast_executemany / bulk insert
# =============================================================================
def copiar_lote_incremental_refresh_filter_tabelaSemID(
    conn_origem_id: str | None = None,
    conn_destino_id: str | None = None,
    tabela_origem: str | None = None,
    tabela_destino: str | None = None,
    colunas_busca: list[str] | None = None,
    colunas_insert: list[str] | None = None,
    joins_list: list[str] | None = None,
    chave_id: str | None = None,
    chave_id_2: str | None = None,
    lote: int | None = None,
    id_inicial: int | None = None,
    id_inicial_2: int | None = None,
    period_filter_status: str | None = "off",
    refresh_workday_status: str | None = "off",
    workday_role: str | None = "workday",
    period_column: str | None = None,
    period_name: str | None = None,
    period_value: int | None = None,
    allowed_dest_conn_ids: set[str] | list[str] | tuple[str, ...] | None = None,
) -> None:
    """
    Copia incremental em lote entre bancos diferentes.

    Suporta:
    - origem MySQL, Postgres ou SQL Server
    - destino MySQL, Postgres ou SQL Server
    - SQL Server via pyodbc
    - Postgres via PostgresHook/conexão nativa
    - MySQL via MySqlHook/conexão nativa
    - chave incremental simples: chave_id
    - chave incremental composta: chave_id + chave_id_2

    Requer que commom.connections tenha:
    - get_src_conn()
    - get_dst_conn()
    - conn_runtime()
    - insert_rows_bulk()

    O conn_runtime() deve retornar:
    - db_type: "mssql" | "postgres" | "mysql"
    - driver: "pyodbc" | "postgres_native" | "mysql_native"
    - param_placeholder: "?" | "%s"
    """

    # =============================================================================
    # Proteção para não inserir em destino errado
    # =============================================================================

    if allowed_dest_conn_ids is None:
        allowed_dest_conn_ids = {
            "sql_udi_apex_azure",
            "pg_dw_apex", 
        }

    allowed_dest_conn_ids = set(allowed_dest_conn_ids)

    if conn_destino_id not in allowed_dest_conn_ids:
        raise RuntimeError(
            "DESTINO BLOQUEADO: INSERT permitido somente nas connections "
            f"{sorted(allowed_dest_conn_ids)}. "
            f"Recebido conn_destino_id='{conn_destino_id}'."
        )

    # =============================================================================
    # Validações básicas
    # =============================================================================

    if not conn_origem_id:
        raise ValueError("conn_origem_id é obrigatório.")

    if not conn_destino_id:
        raise ValueError("conn_destino_id é obrigatório.")

    if not tabela_origem:
        raise ValueError("tabela_origem é obrigatório.")

    if not tabela_destino:
        raise ValueError("tabela_destino é obrigatório.")

    if not colunas_busca:
        raise ValueError("colunas_busca é obrigatório.")

    if not colunas_insert:
        raise ValueError("colunas_insert é obrigatório.")

    if len(colunas_busca) != len(colunas_insert):
        raise ValueError(
            "colunas_busca e colunas_insert precisam ter a mesma quantidade de colunas. "
            f"Recebido colunas_busca={len(colunas_busca)} e colunas_insert={len(colunas_insert)}."
        )

    if not chave_id:
        raise ValueError("chave_id é obrigatório.")

    chave_id = chave_id.strip()
    chave_id_2 = chave_id_2.strip() if chave_id_2 else None

    if lote is None or lote <= 0:
        raise ValueError("lote é obrigatório e precisa ser maior que zero.")

    if id_inicial is None:
        raise ValueError("id_inicial é obrigatório.")

    if chave_id_2 and id_inicial_2 is None:
        raise ValueError("id_inicial_2 é obrigatório quando chave_id_2 for informada.")

    period_filter_status = (period_filter_status or "off").strip().lower()
    refresh_workday_status = (refresh_workday_status or "off").strip().lower()

    if period_filter_status not in {"on", "off"}:
        raise ValueError("period_filter_status deve ser 'on' ou 'off'.")

    if refresh_workday_status not in {"on", "off"}:
        raise ValueError("refresh_workday_status deve ser 'on' ou 'off'.")

    if refresh_workday_status == "on" and period_filter_status != "on":
        raise ValueError(
            "refresh_workday_status='on' só é permitido quando period_filter_status='on'."
        )

    if period_filter_status == "on":
        if not period_column or not period_name or period_value is None:
            raise ValueError(
                "period_filter_status='on' exige period_column, period_name e period_value."
            )
        period_column = period_column.strip()

    # =============================================================================
    # Joins / aliases
    # =============================================================================

    joins_list = joins_list or []
    joins_sql = "\n".join(joins_list)

    if joins_list:
        if " " not in tabela_origem.strip():
            raise ValueError(
                "joins_list foi informado, mas tabela_origem não tem alias. "
                "Use ex: 'dbo.tb_Fato t'."
            )

        if "." not in chave_id:
            raise ValueError(
                "joins_list foi informado, mas chave_id não está prefixado com alias. "
                "Use ex: 't.Cod_Record'."
            )

        if chave_id_2 and "." not in chave_id_2:
            raise ValueError(
                "joins_list foi informado, mas chave_id_2 não está prefixado com alias. "
                "Use ex: 't.Cod_Record_2'."
            )

        if period_filter_status == "on" and (not period_column or "." not in period_column):
            raise ValueError(
                "Filtro por período com joins exige period_column com alias. "
                "Use ex: 't.Dtt_Record'."
            )

    chave_id_dest = chave_id.split(".")[-1]
    chave_id_2_dest = chave_id_2.split(".")[-1] if chave_id_2 else None
    period_col_dest = period_column.split(".")[-1] if period_column else None

    # =============================================================================
    # Runtime / Dialeto / Placeholder
    # =============================================================================

    runtime_src = conn_runtime(conn_origem_id)
    runtime_dst = conn_runtime(conn_destino_id)

    dialect_src = get_dialect(runtime_src.db_type)
    dialect_dst = get_dialect(runtime_dst.db_type)

    SRC_PARAM = runtime_src.param_placeholder
    DST_PARAM = runtime_dst.param_placeholder

    print(
        f"[DIALECT] origem={dialect_src.name}/{runtime_src.driver} | "
        f"destino={dialect_dst.name}/{runtime_dst.driver} | "
        f"SRC_PARAM={SRC_PARAM} | DST_PARAM={DST_PARAM}"
    )

    select_limit_prefix_src = dialect_src.select_limit_prefix(lote)
    select_limit_suffix_src = dialect_src.select_limit_suffix(lote)
    src_nolock = dialect_src.nolock_hint

    select_limit_prefix_dst_1 = dialect_dst.select_limit_prefix(1)
    select_limit_suffix_dst_1 = dialect_dst.select_limit_suffix(1)

    chave_id_dest_q = dialect_dst.quote_ident(chave_id_dest)
    chave_id_2_dest_q = dialect_dst.quote_ident(chave_id_2_dest) if chave_id_2_dest else None
    period_col_dest_q = dialect_dst.quote_ident(period_col_dest) if period_col_dest else None

    # =============================================================================
    # Descobrir último ID no destino
    # =============================================================================

    t0 = time.time()
    ultimo_id_2 = None

    if chave_id_2:
        if dialect_dst.name == "mssql":
            sql_ultimo = f"""
            SELECT
                ISNULL(
                    (SELECT {select_limit_prefix_dst_1} {chave_id_dest_q}
                     FROM {tabela_destino}
                     ORDER BY {chave_id_dest_q} DESC, {chave_id_2_dest_q} DESC),
                    {DST_PARAM}
                ) AS ultimo_id_1,

                ISNULL(
                    (SELECT {select_limit_prefix_dst_1} {chave_id_2_dest_q}
                     FROM {tabela_destino}
                     ORDER BY {chave_id_dest_q} DESC, {chave_id_2_dest_q} DESC),
                    {DST_PARAM}
                ) AS ultimo_id_2
            """
        else:
            sql_ultimo = f"""
            SELECT
                {dialect_dst.null_func}(
                    (SELECT {chave_id_dest_q}
                     FROM {tabela_destino}
                     ORDER BY {chave_id_dest_q} DESC, {chave_id_2_dest_q} DESC
                     {select_limit_suffix_dst_1}),
                    {DST_PARAM}
                ) AS ultimo_id_1,

                {dialect_dst.null_func}(
                    (SELECT {chave_id_2_dest_q}
                     FROM {tabela_destino}
                     ORDER BY {chave_id_dest_q} DESC, {chave_id_2_dest_q} DESC
                     {select_limit_suffix_dst_1}),
                    {DST_PARAM}
                ) AS ultimo_id_2
            """

        with closing(get_dst_conn(conn_destino_id)) as cn_dst:
            cur = cn_dst.cursor()
            cur.execute(sql_ultimo, (id_inicial, id_inicial_2))
            row = cur.fetchone()

        ultimo_id = row[0] if row and row[0] is not None else id_inicial
        ultimo_id_2 = row[1] if row and row[1] is not None else id_inicial_2

        print(
            f"[INFO] ultimo_id destino=({ultimo_id}, {ultimo_id_2}) | "
            f"TIME ultimo_id: {time.time() - t0:.2f}s"
        )

    else:
        if dialect_dst.name == "mssql":
            sql_ultimo = f"""
            SELECT ISNULL(
                (SELECT {select_limit_prefix_dst_1} {chave_id_dest_q}
                 FROM {tabela_destino}
                 ORDER BY {chave_id_dest_q} DESC),
                {DST_PARAM}
            )
            """
        else:
            sql_ultimo = f"""
            SELECT {dialect_dst.null_func}(
                (SELECT {chave_id_dest_q}
                 FROM {tabela_destino}
                 ORDER BY {chave_id_dest_q} DESC
                 {select_limit_suffix_dst_1}),
                {DST_PARAM}
            )
            """

        with closing(get_dst_conn(conn_destino_id)) as cn_dst:
            cur = cn_dst.cursor()
            cur.execute(sql_ultimo, (id_inicial,))
            row = cur.fetchone()

        ultimo_id = row[0] if row and row[0] is not None else id_inicial

        print(
            f"[INFO] ultimo_id destino={ultimo_id} | "
            f"TIME ultimo_id: {time.time() - t0:.2f}s"
        )

    # =============================================================================
    # Workday / filtro de data
    # =============================================================================

    use_workday = period_filter_status == "on" or refresh_workday_status == "on"
    data_filter = None

    if use_workday:
        t2 = time.time()
        data_filter = fn_GetWorkDay(period_name, period_value, workday_role)
        t3 = time.time()
        print(f"[INFO] Buscou WorkDay = {data_filter} | TIME getWorkday {t3 - t2:.2f}s")

    # =============================================================================
    # SELECT origem - incremental histórico
    # =============================================================================

    if chave_id_2:
        where_incremental = f"""
            (
                   {chave_id} > {SRC_PARAM}
                OR (
                       {chave_id} = {SRC_PARAM}
                   AND {chave_id_2} > {SRC_PARAM}
                )
            )
        """
        order_incremental = f"ORDER BY {chave_id} ASC, {chave_id_2} ASC"
        params: list[object] = [ultimo_id, ultimo_id, ultimo_id_2]
    else:
        where_incremental = f"{chave_id} > {SRC_PARAM}"
        order_incremental = f"ORDER BY {chave_id} ASC"
        params = [ultimo_id]

    if period_filter_status == "on":
        where_incremental += f"\n              AND {period_column} < {SRC_PARAM}"
        params.append(data_filter)

    sql_busca = f"""
        SELECT {select_limit_prefix_src} {", ".join(colunas_busca)}
        FROM {tabela_origem} {src_nolock}
        {joins_sql}
        WHERE {where_incremental}
        {order_incremental}
        {select_limit_suffix_src}
    """

    t4 = time.time()

    with closing(get_src_conn(conn_origem_id)) as cn_src:
        cur = cn_src.cursor()
        cur.execute(sql_busca, tuple(params))
        rows = cur.fetchall()

    t5 = time.time()

    # =============================================================================
    # Sem registros históricos
    # =============================================================================

    if not rows:
        print(f"[INFO] Nenhum registro histórico para copiar. TIME select: {t5 - t4:.2f}s")

        if refresh_workday_status != "on":
            return

        print(f"""
        [INFO] Refresh_Workday = ON.
        Iniciando DELETE na tabela por PERIODO
        Periodo = ({data_filter})
        Banco = ({conn_destino_id})
        Tabela = ({tabela_destino})
        """)

        ## AQUI VAMOS PEGAR A DATA MAIS NOVA ANTES DO DELETE E USAR COMO BUSCA PRO REFRESH. 


        sql_delete = f"""
        DELETE FROM {tabela_destino}
        WHERE {period_col_dest_q} >= {DST_PARAM}
        """

        t6 = time.time()

        with closing(get_dst_conn(conn_destino_id)) as cn_dst:
            cur = cn_dst.cursor()
            cur.execute(sql_delete, (data_filter,))
            rows_deleted = cur.rowcount
            cn_dst.commit()

        t7 = time.time()
        print(
            f"[INFO] DELETE OK! Registros deletados ({rows_deleted}). "
            f"TIME Delete: {t7 - t6:.2f}s"
        )

        order_refresh = (
            f"ORDER BY {chave_id} ASC, {chave_id_2} ASC"
            if chave_id_2
            else f"ORDER BY {chave_id} ASC"
        )

        sql_busca_datafilter = f"""
            SELECT {select_limit_prefix_src} {", ".join(colunas_busca)}
            FROM {tabela_origem} {src_nolock}
            {joins_sql}
            WHERE {period_column} >= {SRC_PARAM}
            {order_refresh}
            {select_limit_suffix_src}
        """

        t8 = time.time()

        with closing(get_src_conn(conn_origem_id)) as cn_src:
            cur = cn_src.cursor()
            cur.execute(sql_busca_datafilter, (data_filter,))
            rows = cur.fetchall()

        linhas = [tuple(r) for r in rows]
        qtde = len(linhas)

        t9 = time.time()

        if not linhas:
            print("[INFO] Sem registros em WORKDAY para copiar!")
            return

        print(f"[TIME] SELECT WORKDAY origem: {t9 - t8:.2f}s | linhas={qtde}")

        t10 = time.time()

        sql_insert = ""

        try:
            with closing(get_dst_conn(conn_destino_id)) as cn_dst:
                cur = cn_dst.cursor()

                sql_insert = insert_rows_bulk(
                    conn_id=conn_destino_id,
                    cur=cur,
                    tabela_destino=tabela_destino,
                    colunas_insert=colunas_insert,
                    linhas=linhas,
                    page_size=5000,
                )

                cn_dst.commit()

        except Exception as e:
            print(f"[ERRO] Falha ao inserir lote WORKDAY: {e}")
            print(f"[ERRO] SQL INSERT: {sql_insert}")
            raise

        t11 = time.time()

        maior_id_lote = max(r[0] for r in linhas if r and r[0] is not None)
        maior_id_lote_2 = (
            max(r[1] for r in linhas if len(r) > 1 and r[1] is not None)
            if chave_id_2
            else None
        )

        if chave_id_2:
            print(
                f"[TIME] INSERT WORKDAY destino: {t11 - t10:.2f}s | "
                f"inseridas={qtde} | maior_id=({maior_id_lote}, {maior_id_lote_2})"
            )
        else:
            print(
                f"[TIME] INSERT WORKDAY destino: {t11 - t10:.2f}s | "
                f"inseridas={qtde} | maior_id={maior_id_lote}"
            )

        return

    # =============================================================================
    # Inserção incremental normal
    # =============================================================================

    linhas = [tuple(r) for r in rows]
    qtde = len(linhas)

    print(f"[TIME] SELECT origem: {t5 - t4:.2f}s | linhas={qtde}")

    t12 = time.time()

    sql_insert = ""

    try:
        with closing(get_dst_conn(conn_destino_id)) as cn_dst:
            cur = cn_dst.cursor()

            sql_insert = insert_rows_bulk(
                conn_id=conn_destino_id,
                cur=cur,
                tabela_destino=tabela_destino,
                colunas_insert=colunas_insert,
                linhas=linhas,
                page_size=5000,
            )

            cn_dst.commit()

    except Exception as e:
        print(f"[ERRO] Falha ao inserir lote: {e}")
        print(f"[ERRO] SQL INSERT: {sql_insert}")
        raise

    t13 = time.time()

    maior_id_lote = max(r[0] for r in linhas if r and r[0] is not None)
    maior_id_lote_2 = (
        max(r[1] for r in linhas if len(r) > 1 and r[1] is not None)
        if chave_id_2
        else None
    )

    if chave_id_2:
        print(
            f"[TIME] INSERT destino: {t13 - t12:.2f}s | "
            f"inseridas={qtde} | maior_id=({maior_id_lote}, {maior_id_lote_2})"
        )
    else:
        print(
            f"[TIME] INSERT destino: {t13 - t12:.2f}s | "
            f"inseridas={qtde} | maior_id={maior_id_lote}"
        )
