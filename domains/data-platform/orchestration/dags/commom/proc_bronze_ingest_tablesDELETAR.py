from __future__ import annotations

import time
import pyodbc
from commom.connections import get_src_conn, get_dst_conn

#############################################################
"""
ATENÇÃO! ESTE COMMON SERA DESCONTINUADO, NAO UTILIZE ELE!!!!!
"""
############################################################

# =============================================================================
# Função genérica: copia incremental em lote com fast_executemany
# =============================================================================
def copiar_lote_incremental_odbc(
    conn_origem_id: str,
    conn_destino_id: str,
    tabela_origem: str,
    tabela_destino: str,
    colunas_busca: list[str],
    colunas_insert: list[str],
    chave_id: str,
    lote: int,
    id_inicial: int,
   # period_filter_on: str,
   # period_name: str,
   # period_value: int,
) -> None:

    ## PROTECAO PRA NAO FAZER CAGADA E DAR INSERT EM BANCO DE PRODUÇÃO
    ALLOWED_DEST_CONN_ID = "sql_udi_apex_azure"
    if conn_destino_id != ALLOWED_DEST_CONN_ID:
        raise RuntimeError(
            f"DESTINO BLOQUEADO: INSERT permitido somente em '{ALLOWED_DEST_CONN_ID}'. "
            f"Recebido conn_destino_id='{conn_destino_id}'."
        )

    # 1) Descobrir ultimo_id no destino
    sql_ultimo = f"""SELECT ISNULL((SELECT TOP (1) [{chave_id}] FROM {tabela_destino} ORDER BY [{chave_id}] DESC), ?)"""

    t0 = time.time()
    with get_dst_conn(conn_destino_id) as cn_dst:
        cur = cn_dst.cursor()
        cur.execute(sql_ultimo, (id_inicial,))
        ultimo_id = int(cur.fetchone()[0] or id_inicial)
    t1 = time.time()

    print(f"[INFO] ultimo_id destino={ultimo_id} | TIME ultimo_id: {t1 - t0:.2f}s")

    # 2) Buscar lote na origem (parametrizado)
    sql_busca = f"""
        SELECT TOP ({lote}) {", ".join(colunas_busca)}
        FROM {tabela_origem} WITH (NOLOCK)
        WHERE {chave_id} > ?
        ORDER BY {chave_id} ASC
    """

    t2 = time.time()
    with get_src_conn(conn_origem_id) as cn_src:
        cur = cn_src.cursor()
        cur.execute(sql_busca, (ultimo_id,))
        rows = cur.fetchall()
    t3 = time.time()

    if not rows:
        print(f"[INFO] Nenhum registro novo. TIME select: {t3 - t2:.2f}s")
        return

    linhas = [tuple(r) for r in rows]
    qtde = len(linhas)
    print(f"[TIME] SELECT origem: {t3 - t2:.2f}s | linhas={qtde}")

    # 3) Insert batch no destino (fast_executemany)
    colunas_sql = ", ".join(f"[{c}]" for c in colunas_insert)
    placeholders = ", ".join("?" for _ in colunas_insert)
    sql_insert = f"INSERT INTO {tabela_destino} ({colunas_sql}) VALUES ({placeholders})"

    t4 = time.time()
    try:
        with get_dst_conn(conn_destino_id) as cn_dst:
            cur = cn_dst.cursor()
            cur.fast_executemany = True
            cur.executemany(sql_insert, linhas)
            cn_dst.commit()
    except Exception as e:
        print(f"[ERRO] Falha ao inserir lote: {e}")
        raise
    t5 = time.time()

    maior_id_lote = max(r[0] for r in linhas if r and r[0] is not None)
    print(f"[TIME] INSERT destino: {t5 - t4:.2f}s | inseridas={qtde} | maior_id={maior_id_lote}")
