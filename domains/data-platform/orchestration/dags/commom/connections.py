from __future__ import annotations

import pyodbc
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Literal, Sequence

from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from sqlalchemy import create_engine
from sqlalchemy.engine import URL

# LIBS AIRFLOW 3X
from airflow.sdk.bases.hook import BaseHook

try:
    from psycopg2.extras import execute_values
except Exception:
    execute_values = None


DbType = Literal["mysql", "postgres", "mssql"]
DbDriver = Literal["mysql_native", "postgres_native", "pyodbc"]


@dataclass(frozen=True)
class ConnRuntime:
    """
    Informações reais de execução da connection:
    - db_type: dialeto SQL do banco
    - driver: driver Python usado para conectar
    - param_placeholder: placeholder correto para cursor.execute()
    - use_fast_executemany: se pode usar fast_executemany
    """
    conn_id: str
    db_type: DbType
    driver: DbDriver
    param_placeholder: str
    use_fast_executemany: bool = False


@lru_cache(maxsize=64)
def _conn(conn_id: str):
    """Cache do objeto Connection do Airflow."""
    return BaseHook.get_connection(conn_id)


def conn_type(conn_id: str) -> str:
    return (_conn(conn_id).conn_type or "").lower().strip()


def conn_extra(conn_id: str) -> dict:
    return _conn(conn_id).extra_dejson or {}


@lru_cache(maxsize=64)
def _odbc_connect_str(conn_id: str) -> str:
    extra = conn_extra(conn_id)
    odbc_connect = extra.get("odbc_connect")

    if not odbc_connect:
        raise ValueError(
            f"Connection '{conn_id}' precisa ter Extra JSON com 'odbc_connect' "
            "para usar conexão via ODBC."
        )

    return odbc_connect


def _detect_db_type_from_string(value: str) -> DbType:
    s = (value or "").lower().strip()

    # Airflow conn_type / SQLAlchemy style
    if s in ("mysql", "mariadb", "mysql+mysqlconnector", "mysql+pymysql"):
        return "mysql"

    if s in ("postgres", "postgresql", "postgresql+psycopg2", "postgresql+psycopg"):
        return "postgres"

    if s in ("mssql", "microsoft mssql", "ms_sql", "mssql+pyodbc", "sqlserver"):
        return "mssql"

    # Fallback por string ODBC
    if (
        "sql server" in s
        or "odbc driver 18 for sql server" in s
        or "odbc driver 17 for sql server" in s
        or "driver={sql server" in s
    ):
        return "mssql"

    if "postgresql" in s or "postgres" in s:
        return "postgres"

    if "mariadb" in s or "mysql" in s:
        return "mysql"

    raise ValueError(f"Banco não suportado/indetectável a partir de '{value}'.")


def conn_db_type(conn_id: str) -> DbType:
    """
    Detecta o banco pelo conn_type.
    Se não conseguir, tenta detectar pelo odbc_connect.
    """
    ctype = conn_type(conn_id)

    try:
        return _detect_db_type_from_string(ctype)
    except ValueError:
        pass

    extra = conn_extra(conn_id)
    if extra.get("odbc_connect"):
        return _detect_db_type_from_string(extra["odbc_connect"])

    raise ValueError(
        f"Banco não suportado/indetectável para conn_id='{conn_id}', conn_type='{ctype}'."
    )


def conn_driver(conn_id: str) -> DbDriver:
    """
    Define qual driver Python será usado.

    Regra:
    - postgres -> PostgresHook nativo
    - mysql -> MySqlHook nativo
    - mssql -> pyodbc, mantendo o padrão atual do seu projeto
    - se conn_type não for nativo, mas tiver odbc_connect, usa pyodbc
    """
    ctype = conn_type(conn_id)

    if ctype in ("postgres", "postgresql", "postgresql+psycopg2", "postgresql+psycopg"):
        return "postgres_native"

    if ctype in ("mysql", "mariadb", "mysql+mysqlconnector", "mysql+pymysql"):
        return "mysql_native"

    if ctype in ("mssql", "microsoft mssql", "ms_sql", "mssql+pyodbc", "sqlserver"):
        return "pyodbc"

    if conn_extra(conn_id).get("odbc_connect"):
        return "pyodbc"

    db_type = conn_db_type(conn_id)

    if db_type == "postgres":
        return "postgres_native"

    if db_type == "mysql":
        return "mysql_native"

    return "pyodbc"


def conn_runtime(conn_id: str) -> ConnRuntime:
    db_type = conn_db_type(conn_id)
    driver = conn_driver(conn_id)

    if driver == "pyodbc":
        return ConnRuntime(
            conn_id=conn_id,
            db_type=db_type,
            driver=driver,
            param_placeholder="?",
            use_fast_executemany=(db_type == "mssql"),
        )

    if driver in ("postgres_native", "mysql_native"):
        return ConnRuntime(
            conn_id=conn_id,
            db_type=db_type,
            driver=driver,
            param_placeholder="%s",
            use_fast_executemany=False,
        )

    raise ValueError(f"Driver não suportado para conn_id='{conn_id}': {driver}")


def get_db_conn(conn_id: str):
    """
    Conexão DB-API genérica:
    - MySQL nativo via MySqlHook
    - Postgres nativo via PostgresHook
    - SQL Server/ODBC via pyodbc
    """
    runtime = conn_runtime(conn_id)

    if runtime.driver == "mysql_native":
        return MySqlHook(mysql_conn_id=conn_id).get_conn()

    if runtime.driver == "postgres_native":
        return PostgresHook(postgres_conn_id=conn_id).get_conn()

    if runtime.driver == "pyodbc":
        return pyodbc.connect(_odbc_connect_str(conn_id))

    raise ValueError(f"Driver não suportado: {runtime.driver}")

def get_sqlalchemy_engine(conn_id: str):
    """SQLAlchemy Engine a partir de uma connection do Airflow."""
    if conn_driver(conn_id) == "pyodbc":
        return create_engine(
            URL.create(
                "mssql+pyodbc",
                query={"odbc_connect": _odbc_connect_str(conn_id)},
            ),
            fast_executemany=True,
        )

    return create_engine(_conn(conn_id).get_uri())

def get_src_conn(conn_id: str):
    """Conexão de ORIGEM."""
    return get_db_conn(conn_id)


def get_dst_conn(conn_id: str):
    """Conexão de DESTINO."""
    return get_db_conn(conn_id)


def prepare_cursor_for_bulk_insert(cur, conn_id: str) -> None:
    """
    Ativa fast_executemany apenas quando o driver suportar.
    Em PostgresHook/MySqlHook isso não deve ser usado.
    """
    runtime = conn_runtime(conn_id)

    if runtime.use_fast_executemany and hasattr(cur, "fast_executemany"):
        cur.fast_executemany = True


def insert_rows_bulk(
    conn_id: str,
    cur,
    tabela_destino: str,
    colunas_insert: Sequence[str],
    linhas: Sequence[Sequence[Any]],
    page_size: int = 5000,
) -> str:
    """
    Insere várias linhas usando a melhor estratégia conforme banco/driver.

    Estratégia:
    - Postgres nativo: psycopg2.extras.execute_values
    - SQL Server via pyodbc: executemany + fast_executemany
    - MySQL nativo: executemany
    """

    if not linhas:
        return ""

    runtime = conn_runtime(conn_id)

    from commom.sql_dialect import get_dialect

    dialect = get_dialect(runtime.db_type)

    tabela_sql = dialect.quote_qualified_ident(tabela_destino)

    colunas_sql = ", ".join(
        dialect.quote_ident(c)
        for c in colunas_insert
    )

    linhas_tuple = [tuple(r) for r in linhas]

    if (
        runtime.db_type == "postgres"
        and runtime.driver == "postgres_native"
        and execute_values is not None
    ):
        sql_insert = f"""
        INSERT INTO {tabela_sql} ({colunas_sql})
        VALUES %s
        """

        execute_values(
            cur,
            sql_insert,
            linhas_tuple,
            page_size=page_size,
        )

        return sql_insert

    placeholders = ", ".join(
        runtime.param_placeholder
        for _ in colunas_insert
    )

    sql_insert = f"""
    INSERT INTO {tabela_sql} ({colunas_sql})
    VALUES ({placeholders})
    """

    prepare_cursor_for_bulk_insert(cur, conn_id)
    cur.executemany(sql_insert, linhas_tuple)

    return sql_insert