from __future__ import annotations

try:
    # Airflow 3+
    from airflow.sdk import DAG, task
except ImportError:
    # Airflow 2.x
    from airflow import DAG
    from airflow.decorators import task

from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP

import math
import os
import re
import boto3
import pandas as pd
import tempfile
import json
import traceback

import psycopg2
from psycopg2 import sql
from psycopg2.extras import NamedTupleCursor, execute_values


#####################################
######## LOGICA DE EXECUÇÃO #########
#####################################
"""
ENV -> conexão banco/S3
Helpers -> validar, mapear, converter, inserir
DAG -> recebe conf
Task process ->
    atualiza histórico para PROCESSING
    busca rota
    valida destino
    baixa arquivo
    lê CSV/Excel
    aplica map
    valida colunas
    adiciona upload_history_id
    insere no Postgres
    calcula range de ID
    atualiza histórico para DONE
    se erro -> FAILED
    remove arquivo temporário
"""


# ===============================================
# ENV
# Variaveis de ambiente compartilhadas com apps.
# ===============================================
S3_ENDPOINT = os.getenv("S3_ENDPOINT") or os.getenv("UPLOAD_S3_ENDPOINT") or "http://minio:9000"
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY") or os.getenv("UPLOAD_S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY") or os.getenv("UPLOAD_S3_SECRET_KEY")
BUCKET = os.getenv("S3_BUCKET") or os.getenv("UPLOAD_S3_BUCKET") or "fileupload"
UPLOAD_PG_CONN_STR = os.getenv("UPLOAD_PG_CONN_STR")

if not S3_ACCESS_KEY or not S3_SECRET_KEY:
    raise RuntimeError(
        "Credenciais S3 não definidas. Verifique S3_ACCESS_KEY/S3_SECRET_KEY "
        "ou UPLOAD_S3_ACCESS_KEY/UPLOAD_S3_SECRET_KEY."
    )

INGEST_COL = "upload_history_id"
INSERT_PAGE_SIZE = int(os.getenv("UPLOAD_INSERT_PAGE_SIZE", "5000"))


# =============================================================================
# Helpers
# =============================================================================
@contextmanager
def get_conn():
    pg_conn = os.getenv("UPLOAD_PG_CONN_STR")

    if not pg_conn:
        raise RuntimeError("UPLOAD_PG_CONN_STR não definido no ambiente do Airflow.")

    c = psycopg2.connect(
        pg_conn,
        cursor_factory=NamedTupleCursor,
    )

    try:
        yield c
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()


s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
)


_valid_ident = re.compile(r"^[A-Za-z0-9_]+$")


def _validate_ident(x: str, label: str) -> str:
    x = (x or "").strip()

    if not _valid_ident.fullmatch(x):
        raise Exception(f"{label} inválido: '{x}'. Use apenas letras, números e underscore.")

    return x


def normalize_target(target_schema: str | None, target_table: str) -> tuple[sql.Identifier, dict]:
    """
    Retorna:
      - target_sql: schema.table
      - parts: dict com schema/table
    """
    t = (target_table or "").strip()

    if not t:
        raise Exception("target_table é obrigatório.")

    if t.count(".") == 1:
        schema_name, table_name = t.split(".", 1)

        schema_name = _validate_ident(schema_name, "Schema")
        table_name = _validate_ident(table_name, "Table")

        return sql.Identifier(schema_name, table_name), {
            "schema": schema_name,
            "table": table_name,
        }

    if t.count(".") > 1:
        raise Exception("target_table deve estar no formato tabela ou schema.tabela")

    if not target_schema:
        raise Exception("target_schema é obrigatório quando target_table não está no formato schema.tabela")

    schema_name = _validate_ident(target_schema, "Schema")
    table_name = _validate_ident(t, "Table")

    return sql.Identifier(schema_name, table_name), {
        "schema": schema_name,
        "table": table_name,
    }


def parse_expected_columns(expected_columns: str | None) -> list[str] | None:
    if not expected_columns:
        return None

    cols = [c.strip() for c in expected_columns.split(",")]
    cols = [c for c in cols if c]

    return cols or None


def normalize_sheet_name(sheet_name):
    """
    Normaliza o sheet_name vindo do banco para o formato aceito pelo pandas.

    Pandas aceita:
      - int: índice da aba, exemplo 0
      - str: nome da aba, exemplo "Planilha1"

    Evita erro quando o banco retorna:
      - None
      - string vazia
      - "0"
      - Decimal("0")
      - 0.0
    """
    if sheet_name is None:
        return 0

    if isinstance(sheet_name, str):
        v = sheet_name.strip()

        if not v:
            return 0

        if v.isdigit():
            return int(v)

        return v

    if isinstance(sheet_name, int):
        return sheet_name

    try:
        f = float(sheet_name)

        if f.is_integer():
            return int(f)
    except Exception:
        pass

    raise Exception(f"sheet_name inválido: {sheet_name!r} ({type(sheet_name)})")


def filter_df_to_expected(df: pd.DataFrame, expected: list[str] | None) -> pd.DataFrame:
    """
    Mantém somente as colunas esperadas.
    Match case-insensitive.
    Erra se faltar coluna.
    """
    if expected is None:
        return df

    if df is None or df.empty:
        return df

    colmap = {str(c).strip().lower(): c for c in df.columns}

    missing = []
    real_cols = []

    for c in expected:
        key = c.strip().lower()

        if key not in colmap:
            missing.append(c)
        else:
            real_cols.append(colmap[key])

    if missing:
        raise Exception(f"Colunas faltando no arquivo após map: {missing}")

    return df[real_cols]


def apply_column_mapping_manual(df: pd.DataFrame, column_map_json: str | None) -> pd.DataFrame:
    """
    column_map_json: {"Coluna no arquivo": "coluna_sql", ...}

    - exige correspondência exata do lado esquerdo com o header do arquivo
    - mantém só as colunas mapeadas
    - renomeia para o nome exato da coluna SQL
    """
    if not column_map_json:
        return df

    try:
        mapping = json.loads(column_map_json)
    except Exception as e:
        raise Exception(f"column_map_json inválido: {e}")

    if not isinstance(mapping, dict) or not mapping:
        raise Exception("column_map_json deve ser um JSON object não-vazio: {source: target}")

    missing = [src for src in mapping.keys() if src not in df.columns]

    if missing:
        raise Exception(f"Colunas do arquivo não encontradas no map: {missing}")

    df = df[list(mapping.keys())].copy()
    df.rename(columns=mapping, inplace=True)

    return df


def ensure_ingest_col_exists(cn, parts: dict):
    """
    Confirma se a tabela destino tem a coluna upload_history_id.
    """
    schema_name = parts["schema"]
    table_name = parts["table"]

    cur = cn.cursor()

    cur.execute(
        """
        SELECT 1
          FROM information_schema.columns
         WHERE table_schema = %s
           AND table_name = %s
           AND column_name = %s
        """,
        (schema_name, table_name, INGEST_COL),
    )

    row = cur.fetchone()

    if not row:
        raise Exception(
            f"A tabela destino não possui a coluna '{INGEST_COL}'. "
            f"Adicione: ALTER TABLE {schema_name}.{table_name} "
            f"ADD COLUMN {INGEST_COL} BIGINT NULL;"
        )


def get_table_schema(cn, parts: dict) -> dict:
    """
    Retorna dict com o schema da tabela destino.
    """
    schema_name = parts["schema"]
    table_name = parts["table"]

    cur = cn.cursor()

    cur.execute(
        """
        SELECT
            column_name,
            data_type,
            character_maximum_length,
            numeric_precision,
            numeric_scale
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        """,
        (schema_name, table_name),
    )

    rows = cur.fetchall()

    result = {}

    for r in rows:
        result[r.column_name.lower()] = {
            "column_name": r.column_name,
            "data_type": (r.data_type or "").lower(),
            "max_length": r.character_maximum_length,
            "precision": r.numeric_precision,
            "scale": r.numeric_scale,
        }

    return result


def normalize_empty_tokens(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza placeholders comuns de Excel/CSV para None.
    """
    df = df.copy()

    empty_tokens = {
        "",
        "-",
        "--",
        "---",
        "N/A",
        "n/a",
        "NA",
        "na",
        "NULL",
        "null",
        "None",
        "none",
        "nan",
        "NaN",
    }

    def clean_value(x):
        if pd.isna(x):
            return None

        if isinstance(x, str):
            v = x.strip()

            if v in empty_tokens:
                return None

            return v

        return x

    for col in df.columns:
        df[col] = df[col].apply(clean_value)

    return df


def normalize_df_to_sql_types(cn, parts: dict, df: pd.DataFrame) -> pd.DataFrame:
    """
    Converte o DataFrame de acordo com o tipo real da tabela.
    """
    df = normalize_empty_tokens(df)
    df = df.copy()

    schema_info = get_table_schema(cn, parts)

    for col in df.columns:
        key = str(col).strip().lower()

        if key not in schema_info:
            continue

        sql_type = schema_info[key]["data_type"]
        scale = schema_info[key]["scale"]

        if sql_type in ("character varying", "varchar", "character", "char", "text", "uuid"):
            df[col] = df[col].apply(lambda x: None if x is None else str(x))

        elif sql_type in ("bigint", "integer", "smallint"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].apply(lambda x: None if pd.isna(x) else int(x))

        elif sql_type in ("numeric", "decimal"):
            df[col] = pd.to_numeric(df[col], errors="coerce")

            def conv_decimal(x):
                if pd.isna(x):
                    return None

                xf = float(x)

                if not math.isfinite(xf):
                    return None

                if scale is not None:
                    q = Decimal("1." + ("0" * int(scale)))
                    return Decimal(str(xf)).quantize(q, rounding=ROUND_HALF_UP)

                return Decimal(str(xf))

            df[col] = df[col].apply(conv_decimal)

        elif sql_type in ("double precision", "real"):
            df[col] = pd.to_numeric(df[col], errors="coerce")

            df[col] = df[col].apply(
                lambda x: None if pd.isna(x) or not math.isfinite(float(x)) else float(x)
            )

        elif sql_type in (
            "date",
            "timestamp without time zone",
            "timestamp with time zone",
            "time without time zone",
            "time with time zone",
        ):
            df[col] = pd.to_datetime(df[col], errors="coerce")
            df[col] = df[col].astype(object)

            df[col] = df[col].apply(
                lambda x: None if pd.isna(x) else x.to_pydatetime()
            )

        elif sql_type == "boolean":

            def conv_bool(x):
                if x is None:
                    return None

                if isinstance(x, str):
                    v = x.strip().lower()

                    if v in ("1", "true", "t", "sim", "yes", "y", "s"):
                        return True

                    if v in ("0", "false", "f", "nao", "não", "no", "n"):
                        return False

                    return None

                return bool(x)

            df[col] = df[col].apply(conv_bool)

        else:
            df[col] = df[col].apply(lambda x: None if pd.isna(x) else x)

    return df


def insert_dataframe(cn, target_sql: sql.Identifier, parts: dict, df: pd.DataFrame) -> int:
    """
    Insere o DataFrame no Postgres usando bulk insert real.
    """
    if df is None or df.empty:
        return 0

    df = normalize_df_to_sql_types(cn, parts, df)
    df = df.astype(object).where(pd.notna(df), None)

    cols = list(df.columns)

    if not cols:
        return 0

    records = [tuple(row) for row in df.itertuples(index=False, name=None)]

    if not records:
        return 0

    print("==================================================")
    print("TABELA DESTINO:", f'{parts["schema"]}.{parts["table"]}')
    print("COLUNAS DF:", cols)
    print("QTD LINHAS:", len(df))
    print("INSERT_PAGE_SIZE:", INSERT_PAGE_SIZE)
    print("DTYPES:")
    print(df.dtypes)

    print("==== TIPOS PYTHON DA PRIMEIRA LINHA ====")
    first = records[0]

    for i, v in enumerate(first):
        print(i, cols[i], type(v), repr(v))

    print("========================================")

    col_list = sql.SQL(", ").join(sql.Identifier(str(c)) for c in cols)

    insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
        target_sql,
        col_list,
    )

    cur = cn.cursor()

    cur.execute("SET LOCAL lock_timeout = '30s'")
    cur.execute("SET LOCAL statement_timeout = '10min'")

    print(f"[DEBUG] iniciando bulk insert com {len(records)} registros...")

    execute_values(
        cur,
        insert_sql.as_string(cn),
        records,
        page_size=INSERT_PAGE_SIZE,
    )

    print("[DEBUG] bulk insert finalizado. iniciando commit...")

    cn.commit()

    print("[DEBUG] commit finalizado.")

    return len(records)


def override_delete_all(cn, target_sql: sql.Identifier):
    """
    OVERRIDE: apaga tudo antes de inserir.
    DELETE é mais compatível que TRUNCATE.
    """
    cur = cn.cursor()

    delete_sql = sql.SQL("DELETE FROM {};").format(target_sql)

    cur.execute(delete_sql)

    cn.commit()


def compute_id_range_for_upload(
    cn,
    target_sql: sql.Identifier,
    id_column_name: str,
    upload_history_id: int,
) -> tuple[int | None, int | None]:
    """
    Retorna min/max do id_column_name para linhas com upload_history_id.
    """
    id_col = _validate_ident(id_column_name, "id_column_name")

    cur = cn.cursor()

    range_sql = sql.SQL(
        """
        SELECT
            MIN({id_col}) AS id_min,
            MAX({id_col}) AS id_max
        FROM {target_sql}
        WHERE {ingest_col} = %s
        """
    ).format(
        id_col=sql.Identifier(id_col),
        target_sql=target_sql,
        ingest_col=sql.Identifier(INGEST_COL),
    )

    cur.execute(range_sql, (upload_history_id,))

    row = cur.fetchone()

    if not row:
        return None, None

    return row.id_min, row.id_max


# =============================================================================
# DAG
# =============================================================================
with DAG(
    dag_id="file_upload_job",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["fileupload"],
) as dag:

    @task
    def process(**ctx):
        conf = ctx["dag_run"].conf or {}

        upload_id = conf["upload_id"]
        upload_history_id = conf["upload_history_id"]
        route_key = conf["route_key"]
        s3_key = conf["s3_key"]

        tmp_path = None
        total_inserted = 0

        try:
            # 1) PROCESSING
            with get_conn() as cn:
                cn.cursor().execute(
                    """
                    UPDATE control.tb_file_upload_history
                       SET status = 'PROCESSING',
                           processing_start_at = CURRENT_TIMESTAMP(0)
                     WHERE upload_id = %s
                    """,
                    (upload_id,),
                )

                cn.commit()

            # 2) Resolve rota
            with get_conn() as cn:
                cur = cn.cursor()

                cur.execute(
                    """
                    SELECT target_schema,
                           target_table,
                           sheet_name,
                           expected_columns,
                           load_mode,
                           id_column_name,
                           column_map_json
                      FROM control.tb_file_route_upload
                     WHERE route_key = %s
                    """,
                    (route_key,),
                )

                route = cur.fetchone()

                if not route:
                    raise Exception("route_key não encontrada em control.tb_file_route_upload")

                target_schema = route.target_schema
                target_table = route.target_table
                sheet_name = route.sheet_name
                expected_columns = route.expected_columns
                load_mode = (route.load_mode or "INCREMENTAL").strip().upper()
                id_column_name = route.id_column_name
                column_map_json = route.column_map_json

            if load_mode not in ("INCREMENTAL", "OVERRIDE"):
                raise Exception(f"load_mode inválido: '{load_mode}'. Use INCREMENTAL ou OVERRIDE.")

            expected = parse_expected_columns(expected_columns)

            target_sql, parts = normalize_target(target_schema, target_table)

            # 3) Confirma upload_history_id na tabela destino
            with get_conn() as cn:
                ensure_ingest_col_exists(cn, parts)

            print(f"[DEBUG] S3_ENDPOINT={S3_ENDPOINT}")
            print(f"[DEBUG] BUCKET={BUCKET}")
            print(f"[DEBUG] s3_key={s3_key}")
            print(f"[DEBUG] upload_id={upload_id}")
            print(f"[DEBUG] upload_history_id={upload_history_id}")
            print(f"[DEBUG] route_key={route_key}")
            print(f"[DEBUG] sheet_name original={sheet_name!r} type={type(sheet_name)}")
            print(f"[DEBUG] S3_ACCESS_KEY={S3_ACCESS_KEY}")
            print(f"[DEBUG] S3_SECRET_KEY set?={'YES' if S3_SECRET_KEY else 'NO'}")

            # 4) Download MinIO
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                s3.download_fileobj(BUCKET, s3_key, tmp)
                tmp_path = tmp.name

            sk = (s3_key or "").lower()

            # 5) OVERRIDE: apaga tudo antes de inserir
            if load_mode == "OVERRIDE":
                with get_conn() as cn:
                    override_delete_all(cn, target_sql)

            # 6) Ingest
            if sk.endswith(".csv"):
                for chunk in pd.read_csv(tmp_path, chunksize=50_000):
                    chunk = apply_column_mapping_manual(chunk, column_map_json)
                    chunk = filter_df_to_expected(chunk, expected)
                    chunk[INGEST_COL] = int(upload_history_id)

                    with get_conn() as cn:
                        total_inserted += insert_dataframe(cn, target_sql, parts, chunk)

            elif sk.endswith((".xlsx", ".xlsm")):
                sheet = normalize_sheet_name(sheet_name)

                print(f"[DEBUG] sheet_name usado={sheet!r} type={type(sheet)}")
                print(f"[DEBUG] lendo Excel com engine=openpyxl")

                df = pd.read_excel(
                    tmp_path,
                    sheet_name=sheet,
                    engine="openpyxl",
                )

                df = apply_column_mapping_manual(df, column_map_json)
                df = filter_df_to_expected(df, expected)
                df[INGEST_COL] = int(upload_history_id)

                with get_conn() as cn:
                    total_inserted += insert_dataframe(cn, target_sql, parts, df)

            else:
                raise Exception("Formato não suportado. Somente .csv, .xlsx ou .xlsm.")

            # 7) Calcula range se id_column_name estiver configurado
            id_min = None
            id_max = None
            id_col_name = None

            if id_column_name:
                id_col_name = id_column_name.strip()

                with get_conn() as cn:
                    id_min, id_max = compute_id_range_for_upload(
                        cn,
                        target_sql,
                        id_col_name,
                        int(upload_history_id),
                    )

            # 8) DONE + range
            with get_conn() as cn:
                cn.cursor().execute(
                    """
                    UPDATE control.tb_file_upload_history
                       SET status = 'DONE',
                           rows_total = %s,
                           rows_valid = %s,
                           id_col_name = %s,
                           id_min = %s,
                           id_max = %s,
                           processing_end_at = CURRENT_TIMESTAMP(0)
                     WHERE upload_id = %s
                    """,
                    (
                        total_inserted,
                        total_inserted,
                        id_col_name,
                        id_min,
                        id_max,
                        upload_id,
                    ),
                )

                cn.commit()

        except Exception as e:
            print("[ERROR] Exception type:", type(e))
            print("[ERROR] Exception repr:", repr(e))
            print(traceback.format_exc())

            with get_conn() as cn:
                cn.cursor().execute(
                    """
                    UPDATE control.tb_file_upload_history
                       SET status = 'FAILED',
                           error_message = %s,
                           processing_end_at = CURRENT_TIMESTAMP(0)
                     WHERE upload_id = %s
                    """,
                    (
                        str(e),
                        upload_id,
                    ),
                )

                cn.commit()

            raise

        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)

    process()