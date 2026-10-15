import pyodbc
from contextlib import contextmanager
from app.core.config import settings


def _get_conn_str() -> str:
    # Prioriza o nome “oficial”, mas aceita fallback
    conn_str = getattr(settings, "UPLOAD_SQL_CONN_STR", None) or getattr(settings, "SQL_CONN_STR", None)
    if not conn_str:
        raise RuntimeError("UPLOAD_SQL_CONN_STR (ou SQL_CONN_STR) não definido no ambiente (.env).")
    return conn_str


@contextmanager
def conn():
    c = pyodbc.connect(_get_conn_str())
    try:
        yield c
    finally:
        c.close()


def get_route(route_key: str):
    """
    Valida route_key e lê allowed_ext / is_active.
    """
    with conn() as cn:
        row = cn.cursor().execute(
            """
            SELECT route_key, allowed_ext, is_active
            FROM control.tb_file_route_upload
            WHERE route_key = ?
            """,
            route_key,
        ).fetchone()

    if not row:
        return None
    if getattr(row, "is_active", 0) in (0, False):
        return None
    return row


def insert_log(
    upload_id: str,
    route_key: str,
    file_name: str,
    file_size_bytes: int,
    bucket: str,
    s3_key: str,
    uploaded_by: str | None = None,
    source_system: str | None = None,
) -> int:
    """
    Insere histórico do upload (control.tb_file_upload_history)
    e retorna o ID gerado.
    """
    with conn() as cn:
        cur = cn.cursor()
        row = cur.execute(
            """
            INSERT INTO control.tb_file_upload_history
            (upload_id, route_key, file_name, file_size_bytes, s3_bucket, s3_key, uploaded_by, source_system, status)
            OUTPUT INSERTED.id
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'UPLOADED')
            """,
            upload_id,
            route_key,
            file_name,
            file_size_bytes,
            bucket,
            s3_key,
            uploaded_by,
            source_system,
        ).fetchone()

        cn.commit()

        if not row:
            raise RuntimeError("Não foi possível obter o ID do histórico de upload.")

        return int(row[0])


def get_status(upload_id: str):
    with conn() as cn:
        row = cn.cursor().execute(
            """
            SELECT status, error_message
            FROM control.tb_file_upload_history
            WHERE upload_id = ?
            """,
            upload_id,
        ).fetchone()
    return row
