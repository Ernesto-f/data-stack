import uuid
from fastapi import APIRouter, UploadFile, File, Form, HTTPException

from app.core.minios3 import upload_fileobj
from app.core.config import settings
from app.core.postgres import get_route, insert_log, get_status
from app.core.airflow import trigger

router = APIRouter(prefix="/upload", tags=["fileupload"])


def _ext_of(filename: str) -> str:
    """
    Extrai a extensão do arquivo.
    Ex:
      teste.xlsx -> xlsx
      teste.xlsm -> xlsm
      teste.csv  -> csv
    """
    if not filename or "." not in filename:
        return ""

    return filename.rsplit(".", 1)[-1].lower().strip()


def _get(row, field: str, default=None):
    """
    Permite acessar tanto dict quanto objeto.
    Útil caso o retorno do Postgres venha como dict.
    """
    if row is None:
        return default

    if isinstance(row, dict):
        return row.get(field, default)

    return getattr(row, field, default)


def _allowed_ext_set(route) -> set[str]:
    """
    Lê allowed_ext da route_key.

    Exemplo no banco:
      xlsx,csv
      xlsx,xlsm,csv
      xlsm

    Retorna:
      {"xlsx", "xlsm", "csv"}
    """
    allowed_ext = _get(route, "allowed_ext", "xlsx")

    allowed = str(allowed_ext or "xlsx").lower()
    allowed = allowed.replace(" ", "")

    return {
        ext.strip().lower().lstrip(".")
        for ext in allowed.split(",")
        if ext.strip()
    }


@router.get("/allowed-ext/{route_key}")
def allowed_ext(route_key: str):
    """
    Endpoint para o front descobrir quais extensões são permitidas
    para uma determinada route_key.
    """
    route_key = route_key.strip().upper()

    route = get_route(route_key)

    if not route:
        raise HTTPException(404, "route_key inválida ou inativa")

    allowed_set = _allowed_ext_set(route)

    return {
        "route_key": route_key,
        "allowed_ext": sorted(allowed_set),
        "accept": ",".join(f".{ext}" for ext in sorted(allowed_set)),
    }


@router.post("/")
async def fileupload(
    route_key: str = Form(...),
    file: UploadFile = File(...),
):
    route_key = route_key.strip().upper()

    route = get_route(route_key)

    if not route:
        raise HTTPException(404, "route_key inválida ou inativa")

    filename = file.filename or "upload"
    ext = _ext_of(filename)

    allowed_set = _allowed_ext_set(route)

    if ext not in allowed_set:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Extensão não permitida: .{ext or 'sem extensão'}. "
                f"Permitidas para esta route_key: "
                f"{', '.join('.' + x for x in sorted(allowed_set))}"
            ),
        )

    upload_id = str(uuid.uuid4())
    s3_key = f"uploads/{route_key}/{upload_id}__{filename}"

    # 1) Salva no MinIO/S3
    upload_fileobj(
        bucket=settings.S3_BUCKET,
        key=s3_key,
        fileobj=file.file,
        content_type=file.content_type,
    )

    # 2) Grava histórico no PostgreSQL
    upload_history_id = insert_log(
        upload_id=upload_id,
        route_key=route_key,
        file_name=filename,
        file_size_bytes=getattr(file, "size", None) or 0,
        bucket=settings.S3_BUCKET,
        s3_key=s3_key,
        uploaded_by=None,
        source_system="portal-fileupload",
    )

    # 3) Dispara Airflow
    dag_run_id = trigger(
        upload_id,
        upload_history_id,
        route_key,
        s3_key,
    )

    return {
        "upload_id": upload_id,
        "upload_history_id": upload_history_id,
        "dag_run_id": dag_run_id,
    }


@router.get("/status/{upload_id}")
def status(upload_id: str):
    row = get_status(upload_id)

    if not row:
        return {"status": "NOT_FOUND"}

    return {
        "status": _get(row, "status"),
        "error": _get(row, "error_message"),
    }