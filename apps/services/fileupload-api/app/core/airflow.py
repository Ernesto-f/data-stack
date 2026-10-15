import requests
from datetime import datetime, timezone

# essa config vem do nosso app fileupload,
# pegando variavel de la para centralizar variaveis que envolvam FILE UPLOAD
# todas as variaveis são buscadas de .env
from app.core.config import settings

DAG_ID = "file_upload_job"


def _get_token() -> str:
    r = requests.post(
        f"{settings.AIRFLOW_URL}/auth/token",
        json={"username": settings.AIRFLOW_USER, "password": settings.AIRFLOW_PASS},
        timeout=15,
    )
    r.raise_for_status()
    j = r.json()
    token = j.get("access_token") or j.get("token")
    if not token:
        raise RuntimeError(f"Resposta de token sem token. Keys={list(j.keys())}")
    return token


def trigger(upload_id: str, upload_history_id: int, route_key: str, s3_key: str) -> str:
    token = _get_token()

    logical_date = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    r = requests.post(
        f"{settings.AIRFLOW_URL}/api/v2/dags/{DAG_ID}/dagRuns",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "dag_run_id": f"upload__{upload_id}",
            "logical_date": logical_date,
            "conf": {"upload_id": upload_id, "upload_history_id": upload_history_id,"route_key": route_key, "s3_key": s3_key},
        },
        timeout=15,
    )
    r.raise_for_status()
    return r.json().get("dag_run_id", f"upload__{upload_id}")