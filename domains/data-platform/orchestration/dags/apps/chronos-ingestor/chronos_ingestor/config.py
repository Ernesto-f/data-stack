from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


load_env(BASE_DIR / ".env")


@dataclass(frozen=True)
class Settings:
    api_auth: str
    api_origin: str
    api_referer: str
    history_url: str
    database_path: Path
    database_backend: str
    sqlalchemy_url: str
    sqlserver_schema: str
    sqlserver_table: str
    http_timeout: int
    fetch_max_retries: int
    fetch_retry_delay: float
    fetch_module_delay: float

    @property
    def headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json, text/plain, */*",
            "Authorization": self.api_auth,
            "Origin": self.api_origin,
            "Referer": self.api_referer,
            "User-Agent": "Mozilla/5.0 (compatible; Chronos-Ingestor/1.0)",
        }


def get_settings() -> Settings:
    database_path = Path(os.getenv("DATABASE_PATH", "data/chronos.sqlite3"))
    if not database_path.is_absolute():
        database_path = BASE_DIR / database_path
    return Settings(
        api_auth=os.getenv("API_AUTH", ""),
        api_origin=os.getenv("API_ORIGIN", "https://app.uat.ups-br.global.batgen.com"),
        api_referer=os.getenv("API_REFERER", "https://app.uat.ups-br.global.batgen.com/"),
        history_url=os.getenv("HISTORY_URL", "https://server.uat.ups-br.global.batgen.com/Timeline/"),
        database_path=database_path,
        database_backend=os.getenv("DATABASE_BACKEND", "sqlite").lower(),
        sqlalchemy_url=os.getenv("SQLALCHEMY_URL", ""),
        sqlserver_schema=os.getenv("SQLSERVER_SCHEMA", "mesAdapter"),
        sqlserver_table=os.getenv("SQLSERVER_TABLE", "tb_chronos_events"),
        http_timeout=int(os.getenv("HTTP_TIMEOUT", "60")),
        fetch_max_retries=int(os.getenv("FETCH_MAX_RETRIES", "3")),
        fetch_retry_delay=float(os.getenv("FETCH_RETRY_DELAY", "5")),
        fetch_module_delay=float(os.getenv("FETCH_MODULE_DELAY", "3")),
    )
