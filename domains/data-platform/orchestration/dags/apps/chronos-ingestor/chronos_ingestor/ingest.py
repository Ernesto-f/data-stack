from __future__ import annotations

import time
from datetime import date
from typing import Any

from .chronos import active_modules, fetch_history, iter_days, load_lookup_tables
from .config import Settings, get_settings
from .db import connect, upsert_events, upsert_events_sqlserver
from .normalize import flatten_history


def ingest_chronos(
    date_start: str | None = None,
    date_end: str | None = None,
    modules: str = "",
    settings: Settings | None = None,
    sql_engine: Any | None = None,
) -> dict[str, int]:
    settings = settings or get_settings()
    if not settings.api_auth:
        raise RuntimeError("API_AUTH nao configurado. Crie um .env a partir do .env.example.")

    date_start = date_start or date.today().isoformat()
    date_end = date_end or date_start
    start = date.fromisoformat(date_start)
    end = date.fromisoformat(date_end)
    if end < start:
        raise ValueError("date_end nao pode ser menor que date_start")

    only_ids = {int(value) for value in modules.split(",") if value.strip()}
    all_modules, cells_by_id, modules_by_id = load_lookup_tables()
    selected_modules = active_modules(all_modules, only_ids or None)
    if not selected_modules:
        raise RuntimeError("Nenhum modulo ativo selecionado.")

    sqlite_conn = None
    if settings.database_backend == "sqlite":
        sqlite_conn = connect(settings.database_path)
        print(f"Banco SQLite: {settings.database_path}")
    elif settings.database_backend == "sqlserver":
        if sql_engine is None:
            if not settings.sqlalchemy_url:
                raise RuntimeError("SQLALCHEMY_URL nao configurado para DATABASE_BACKEND=sqlserver.")
            from sqlalchemy import create_engine

            sql_engine = create_engine(settings.sqlalchemy_url, fast_executemany=True)
        print(f"Banco SQL Server: {settings.sqlserver_schema}.{settings.sqlserver_table}")
    else:
        raise RuntimeError(f"DATABASE_BACKEND invalido: {settings.database_backend}")

    total_rows = 0
    total_errors = 0
    print(f"Modulos: {', '.join(str(module['id']) for module in selected_modules)}")

    try:
        for day in iter_days(start, end):
            day_text = day.isoformat()
            print(f"[{day_text}]")
            for module in selected_modules:
                module_id = int(module["id"])
                try:
                    raw_data = fetch_history(settings, module_id, day_text)
                    rows = flatten_history(raw_data, modules_by_id, cells_by_id)
                    if settings.database_backend == "sqlite":
                        saved = upsert_events(sqlite_conn, rows)  # type: ignore[arg-type]
                    else:
                        saved = upsert_events_sqlserver(
                            sql_engine,
                            rows,
                            settings.sqlserver_schema,
                            settings.sqlserver_table,
                        )
                    total_rows += saved
                    print(f"  {module['name']} ({module_id}): {saved} eventos")
                except Exception as exc:
                    total_errors += 1
                    print(f"  {module['name']} ({module_id}): ERRO {exc}")
                time.sleep(settings.fetch_module_delay)
    finally:
        if sqlite_conn is not None:
            sqlite_conn.close()

    print(f"Concluido. {total_rows} eventos gravados/atualizados. Erros: {total_errors}.")
    return {"rows": total_rows, "errors": total_errors}
