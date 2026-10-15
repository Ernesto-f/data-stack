from __future__ import annotations

import json
import ssl
import time
from datetime import date, timedelta
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .config import BASE_DIR, Settings


def load_lookup_tables() -> tuple[list[dict[str, Any]], dict[int, str], dict[int, dict[str, Any]]]:
    cells = json.loads((BASE_DIR / "data" / "celula.json").read_text(encoding="utf-8"))
    modules = json.loads((BASE_DIR / "data" / "modulo.json").read_text(encoding="utf-8"))
    cells_by_id = {int(item["id"]): str(item["name"]) for item in cells}
    modules_by_id = {int(item["id"]): item for item in modules}
    return modules, cells_by_id, modules_by_id


def active_modules(modules: list[dict[str, Any]], only_ids: set[int] | None = None) -> list[dict[str, Any]]:
    selected = [module for module in modules if module.get("activate")]
    if only_ids:
        selected = [module for module in selected if int(module["id"]) in only_ids]
    return selected


def iter_days(start: date, end: date):
    total = (end - start).days + 1
    for index in range(total):
        yield start + timedelta(days=index)


def fetch_history(settings: Settings, module_id: int, day: str) -> dict[str, Any]:
    last_exc: Exception | None = None
    query = urlencode({"module_id": str(module_id), "date_start": day})
    url = f"{settings.history_url}?{query}"
    context = ssl._create_unverified_context()

    for attempt in range(settings.fetch_max_retries):
        try:
            request = Request(url, headers=settings.headers, method="GET")
            with urlopen(request, timeout=settings.http_timeout, context=context) as response:
                data = json.loads(response.read().decode("utf-8"))
            return data if isinstance(data, dict) else {}
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_exc = exc
            if attempt < settings.fetch_max_retries - 1:
                time.sleep(settings.fetch_retry_delay * (2 ** attempt))
    raise RuntimeError(f"erro Chronos module_id={module_id} date={day}: {last_exc}")
