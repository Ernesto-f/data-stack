#!/usr/bin/env python3
"""Provision monitoring-only PostgreSQL roles for local exporters."""

from __future__ import annotations

import os
import secrets
import subprocess
from pathlib import Path


DATA_PLATFORM = Path(__file__).resolve().parents[2]
ENV_FILE = DATA_PLATFORM / ".env"


def read_env(path: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        result[key.strip()] = value.strip().strip("\"'")
    return result


def ensure_exporter_secrets() -> dict[str, str]:
    if not ENV_FILE.is_file():
        raise SystemExit(f"Private environment file not found: {ENV_FILE}")
    env = read_env(ENV_FILE)
    secret_keys = (
        "PG_EXPORTER_WAREHOUSE_PASSWORD",
        "PG_EXPORTER_AIRFLOW_PASSWORD",
        "PG_EXPORTER_WEKNORA_PASSWORD",
        "PG_EXPORTER_ARCHIVE_PASSWORD",
        "PG_EXPORTER_SALUS_ERP_PASSWORD",
        "PG_EXPORTER_SALUS_LIMS_PASSWORD",
        "PG_EXPORTER_TRACCAR_PASSWORD",
    )
    for key in secret_keys:
        if not env.get(key) or env[key].startswith("replace-"):
            env[key] = secrets.token_hex(32)

    threshold = env.get("PG_SLOW_QUERY_THRESHOLD_SECONDS", "180")
    if not threshold.isdigit() or not 1 <= int(threshold) <= 86400:
        raise SystemExit("PG_SLOW_QUERY_THRESHOLD_SECONDS must be an integer from 1 to 86400")
    env["PG_SLOW_QUERY_THRESHOLD_SECONDS"] = threshold

    keys_to_persist = set(secret_keys) | {"PG_SLOW_QUERY_THRESHOLD_SECONDS"}
    lines = [
        line for line in ENV_FILE.read_text().splitlines()
        if line.split("=", 1)[0].strip() not in keys_to_persist
    ]
    lines.extend(f"{key}={env[key]}" for key in secret_keys)
    lines.append(f"PG_SLOW_QUERY_THRESHOLD_SECONDS={threshold}")
    ENV_FILE.write_text("\n".join(lines) + "\n")
    ENV_FILE.chmod(0o600)
    return env


def provision_role(container: str, db_user: str, database: str, password: str, threshold: str) -> None:
    sql = r"""\getenv exporter_password PG_EXPORTER_PASSWORD
\getenv slow_query_threshold PG_SLOW_QUERY_THRESHOLD_SECONDS
SELECT format(
  'CREATE ROLE postgres_exporter LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT PASSWORD %L',
  :'exporter_password'
)
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'postgres_exporter')
\gexec
SELECT format(
  'ALTER ROLE postgres_exporter LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT PASSWORD %L',
  :'exporter_password'
)
\gexec
GRANT pg_read_all_stats TO postgres_exporter;
SELECT format(
  'ALTER ROLE postgres_exporter SET pulse.slow_query_threshold_seconds = %L',
  :'slow_query_threshold'
)
\gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO postgres_exporter', current_database())
\gexec
"""
    env = os.environ.copy()
    env["PG_EXPORTER_PASSWORD"] = password
    env["PG_SLOW_QUERY_THRESHOLD_SECONDS"] = threshold
    subprocess.run(
        [
            "docker", "exec", "-i", "-u", "postgres", "-e", "PG_EXPORTER_PASSWORD",
            "-e", "PG_SLOW_QUERY_THRESHOLD_SECONDS",
            container, "psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", db_user, "-d", database,
        ],
        input=sql.encode(),
        env=env,
        check=True,
    )


def main() -> None:
    env = ensure_exporter_secrets()
    weknora = read_env(Path("/srv/data-stack/domains/rag/weknora/WeKnora/.env"))
    salus = read_env(Path("/srv/data-stack/domains/salus-demo/.env"))
    traccar = read_env(Path("/srv/data-stack/domains/pulse-tracking/.env"))

    targets = (
        ("warehouse", "data-platform-warehouse", env.get("PG_USER", "warehouse"), env.get("PG_DB", "warehouse"), "PG_EXPORTER_WAREHOUSE_PASSWORD"),
        ("Airflow metadata", "data-platform-airflow-postgres", "airflow", "airflow", "PG_EXPORTER_AIRFLOW_PASSWORD"),
        ("WeKnora ParadeDB", "WeKnora-postgres", weknora["DB_USER"], weknora["DB_NAME"], "PG_EXPORTER_WEKNORA_PASSWORD"),
        ("Pulse Archive", "pulse-archive-postgres", "paperless", "paperless", "PG_EXPORTER_ARCHIVE_PASSWORD"),
        ("SALUS ERP demo", "salus-demo-erp-postgres", salus["ERP_POSTGRES_USER"], "salus_erp_demo", "PG_EXPORTER_SALUS_ERP_PASSWORD"),
        ("SALUS LIMS demo", "salus-demo-lims-postgres", salus["LIMS_POSTGRES_USER"], "salus_ambiental_demo", "PG_EXPORTER_SALUS_LIMS_PASSWORD"),
        ("Pulse Tracking TimescaleDB", "pulse-tracking-db", traccar["TRACCAR_DB_USER"], "traccar", "PG_EXPORTER_TRACCAR_PASSWORD"),
    )

    for label, container, db_user, database, password_key in targets:
        provision_role(container, db_user, database, env[password_key], env["PG_SLOW_QUERY_THRESHOLD_SECONDS"])
        print(f"Read-only statistics role configured: {label}; slow-query threshold={env['PG_SLOW_QUERY_THRESHOLD_SECONDS']}s")


if __name__ == "__main__":
    main()
