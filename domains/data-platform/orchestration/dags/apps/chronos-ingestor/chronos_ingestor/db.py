from __future__ import annotations

import json
import sqlite3
import re
from pathlib import Path
from typing import Any


CREATE_EVENTS_SQL = """
CREATE TABLE IF NOT EXISTS chronos_events (
    event_id INTEGER PRIMARY KEY,
    machine_type TEXT,
    date TEXT,
    turno TEXT,
    turno_data TEXT,
    action TEXT,
    description TEXT,
    type_event TEXT,
    interaction TEXT,
    interaction_time TEXT,
    created_at TEXT,
    loss_name TEXT,
    machine_name TEXT,
    machine_id INTEGER,
    module_id INTEGER,
    module_name TEXT,
    cell_id INTEGER,
    cell_name TEXT,
    ingested_at TEXT DEFAULT CURRENT_TIMESTAMP
)
"""


def validate_identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Identificador SQL invalido: {value}")
    return value


UPSERT_EVENT_SQL = """
INSERT INTO chronos_events (
    event_id, machine_type, date, turno, turno_data, action, description,
    type_event, interaction, interaction_time, created_at, loss_name,
    machine_name, machine_id, module_id, module_name, cell_id, cell_name
) VALUES (
    :event_id, :machine_type, :date, :turno, :turno_data, :action, :description,
    :type_event, :interaction, :interaction_time, :created_at, :loss_name,
    :machine_name, :machine_id, :module_id, :module_name, :cell_id, :cell_name
)
ON CONFLICT(event_id) DO UPDATE SET
    machine_type = excluded.machine_type,
    date = excluded.date,
    turno = excluded.turno,
    turno_data = excluded.turno_data,
    action = excluded.action,
    description = excluded.description,
    type_event = excluded.type_event,
    interaction = excluded.interaction,
    interaction_time = excluded.interaction_time,
    created_at = excluded.created_at,
    loss_name = excluded.loss_name,
    machine_name = excluded.machine_name,
    machine_id = excluded.machine_id,
    module_id = excluded.module_id,
    module_name = excluded.module_name,
    cell_id = excluded.cell_id,
    cell_name = excluded.cell_name,
    ingested_at = CURRENT_TIMESTAMP
"""


def connect(database_path: Path) -> sqlite3.Connection:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(database_path)
    conn.row_factory = sqlite3.Row
    conn.execute(CREATE_EVENTS_SQL)
    conn.commit()
    return conn


def upsert_events(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    conn.executemany(UPSERT_EVENT_SQL, rows)
    conn.commit()
    return len(rows)


def ensure_sqlserver_table(engine: Any, schema: str, table: str) -> None:
    from sqlalchemy import text

    schema = validate_identifier(schema)
    table = validate_identifier(table)
    sql = f"""
    IF SCHEMA_ID(:schema_name) IS NULL
        EXEC('CREATE SCHEMA {schema}')

    IF OBJECT_ID(:full_table_name, 'U') IS NULL
    BEGIN
        CREATE TABLE {schema}.{table} (
            event_id BIGINT NOT NULL PRIMARY KEY,
            machine_type NVARCHAR(MAX) NULL,
            event_date DATE NULL,
            turno NVARCHAR(MAX) NULL,
            turno_data NVARCHAR(MAX) NULL,
            event_action NVARCHAR(MAX) NULL,
            event_description NVARCHAR(MAX) NULL,
            type_event NVARCHAR(MAX) NULL,
            interaction NVARCHAR(MAX) NULL,
            interaction_time DATETIME2 NULL,
            created_at DATETIME2 NULL,
            loss_name NVARCHAR(MAX) NULL,
            machine_name NVARCHAR(MAX) NULL,
            machine_id BIGINT NULL,
            module_id BIGINT NULL,
            module_name NVARCHAR(MAX) NULL,
            cell_id BIGINT NULL,
            cell_name NVARCHAR(MAX) NULL,
            ingested_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        )
    END
    """
    with engine.begin() as conn:
        conn.execute(text(sql), {"schema_name": schema, "full_table_name": f"{schema}.{table}"})


def upsert_events_sqlserver(engine: Any, rows: list[dict[str, Any]], schema: str, table: str) -> int:
    if not rows:
        return 0

    from sqlalchemy import text

    schema = validate_identifier(schema)
    table = validate_identifier(table)
    ensure_sqlserver_table(engine, schema, table)

    create_stage_sql = text(
        """
        DROP TABLE IF EXISTS #chronos_events_stage;

        CREATE TABLE #chronos_events_stage (
            event_id BIGINT NOT NULL,
            machine_type NVARCHAR(MAX) NULL,
            event_date DATE NULL,
            turno NVARCHAR(MAX) NULL,
            turno_data NVARCHAR(MAX) NULL,
            event_action NVARCHAR(MAX) NULL,
            event_description NVARCHAR(MAX) NULL,
            type_event NVARCHAR(MAX) NULL,
            interaction NVARCHAR(MAX) NULL,
            interaction_time DATETIME2 NULL,
            created_at DATETIME2 NULL,
            loss_name NVARCHAR(MAX) NULL,
            machine_name NVARCHAR(MAX) NULL,
            machine_id BIGINT NULL,
            module_id BIGINT NULL,
            module_name NVARCHAR(MAX) NULL,
            cell_id BIGINT NULL,
            cell_name NVARCHAR(MAX) NULL
        )
        """
    )
    drop_stage_sql = text("DROP TABLE IF EXISTS #chronos_events_stage")
    insert_stage_sql = text(
        """
        INSERT INTO #chronos_events_stage (
            event_id, machine_type, event_date, turno, turno_data, event_action,
            event_description, type_event, interaction, interaction_time, created_at,
            loss_name, machine_name, machine_id, module_id, module_name, cell_id, cell_name
        )
        SELECT
            event_id,
            machine_type,
            NULLIF(event_date, ''),
            turno,
            turno_data,
            event_action,
            event_description,
            type_event,
            interaction,
            NULLIF(interaction_time, ''),
            NULLIF(created_at, ''),
            loss_name,
            machine_name,
            machine_id,
            module_id,
            module_name,
            cell_id,
            cell_name
        FROM OPENJSON(:payload)
        WITH (
            event_id BIGINT '$.event_id',
            machine_type NVARCHAR(MAX) '$.machine_type',
            event_date NVARCHAR(30) '$.date',
            turno NVARCHAR(MAX) '$.turno',
            turno_data NVARCHAR(MAX) '$.turno_data',
            event_action NVARCHAR(MAX) '$.action',
            event_description NVARCHAR(MAX) '$.description',
            type_event NVARCHAR(MAX) '$.type_event',
            interaction NVARCHAR(MAX) '$.interaction',
            interaction_time NVARCHAR(50) '$.interaction_time',
            created_at NVARCHAR(50) '$.created_at',
            loss_name NVARCHAR(MAX) '$.loss_name',
            machine_name NVARCHAR(MAX) '$.machine_name',
            machine_id BIGINT '$.machine_id',
            module_id BIGINT '$.module_id',
            module_name NVARCHAR(MAX) '$.module_name',
            cell_id BIGINT '$.cell_id',
            cell_name NVARCHAR(MAX) '$.cell_name'
        )
        """
    )
    merge_sql = text(
        f"""
        MERGE {schema}.{table} AS target
        USING (
            SELECT
                event_id,
                MAX(machine_type) AS machine_type,
                MAX(event_date) AS event_date,
                MAX(turno) AS turno,
                MAX(turno_data) AS turno_data,
                MAX(event_action) AS event_action,
                MAX(event_description) AS event_description,
                MAX(type_event) AS type_event,
                MAX(interaction) AS interaction,
                MAX(interaction_time) AS interaction_time,
                MAX(created_at) AS created_at,
                MAX(loss_name) AS loss_name,
                MAX(machine_name) AS machine_name,
                MAX(machine_id) AS machine_id,
                MAX(module_id) AS module_id,
                MAX(module_name) AS module_name,
                MAX(cell_id) AS cell_id,
                MAX(cell_name) AS cell_name
            FROM #chronos_events_stage
            GROUP BY event_id
        ) AS source
        ON target.event_id = source.event_id
        WHEN MATCHED THEN UPDATE SET
            machine_type = source.machine_type,
            event_date = source.event_date,
            turno = source.turno,
            turno_data = source.turno_data,
            event_action = source.event_action,
            event_description = source.event_description,
            type_event = source.type_event,
            interaction = source.interaction,
            interaction_time = source.interaction_time,
            created_at = source.created_at,
            loss_name = source.loss_name,
            machine_name = source.machine_name,
            machine_id = source.machine_id,
            module_id = source.module_id,
            module_name = source.module_name,
            cell_id = source.cell_id,
            cell_name = source.cell_name,
            ingested_at = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN INSERT (
            event_id, machine_type, event_date, turno, turno_data, event_action, event_description,
            type_event, interaction, interaction_time, created_at, loss_name,
            machine_name, machine_id, module_id, module_name, cell_id, cell_name
        ) VALUES (
            source.event_id, source.machine_type, source.event_date, source.turno, source.turno_data,
            source.event_action, source.event_description, source.type_event, source.interaction,
            source.interaction_time, source.created_at, source.loss_name, source.machine_name,
            source.machine_id, source.module_id, source.module_name, source.cell_id, source.cell_name
        );
        """
    )

    with engine.begin() as conn:
        conn.execute(create_stage_sql)
        conn.execute(insert_stage_sql, {"payload": json.dumps(rows, ensure_ascii=False)})
        conn.execute(merge_sql)
        conn.execute(drop_stage_sql)
    return len(rows)
