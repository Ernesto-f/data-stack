from __future__ import annotations

import os
import json
from typing import Optional, Any, Dict, List

import psycopg
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, field_validator

router = APIRouter(prefix="/admin/routes", tags=["fileupload-admin"])


# CONEXÃO POSTGRES
# pega variável de conexão no .env
PG_CONN = os.getenv("UPLOAD_PG_CONN_STR")


# Função para conexão genérica
def get_conn():
    if not PG_CONN:
        raise RuntimeError("UPLOAD_PG_CONN_STR não definido no ambiente.")
    return psycopg.connect(PG_CONN)


# CONVERTE RESPOSTA SQL EM JSON
# como não estou usando SQLAlchemy, precisamos disso.
# por enquanto vamos sem SQLAlchemy para não aumentar complexidade.

def fetch_all_dict(cur) -> List[Dict[str, Any]]:
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def fetch_one_dict(cur) -> Optional[Dict[str, Any]]:
    row = cur.fetchone()
    if not row:
        return None
    cols = [c[0] for c in cur.description]
    return dict(zip(cols, row))


# Classe BaseModel pydantic para tipagem e normalização
# com dados normalizados e tipados, ganhamos performance.
class RouteCreate(BaseModel):
    # opcional: trigger gera se vier vazio/NULL
    route_key: Optional[str] = None

    area_name: str
    dataset_id: str

    target_system: str
    airflow_conn_id: Optional[str] = None

    target_schema: str
    target_table: str

    merge_proc: Optional[str] = None
    allowed_ext: Optional[str] = "xlsx,csv"
    sheet_name: Optional[str] = None
    max_file_mb: Optional[int] = None
    is_active: bool = True
    notes: Optional[str] = None

    expected_columns: Optional[str] = None
    load_mode: str = "INCREMENTAL"
    id_column_name: Optional[str] = None
    column_map_json: Optional[str] = None

    @field_validator("target_system")
    @classmethod
    def _ts(cls, v: str):
        v = (v or "").strip().upper()
        if not v:
            raise ValueError("target_system é obrigatório, ex: SQLSERVER")
        return v

    @field_validator("load_mode")
    @classmethod
    def _lm(cls, v: str):
        v = (v or "INCREMENTAL").strip().upper()
        if v not in ("INCREMENTAL", "OVERRIDE"):
            raise ValueError("load_mode deve ser INCREMENTAL ou OVERRIDE")
        return v

    @field_validator("column_map_json")
    @classmethod
    def _validate_json(cls, v: Optional[str]):
        if not v:
            return v

        obj = json.loads(v)

        if not isinstance(obj, dict):
            raise ValueError("column_map_json deve ser um JSON object {source: target}")

        return v


# Classe BaseModel pydantic para tipagem e normalização
# com dados normalizados e tipados, ganhamos performance.
class RouteUpdate(BaseModel):
    area_name: Optional[str] = None
    dataset_id: Optional[str] = None

    target_system: Optional[str] = None
    airflow_conn_id: Optional[str] = None

    target_schema: Optional[str] = None
    target_table: Optional[str] = None

    merge_proc: Optional[str] = None
    allowed_ext: Optional[str] = None
    sheet_name: Optional[str] = None
    max_file_mb: Optional[int] = None
    is_active: Optional[bool] = None
    notes: Optional[str] = None

    expected_columns: Optional[str] = None
    load_mode: Optional[str] = None
    id_column_name: Optional[str] = None
    column_map_json: Optional[str] = None

    @field_validator("target_system")
    @classmethod
    def _ts(cls, v: Optional[str]):
        if v is None:
            return v
        return v.strip().upper()

    @field_validator("load_mode")
    @classmethod
    def _lm(cls, v: Optional[str]):
        if v is None:
            return v

        v = v.strip().upper()

        if v not in ("INCREMENTAL", "OVERRIDE"):
            raise ValueError("load_mode deve ser INCREMENTAL ou OVERRIDE")

        return v

    @field_validator("column_map_json")
    @classmethod
    def _validate_json(cls, v: Optional[str]):
        if v is None or v == "":
            return v

        obj = json.loads(v)

        if not isinstance(obj, dict):
            raise ValueError("column_map_json deve ser um JSON object {source: target}")

        return v


# rota get para lista de registros
@router.get("")
def list_routes():
    with get_conn() as cn:
        cur = cn.cursor()
        cur.execute("""
            SELECT
                route_key,
                area_name,
                dataset_id,
                target_system,
                target_schema,
                target_table,
                load_mode,
                is_active,
                updated_at
            FROM control.tb_file_route_upload
            ORDER BY updated_at DESC, route_key ASC
        """)

        return fetch_all_dict(cur)


# rota get para editar um registro
# aqui ele usa o código route_key para buscar o registro.
@router.get("/{route_key}")
def get_route(route_key: str):
    rk = (route_key or "").strip().upper()

    with get_conn() as cn:
        cur = cn.cursor()
        cur.execute("""
            SELECT
                route_key,
                area_name,
                dataset_id,
                target_system,
                airflow_conn_id,
                target_schema,
                target_table,
                merge_proc,
                allowed_ext,
                sheet_name,
                max_file_mb,
                is_active,
                notes,
                expected_columns,
                load_mode,
                id_column_name,
                column_map_json,
                created_at,
                updated_at
            FROM control.tb_file_route_upload
            WHERE route_key = %s
        """, (rk,))

        data = fetch_one_dict(cur)

        if not data:
            raise HTTPException(status_code=404, detail="route_key não encontrada")

        return data


# rota para criar registro
@router.post("")
def create_route(payload: RouteCreate):
    rk = (payload.route_key or "").strip().upper() or None

    try:
        with get_conn() as cn:
            cur = cn.cursor()

            cur.execute("""
                INSERT INTO control.tb_file_route_upload
                (
                    route_key,
                    area_name,
                    dataset_id,
                    target_system,
                    airflow_conn_id,
                    target_schema,
                    target_table,
                    merge_proc,
                    allowed_ext,
                    sheet_name,
                    max_file_mb,
                    is_active,
                    notes,
                    expected_columns,
                    load_mode,
                    id_column_name,
                    column_map_json
                )
                VALUES
                (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
                RETURNING route_key
            """, (
                rk,
                payload.area_name,
                payload.dataset_id,
                payload.target_system,
                payload.airflow_conn_id,
                payload.target_schema,
                payload.target_table,
                payload.merge_proc,
                payload.allowed_ext,
                payload.sheet_name,
                payload.max_file_mb,
                payload.is_active,
                payload.notes,
                payload.expected_columns,
                payload.load_mode,
                payload.id_column_name,
                payload.column_map_json
            ))

            row = cur.fetchone()
            cn.commit()

            final_route_key = row[0] if row else rk

        return {
            "ok": True,
            "route_key": final_route_key,
            "mode": "CREATED"
        }

    except psycopg.Error as e:
        raise HTTPException(status_code=500, detail=str(e))


# rota para atualizar registro
# aqui ele usa o código route_key para buscar o registro.
@router.put("/{route_key}")
def update_route(route_key: str, payload: RouteUpdate):
    rk = (route_key or "").strip().upper()

    sets = []
    params: List[Any] = []

    def add(col: str, val: Any):
        sets.append(f"{col} = %s")
        params.append(val)

    # payload para fazer update só no que for preciso.
    if payload.area_name is not None:
        add("area_name", payload.area_name)

    if payload.dataset_id is not None:
        add("dataset_id", payload.dataset_id)

    if payload.target_system is not None:
        add("target_system", payload.target_system)

    if payload.airflow_conn_id is not None:
        add("airflow_conn_id", payload.airflow_conn_id)

    if payload.target_schema is not None:
        add("target_schema", payload.target_schema)

    if payload.target_table is not None:
        add("target_table", payload.target_table)

    if payload.merge_proc is not None:
        add("merge_proc", payload.merge_proc)

    if payload.allowed_ext is not None:
        add("allowed_ext", payload.allowed_ext)

    if payload.sheet_name is not None:
        add("sheet_name", payload.sheet_name)

    if payload.max_file_mb is not None:
        add("max_file_mb", payload.max_file_mb)

    if payload.is_active is not None:
        add("is_active", payload.is_active)

    if payload.notes is not None:
        add("notes", payload.notes)

    if payload.expected_columns is not None:
        add("expected_columns", payload.expected_columns)

    if payload.load_mode is not None:
        add("load_mode", payload.load_mode)

    if payload.id_column_name is not None:
        add("id_column_name", payload.id_column_name)

    if payload.column_map_json is not None:
        add("column_map_json", payload.column_map_json)

    if not sets:
        return {
            "ok": True,
            "route_key": rk,
            "mode": "NO_CHANGES"
        }

    # sempre atualiza updated_at
    sets.append("updated_at = CURRENT_TIMESTAMP(0)")
    params.append(rk)

    try:
        with get_conn() as cn:
            cur = cn.cursor()

            cur.execute(f"""
                UPDATE control.tb_file_route_upload
                   SET {", ".join(sets)}
                 WHERE route_key = %s
            """, params)

            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="route_key não encontrada")

            cn.commit()

        return {
            "ok": True,
            "route_key": rk,
            "mode": "UPDATED"
        }

    except psycopg.Error as e:
        raise HTTPException(status_code=500, detail=str(e))