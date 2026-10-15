from datetime import datetime
from typing import Optional

from fastapi import HTTPException

from config import DB
from database import Database
from models import ExceptionCreate, ExceptionResponse


class ExceptionService:
    def __init__(self, db: Database):
        self.db = db

    # ── Query ─────────────────────────────────────────────────────────────────

    def list(
        self,
        val_equipment: Optional[str]      = None,
        dtt_start:     Optional[datetime] = None,
        dtt_end:       Optional[datetime] = None,
    ) -> list[ExceptionResponse]:
        sql = f"""
            SELECT
                EE.Cod_Record, EE.Val_Equipment, EE.Val_WorkCenter, EE.Val_Department,
                EED.Dtt_Start, EED.Dtt_End, EED.Hrr_Start, EED.Hrr_End, EED.Cod_Shift,
                ER.Nam_ExceptionReason, EE.Des_Exception, EE.Val_Status, EE.Dtt_Record
            FROM {DB}.mesAdapter.tb_ExceptionEntry EE WITH(NOLOCK)
            JOIN {DB}.mesAdapter.tb_ExceptionEntryDate EED WITH(NOLOCK)
                ON EED.Cod_ExceptionEntry = EE.Cod_Record
            LEFT JOIN {DB}.mesAdapter.tb_ExceptionReason ER WITH(NOLOCK)
                ON ER.Cod_Record = EE.Cod_ExceptionReason
            WHERE EE.Stt_Record = 1
        """
        params: list = []
        if val_equipment:
            sql += " AND EE.Val_Equipment = ?"
            params.append(val_equipment)
        if dtt_start:
            sql += " AND EED.Dtt_End >= ?"
            params.append(dtt_start)
        if dtt_end:
            sql += " AND EED.Dtt_Start <= ?"
            params.append(dtt_end)
        sql += " ORDER BY EED.Dtt_Start DESC"

        try:
            cursor = self.db.cursor()
            cursor.execute(sql, params)
            rows = cursor.fetchall()
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Erro ao listar exceções: {str(e)}")

        return [
            ExceptionResponse(
                cod_record=r[0],   val_equipment=r[1],        val_workcenter=r[2],
                val_department=r[3], dtt_start=r[4],          dtt_end=r[5],
                hrr_start=r[6],    hrr_end=r[7],              cod_shift=r[8],
                nam_exception_reason=r[9], des_comment=r[10],
                usr_grafana=r[11], dtt_record=r[12],
            )
            for r in rows
        ]

    # ── Write ─────────────────────────────────────────────────────────────────

    def create(self, payload: ExceptionCreate) -> dict:
        if not self._validate_reason(payload.cod_exception_reason):
            raise HTTPException(status_code=400, detail="Motivo de excepção inválido")

        conflict = self._check_overlap(payload.val_equipment, payload.dtt_start, payload.dtt_end)
        if conflict:
            raise HTTPException(
                status_code=409,
                detail={
                    "message": f"Já existe uma excepção para {payload.val_equipment} neste período",
                    "existing": {
                        "cod_record": conflict[0],
                        "dtt_start":  conflict[1].isoformat() if conflict[1] else None,
                        "dtt_end":    conflict[2].isoformat() if conflict[2] else None,
                    },
                },
            )

        hrr_start = payload.dtt_start.strftime("%H:%M")
        hrr_end   = payload.dtt_end.strftime("%H:%M")
        cod_shift = self._get_shift(payload.dtt_start)

        try:
            cursor = self.db.cursor()
            cursor.execute(f"""
                INSERT INTO {DB}.mesAdapter.tb_ExceptionEntry
                    (Cod_ExceptionReason, Val_Department, Val_WorkCenter, Val_Equipment,
                     Des_Exception, Stt_Record, Dtt_Record, Usr_Record, Val_Status)
                OUTPUT INSERTED.Cod_Record
                VALUES (?, ?, ?, ?, ?, 1, GETDATE(), 1, 'ACTIVE')
            """, [
                payload.cod_exception_reason,
                payload.val_department,
                payload.val_workcenter,
                payload.val_equipment,
                payload.des_comment,
            ])

            row = cursor.fetchone()
            if not row or row[0] is None:
                raise ValueError("Não foi possível obter o Cod_Record após INSERT")
            cod_entry = int(row[0])

            entry_date_columns = self._get_entry_date_columns()
            columns    = ["Cod_ExceptionEntry", "Dtt_Start", "Dtt_End",
                          "Hrr_Start", "Hrr_End", "Cod_Shift",
                          "Stt_Record", "Dtt_Record", "Usr_Record"]
            values_sql = ["?", "?", "?", "?", "?", "?", "1", "GETDATE()", "1"]
            params     = [cod_entry, payload.dtt_start, payload.dtt_end,
                          hrr_start, hrr_end, cod_shift]

            if "Typ_Request" in entry_date_columns:
                columns.append("Typ_Request"); values_sql.append("?"); params.append("GRAFANA")
            if "Val_Status" in entry_date_columns:
                columns.append("Val_Status");  values_sql.append("?"); params.append("ACTIVE")
            if "Ind_TypeCalc" in entry_date_columns:
                columns.append("Ind_TypeCalc"); values_sql.append("?"); params.append("Sch_In")

            cursor.execute(
                f"INSERT INTO {DB}.mesAdapter.tb_ExceptionEntryDate "
                f"({', '.join(columns)}) VALUES ({', '.join(values_sql)})",
                params,
            )
            self.db.commit()
            return {"cod_record": cod_entry, "status": "created", "message": "Excepção registrada com sucesso"}
        except HTTPException:
            raise
        except Exception as e:
            self.db.rollback()
            raise HTTPException(status_code=500, detail=f"Erro ao gravar excepção: {str(e)}")

    # ── Private helpers ───────────────────────────────────────────────────────

    def _check_overlap(
        self,
        val_equipment: str,
        dtt_start:     datetime,
        dtt_end:       datetime,
        exclude_id:    Optional[int] = None,
    ):
        cursor = self.db.cursor()
        sql = f"""
            SELECT EE.Cod_Record, EED.Dtt_Start, EED.Dtt_End
            FROM {DB}.mesAdapter.tb_ExceptionEntry EE WITH(NOLOCK)
            JOIN {DB}.mesAdapter.tb_ExceptionEntryDate EED WITH(NOLOCK)
                ON EED.Cod_ExceptionEntry = EE.Cod_Record
            WHERE EE.Val_Equipment = ?
              AND EE.Stt_Record = 1
              AND ISNULL(EED.Stt_Record, 1) = 1
              AND EED.Dtt_Start < ?
              AND EED.Dtt_End   > ?
        """
        params = [val_equipment, dtt_end, dtt_start]
        if exclude_id:
            sql += " AND EE.Cod_Record != ?"
            params.append(exclude_id)
        cursor.execute(sql, params)
        return cursor.fetchone()

    def _get_shift(self, dtt_start: datetime) -> str:
        try:
            cursor = self.db.cursor()
            cursor.execute(f"SELECT {DB}.mesAdapter.fn_GetShift(?)", [dtt_start])
            row = cursor.fetchone()
            return str(row[0]) if row and row[0] is not None else "0"
        except Exception:
            return "0"

    def _validate_reason(self, cod_reason: int) -> bool:
        cursor = self.db.cursor()
        cursor.execute(
            f"SELECT Cod_Record FROM {DB}.mesAdapter.tb_ExceptionReason "
            f"WHERE Cod_Record = ? AND Stt_Record = 1",
            [cod_reason],
        )
        return cursor.fetchone() is not None

    def _get_entry_date_columns(self) -> set[str]:
        cursor = self.db.cursor()
        cursor.execute(f"""
            SELECT COLUMN_NAME
            FROM {DB}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'mesAdapter'
              AND TABLE_NAME   = 'tb_ExceptionEntryDate'
        """)
        return {str(r[0]) for r in cursor.fetchall() if r and r[0]}
