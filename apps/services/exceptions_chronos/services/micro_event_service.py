from datetime import datetime
from typing import Optional

from fastapi import HTTPException

from config import DB, DEPARTMENT, FACILITY
from database import Database
from models import ClassificationUpdate, ManualStopCreate


class MicroEventService:
    def __init__(self, db: Database):
        self.db = db

    # ── Lookups ───────────────────────────────────────────────────────────────

    def list_equipments(self) -> list[str]:
        cursor = self.db.cursor()
        cursor.execute(f"""
            SELECT Val_Equipment
            FROM {DB}.mesAdapter.tb_MicroEvents WITH(NOLOCK)
            WHERE Val_Equipment IS NOT NULL
              AND Val_Equipment NOT IN ('NAO_EQUIPAMENTO', 'DESCONHECIDO')
              AND Val_Department = '{DEPARTMENT}'
            GROUP BY Val_Equipment
            ORDER BY Val_Equipment
        """)
        return [r[0] for r in cursor.fetchall()]

    def list_departments(self) -> list[str]:
        cursor = self.db.cursor()
        cursor.execute(f"""
            SELECT Val_Department
            FROM {DB}.mesAdapter.tb_MicroEvents WITH(NOLOCK)
            WHERE Val_Department IS NOT NULL
            GROUP BY Val_Department
            ORDER BY Val_Department
        """)
        return [r[0] for r in cursor.fetchall()]

    def list_natures(self) -> list[dict]:
        cursor = self.db.cursor()
        cursor.execute(f"""
            SELECT Cod_Record, Nam_Nature
            FROM {DB}.mesAdapter.tb_NatureMicroEvents WITH(NOLOCK)
            WHERE Nam_Nature IS NOT NULL
            ORDER BY Nam_Nature
        """)
        return [{"cod_record": r[0], "nam_nature": r[1]} for r in cursor.fetchall()]

    def list_reason_codes(self, val_equipment: Optional[str] = None) -> list[dict]:
        cursor = self.db.cursor()
        base_sql = f"""
            SELECT DISTINCT Val_ReasonCode, Des_ReasonCode
            FROM {DB}.mesAdapter.tb_ReasonCodeMicroEvents WITH(NOLOCK)
            WHERE Val_ReasonCode IS NOT NULL
              AND Des_ReasonCode IS NOT NULL
        """
        if val_equipment:
            cursor.execute(base_sql + " AND Val_Equipment = ? ORDER BY Des_ReasonCode", [val_equipment])
        else:
            cursor.execute(base_sql + " ORDER BY Des_ReasonCode")
        return [{"val_reason_code": r[0], "des_reason_code": r[1]} for r in cursor.fetchall()]

    def list_stop_classifications(self) -> list[dict]:
        cursor = self.db.cursor()
        cursor.execute(f"""
            SELECT Cod_Record, Nam_StopClassification
            FROM {DB}.mesAdapter.tb_StopClassification WITH(NOLOCK)
            ORDER BY Nam_StopClassification
        """)
        return [{"cod_record": r[0], "nam_classification": r[1]} for r in cursor.fetchall()]

    # ── Query ─────────────────────────────────────────────────────────────────

    def list(
        self,
        val_equipment:  Optional[str]      = None,
        val_workcenter: Optional[str]      = None,
        cod_nature:     Optional[int]      = None,
        cod_shift:      Optional[str]      = None,
        dtt_start:      Optional[datetime] = None,
        dtt_end:        Optional[datetime] = None,
        limit:          int                = 200,
    ) -> list[dict]:
        sql = f"""
            SELECT TOP (?)
                ME.Cod_Record,
                DATEADD(HOUR, 3, ME.Dtt_Start)  AS Dtt_Start,
                DATEADD(HOUR, 3, ME.Dtt_End)    AS Dtt_End,
                CASE
                    WHEN LEFT(
                        {DB}.mesAdapter.fnCalcDiffTime(ME.Dtt_Start, ME.Dtt_End),
                        CHARINDEX('d', {DB}.mesAdapter.fnCalcDiffTime(ME.Dtt_Start, ME.Dtt_End)) - 1
                    ) = '0'
                        THEN RIGHT({DB}.mesAdapter.fnCalcDiffTime(ME.Dtt_Start, ME.Dtt_End), 8)
                    ELSE {DB}.mesAdapter.fnCalcDiffTime(ME.Dtt_Start, ME.Dtt_End)
                END                             AS Duracao,
                ME.Val_Equipment,
                RCME.Des_ReasonCode,
                ME.Cod_ReasonCode,
                NME.Nam_Nature,
                SC.Nam_StopClassification       AS Nam_Classification,
                ME.Val_Department,
                ME.Val_Facility,
                ME.Val_WorkCenter,
                ET.Nam_Type                     AS Nam_EquipmentType,
                ME.Cod_Shift,
                ME.Ind_Manual,
                ME.Des_Comment,
                EX.Nam_ExceptionReason,
                D.Nam_Domain                    AS Nam_Type,
                PTA.Nam_PTA,
                US.Des_Username,
                DATEADD(HOUR, 3, ME.Dtt_Update) AS Dtt_Update
            FROM {DB}.mesAdapter.tb_MicroEvents ME WITH(NOLOCK)
            LEFT JOIN db_mcIOTGatewayService.dbo.tb_Equipment EQ WITH(NOLOCK)
                ON ME.Val_Equipment = EQ.Nam_Equipment
            LEFT JOIN db_mcIOTGatewayService.dbo.tb_EquipmentType ET WITH(NOLOCK)
                ON EQ.Cod_EquipmentType_FK = ET.Cod_Record
            LEFT JOIN {DB}.mesAdapter.tb_NatureMicroEvents NME WITH(NOLOCK)
                ON ME.Cod_Nature = NME.Cod_Record
            LEFT JOIN {DB}.mesAdapter.tb_ReasonCodeMicroEvents RCME WITH(NOLOCK)
                ON CAST(ME.Cod_ReasonCode AS VARCHAR) = RCME.Val_ReasonCode
               AND ME.Val_Equipment = RCME.Val_Equipment
            LEFT JOIN {DB}.mesAdapter.tb_StopClassification SC WITH(NOLOCK)
                ON ME.Cod_StopClassification = SC.Cod_Record
            LEFT JOIN {DB}.core.tb_User US WITH(NOLOCK)
                ON ME.Usr_Update = US.Cod_Record
            LEFT JOIN {DB}.mesAdapter.tb_PTA PTA WITH(NOLOCK)
                ON ME.Cod_PTA = PTA.Cod_Record
            LEFT JOIN {DB}.Core.tb_Domain D WITH(NOLOCK)
                ON ME.Ind_TypeCalc = D.Val_Domain
               AND D.Nam_DomainType = 'TypeExceptionReason'
            OUTER APPLY (
                SELECT TOP (1) ER.Nam_ExceptionReason
                FROM {DB}.mesAdapter.tb_ExceptionEntry EE WITH(NOLOCK)
                JOIN {DB}.mesAdapter.tb_ExceptionEntryDate EED WITH(NOLOCK)
                    ON EED.Cod_ExceptionEntry = EE.Cod_Record
                LEFT JOIN {DB}.mesAdapter.tb_ExceptionReason ER WITH(NOLOCK)
                    ON ER.Cod_Record = EE.Cod_ExceptionReason
                WHERE EE.Stt_Record = 1
                  AND ISNULL(EED.Stt_Record, 1) = 1
                  AND EE.Val_Equipment = ME.Val_Equipment
                  AND EED.Dtt_Start < DATEADD(HOUR, 3, ME.Dtt_End)
                  AND EED.Dtt_End   > DATEADD(HOUR, 3, ME.Dtt_Start)
                ORDER BY EE.Cod_Record DESC
            ) EX
            WHERE ME.Dtt_End IS NOT NULL
              AND ME.Val_Department = '{DEPARTMENT}'
              AND ME.Val_Facility   = '{FACILITY}'
        """
        params: list = [limit]

        if val_equipment:
            sql += " AND ME.Val_Equipment = ?"
            params.append(val_equipment)
        if val_workcenter:
            sql += " AND ME.Val_WorkCenter = ?"
            params.append(val_workcenter)
        if cod_nature is not None:
            sql += " AND ME.Cod_Nature = ?"
            params.append(cod_nature)
        if cod_shift:
            sql += " AND ME.Cod_Shift = ?"
            params.append(cod_shift)
        if dtt_start:
            sql += " AND DATEADD(HOUR, 3, ME.Dtt_Start) >= ?"
            params.append(dtt_start)
        if dtt_end:
            sql += " AND DATEADD(HOUR, 3, ME.Dtt_Start) <= ?"
            params.append(dtt_end)

        sql += " ORDER BY ME.Dtt_Start DESC"

        cursor = self.db.cursor()
        cursor.execute(sql, params)
        cols = [d[0] for d in cursor.description]
        rows = cursor.fetchall()

        return [
            dict(zip(cols, [v.isoformat() if isinstance(v, datetime) else v for v in row]))
            for row in rows
        ]

    # ── Write ─────────────────────────────────────────────────────────────────

    def create_manual(self, payload: ManualStopCreate) -> dict:
        if payload.dtt_end <= payload.dtt_start:
            raise HTTPException(status_code=400, detail="Data fim deve ser posterior à data início")

        cod_shift     = self._get_shift(payload.dtt_start)
        val_workcenter = self._get_latest_workcenter(payload.val_equipment)

        try:
            cursor = self.db.cursor()
            cursor.execute(f"""
                INSERT INTO {DB}.mesAdapter.tb_MicroEvents
                    (Val_Equipment, Cod_ReasonCode, Cod_Nature, Dtt_Start, Dtt_End,
                     Val_Department, Val_Facility, Val_WorkCenter, Cod_Shift, Ind_Manual,
                     Des_Comment, Dtt_Update)
                OUTPUT INSERTED.Cod_Record
                VALUES (?, ?, ?, ?, ?, '{DEPARTMENT}', '{FACILITY}', ?, ?, 1, ?, GETDATE())
            """, [
                payload.val_equipment,
                payload.cod_reason_code,
                payload.cod_nature,
                payload.dtt_start,
                payload.dtt_end,
                val_workcenter,
                cod_shift,
                payload.des_comment,
            ])
            row = cursor.fetchone()
            if not row or row[0] is None:
                raise ValueError("Não foi possível obter o Cod_Record após INSERT")
            cod_record = int(row[0])
            self.db.commit()
            return {"cod_record": cod_record, "status": "created", "message": "Parada manual registrada com sucesso"}
        except HTTPException:
            raise
        except Exception as e:
            self.db.rollback()
            raise HTTPException(status_code=500, detail=f"Erro ao registrar parada manual: {str(e)}")

    def delete(self, cod_record: int) -> dict:
        cursor = self.db.cursor()
        cursor.execute(
            f"SELECT Ind_Manual FROM {DB}.mesAdapter.tb_MicroEvents WHERE Cod_Record = ?",
            [cod_record],
        )
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Registro não encontrado")
        if not row[0]:
            raise HTTPException(status_code=403, detail="Apenas registros manuais podem ser removidos")
        try:
            cursor.execute(
                f"DELETE FROM {DB}.mesAdapter.tb_MicroEvents WHERE Cod_Record = ?",
                [cod_record],
            )
            self.db.commit()
            return {"deleted": cod_record, "status": "ok"}
        except Exception as e:
            self.db.rollback()
            raise HTTPException(status_code=500, detail=f"Erro ao remover: {str(e)}")

    def update_classification(self, payload: ClassificationUpdate) -> dict:
        if not payload.cod_records:
            raise HTTPException(status_code=400, detail="Nenhum registro informado")

        cursor = self.db.cursor()
        cursor.execute(
            f"SELECT Cod_Record FROM {DB}.mesAdapter.tb_StopClassification WHERE Cod_Record = ?",
            [payload.cod_stop_classification],
        )
        if not cursor.fetchone():
            raise HTTPException(status_code=400, detail="Classificação inválida")

        placeholders = ",".join(["?" for _ in payload.cod_records])
        params = [payload.cod_stop_classification] + payload.cod_records
        try:
            cursor.execute(
                f"UPDATE {DB}.mesAdapter.tb_MicroEvents "
                f"SET Cod_StopClassification = ? WHERE Cod_Record IN ({placeholders})",
                params,
            )
            self.db.commit()
            return {"updated": cursor.rowcount, "status": "ok"}
        except Exception as e:
            self.db.rollback()
            raise HTTPException(status_code=500, detail=f"Erro ao atualizar: {str(e)}")

    # ── Private helpers ───────────────────────────────────────────────────────

    def _get_shift(self, dtt_start: datetime) -> str:
        try:
            cursor = self.db.cursor()
            cursor.execute(f"SELECT {DB}.mesAdapter.fn_GetShift(?)", [dtt_start])
            row = cursor.fetchone()
            return str(row[0]) if row and row[0] is not None else "0"
        except Exception:
            return "0"

    def _get_latest_workcenter(self, val_equipment: str) -> Optional[str]:
        try:
            cursor = self.db.cursor()
            cursor.execute(f"""
                SELECT TOP 1 Val_WorkCenter
                FROM {DB}.mesAdapter.tb_MicroEvents WITH(NOLOCK)
                WHERE Val_Equipment = ?
                  AND Val_WorkCenter IS NOT NULL
                ORDER BY Dtt_Start DESC
            """, [val_equipment])
            row = cursor.fetchone()
            return row[0] if row else None
        except Exception:
            return None
