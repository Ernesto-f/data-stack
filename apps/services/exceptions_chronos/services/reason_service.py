from config import DB
from database import Database
from models import ReasonResponse


class ReasonService:
    def __init__(self, db: Database):
        self.db = db

    def list(self) -> list[ReasonResponse]:
        cursor = self.db.cursor()
        cursor.execute(f"""
            SELECT Cod_Record, Nam_ExceptionReason, Val_ExternalCode
            FROM {DB}.mesAdapter.tb_ExceptionReason WITH(NOLOCK)
            WHERE Stt_Record = 1
            ORDER BY Nam_ExceptionReason
        """)
        return [
            ReasonResponse(cod_record=r[0], nam_exception_reason=r[1], val_external_code=r[2])
            for r in cursor.fetchall()
        ]
