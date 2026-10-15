from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, Query

from database import Database, get_db
from models import ExceptionCreate, ExceptionResponse
from services.exception_service import ExceptionService

router = APIRouter()


def _svc(db: Database = Depends(get_db)) -> ExceptionService:
    return ExceptionService(db)


@router.post("", status_code=201)
def create_exception(payload: ExceptionCreate, svc: ExceptionService = Depends(_svc)):
    return svc.create(payload)


@router.get("", response_model=List[ExceptionResponse])
def list_exceptions(
    val_equipment: Optional[str]      = Query(None),
    dtt_start:     Optional[datetime] = Query(None),
    dtt_end:       Optional[datetime] = Query(None),
    svc: ExceptionService = Depends(_svc),
):
    return svc.list(val_equipment, dtt_start, dtt_end)
