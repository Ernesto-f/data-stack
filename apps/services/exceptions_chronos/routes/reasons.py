from typing import List

from fastapi import APIRouter, Depends

from database import Database, get_db
from models import ReasonResponse
from services.reason_service import ReasonService

router = APIRouter()


def _svc(db: Database = Depends(get_db)) -> ReasonService:
    return ReasonService(db)


@router.get("", response_model=List[ReasonResponse])
def list_reasons(svc: ReasonService = Depends(_svc)):
    return svc.list()
