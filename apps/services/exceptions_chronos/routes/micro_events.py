from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query

from database import Database, get_db
from models import ClassificationUpdate, ManualStopCreate
from services.micro_event_service import MicroEventService

router = APIRouter()


def _svc(db: Database = Depends(get_db)) -> MicroEventService:
    return MicroEventService(db)


@router.get("")
def list_micro_events(
    val_equipment:  Optional[str]      = Query(None),
    val_workcenter: Optional[str]      = Query(None),
    cod_nature:     Optional[int]      = Query(None),
    cod_shift:      Optional[str]      = Query(None),
    dtt_start:      Optional[datetime] = Query(None),
    dtt_end:        Optional[datetime] = Query(None),
    limit:          int                = Query(200, le=1000),
    svc: MicroEventService = Depends(_svc),
):
    return svc.list(val_equipment, val_workcenter, cod_nature, cod_shift, dtt_start, dtt_end, limit)


@router.get("/equipments")
def list_equipments(svc: MicroEventService = Depends(_svc)):
    return svc.list_equipments()


@router.get("/departments")
def list_departments(svc: MicroEventService = Depends(_svc)):
    return svc.list_departments()


@router.get("/natures")
def list_natures(svc: MicroEventService = Depends(_svc)):
    return svc.list_natures()


@router.get("/reason-codes")
def list_reason_codes(
    val_equipment: Optional[str] = Query(None),
    svc: MicroEventService = Depends(_svc),
):
    return svc.list_reason_codes(val_equipment)


@router.get("/stop-classifications")
def list_stop_classifications(svc: MicroEventService = Depends(_svc)):
    return svc.list_stop_classifications()


@router.post("", status_code=201)
def create_manual_stop(payload: ManualStopCreate, svc: MicroEventService = Depends(_svc)):
    return svc.create_manual(payload)


@router.delete("/{cod_record}")
def delete_manual_stop(cod_record: int, svc: MicroEventService = Depends(_svc)):
    return svc.delete(cod_record)


@router.patch("/classification")
def update_classification(payload: ClassificationUpdate, svc: MicroEventService = Depends(_svc)):
    return svc.update_classification(payload)



