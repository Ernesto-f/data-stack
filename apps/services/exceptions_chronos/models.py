from pydantic import BaseModel, model_validator
from datetime import datetime
from typing import Optional, List


class ExceptionCreate(BaseModel):
    val_equipment:        str
    val_workcenter:       Optional[str] = None
    val_department:       Optional[str] = None
    dtt_start:            datetime
    dtt_end:              datetime
    cod_exception_reason: int
    des_comment:          Optional[str] = None
    usr_grafana:          Optional[str] = "grafana"

    @model_validator(mode="after")
    def validate_dates(self):
        if self.dtt_end <= self.dtt_start:
            raise ValueError("dtt_end deve ser maior que dtt_start")
        if (self.dtt_end - self.dtt_start).total_seconds() > 86400:
            raise ValueError("Período máximo de 24 horas")
        return self


class ExceptionResponse(BaseModel):
    cod_record:           int
    val_equipment:        str
    val_workcenter:       Optional[str]
    val_department:       Optional[str]
    dtt_start:            datetime
    dtt_end:              datetime
    hrr_start:            Optional[str]
    hrr_end:              Optional[str]
    cod_shift:            Optional[str]
    nam_exception_reason: Optional[str]
    des_comment:          Optional[str]
    usr_grafana:          Optional[str]
    dtt_record:           datetime


class ExceptionConflict(BaseModel):
    cod_record: int
    dtt_start:  datetime
    dtt_end:    datetime


class ReasonResponse(BaseModel):
    cod_record:           int
    nam_exception_reason: str
    val_external_code:    Optional[str]


class ClassificationUpdate(BaseModel):
    cod_records:             List[int]
    cod_stop_classification: int


class ManualStopCreate(BaseModel):
    val_equipment:   str
    cod_nature:      int
    dtt_start:       datetime
    dtt_end:         datetime
    cod_reason_code: Optional[str] = None
    des_comment:     Optional[str] = None
