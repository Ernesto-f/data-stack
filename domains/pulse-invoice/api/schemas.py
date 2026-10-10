"""Request models for human review of extracted invoice fields."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field


class InvoiceReviewUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    invoice_number: str | None = Field(default=None, max_length=128)
    series: str | None = Field(default=None, max_length=64)
    verification_code: str | None = Field(default=None, max_length=128)
    issuer_name: str | None = Field(default=None, max_length=512)
    issuer_tax_id: str | None = Field(default=None, max_length=32)
    recipient_name: str | None = Field(default=None, max_length=512)
    recipient_tax_id: str | None = Field(default=None, max_length=32)
    service_code: str | None = Field(default=None, max_length=64)
    service_description: str | None = Field(default=None, max_length=4000)
    issue_date: date | None = None
    competence_date: date | None = None
    due_date: date | None = None
    payment_date: date | None = None
    currency: str | None = Field(default=None, min_length=3, max_length=3)
    amount_gross: Decimal | None = None
    amount_deductions: Decimal | None = None
    tax_iss: Decimal | None = None
    iss_rate_percent: Decimal | None = Field(
        default=None,
        ge=Decimal("0"),
        le=Decimal("100"),
        max_digits=8,
        decimal_places=5,
    )
    tax_pis: Decimal | None = None
    tax_cofins: Decimal | None = None
    tax_irrf: Decimal | None = None
    tax_inss: Decimal | None = None
    amount_total: Decimal | None = None


class InvoiceRejectRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason: str = Field(min_length=1, max_length=2000)
