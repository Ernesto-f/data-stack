"""Tenant-scoped upload, review, approval, and export routes."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import time
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import asyncpg
from fastapi import APIRouter, Depends, File, HTTPException, Query, Request, UploadFile, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, StreamingResponse

from archive import ArchiveIntegrationError
from auth import Identity, get_current_identity
from extraction import LocalExtractionError
from schemas import InvoiceRejectRequest, InvoiceReviewUpdate
from tenant import TenantContext, get_tenant_context, tenant_transaction


router = APIRouter(prefix="/v1")
MAX_UPLOAD_BYTES = 25 * 1024 * 1024
PIPELINE_VERSION = "0.3.0"
WRITABLE_FIELDS = {
    "invoice_number",
    "series",
    "verification_code",
    "issuer_name",
    "issuer_tax_id",
    "recipient_name",
    "recipient_tax_id",
    "service_code",
    "service_description",
    "issue_date",
    "competence_date",
    "due_date",
    "payment_date",
    "currency",
    "amount_gross",
    "amount_deductions",
    "tax_iss",
    "iss_rate_percent",
    "tax_pis",
    "tax_cofins",
    "tax_irrf",
    "tax_inss",
    "amount_total",
}


def require_reviewer(context: TenantContext) -> None:
    if context.tenant_role not in {"owner", "reviewer"}:
        raise HTTPException(status_code=403, detail="Tenant reviewer role required")


def to_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise LocalExtractionError("DocETL returned a non-numeric amount") from error
    if not result.is_finite():
        raise LocalExtractionError("DocETL returned a non-finite amount")
    return result


def to_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    try:
        return date.fromisoformat(str(value))
    except ValueError as error:
        raise LocalExtractionError("DocETL returned an invalid date") from error


def extraction_values(result: dict[str, Any]) -> dict[str, Any]:
    values = result.get("values", {})
    return {
        "invoice_number": values.get("invoice_number"),
        "series": values.get("series"),
        "verification_code": values.get("verification_code"),
        "issuer_name": values.get("issuer_name"),
        "issuer_tax_id": values.get("issuer_tax_id"),
        "recipient_name": values.get("recipient_name"),
        "recipient_tax_id": values.get("recipient_tax_id"),
        "service_code": values.get("service_code"),
        "service_description": values.get("service_description"),
        "issue_date": to_date(values.get("issue_date")),
        "competence_date": to_date(values.get("competence_date")),
        "due_date": to_date(values.get("due_date")),
        "payment_date": to_date(values.get("payment_date")),
        "currency": (values.get("currency") or "BRL")[:3].upper(),
        "amount_gross": to_decimal(values.get("amount_gross")),
        "amount_deductions": to_decimal(values.get("amount_deductions")),
        "tax_iss": to_decimal(values.get("tax_iss")),
        "iss_rate_percent": to_decimal(values.get("iss_rate_percent")),
        "tax_pis": to_decimal(values.get("tax_pis")),
        "tax_cofins": to_decimal(values.get("tax_cofins")),
        "tax_irrf": to_decimal(values.get("tax_irrf")),
        "tax_inss": to_decimal(values.get("tax_inss")),
        "amount_total": to_decimal(values.get("amount_total")),
        "payment_status": values.get("payment_status") or "unknown",
    }


def decode_json_fields(row: asyncpg.Record | dict, names: tuple[str, ...]) -> dict:
    data = dict(row)
    for name in names:
        value = data.get(name)
        if isinstance(value, str):
            try:
                data[name] = json.loads(value)
            except json.JSONDecodeError:
                pass
    return data


async def existing_invoice(pool: asyncpg.Pool, identity: Identity, checksum: str) -> dict | None:
    async with tenant_transaction(pool, identity) as context:
        row = await context.connection.fetchrow(
            """
            SELECT invoice_id, source_document_id, invoice_number, review_status
            FROM pulse_invoice.invoices
            WHERE source_sha256 = $1
            ORDER BY created_at DESC, invoice_id DESC
            LIMIT 1
            """,
            checksum,
        )
        return dict(row) if row else None


async def record_failed_extraction(
    pool: asyncpg.Pool,
    identity: Identity,
    *,
    tenant_key: str,
    run_id: uuid.UUID,
    document_id: int,
    checksum: str,
    duration_ms: int,
) -> None:
    async with tenant_transaction(pool, identity) as context:
        if context.tenant_key != tenant_key:
            raise HTTPException(status_code=409, detail="Tenant membership changed during upload")
        await context.connection.execute(
            """
            INSERT INTO pulse_invoice.extraction_runs (
                run_id, tenant_key, source_document_id, source_sha256,
                pipeline_version, run_status, extractor_mode, extractor_name,
                finished_at, duration_ms, error_code
            ) VALUES ($1,$2,$3,$4,$5,'failed','local-deterministic','Docling+DocETL',now(),$6,'local_extraction_failed')
            """,
            run_id,
            tenant_key,
            document_id,
            checksum,
            PIPELINE_VERSION,
            duration_ms,
        )


async def create_invoice_draft(
    pool: asyncpg.Pool,
    identity: Identity,
    *,
    tenant_key: str,
    run_id: uuid.UUID,
    document_id: int,
    checksum: str,
    layout_family: str,
    fields: dict[str, Any],
    confidence: dict[str, Any],
    source_pages: dict[str, Any],
    duration_ms: int,
) -> int:
    async with tenant_transaction(pool, identity) as context:
        if context.tenant_key != tenant_key:
            raise HTTPException(status_code=409, detail="Tenant membership changed during upload")
        if context.tenant_role not in {"owner", "reviewer"}:
            raise HTTPException(status_code=403, detail="Tenant reviewer role required")
        prior = await context.connection.fetchrow(
            """
            SELECT invoice_id FROM pulse_invoice.invoices
            WHERE source_sha256 = $1
            LIMIT 1
            """,
            checksum,
        )
        if prior:
            return int(prior["invoice_id"])

        await context.connection.execute(
            """
            INSERT INTO pulse_invoice.extraction_runs (
                run_id, tenant_key, source_document_id, source_sha256,
                pipeline_version, run_status, extractor_mode, extractor_name,
                layout_family, finished_at, duration_ms
            ) VALUES ($1,$2,$3,$4,$5,'needs_review','local-deterministic','Docling+DocETL',$6,now(),$7)
            """,
            run_id,
            tenant_key,
            document_id,
            checksum,
            PIPELINE_VERSION,
            layout_family,
            duration_ms,
        )

        row = await context.connection.fetchrow(
            """
            INSERT INTO pulse_invoice.invoices (
                tenant_key, source_document_id, source_sha256, extraction_run_id,
                document_type, invoice_number, series, verification_code,
                issuer_name, issuer_tax_id, recipient_name, recipient_tax_id,
                service_code, service_description, issue_date, competence_date,
                due_date, payment_date, currency, amount_gross, amount_deductions,
                tax_iss, iss_rate_percent, tax_pis, tax_cofins, tax_irrf, tax_inss, amount_total,
                review_status, payment_status, field_confidence, source_pages
            ) VALUES (
                $1,$2,$3,$4,'nfse',$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,
                $18,$19,$20,$21,$22,$23,$24,$25,$26,$27,'pending_review',$28,$29::jsonb,$30::jsonb
            ) RETURNING invoice_id
            """,
            tenant_key,
            document_id,
            checksum,
            run_id,
            fields["invoice_number"],
            fields["series"],
            fields["verification_code"],
            fields["issuer_name"],
            fields["issuer_tax_id"],
            fields["recipient_name"],
            fields["recipient_tax_id"],
            fields["service_code"],
            fields["service_description"],
            fields["issue_date"],
            fields["competence_date"],
            fields["due_date"],
            fields["payment_date"],
            fields["currency"],
            fields["amount_gross"],
            fields["amount_deductions"],
            fields["tax_iss"],
            fields["iss_rate_percent"],
            fields["tax_pis"],
            fields["tax_cofins"],
            fields["tax_irrf"],
            fields["tax_inss"],
            fields["amount_total"],
            fields["payment_status"],
            json.dumps(confidence),
            json.dumps(source_pages),
        )
        invoice_id = int(row["invoice_id"])
        changed_fields = [name for name, value in fields.items() if value is not None]
        await context.connection.execute(
            """
            INSERT INTO pulse_invoice.review_events (
                invoice_id, tenant_key, actor_id, from_status, to_status,
                changed_fields, event_payload
            ) VALUES ($1,$2,$3,NULL,'pending_review',$4,$5::jsonb)
            """,
            invoice_id,
            tenant_key,
            identity.subject,
            changed_fields,
            json.dumps({
                "event": "local_extraction_created",
                "run_id": str(run_id),
                "extractor_mode": "local-deterministic",
                "pipeline_version": PIPELINE_VERSION,
                "layout_family": layout_family,
            }),
        )
        return invoice_id


@router.get("/me", tags=["tenant"])
async def current_tenant(context: TenantContext = Depends(get_tenant_context)) -> dict[str, str]:
    return {
        "tenant_key": context.tenant_key,
        "tenant_name": context.display_name,
        "tenant_role": context.tenant_role,
    }


@router.post("/invoices", tags=["invoices"], status_code=status.HTTP_201_CREATED)
async def upload_invoice(
    request: Request,
    file: UploadFile = File(...),
    identity: Identity = Depends(get_current_identity),
) -> JSONResponse:
    if not identity.username or not identity.email_verified:
        raise HTTPException(status_code=403, detail="A verified Archive-linked Pulse identity is required")
    if not (file.filename or "").lower().endswith(".pdf"):
        raise HTTPException(status_code=415, detail="Only PDF NFS-e files are supported in this MVP")

    content = await file.read(MAX_UPLOAD_BYTES + 1)
    await file.close()
    if len(content) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="PDF exceeds the 25 MiB MVP upload limit")
    if not content.startswith(b"%PDF-"):
        raise HTTPException(status_code=415, detail="Uploaded file is not a valid PDF")
    checksum = hashlib.sha256(content).hexdigest()
    pool: asyncpg.Pool = request.app.state.database_pool

    async with tenant_transaction(pool, identity) as context:
        require_reviewer(context)
        existing = await context.connection.fetchrow(
            "SELECT invoice_id, source_document_id, review_status FROM pulse_invoice.invoices WHERE source_sha256=$1 LIMIT 1",
            checksum,
        )
        if existing:
            return JSONResponse(
                status_code=200,
                content={**dict(existing), "already_processed": True},
            )
        tenant_key = context.tenant_key

    paperless: PaperlessClient = request.app.state.paperless_client
    try:
        owner_id = await paperless.find_owner_id(identity)
        document_id = await paperless.upload_original(
            content,
            Path(file.filename).name,
            f"NFS-e · {Path(file.filename).name}",
            owner_id,
        )
    except ArchiveIntegrationError as error:
        raise HTTPException(status_code=502, detail=str(error)) from error

    run_id = uuid.uuid4()
    started = time.monotonic()
    extractor = request.app.state.invoice_extractor
    try:
        result = await extractor.extract(content, Path(file.filename).name, document_id, checksum)
        fields = extraction_values(result)
    except LocalExtractionError as error:
        await record_failed_extraction(
            pool,
            identity,
            tenant_key=tenant_key,
            run_id=run_id,
            document_id=document_id,
            checksum=checksum,
            duration_ms=int((time.monotonic() - started) * 1000),
        )
        raise HTTPException(
            status_code=422,
            detail={
                "message": "Original saved in Archive, but local extraction did not produce a draft.",
                "source_document_id": document_id,
                "extraction_run_id": str(run_id),
                "error": str(error),
            },
        ) from error

    invoice_id = await create_invoice_draft(
        pool,
        identity,
        tenant_key=tenant_key,
        run_id=run_id,
        document_id=document_id,
        checksum=checksum,
        layout_family=str(result.get("layout_family") or "unclassified"),
        fields=fields,
        confidence=result.get("field_confidence", {}),
        source_pages=result.get("source_pages", {}),
        duration_ms=int((time.monotonic() - started) * 1000),
    )
    return JSONResponse(
        status_code=201,
        content={
            "invoice_id": invoice_id,
            "source_document_id": document_id,
            "layout_family": result.get("layout_family", "unclassified"),
            "review_status": "pending_review",
            "values": jsonable_encoder(fields),
            "field_confidence": result.get("field_confidence", {}),
            "source_pages": result.get("source_pages", {}),
            "review_reasons": result.get("review_reasons", []),
        },
    )


@router.get("/invoices", tags=["invoices"])
async def list_invoices(
    limit: int = Query(default=50, ge=1, le=100),
    context: TenantContext = Depends(get_tenant_context),
) -> list[dict]:
    rows = await context.connection.fetch(
        """
        SELECT invoice_id, source_document_id, invoice_number, issuer_name,
               issue_date, due_date, currency, amount_total, review_status,
               payment_status, created_at
        FROM pulse_invoice.invoices
        ORDER BY created_at DESC, invoice_id DESC
        LIMIT $1
        """,
        limit,
    )
    return jsonable_encoder([dict(row) for row in rows])


@router.get("/invoices/export.csv", tags=["invoices"])
async def export_approved_csv(context: TenantContext = Depends(get_tenant_context)) -> StreamingResponse:
    rows = await context.connection.fetch(
        """
        SELECT invoice_number, issuer_name, issuer_tax_id, recipient_name,
               recipient_tax_id, issue_date, due_date, payment_date, currency,
               amount_total, iss_rate_percent, payment_status, source_document_id
        FROM pulse_invoice.invoices
        WHERE review_status='approved'
        ORDER BY issue_date DESC NULLS LAST, invoice_id DESC
        """
    )
    output = io.StringIO(newline="")
    columns = (
        "invoice_number", "issuer_name", "issuer_tax_id", "recipient_name",
        "recipient_tax_id", "issue_date", "due_date", "payment_date",
        "currency", "amount_total", "iss_rate_percent", "payment_status", "source_document_id",
    )
    writer = csv.DictWriter(output, fieldnames=columns)
    writer.writeheader()
    for row in rows:
        writer.writerow({key: row[key] for key in columns})
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=pulse-invoice-approved.csv"},
    )


@router.get("/invoices/{invoice_id}/history", tags=["invoices"])
async def invoice_history(
    invoice_id: int,
    context: TenantContext = Depends(get_tenant_context),
) -> list[dict]:
    rows = await context.connection.fetch(
        """
        SELECT event_id, actor_id, from_status, to_status, changed_fields,
               event_payload, event_at
        FROM pulse_invoice.review_events
        WHERE invoice_id=$1
        ORDER BY event_at, event_id
        """,
        invoice_id,
    )
    return jsonable_encoder([decode_json_fields(row, ("event_payload",)) for row in rows])


@router.get("/invoices/{invoice_id}", tags=["invoices"])
async def get_invoice(
    invoice_id: int,
    context: TenantContext = Depends(get_tenant_context),
) -> dict:
    row = await context.connection.fetchrow(
        """
        SELECT invoice_id, source_document_id, source_sha256, extraction_run_id,
               document_type, invoice_number, series, verification_code,
               issuer_name, issuer_tax_id, recipient_name, recipient_tax_id,
               service_code, service_description, issue_date, competence_date,
                due_date, payment_date, currency, amount_gross, amount_deductions,
                tax_iss, iss_rate_percent, tax_pis, tax_cofins, tax_irrf, tax_inss, amount_total,
                review_status, payment_status, field_confidence, source_pages,
                reviewed_by, reviewed_at, approved_at, created_at, updated_at,
                (SELECT run.layout_family FROM pulse_invoice.extraction_runs AS run
                 WHERE run.run_id=invoice.extraction_run_id
                   AND run.tenant_key=invoice.tenant_key) AS layout_family
         FROM pulse_invoice.invoices AS invoice
         WHERE invoice.invoice_id=$1
        """,
        invoice_id,
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Invoice not found")
    return jsonable_encoder(decode_json_fields(row, ("field_confidence", "source_pages")))


@router.patch("/invoices/{invoice_id}/review", tags=["review"])
async def update_invoice_review(
    invoice_id: int,
    changes: InvoiceReviewUpdate,
    identity: Identity = Depends(get_current_identity),
    context: TenantContext = Depends(get_tenant_context),
) -> dict:
    require_reviewer(context)
    updates = changes.model_dump(exclude_unset=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No invoice fields were supplied")
    if context.tenant_role == "viewer":
        raise HTTPException(status_code=403, detail="Tenant reviewer role required")
    current = await context.connection.fetchrow(
        "SELECT review_status," + ",".join(sorted(WRITABLE_FIELDS))
        + " FROM pulse_invoice.invoices WHERE invoice_id=$1",
        invoice_id,
    )
    if current is None:
        raise HTTPException(status_code=404, detail="Invoice not found")
    if current["review_status"] != "pending_review":
        raise HTTPException(status_code=409, detail="Only pending invoices can be edited")

    fields = sorted(
        field for field, value in updates.items()
        if current[field] != value
    )
    if not fields:
        unchanged = await context.connection.fetchrow(
            "SELECT invoice_id,review_status,field_confidence,source_pages,updated_at "
            "FROM pulse_invoice.invoices WHERE invoice_id=$1",
            invoice_id,
        )
        return jsonable_encoder(dict(unchanged))

    assignments = [f"{field}=${index + 2}" for index, field in enumerate(fields)]
    values = [updates[field] for field in fields]
    await context.connection.execute(
        "UPDATE pulse_invoice.invoices SET " + ",".join(assignments)
        + ",updated_at=now() WHERE invoice_id=$1",
        invoice_id,
        *values,
    )
    event_payload = {
        "before": {field: jsonable_encoder(current[field]) for field in fields},
        "after": jsonable_encoder(updates),
    }
    await context.connection.execute(
        """
        INSERT INTO pulse_invoice.review_events (
            invoice_id,tenant_key,actor_id,from_status,to_status,
            changed_fields,event_payload
        ) VALUES ($1,$2,$3,'pending_review','pending_review',$4,$5::jsonb)
        """,
        invoice_id,
        context.tenant_key,
        identity.subject,
        fields,
        json.dumps(event_payload),
    )
    updated = await context.connection.fetchrow(
        "SELECT invoice_id,review_status,field_confidence,source_pages,updated_at "
        "FROM pulse_invoice.invoices WHERE invoice_id=$1",
        invoice_id,
    )
    return jsonable_encoder(dict(updated))


@router.post("/invoices/{invoice_id}/approve", tags=["review"])
async def approve_invoice(
    invoice_id: int,
    identity: Identity = Depends(get_current_identity),
    context: TenantContext = Depends(get_tenant_context),
) -> dict:
    require_reviewer(context)
    row = await context.connection.fetchrow(
        """
        SELECT invoice_id,review_status,invoice_number,issue_date,issuer_tax_id,amount_total
        FROM pulse_invoice.invoices WHERE invoice_id=$1
        """,
        invoice_id,
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Invoice not found")
    if row["review_status"] != "pending_review":
        raise HTTPException(status_code=409, detail="Only pending invoices can be approved")
    missing = [field for field in ("invoice_number", "issue_date", "issuer_tax_id", "amount_total") if row[field] is None]
    if missing:
        raise HTTPException(status_code=422, detail={"missing_required_fields": missing})
    await context.connection.execute(
        """
        UPDATE pulse_invoice.invoices
        SET review_status='approved',reviewed_by=$2,reviewed_at=now(),
            approved_at=now(),updated_at=now()
        WHERE invoice_id=$1
        """,
        invoice_id,
        identity.subject,
    )
    await context.connection.execute(
        """
        INSERT INTO pulse_invoice.review_events (
            invoice_id,tenant_key,actor_id,from_status,to_status,
            changed_fields,event_payload
        ) VALUES ($1,$2,$3,'pending_review','approved',ARRAY['review_status'], $4::jsonb)
        """,
        invoice_id,
        context.tenant_key,
        identity.subject,
        json.dumps({"approved_at": datetime.now(timezone.utc).isoformat()}),
    )
    return {"invoice_id": invoice_id, "review_status": "approved"}


@router.post("/invoices/{invoice_id}/reject", tags=["review"])
async def reject_invoice(
    invoice_id: int,
    request_body: InvoiceRejectRequest,
    identity: Identity = Depends(get_current_identity),
    context: TenantContext = Depends(get_tenant_context),
) -> dict:
    require_reviewer(context)
    row = await context.connection.fetchrow(
        "SELECT review_status FROM pulse_invoice.invoices WHERE invoice_id=$1",
        invoice_id,
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Invoice not found")
    if row["review_status"] != "pending_review":
        raise HTTPException(status_code=409, detail="Only pending invoices can be rejected")
    await context.connection.execute(
        "UPDATE pulse_invoice.invoices SET review_status='rejected',reviewed_by=$2,reviewed_at=now(),updated_at=now() WHERE invoice_id=$1",
        invoice_id,
        identity.subject,
    )
    await context.connection.execute(
        """
        INSERT INTO pulse_invoice.review_events (
            invoice_id,tenant_key,actor_id,from_status,to_status,
            changed_fields,event_payload
        ) VALUES ($1,$2,$3,'pending_review','rejected',ARRAY['review_status'], $4::jsonb)
        """,
        invoice_id,
        context.tenant_key,
        identity.subject,
        json.dumps({"reason": request_body.reason}),
    )
    return {"invoice_id": invoice_id, "review_status": "rejected"}
