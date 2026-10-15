from __future__ import annotations

from decimal import Decimal
from io import BytesIO

import pytest
from fastapi.testclient import TestClient

from auth import Identity, get_current_identity
from main import app


@pytest.fixture
def client():
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


def authenticate_as(subject: str) -> None:
    app.dependency_overrides[get_current_identity] = lambda: Identity(
        subject=subject,
        authorized_party="pulse-oauth2-proxy",
        groups=("/pulse/home",),
        username=subject,
        email=f"{subject}@example.invalid",
        email_verified=True,
    )


def test_unchanged_review_submission_does_not_create_feedback_event(client):
    authenticate_as("api-sub-a")
    before = client.get("/v1/invoices/9102/history").json()
    response = client.patch(
        "/v1/invoices/9102/review",
        json={"invoice_number": "API-A-DRAFT", "amount_total": "32.00"},
    )
    assert response.status_code == 200
    assert len(client.get("/v1/invoices/9102/history").json()) == len(before)


def test_tenant_is_resolved_from_subject_and_rls_is_transaction_local(client):
    authenticate_as("api-sub-a")
    response = client.get("/v1/me")
    assert response.status_code == 200
    assert response.json() == {
        "tenant_key": "tenant-a",
        "tenant_name": "Synthetic tenant A",
        "tenant_role": "reviewer",
    }

    # A caller-supplied tenant query is ignored; database RLS scopes the result.
    response = client.get("/v1/invoices?tenant_key=tenant-b")
    assert response.status_code == 200
    assert {row["invoice_number"] for row in response.json()} == {"API-A", "API-A-DRAFT"}

    # A reviewer can correct a draft and approve it within the same tenant.
    response = client.patch(
        "/v1/invoices/9102/review",
        json={"invoice_number": "API-A-CORRECTED", "amount_total": "35.50"},
    )
    assert response.status_code == 200
    response = client.post("/v1/invoices/9102/approve")
    assert response.status_code == 200
    assert response.json()["review_status"] == "approved"
    history = client.get("/v1/invoices/9102/history")
    assert history.status_code == 200
    assert [event["to_status"] for event in history.json()] == ["pending_review", "approved"]
    exported = client.get("/v1/invoices/export.csv")
    assert exported.status_code == 200
    assert "API-A-CORRECTED" in exported.text
    assert "API-B" not in exported.text

    # Reusing the pool connection must not retain tenant A's SET LOCAL values.
    authenticate_as("api-sub-b")
    response = client.get("/v1/invoices")
    assert response.status_code == 200
    assert [row["invoice_number"] for row in response.json()] == ["API-B"]

    # Tenant B cannot address tenant A's invoice by its primary key.
    response = client.get("/v1/invoices/9102")
    assert response.status_code == 404
    response = client.patch("/v1/invoices/9201/review", json={"invoice_number": "HACKED"})
    assert response.status_code == 403

    authenticate_as("api-sub-a")
    response = client.get("/v1/invoices")
    assert {row["invoice_number"] for row in response.json()} == {"API-A", "API-A-CORRECTED"}


def test_missing_membership_fails_closed(client):
    authenticate_as("unmapped-subject")
    response = client.get("/v1/me")
    assert response.status_code == 403
    assert response.json()["detail"] == "No active Pulse Invoice tenant membership"


def test_api_requires_a_bearer_token(client):
    app.dependency_overrides.clear()
    response = client.get("/v1/me")
    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"


def test_upload_review_approval_and_duplicate_are_tenant_scoped(client, monkeypatch):
    class ArchiveStub:
        uploads = 0

        async def find_owner_id(self, identity):
            assert identity.subject == "api-sub-a"
            return 42

        async def upload_original(self, content, filename, title, owner_id):
            self.uploads += 1
            assert content.startswith(b"%PDF-")
            assert owner_id == 42
            return 7777

    class ExtractorStub:
        async def extract(self, content, filename, document_id, checksum):
            assert document_id == 7777
            return {
                "layout_family": "municipal_classic_v1",
                "extractor_version": "test",
                "values": {
                    "invoice_number": "DEMO-7777",
                    "issuer_name": "Fornecedor Sintético Ltda",
                    "issuer_tax_id": "11111111000111",
                    "recipient_name": "Cliente Sintético Ltda",
                    "recipient_tax_id": "22222222000122",
                    "issue_date": "2026-05-12",
                    "currency": "BRL",
                    "iss_rate_percent": "2.90",
                    "amount_total": "125.50",
                    "payment_status": "unknown",
                },
                "field_confidence": {"invoice_number": 0.99, "recipient_name": 0.75, "iss_rate_percent": 0.94},
                "source_pages": {
                    "invoice_number": {"page": 1, "text": "Número da Nota: DEMO-7777"},
                    "iss_rate_percent": {"page": 1, "text": "Alíquota ISS: 2,90%"},
                },
                "review_reasons": ["human_review_required_for_all_extractions"],
            }

    archive = ArchiveStub()
    monkeypatch.setattr(app.state, "paperless_client", archive)
    monkeypatch.setattr(app.state, "invoice_extractor", ExtractorStub())
    authenticate_as("api-sub-a")

    response = client.post(
        "/v1/invoices",
        files={"file": ("demo.pdf", BytesIO(b"%PDF-synthetic"), "application/pdf")},
    )
    assert response.status_code == 201
    created = response.json()
    assert created["review_status"] == "pending_review"
    assert created["source_document_id"] == 7777
    assert Decimal(str(created["values"]["iss_rate_percent"])) == Decimal("2.90")

    duplicate = client.post(
        "/v1/invoices",
        files={"file": ("demo.pdf", BytesIO(b"%PDF-synthetic"), "application/pdf")},
    )
    assert duplicate.status_code == 200
    assert duplicate.json()["already_processed"] is True
    assert archive.uploads == 1

    update = client.patch(
        f"/v1/invoices/{created['invoice_id']}/review",
        json={"recipient_name": "Cliente revisado Ltda", "amount_total": "130.00", "iss_rate_percent": "3.00"},
    )
    assert update.status_code == 200
    approved = client.post(f"/v1/invoices/{created['invoice_id']}/approve")
    assert approved.status_code == 200

    detail = client.get(f"/v1/invoices/{created['invoice_id']}")
    assert detail.status_code == 200
    assert detail.json()["layout_family"] == "municipal_classic_v1"
    assert detail.json()["recipient_name"] == "Cliente revisado Ltda"
    assert detail.json()["review_status"] == "approved"
    assert Decimal(str(detail.json()["amount_total"])) == Decimal("130.00")
    assert Decimal(str(detail.json()["iss_rate_percent"])) == Decimal("3.00")
    history = client.get(f"/v1/invoices/{created['invoice_id']}/history")
    assert history.status_code == 200
    assert len(history.json()) == 3
    assert history.json()[1]["changed_fields"] == ["amount_total", "iss_rate_percent", "recipient_name"]
    export = client.get("/v1/invoices/export.csv")
    assert export.status_code == 200
    assert "iss_rate_percent" in export.text.splitlines()[0]
    assert "DEMO-7777" in export.text

    authenticate_as("api-sub-b")
    assert client.get(f"/v1/invoices/{created['invoice_id']}").status_code == 404
