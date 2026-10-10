# Pulse Invoice API — demo MVP

The MVP validates a Keycloak access token, resolves the signed `sub` through `pulse_invoice.tenant_memberships`, applies the tenant and subject as transaction-local PostgreSQL settings, and relies on RLS for invoice queries. Uploads retain the PDF in Paperless, convert it with the existing local Docling service, and invoke the existing deterministic DocETL pipeline; no LLM is called.

## Endpoints

- `GET /health`: process health only; no database or tenant details.
- `GET /v1/me`: returns the caller's resolved tenant key, display name, and tenant role.
- `GET /v1/invoices`: lists invoices visible through RLS. It accepts a page limit, not a tenant selector.
- `GET /v1/invoices/{invoice_id}`: reads one invoice; a cross-tenant ID is indistinguishable from a missing invoice.
- `POST /v1/invoices`: uploads a PDF, archives its original, runs local extraction, and creates a `pending_review` draft.
- `PATCH /v1/invoices/{invoice_id}/review`: saves changed values and an audit event containing only fields whose values actually changed; a no-op save does not create a correction event.
- `POST /v1/invoices/{invoice_id}/approve` / `reject`: records the review decision.
- `GET /v1/invoices/export.csv`: exports only approved records visible to the active tenant.

All `/v1` routes require `Authorization: Bearer <Keycloak access token>`. The verifier checks the Pulse issuer, RS256 signature/JWKS, expiry, subject, and an allowlisted `azp` (default: `pulse-oauth2-proxy`). It does not trust email, URL parameters, or a tenant header.

## Database contract

Each request acquires one pooled connection and begins a transaction. It switches locally to `pulse_invoice_runtime`, sets `pulse_invoice.keycloak_subject`, resolves the active membership, sets `pulse_invoice.tenant_key`, then executes the request query. Both settings are `SET LOCAL`/transaction-local and are cleared when the transaction ends, including on error. The runtime role must not own tables, be a superuser, or have `BYPASSRLS`.

The RLS migration is applied to the active warehouse, and the dedicated `pulse_invoice_api` login is a non-owner/non-superuser member only of `pulse_invoice_runtime`; its DSN and Paperless service token are in the private `/srv/data-stack/domains/pulse-invoice/.env`. The API binds to loopback and the Portal route overwrites client-supplied `Authorization` with the OAuth2 Proxy access token; `/oauth2/auth` remains blocked to browser callers. The active warehouse has one internal demo membership, one approved municipal invoice, and one pending National NFS-e candidate; no customer tenant is provisioned.

## Disposable verification

Run `domains/pulse-invoice/tests/api-isolation.compose.yml`. It creates an isolated PostgreSQL cluster with two synthetic tenants, a non-owner API login, and no route to the active warehouse. Tests cover JWT validation, missing membership, query-string tenant spoofing, and connection-pool context cleanup.
