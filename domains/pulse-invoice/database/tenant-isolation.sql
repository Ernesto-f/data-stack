-- Tenant boundary for Pulse Invoice. Apply only after reviewing the auth
-- adapter and mapping verified Keycloak subjects to tenant memberships.
-- The runtime role must be non-owner, non-superuser and must not BYPASSRLS.
-- The API sets both custom settings transaction-locally after token validation.

BEGIN;

-- Covers canonical DDL extensions for warehouses created before these fields.
ALTER TABLE pulse_invoice.invoices
    ADD COLUMN IF NOT EXISTS payment_date DATE;

ALTER TABLE pulse_invoice.invoices
    ADD COLUMN IF NOT EXISTS iss_rate_percent NUMERIC(8, 5);

ALTER TABLE pulse_invoice.extraction_runs
    ADD COLUMN IF NOT EXISTS layout_family TEXT NOT NULL DEFAULT 'unclassified';

ALTER TABLE pulse_invoice.review_events
    ADD COLUMN IF NOT EXISTS event_payload JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE TABLE IF NOT EXISTS pulse_invoice.tenant_registry (
    tenant_key TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    tenant_status TEXT NOT NULL DEFAULT 'active'
        CHECK (tenant_status IN ('active', 'suspended', 'closed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One Keycloak subject maps to one customer tenant in the initial product.
-- Use the stable OIDC `sub`, never an email or a client-supplied tenant field.
CREATE TABLE IF NOT EXISTS pulse_invoice.tenant_memberships (
    keycloak_subject TEXT PRIMARY KEY,
    tenant_key TEXT NOT NULL REFERENCES pulse_invoice.tenant_registry(tenant_key),
    tenant_role TEXT NOT NULL DEFAULT 'viewer'
        CHECK (tenant_role IN ('owner', 'reviewer', 'viewer')),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tenant_memberships_tenant
    ON pulse_invoice.tenant_memberships (tenant_key);

-- Add the tenant discriminator to legacy invoice_lines and backfill only from
-- the already-owned parent invoice. Abort rather than guessing if any row has
-- no matching parent.
ALTER TABLE pulse_invoice.invoice_lines
    ADD COLUMN IF NOT EXISTS tenant_key TEXT;

UPDATE pulse_invoice.invoice_lines AS line
SET tenant_key = invoice.tenant_key
FROM pulse_invoice.invoices AS invoice
WHERE line.invoice_id = invoice.invoice_id
  AND line.tenant_key IS NULL;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pulse_invoice.invoice_lines WHERE tenant_key IS NULL
    ) THEN
        RAISE EXCEPTION 'invoice_lines contains rows without an invoice tenant';
    END IF;
END $$;

ALTER TABLE pulse_invoice.invoice_lines
    ALTER COLUMN tenant_key SET NOT NULL;

-- Composite foreign keys prevent a row from claiming one tenant while pointing
-- at another tenant's run or invoice.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'pulse_invoice.extraction_runs'::regclass
          AND conname = 'uq_extraction_runs_run_tenant'
    ) THEN
        ALTER TABLE pulse_invoice.extraction_runs
            ADD CONSTRAINT uq_extraction_runs_run_tenant UNIQUE (run_id, tenant_key);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'pulse_invoice.invoices'::regclass
          AND conname = 'uq_invoices_id_tenant'
    ) THEN
        ALTER TABLE pulse_invoice.invoices
            ADD CONSTRAINT uq_invoices_id_tenant UNIQUE (invoice_id, tenant_key);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'pulse_invoice.invoices'::regclass
          AND conname = 'fk_invoices_run_tenant'
    ) THEN
        ALTER TABLE pulse_invoice.invoices
            ADD CONSTRAINT fk_invoices_run_tenant
            FOREIGN KEY (extraction_run_id, tenant_key)
            REFERENCES pulse_invoice.extraction_runs(run_id, tenant_key);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'pulse_invoice.invoice_lines'::regclass
          AND conname = 'fk_invoice_lines_invoice_tenant'
    ) THEN
        ALTER TABLE pulse_invoice.invoice_lines
            ADD CONSTRAINT fk_invoice_lines_invoice_tenant
            FOREIGN KEY (invoice_id, tenant_key)
            REFERENCES pulse_invoice.invoices(invoice_id, tenant_key) ON DELETE CASCADE;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'pulse_invoice.review_events'::regclass
          AND conname = 'fk_review_events_invoice_tenant'
    ) THEN
        ALTER TABLE pulse_invoice.review_events
            ADD CONSTRAINT fk_review_events_invoice_tenant
            FOREIGN KEY (invoice_id, tenant_key)
            REFERENCES pulse_invoice.invoices(invoice_id, tenant_key) ON DELETE CASCADE;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pulse_invoice_runtime') THEN
        CREATE ROLE pulse_invoice_runtime NOLOGIN NOSUPERUSER NOBYPASSRLS;
    END IF;
END $$;

ALTER ROLE pulse_invoice_runtime NOLOGIN NOSUPERUSER NOBYPASSRLS;

GRANT USAGE ON SCHEMA pulse_invoice TO pulse_invoice_runtime;
GRANT SELECT ON pulse_invoice.tenant_registry,
                pulse_invoice.tenant_memberships
    TO pulse_invoice_runtime;
GRANT SELECT, INSERT, UPDATE, DELETE
    ON pulse_invoice.extraction_runs,
       pulse_invoice.invoices,
       pulse_invoice.invoice_lines,
       pulse_invoice.review_events
    TO pulse_invoice_runtime;
GRANT SELECT ON pulse_invoice.v_dashboard_monthly,
                pulse_invoice.v_dashboard_review_queue,
                pulse_invoice.v_dashboard_due_dates
    TO pulse_invoice_runtime;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA pulse_invoice
    TO pulse_invoice_runtime;

ALTER TABLE pulse_invoice.tenant_registry ENABLE ROW LEVEL SECURITY;
ALTER TABLE pulse_invoice.tenant_registry FORCE ROW LEVEL SECURITY;
ALTER TABLE pulse_invoice.tenant_memberships ENABLE ROW LEVEL SECURITY;
ALTER TABLE pulse_invoice.tenant_memberships FORCE ROW LEVEL SECURITY;
ALTER TABLE pulse_invoice.extraction_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE pulse_invoice.extraction_runs FORCE ROW LEVEL SECURITY;
ALTER TABLE pulse_invoice.invoices ENABLE ROW LEVEL SECURITY;
ALTER TABLE pulse_invoice.invoices FORCE ROW LEVEL SECURITY;
ALTER TABLE pulse_invoice.invoice_lines ENABLE ROW LEVEL SECURITY;
ALTER TABLE pulse_invoice.invoice_lines FORCE ROW LEVEL SECURITY;
ALTER TABLE pulse_invoice.review_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE pulse_invoice.review_events FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_registry_scope ON pulse_invoice.tenant_registry;
CREATE POLICY tenant_registry_scope ON pulse_invoice.tenant_registry
    USING (tenant_key = NULLIF(current_setting('pulse_invoice.tenant_key', true), ''));

DROP POLICY IF EXISTS tenant_membership_subject ON pulse_invoice.tenant_memberships;
CREATE POLICY tenant_membership_subject ON pulse_invoice.tenant_memberships
    USING (
        is_active
        AND keycloak_subject = NULLIF(current_setting('pulse_invoice.keycloak_subject', true), '')
    );

DROP POLICY IF EXISTS extraction_runs_tenant_scope ON pulse_invoice.extraction_runs;
CREATE POLICY extraction_runs_tenant_scope ON pulse_invoice.extraction_runs
    USING (tenant_key = NULLIF(current_setting('pulse_invoice.tenant_key', true), ''))
    WITH CHECK (tenant_key = NULLIF(current_setting('pulse_invoice.tenant_key', true), ''));

DROP POLICY IF EXISTS invoices_tenant_scope ON pulse_invoice.invoices;
CREATE POLICY invoices_tenant_scope ON pulse_invoice.invoices
    USING (tenant_key = NULLIF(current_setting('pulse_invoice.tenant_key', true), ''))
    WITH CHECK (tenant_key = NULLIF(current_setting('pulse_invoice.tenant_key', true), ''));

DROP POLICY IF EXISTS invoice_lines_tenant_scope ON pulse_invoice.invoice_lines;
CREATE POLICY invoice_lines_tenant_scope ON pulse_invoice.invoice_lines
    USING (tenant_key = NULLIF(current_setting('pulse_invoice.tenant_key', true), ''))
    WITH CHECK (tenant_key = NULLIF(current_setting('pulse_invoice.tenant_key', true), ''));

DROP POLICY IF EXISTS review_events_tenant_scope ON pulse_invoice.review_events;
CREATE POLICY review_events_tenant_scope ON pulse_invoice.review_events
    USING (tenant_key = NULLIF(current_setting('pulse_invoice.tenant_key', true), ''))
    WITH CHECK (tenant_key = NULLIF(current_setting('pulse_invoice.tenant_key', true), ''));

-- Views must run with the caller's privileges so their base-table RLS applies.
CREATE OR REPLACE VIEW pulse_invoice.v_dashboard_monthly
WITH (security_invoker = true) AS
SELECT tenant_key,
       date_trunc('month', issue_date)::date AS invoice_month,
       currency,
       count(*) FILTER (WHERE review_status = 'approved') AS approved_count,
       count(*) FILTER (WHERE review_status = 'pending_review') AS pending_review_count,
       count(*) FILTER (WHERE review_status = 'rejected') AS rejected_count,
       coalesce(sum(amount_total) FILTER (WHERE review_status = 'approved'), 0)::NUMERIC(18, 2) AS approved_total
FROM pulse_invoice.invoices
WHERE issue_date IS NOT NULL
GROUP BY tenant_key, date_trunc('month', issue_date)::date, currency;

CREATE OR REPLACE VIEW pulse_invoice.v_dashboard_review_queue
WITH (security_invoker = true) AS
SELECT tenant_key, review_status, count(*) AS invoice_count
FROM pulse_invoice.invoices
GROUP BY tenant_key, review_status;

CREATE OR REPLACE VIEW pulse_invoice.v_dashboard_due_dates
WITH (security_invoker = true) AS
SELECT tenant_key,
       due_date,
       currency,
       count(*) AS invoice_count,
       coalesce(sum(amount_total), 0)::NUMERIC(18, 2) AS amount_total
FROM pulse_invoice.invoices
WHERE review_status = 'approved'
  AND payment_status IN ('unknown', 'open', 'partial', 'overdue')
  AND due_date IS NOT NULL
GROUP BY tenant_key, due_date, currency;

COMMIT;
