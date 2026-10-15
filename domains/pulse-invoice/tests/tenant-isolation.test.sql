\set ON_ERROR_STOP on

-- Run only against an empty disposable PostgreSQL database:
--   psql -v ON_ERROR_STOP=1 -f tenant-isolation.test.sql
\ir ../database/warehouse-schema.sql
\ir ../database/tenant-isolation.sql
-- Migrations are safe to reapply while iterating deployments.
\ir ../database/tenant-isolation.sql

INSERT INTO pulse_invoice.tenant_registry (tenant_key, display_name)
VALUES ('tenant-a', 'Synthetic tenant A'), ('tenant-b', 'Synthetic tenant B');

INSERT INTO pulse_invoice.tenant_memberships (keycloak_subject, tenant_key, tenant_role)
VALUES ('synthetic-sub-a', 'tenant-a', 'reviewer'),
       ('synthetic-sub-b', 'tenant-b', 'reviewer');

INSERT INTO pulse_invoice.extraction_runs (
    run_id, tenant_key, source_document_id, source_sha256, pipeline_version, run_status
) VALUES
    ('00000000-0000-0000-0000-00000000000a', 'tenant-a', 1001, repeat('a', 64), 'test', 'succeeded'),
    ('00000000-0000-0000-0000-00000000000b', 'tenant-b', 2001, repeat('b', 64), 'test', 'succeeded');

INSERT INTO pulse_invoice.invoices (
    invoice_id, tenant_key, source_document_id, source_sha256,
    extraction_run_id, invoice_number, issue_date, currency, amount_total,
    review_status
) VALUES
    (1001, 'tenant-a', 1001, repeat('a', 64), '00000000-0000-0000-0000-00000000000a', 'SYN-A', DATE '2026-01-10', 'BRL', 10.00, 'approved'),
    (2001, 'tenant-b', 2001, repeat('b', 64), '00000000-0000-0000-0000-00000000000b', 'SYN-B', DATE '2026-01-10', 'BRL', 20.00, 'approved');

INSERT INTO pulse_invoice.invoice_lines (invoice_id, tenant_key, line_number, description, amount_total)
VALUES (1001, 'tenant-a', 1, 'synthetic A', 10.00),
       (2001, 'tenant-b', 1, 'synthetic B', 20.00);

INSERT INTO pulse_invoice.review_events (invoice_id, tenant_key, actor_id, to_status)
VALUES (1001, 'tenant-a', 'synthetic-sub-a', 'approved'),
       (2001, 'tenant-b', 'synthetic-sub-b', 'approved');

SET ROLE pulse_invoice_runtime;
SELECT set_config('pulse_invoice.keycloak_subject', 'synthetic-sub-a', false);

DO $$
DECLARE
    resolved_tenant TEXT;
    row_count BIGINT;
BEGIN
    SELECT count(*) INTO row_count FROM pulse_invoice.tenant_memberships;
    IF row_count <> 1 THEN RAISE EXCEPTION 'tenant A can see % memberships', row_count; END IF;

    SELECT tenant_key INTO resolved_tenant FROM pulse_invoice.tenant_memberships;
    IF resolved_tenant <> 'tenant-a' THEN RAISE EXCEPTION 'unexpected tenant mapping'; END IF;
    PERFORM set_config('pulse_invoice.tenant_key', resolved_tenant, false);

    SELECT count(*) INTO row_count FROM pulse_invoice.tenant_registry;
    IF row_count <> 1 THEN RAISE EXCEPTION 'tenant A can see % tenant rows', row_count; END IF;

    SELECT count(*) INTO row_count FROM pulse_invoice.invoices;
    IF row_count <> 1 THEN RAISE EXCEPTION 'tenant A can see % invoices', row_count; END IF;
    SELECT count(*) INTO row_count FROM pulse_invoice.invoice_lines;
    IF row_count <> 1 THEN RAISE EXCEPTION 'tenant A can see % invoice lines', row_count; END IF;
    SELECT count(*) INTO row_count FROM pulse_invoice.review_events;
    IF row_count <> 1 THEN RAISE EXCEPTION 'tenant A can see % review events', row_count; END IF;
    SELECT count(*) INTO row_count FROM pulse_invoice.v_dashboard_monthly;
    IF row_count <> 1 THEN RAISE EXCEPTION 'tenant A can see % monthly view rows', row_count; END IF;

    SELECT count(*) INTO row_count FROM pulse_invoice.invoices WHERE invoice_id = 2001;
    IF row_count <> 0 THEN RAISE EXCEPTION 'tenant A read tenant B invoice'; END IF;

    BEGIN
        INSERT INTO pulse_invoice.invoices (
            tenant_key, source_document_id, source_sha256, invoice_number
        ) VALUES ('tenant-b', 3001, repeat('c', 64), 'CROSS-TENANT');
        RAISE EXCEPTION 'cross-tenant invoice INSERT unexpectedly succeeded';
    EXCEPTION WHEN insufficient_privilege THEN
        NULL;
    END;

    BEGIN
        INSERT INTO pulse_invoice.invoice_lines (invoice_id, tenant_key, line_number)
        VALUES (2001, 'tenant-a', 99);
        RAISE EXCEPTION 'cross-tenant line link unexpectedly succeeded';
    EXCEPTION WHEN foreign_key_violation THEN
        NULL;
    END;
END $$;

-- Missing tenant context fails closed.
SELECT set_config('pulse_invoice.tenant_key', '', false);
DO $$
DECLARE row_count BIGINT;
BEGIN
    SELECT count(*) INTO row_count FROM pulse_invoice.invoices;
    IF row_count <> 0 THEN RAISE EXCEPTION 'unscoped request saw % invoices', row_count; END IF;
END $$;

-- A separately authenticated subject resolves only to tenant B.
SELECT set_config('pulse_invoice.keycloak_subject', 'synthetic-sub-b', false);
DO $$
DECLARE
    resolved_tenant TEXT;
    row_count BIGINT;
BEGIN
    SELECT tenant_key INTO resolved_tenant FROM pulse_invoice.tenant_memberships;
    IF resolved_tenant <> 'tenant-b' THEN RAISE EXCEPTION 'tenant B subject mapped incorrectly'; END IF;
    PERFORM set_config('pulse_invoice.tenant_key', resolved_tenant, false);
    SELECT count(*) INTO row_count FROM pulse_invoice.invoices;
    IF row_count <> 1 THEN RAISE EXCEPTION 'tenant B can see % invoices', row_count; END IF;
END $$;

RESET ROLE;
\echo 'PASS: tenant membership mapping, RLS reads, views, and cross-tenant writes'
