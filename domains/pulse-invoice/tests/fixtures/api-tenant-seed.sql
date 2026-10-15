-- Disposable-only app login: it can SET ROLE to the restricted runtime role.
CREATE ROLE pulse_invoice_test_app
    LOGIN PASSWORD 'pulse-invoice-test-only'
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS;
GRANT pulse_invoice_runtime TO pulse_invoice_test_app;

INSERT INTO pulse_invoice.tenant_registry (tenant_key, display_name)
VALUES ('tenant-a', 'Synthetic tenant A'), ('tenant-b', 'Synthetic tenant B');

INSERT INTO pulse_invoice.tenant_memberships (keycloak_subject, tenant_key, tenant_role)
VALUES ('api-sub-a', 'tenant-a', 'reviewer'),
       ('api-sub-b', 'tenant-b', 'viewer');

INSERT INTO pulse_invoice.extraction_runs (
    run_id, tenant_key, source_document_id, source_sha256, pipeline_version, run_status
) VALUES
    ('10000000-0000-0000-0000-00000000000a', 'tenant-a', 9101, repeat('a', 64), 'api-test', 'succeeded'),
    ('10000000-0000-0000-0000-00000000000b', 'tenant-b', 9201, repeat('b', 64), 'api-test', 'succeeded'),
    ('10000000-0000-0000-0000-00000000000c', 'tenant-a', 9102, repeat('c', 64), 'api-test', 'needs_review');

INSERT INTO pulse_invoice.invoices (
    invoice_id, tenant_key, source_document_id, source_sha256,
    extraction_run_id, invoice_number, issuer_tax_id, issue_date, currency,
    amount_total, review_status
) VALUES
    (9101, 'tenant-a', 9101, repeat('a', 64), '10000000-0000-0000-0000-00000000000a', 'API-A', '11111111000111', DATE '2026-04-10', 'BRL', 31.00, 'approved'),
    (9201, 'tenant-b', 9201, repeat('b', 64), '10000000-0000-0000-0000-00000000000b', 'API-B', '22222222000122', DATE '2026-04-10', 'BRL', 47.00, 'approved'),
    (9102, 'tenant-a', 9102, repeat('c', 64), '10000000-0000-0000-0000-00000000000c', 'API-A-DRAFT', '11111111000111', DATE '2026-04-11', 'BRL', 32.00, 'pending_review');
