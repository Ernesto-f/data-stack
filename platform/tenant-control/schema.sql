CREATE TABLE IF NOT EXISTS product_catalog (
    product_key text PRIMARY KEY,
    display_name text NOT NULL,
    integration_status text NOT NULL CHECK (integration_status IN ('ready', 'planned'))
);

INSERT INTO product_catalog (product_key, display_name, integration_status) VALUES
    ('crm', 'SALUS CRM', 'ready'),
    ('knowledge', 'Pulse Knowledge Base', 'planned'),
    ('analytics', 'Pulse Analytics', 'planned'),
    ('invoice', 'Pulse Invoice', 'planned')
ON CONFLICT (product_key) DO UPDATE SET
    display_name = EXCLUDED.display_name,
    integration_status = EXCLUDED.integration_status;

CREATE TABLE IF NOT EXISTS tenants (
    tenant_key text PRIMARY KEY,
    display_name text NOT NULL,
    crm_host text,
    tenant_status text NOT NULL DEFAULT 'provisioning'
        CHECK (tenant_status IN ('provisioning', 'active', 'suspended')),
    created_by text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE tenants ALTER COLUMN crm_host DROP NOT NULL;

CREATE TABLE IF NOT EXISTS tenant_products (
    tenant_key text NOT NULL REFERENCES tenants (tenant_key) ON DELETE CASCADE,
    product_key text NOT NULL REFERENCES product_catalog (product_key),
    enabled boolean NOT NULL DEFAULT false,
    seat_limit integer CHECK (seat_limit IS NULL OR seat_limit >= 1),
    access_mode text NOT NULL DEFAULT 'assigned'
        CHECK (access_mode IN ('all_members', 'assigned')),
    PRIMARY KEY (tenant_key, product_key)
);

ALTER TABLE tenant_products
    DROP CONSTRAINT IF EXISTS tenant_products_seat_limit_check;
ALTER TABLE tenant_products
    ADD CONSTRAINT tenant_products_seat_limit_check
    CHECK (seat_limit IS NULL OR seat_limit >= 1);

CREATE TABLE IF NOT EXISTS tenant_memberships (
    tenant_key text NOT NULL REFERENCES tenants (tenant_key) ON DELETE CASCADE,
    keycloak_subject text NOT NULL,
    email text NOT NULL,
    display_name text NOT NULL,
    tenant_role text NOT NULL DEFAULT 'member'
        CHECK (tenant_role IN ('owner', 'admin', 'member')),
    membership_status text NOT NULL DEFAULT 'invited'
        CHECK (membership_status IN ('invited', 'active', 'suspended')),
    invited_by text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_key, keycloak_subject),
    UNIQUE (tenant_key, email)
);

CREATE TABLE IF NOT EXISTS membership_products (
    tenant_key text NOT NULL,
    keycloak_subject text NOT NULL,
    product_key text NOT NULL,
    product_role text NOT NULL DEFAULT 'user'
        CHECK (product_role IN ('user', 'manager')),
    assigned_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_key, keycloak_subject, product_key),
    FOREIGN KEY (tenant_key, keycloak_subject)
        REFERENCES tenant_memberships (tenant_key, keycloak_subject) ON DELETE CASCADE,
    FOREIGN KEY (tenant_key, product_key)
        REFERENCES tenant_products (tenant_key, product_key) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS tenant_memberships_subject_idx
    ON tenant_memberships (keycloak_subject, membership_status);
