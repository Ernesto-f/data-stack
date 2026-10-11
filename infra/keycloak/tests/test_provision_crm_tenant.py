from __future__ import annotations

import pytest

from infra.keycloak.provision_crm_tenant import (
    tenant_public_host,
    tenant_values,
    validate_tenant_key,
)


def test_tenant_keys_and_hosts_are_canonical():
    assert validate_tenant_key("demo") == "demo"
    assert tenant_public_host("demo") == "crm.izatacore.com"
    assert tenant_public_host("acme-north") == "acme-north.crm.izatacore.com"

    for invalid in ("", "AcmE", "acme.example", "../escape", "a"):
        with pytest.raises(ValueError):
            validate_tenant_key(invalid)


def test_tenant_credentials_and_oidc_client_are_isolated():
    acme = tenant_values("acme", "acme.crm.izatacore.com", 18101, {})
    bravo = tenant_values("bravo", "bravo.crm.izatacore.com", 18102, {})

    assert acme["CRM_OIDC_CLIENT_ID"] == "pulse-crm-acme"
    assert bravo["CRM_OIDC_CLIENT_ID"] == "pulse-crm-bravo"
    assert acme["CRM_DB_NAME"] != bravo["CRM_DB_NAME"]
    assert acme["CRM_DB_USER"] != bravo["CRM_DB_USER"]
    assert acme["CRM_DB_PASSWORD"] != bravo["CRM_DB_PASSWORD"]
    assert acme["CRM_MASTER_PASSWORD"] != bravo["CRM_MASTER_PASSWORD"]


def test_existing_tenant_secret_material_is_stable_on_reprovision():
    existing = {
        "CRM_DB_NAME": "pulse_crm_acme",
        "CRM_DB_USER": "pulse_crm_acme",
        "CRM_DB_PASSWORD": "db-secret",
        "CRM_MASTER_PASSWORD": "master-secret",
        "CRM_INITIAL_ADMIN_PASSWORD": "admin-secret",
        "CRM_OIDC_CLIENT_ID": "pulse-crm-acme",
        "CRM_OIDC_CLIENT_SECRET": "oidc-secret",
    }

    values = tenant_values("acme", "acme.crm.izatacore.com", 18101, existing)

    for key, value in existing.items():
        assert values[key] == value
