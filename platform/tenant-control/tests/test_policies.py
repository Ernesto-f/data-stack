import pytest
from fastapi import HTTPException

from app.auth import Identity
from app.main import tenant_host
from app.policies import all_groups_for_membership, is_platform_admin


def identity(*groups: str) -> Identity:
    return Identity("sub-1", "pulse-oauth2-proxy", tuple(groups))


def test_crm_groups_are_scoped_to_tenant_and_entitlement():
    groups = all_groups_for_membership("acme", "member", [("crm", "user")])
    assert groups == {
        "/pulse/home",
        "/pulse/home/crm",
        "/pulse/tenants/acme/crm",
    }
    assert "/pulse/tenants/other/crm" not in groups


def test_crm_manager_gets_only_that_tenants_manager_role():
    groups = all_groups_for_membership("acme", "member", [("crm", "manager")])
    assert "/pulse/tenants/acme/crm-admin" in groups
    assert "/pulse/tenants/other/crm-admin" not in groups


def test_tenant_admin_role_does_not_imply_crm_subscription():
    groups = all_groups_for_membership("acme", "admin", [])
    assert "/pulse/tenants/acme/admin" in groups
    assert "/pulse/home/crm" not in groups


def test_platform_admin_is_an_explicit_global_group():
    assert is_platform_admin(identity("/pulse/admin"))
    assert not is_platform_admin(identity("/pulse/tenants/acme/admin"))


def test_crm_host_is_derived_from_a_validated_tenant_key():
    assert tenant_host("acme") == "acme.crm.izatacore.com"
    assert tenant_host("demo") == "crm.izatacore.com"
    with pytest.raises(HTTPException):
        tenant_host("acme.example.com")
