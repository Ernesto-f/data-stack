"""Pure authorization and entitlement-to-Keycloak-group mapping rules."""

from __future__ import annotations

from collections.abc import Iterable

from app.auth import Identity


PRODUCT_GROUPS = {
    "crm": ("pulse/home/crm", "crm"),
    "knowledge": (None, "knowledge"),
    "analytics": ("pulse/home/analytics", "analytics"),
    "invoice": (None, "invoice"),
}
MANAGED_TENANT_SUFFIXES = (
    "admin",
    "crm",
    "crm-admin",
    "knowledge",
    "analytics",
    "invoice",
)
MANAGED_GLOBAL_GROUPS = {"/pulse/home", "/pulse/home/crm", "/pulse/home/analytics"}


def is_platform_admin(identity: Identity) -> bool:
    return (
        "/pulse/admin" in identity.groups
        or "/pulse/tenant-control-admin" in identity.groups
    )


def product_group_paths(tenant_key: str, product_key: str, product_role: str) -> set[str]:
    global_group, tenant_suffix = PRODUCT_GROUPS[product_key]
    groups = {f"/pulse/tenants/{tenant_key}/{tenant_suffix}"}
    if global_group:
        groups.add(f"/{global_group}")
    if product_key == "crm" and product_role == "manager":
        groups.add(f"/pulse/tenants/{tenant_key}/crm-admin")
    return groups


def tenant_admin_group(tenant_key: str) -> str:
    return f"/pulse/tenants/{tenant_key}/admin"


def all_groups_for_membership(
    tenant_key: str,
    tenant_role: str,
    product_roles: Iterable[tuple[str, str]],
) -> set[str]:
    groups = {"/pulse/home"}
    if tenant_role in {"owner", "admin"}:
        groups.add(tenant_admin_group(tenant_key))
    for product_key, product_role in product_roles:
        groups.update(product_group_paths(tenant_key, product_key, product_role))
    return groups
