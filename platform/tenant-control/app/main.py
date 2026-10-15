"""Pulse Portal tenant administration and product entitlement API."""

from __future__ import annotations

import json
import logging
import os
import re
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal

import asyncpg
from fastapi import Depends, FastAPI, HTTPException, Request, Response
from pydantic import BaseModel, Field

from app.auth import Identity, build_verifier, current_identity
from app.keycloak import KeycloakAdmin, KeycloakError, KeycloakGroupMissingError
from app.policies import (
    MANAGED_GLOBAL_GROUPS,
    MANAGED_TENANT_SUFFIXES,
    is_platform_admin,
    product_group_paths,
)


TENANT_RE = re.compile(r"^[a-z0-9][a-z0-9-]{1,29}[a-z0-9]$")
READY_PRODUCTS = {"crm"}
logger = logging.getLogger(__name__)
UNSET = object()


def tenant_host(tenant_key: str) -> str:
    if not TENANT_RE.fullmatch(tenant_key):
        raise HTTPException(422, "Invalid tenant key")
    return "democrm.izatacore.com" if tenant_key == "demo" else f"{tenant_key}.crm.izatacore.com"


class ProductEntitlement(BaseModel):
    product_key: str
    enabled: bool = True
    seat_limit: int | None = Field(default=None, ge=1)
    access_mode: Literal["all_members", "assigned"] = "assigned"


class TenantCreate(BaseModel):
    tenant_key: str
    display_name: str = Field(min_length=2, max_length=160)
    crm_host: str | None = None
    products: list[ProductEntitlement]


class TenantUpdate(BaseModel):
    display_name: str | None = Field(default=None, min_length=2, max_length=160)
    tenant_status: Literal["active", "suspended", "provisioning"] | None = None
    crm_host: str | None = None


class TenantProductsUpdate(BaseModel):
    products: list[ProductEntitlement]


class MemberCreate(BaseModel):
    email: str
    display_name: str = Field(min_length=2, max_length=160)
    tenant_role: Literal["owner", "admin", "member"] = "member"
    crm_role: Literal["user", "manager"] = "user"
    products: list[str] = Field(default_factory=list)


class MemberProductsUpdate(BaseModel):
    products: list[str]
    crm_role: Literal["user", "manager"] = "user"
    tenant_role: Literal["owner", "admin", "member"] | None = None


class MemberReactivate(BaseModel):
    products: list[str] = Field(default_factory=list)
    crm_role: Literal["user", "manager"] = "user"


async def subject_group_policy(request: Request, subject: str) -> tuple[set[str], set[str]]:
    pool = request.app.state.pool
    memberships = await pool.fetch(
        """SELECT m.tenant_key,m.tenant_role,m.membership_status,t.tenant_status
           FROM tenant_memberships m JOIN tenants t USING (tenant_key)
           WHERE m.keycloak_subject=$1""",
        subject,
    )
    desired: set[str] = {"/pulse/home"} if any(
        row["membership_status"] != "suspended" and row["tenant_status"] == "active"
        for row in memberships
    ) else set()
    managed = set(MANAGED_GLOBAL_GROUPS)
    for membership in memberships:
        tenant_key = membership["tenant_key"]
        managed.update(f"/pulse/tenants/{tenant_key}/{suffix}" for suffix in MANAGED_TENANT_SUFFIXES)
        if membership["membership_status"] == "suspended" or membership["tenant_status"] != "active":
            continue
        if membership["tenant_role"] in {"owner", "admin"}:
            desired.add(f"/pulse/tenants/{tenant_key}/admin")
        products = await pool.fetch(
            """SELECT mp.product_key, mp.product_role FROM membership_products mp
               JOIN tenant_products tp USING (tenant_key, product_key)
               JOIN product_catalog pc USING (product_key)
               WHERE mp.tenant_key=$1 AND mp.keycloak_subject=$2
                 AND tp.enabled AND pc.integration_status='ready'""",
            tenant_key,
            subject,
        )
        for product in products:
            if product["product_key"] not in READY_PRODUCTS:
                continue
            desired.update(product_group_paths(tenant_key, product["product_key"], product["product_role"]))
    return desired, managed


async def sync_subject_groups(request: Request, subject: str) -> list[str]:
    desired, managed = await subject_group_policy(request, subject)
    return await request.app.state.keycloak.set_groups(subject, desired, managed)


def member_group_drift(
    subject: str, desired: set[str], existing: set[str], managed: set[str]
) -> dict[str, list[str] | str] | None:
    expected_managed = desired & managed
    actual_managed = existing & managed
    missing = sorted(expected_managed - actual_managed)
    extra = sorted(actual_managed - expected_managed)
    if not missing and not extra:
        return None
    return {"subject": subject, "missing": missing, "extra": extra}


async def entitlement_rows(request: Request, tenant_key: str) -> list[dict]:
    rows = await request.app.state.pool.fetch(
        """SELECT tp.product_key,tp.enabled,tp.seat_limit,tp.access_mode,
                  pc.display_name,pc.integration_status,
                  (SELECT count(*) FROM membership_products mp
                   JOIN tenant_memberships tm USING (tenant_key,keycloak_subject)
                   WHERE mp.tenant_key=tp.tenant_key AND mp.product_key=tp.product_key
                     AND tm.membership_status!='suspended') AS seats_used
           FROM tenant_products tp JOIN product_catalog pc USING (product_key)
           WHERE tp.tenant_key=$1 ORDER BY tp.product_key""",
        tenant_key,
    )
    return [dict(row) for row in rows]


async def sync_tenant_members(request: Request, tenant_key: str) -> int:
    subjects = await request.app.state.pool.fetch(
        "SELECT keycloak_subject FROM tenant_memberships WHERE tenant_key=$1", tenant_key
    )
    synced = 0
    for member in subjects:
        try:
            await sync_subject_groups(request, member["keycloak_subject"])
            synced += 1
        except KeycloakError as error:
            raise HTTPException(
                503,
                f"Tenant data was saved, but Keycloak synchronization stopped after {synced} memberships; retry the operation.",
            ) from error
    return synced


async def tenant_activation_preflight(
    request: Request, tenant: dict, crm_host_override: str | None | object = UNSET
) -> dict:
    """Check the prerequisites that the control plane can verify before activation."""
    tenant_key = tenant["tenant_key"]
    expected_host = tenant_host(tenant_key)
    crm = await request.app.state.pool.fetchrow(
        """SELECT t.crm_host,
                  EXISTS (SELECT 1 FROM tenant_products tp
                          WHERE tp.tenant_key=t.tenant_key
                            AND tp.product_key='crm' AND tp.enabled) AS crm_enabled
           FROM tenants t WHERE t.tenant_key=$1""",
        tenant_key,
    )
    checks: list[dict[str, str]] = []
    crm_enabled = bool(crm and crm["crm_enabled"])
    if crm_enabled:
        effective_crm_host = crm["crm_host"] if crm_host_override is UNSET else crm_host_override
        host_ready = effective_crm_host == expected_host
        checks.append({
            "key": "crm_host",
            "status": "ready" if host_ready else "blocked",
            "detail": expected_host if host_ready else f"Configure o host CRM como {expected_host}.",
        })
    else:
        checks.append({"key": "crm_host", "status": "not_required", "detail": "CRM não contratado."})

    memberships = await request.app.state.pool.fetch(
        """SELECT tm.tenant_role, mp.product_role
           FROM tenant_memberships tm
           LEFT JOIN membership_products mp
             ON mp.tenant_key=tm.tenant_key
            AND mp.keycloak_subject=tm.keycloak_subject
            AND mp.product_key='crm'
           WHERE tm.tenant_key=$1 AND tm.membership_status!='suspended'""",
        tenant_key,
    )
    # Every tenant needs its admin group so the first owner can manage members
    # immediately after activation, even when CRM is not part of the contract.
    required_groups: set[str] = {f"pulse/tenants/{tenant_key}/admin"}
    if crm_enabled:
        required_groups.add(f"pulse/tenants/{tenant_key}/crm")
        if any(member["product_role"] == "manager" for member in memberships):
            required_groups.add(f"pulse/tenants/{tenant_key}/crm-admin")
    for group_path in sorted(required_groups):
        try:
            await request.app.state.keycloak.group_id(group_path)
            checks.append({"key": f"group:{group_path}", "status": "ready", "detail": f"/{group_path}"})
        except KeycloakGroupMissingError:
            checks.append({"key": f"group:{group_path}", "status": "blocked", "detail": f"Grupo /{group_path} não encontrado no Keycloak."})

    return {
        "tenant_key": tenant_key,
        "current_status": tenant["tenant_status"],
        "ready": all(check["status"] != "blocked" for check in checks),
        "checks": checks,
        "manual_checks": [
            "Confirme que a stack CRM está instalada e acessível no host informado."
        ] if crm_enabled else [],
    }


async def record_audit(
    request: Request,
    identity: Identity,
    tenant_key: str | None,
    action: str,
    details: dict,
) -> None:
    try:
        await request.app.state.pool.execute(
            """INSERT INTO tenant_control_audit
               (actor_subject,tenant_key,action,details) VALUES ($1,$2,$3,$4::jsonb)""",
            identity.subject,
            tenant_key,
            action,
            json.dumps(details, separators=(",", ":")),
        )
    except Exception:
        # Audit persistence must not turn a completed access/tenant change into
        # an apparent API failure that an operator might retry blindly.
        logger.exception("Failed to persist tenant-control audit event: %s", action)


@asynccontextmanager
async def lifespan(app: FastAPI):
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL must be configured")
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=10, command_timeout=15)
    schema = Path(__file__).resolve().parent.parent / "schema.sql"
    async with pool.acquire() as connection:
        await connection.execute(schema.read_text(encoding="utf-8"))
    app.state.pool = pool
    app.state.token_verifier = build_verifier()
    app.state.keycloak = KeycloakAdmin()
    try:
        yield
    finally:
        await app.state.keycloak.close()
        await pool.close()


app = FastAPI(title="Pulse Tenant Control", version="0.1.0", lifespan=lifespan)


@app.middleware("http")
async def protect_browser_mutations(request: Request, call_next):
    if request.method in {"POST", "PUT", "PATCH", "DELETE"}:
        origin = request.headers.get("origin")
        if origin != "https://portal.izatacore.com":
            return Response(status_code=403)
    return await call_next(request)


async def require_tenant_admin(request: Request, identity: Identity, tenant_key: str) -> str:
    if is_platform_admin(identity):
        return "platform_admin"
    row = await request.app.state.pool.fetchrow(
        """SELECT m.tenant_role FROM tenant_memberships m
           JOIN tenants t USING (tenant_key)
           WHERE m.tenant_key=$1 AND m.keycloak_subject=$2
             AND m.membership_status='active' AND t.tenant_status='active'""",
        tenant_key,
        identity.subject,
    )
    if row is None or row["tenant_role"] not in {"owner", "admin"}:
        raise HTTPException(403, "Tenant administrator role required")
    return row["tenant_role"]


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/v1/admin/audit")
async def list_admin_audit(
    request: Request,
    identity: Identity = Depends(current_identity),
    limit: int = 50,
) -> list[dict]:
    if not is_platform_admin(identity):
        raise HTTPException(403, "Platform administrator role required")
    limit = max(1, min(limit, 200))
    rows = await request.app.state.pool.fetch(
        """SELECT audit_id,actor_subject,tenant_key,action,details,occurred_at
           FROM tenant_control_audit ORDER BY occurred_at DESC,audit_id DESC LIMIT $1""",
        limit,
    )
    return [dict(row) for row in rows]


@app.get("/v1/admin/reconciliation")
async def reconcile_keycloak_state(
    request: Request,
    identity: Identity = Depends(current_identity),
) -> dict:
    if not is_platform_admin(identity):
        raise HTTPException(403, "Platform administrator role required")

    pool = request.app.state.pool
    tenants = await pool.fetch("SELECT tenant_key FROM tenants ORDER BY tenant_key")
    memberships = await pool.fetch(
        "SELECT DISTINCT keycloak_subject FROM tenant_memberships ORDER BY keycloak_subject"
    )
    missing_groups: list[dict[str, str]] = []
    group_drift: list[dict[str, list[str] | str]] = []

    try:
        for tenant in tenants:
            tenant_key = tenant["tenant_key"]
            products = await pool.fetch(
                """SELECT tp.product_key FROM tenant_products tp
                   WHERE tp.tenant_key=$1 AND tp.enabled""",
                tenant_key,
            )
            required = {f"pulse/tenants/{tenant_key}/admin"}
            if any(product["product_key"] == "crm" for product in products):
                required.add(f"pulse/tenants/{tenant_key}/crm")
            has_crm_manager = await pool.fetchval(
                """SELECT EXISTS (
                     SELECT 1 FROM membership_products mp
                     JOIN tenant_memberships tm USING (tenant_key,keycloak_subject)
                     WHERE mp.tenant_key=$1 AND mp.product_key='crm'
                       AND mp.product_role='manager' AND tm.membership_status!='suspended'
                   )""",
                tenant_key,
            )
            if has_crm_manager:
                required.add(f"pulse/tenants/{tenant_key}/crm-admin")
            for group_path in sorted(required):
                try:
                    await request.app.state.keycloak.group_id(group_path)
                except KeycloakGroupMissingError:
                    missing_groups.append({"tenant_key": tenant_key, "group_path": f"/{group_path}"})

        for row in memberships:
            subject = row["keycloak_subject"]
            desired, managed = await subject_group_policy(request, subject)
            existing = await request.app.state.keycloak.groups_for_user(subject)
            drift = member_group_drift(subject, desired, existing, managed)
            if drift:
                group_drift.append(drift)
    except KeycloakError as error:
        raise HTTPException(503, "Keycloak is unavailable; reconciliation could not be completed") from error

    return {
        "status": "drift" if missing_groups or group_drift else "ok",
        "tenants_checked": len(tenants),
        "members_checked": len(memberships),
        "missing_groups": missing_groups,
        "member_group_drift": group_drift,
    }


@app.post("/v1/admin/reconciliation/sync")
async def sync_reconciliation_drift(
    request: Request,
    identity: Identity = Depends(current_identity),
) -> dict:
    if not is_platform_admin(identity):
        raise HTTPException(403, "Platform administrator role required")
    report = await reconcile_keycloak_state(request, identity)
    if report["missing_groups"]:
        raise HTTPException(
            409,
            {"message": "Provision missing tenant groups in Keycloak before syncing memberships", "report": report},
        )

    subjects = [entry["subject"] for entry in report["member_group_drift"]]
    synced = 0
    for subject in subjects:
        try:
            await sync_subject_groups(request, subject)
            synced += 1
        except KeycloakError as error:
            await record_audit(
                request,
                identity,
                None,
                "keycloak.reconciliation_partial",
                {"members_synced": synced, "members_remaining": len(subjects) - synced},
            )
            raise HTTPException(
                503,
                f"Keycloak sync stopped after {synced} memberships; rerun reconciliation to finish safely.",
            ) from error

    await record_audit(
        request,
        identity,
        None,
        "keycloak.reconciliation_applied",
        {"members_synced": synced},
    )
    return {"members_synced": synced, "status": "ok"}


@app.get("/v1/me")
async def current_user(
    request: Request, identity: Identity = Depends(current_identity)
) -> dict:
    if identity.email_verified:
        activated_memberships = await request.app.state.pool.fetch(
            """UPDATE tenant_memberships SET membership_status='active'
               WHERE keycloak_subject=$1 AND membership_status='invited'
               RETURNING tenant_key""",
            identity.subject,
        )
        if activated_memberships:
            try:
                await sync_subject_groups(request, identity.subject)
            except KeycloakError as error:
                for membership in activated_memberships:
                    await request.app.state.pool.execute(
                        """UPDATE tenant_memberships SET membership_status='invited'
                           WHERE tenant_key=$1 AND keycloak_subject=$2
                             AND membership_status='active'""",
                        membership["tenant_key"],
                        identity.subject,
                    )
                try:
                    await sync_subject_groups(request, identity.subject)
                except KeycloakError:
                    pass
                raise HTTPException(503, "Identity access synchronization is temporarily unavailable") from error
    memberships = await request.app.state.pool.fetch(
        """SELECT t.tenant_key,t.display_name,t.crm_host,t.tenant_status,
                  m.tenant_role,m.membership_status
           FROM tenant_memberships m JOIN tenants t USING (tenant_key)
           WHERE m.keycloak_subject=$1 AND m.membership_status!='suspended'
           ORDER BY t.display_name""",
        identity.subject,
    )
    return {
        "subject": identity.subject,
        "name": identity.name,
        "email": identity.email,
        "platform_admin": is_platform_admin(identity),
        "tenants": [dict(row) for row in memberships],
    }


@app.get("/v1/catalog")
async def product_catalog(
    request: Request, identity: Identity = Depends(current_identity)
) -> list[dict]:
    del identity
    rows = await request.app.state.pool.fetch(
        "SELECT product_key,display_name,integration_status FROM product_catalog ORDER BY product_key"
    )
    return [dict(row) for row in rows]


@app.get("/v1/tenants")
async def list_tenants(
    request: Request, identity: Identity = Depends(current_identity)
) -> list[dict]:
    if is_platform_admin(identity):
        rows = await request.app.state.pool.fetch(
            "SELECT tenant_key,display_name,crm_host,tenant_status FROM tenants ORDER BY display_name"
        )
    else:
        rows = await request.app.state.pool.fetch(
            """SELECT t.tenant_key,t.display_name,t.crm_host,t.tenant_status,m.tenant_role
               FROM tenants t JOIN tenant_memberships m USING (tenant_key)
               WHERE m.keycloak_subject=$1 AND m.membership_status='active'
               ORDER BY t.display_name""",
            identity.subject,
        )
    return [
        {**dict(row), "products": await entitlement_rows(request, row["tenant_key"])}
        for row in rows
    ]


@app.post("/v1/admin/tenants", status_code=201)
async def create_tenant(
    body: TenantCreate,
    request: Request,
    identity: Identity = Depends(current_identity),
) -> dict:
    if not is_platform_admin(identity):
        raise HTTPException(403, "Platform administrator role required")
    if not TENANT_RE.fullmatch(body.tenant_key):
        raise HTTPException(422, "Invalid tenant key")
    expected_host = tenant_host(body.tenant_key)
    crm_host = body.crm_host.strip() if body.crm_host and body.crm_host.strip() else None
    crm_enabled = any(product.product_key == "crm" and product.enabled for product in body.products)
    if crm_host and crm_host != expected_host:
        raise HTTPException(422, f"CRM host must be {expected_host}")
    if crm_enabled and crm_host != expected_host:
        raise HTTPException(422, f"CRM host must be {expected_host} when CRM is contracted")
    display_name = body.display_name.strip()
    if len(display_name) < 2:
        raise HTTPException(422, "A company name is required")
    seen: set[str] = set()
    async with request.app.state.pool.acquire() as connection:
        async with connection.transaction():
            await connection.execute(
                """INSERT INTO tenants (tenant_key,display_name,crm_host,created_by)
                   VALUES ($1,$2,$3,$4)""",
                body.tenant_key,
                display_name,
                crm_host,
                identity.subject,
            )
            for product in body.products:
                if product.product_key in seen:
                    raise HTTPException(422, "Duplicate product entitlement")
                seen.add(product.product_key)
                known = await connection.fetchval(
                    "SELECT EXISTS (SELECT 1 FROM product_catalog WHERE product_key=$1)",
                    product.product_key,
                )
                if not known:
                    raise HTTPException(422, f"Unknown product: {product.product_key}")
                if product.enabled and product.product_key not in READY_PRODUCTS:
                    raise HTTPException(409, f"{product.product_key} integration is not ready")
                await connection.execute(
                    """INSERT INTO tenant_products
                       (tenant_key,product_key,enabled,seat_limit,access_mode)
                       VALUES ($1,$2,$3,$4,$5)""",
                    body.tenant_key,
                    product.product_key,
                    product.enabled,
                    product.seat_limit,
                    product.access_mode,
                )
    await record_audit(
        request,
        identity,
        body.tenant_key,
        "tenant.created",
        {"products": [item.product_key for item in body.products]},
    )
    return {"tenant_key": body.tenant_key, "tenant_status": "provisioning"}


@app.get("/v1/admin/tenants/{tenant_key}/preflight")
async def get_tenant_activation_preflight(
    tenant_key: str,
    request: Request,
    identity: Identity = Depends(current_identity),
) -> dict:
    if not is_platform_admin(identity):
        raise HTTPException(403, "Platform administrator role required")
    tenant = await request.app.state.pool.fetchrow(
        "SELECT tenant_key,tenant_status FROM tenants WHERE tenant_key=$1", tenant_key
    )
    if tenant is None:
        raise HTTPException(404, "Tenant not found")
    try:
        return await tenant_activation_preflight(request, tenant)
    except KeycloakError as error:
        raise HTTPException(503, "Keycloak is unavailable; activation preflight could not be completed") from error


@app.patch("/v1/admin/tenants/{tenant_key}")
async def update_tenant(
    tenant_key: str,
    body: TenantUpdate,
    request: Request,
    identity: Identity = Depends(current_identity),
) -> dict:
    if not is_platform_admin(identity):
        raise HTTPException(403, "Platform administrator role required")
    if not body.model_fields_set:
        raise HTTPException(422, "Provide at least one tenant field to update")

    current = await request.app.state.pool.fetchrow(
        """SELECT t.tenant_key,t.display_name,t.crm_host,t.tenant_status,
                  EXISTS (SELECT 1 FROM tenant_products tp
                          WHERE tp.tenant_key=t.tenant_key
                            AND tp.product_key='crm' AND tp.enabled) AS crm_enabled
           FROM tenants t WHERE t.tenant_key=$1""",
        tenant_key,
    )
    if current is None:
        raise HTTPException(404, "Tenant not found")

    changes: dict[str, object] = {}
    if "display_name" in body.model_fields_set:
        display_name = (body.display_name or "").strip()
        if len(display_name) < 2:
            raise HTTPException(422, "A company name is required")
        changes["display_name"] = display_name

    if "crm_host" in body.model_fields_set:
        crm_host = body.crm_host.strip().lower() if body.crm_host else None
        expected_host = tenant_host(tenant_key)
        if crm_host and crm_host != expected_host:
            raise HTTPException(422, f"CRM host must be {expected_host}")
        if current["crm_enabled"] and crm_host != expected_host:
            raise HTTPException(409, "Disable the CRM entitlement before clearing its host")
        changes["crm_host"] = crm_host

    target_status = body.tenant_status if "tenant_status" in body.model_fields_set else None
    if "tenant_status" in body.model_fields_set and target_status is None:
        raise HTTPException(422, "Tenant status cannot be empty")
    if target_status == "active":
        tenant_for_check = {
            "tenant_key": tenant_key,
            "tenant_status": current["tenant_status"],
        }
        try:
            readiness = await tenant_activation_preflight(
                request,
                tenant_for_check,
                changes.get("crm_host", UNSET),
            )
        except KeycloakError as error:
            raise HTTPException(503, "Keycloak is unavailable; activation preflight could not be completed") from error
        if not readiness["ready"]:
            raise HTTPException(
                409,
                {"message": "Tenant activation prerequisites are not satisfied", "preflight": readiness},
            )
    if target_status is not None:
        changes["tenant_status"] = target_status

    if not changes:
        raise HTTPException(422, "No tenant changes were provided")

    set_clause = ",".join(f"{column}=${index}" for index, column in enumerate(changes, start=2))
    row = await request.app.state.pool.fetchrow(
        f"UPDATE tenants SET {set_clause} WHERE tenant_key=$1 "
        "RETURNING tenant_key,display_name,tenant_status,crm_host",
        tenant_key,
        *changes.values(),
    )
    if row is None:
        raise HTTPException(404, "Tenant not found")

    if "tenant_status" in changes:
        members = await request.app.state.pool.fetch(
            "SELECT keycloak_subject FROM tenant_memberships WHERE tenant_key=$1", tenant_key
        )
        try:
            for member in members:
                await sync_subject_groups(request, member["keycloak_subject"])
        except KeycloakError as error:
            await request.app.state.pool.execute(
                "UPDATE tenants SET tenant_status=$2 WHERE tenant_key=$1",
                tenant_key,
                current["tenant_status"],
            )
            try:
                for member in members:
                    await sync_subject_groups(request, member["keycloak_subject"])
            except KeycloakError:
                pass
            raise HTTPException(
                503,
                "Tenant state was rolled back because Keycloak access synchronization failed; retry the operation.",
            ) from error
    await record_audit(
        request,
        identity,
        tenant_key,
        "tenant.updated",
        {"changes": changes},
    )
    return dict(row)


@app.put("/v1/admin/tenants/{tenant_key}/products")
async def replace_entitlements(
    tenant_key: str,
    body: TenantProductsUpdate,
    request: Request,
    identity: Identity = Depends(current_identity),
) -> dict:
    if not is_platform_admin(identity):
        raise HTTPException(403, "Platform administrator role required")
    keys = [item.product_key for item in body.products]
    if len(set(keys)) != len(keys):
        raise HTTPException(422, "Duplicate product entitlement")
    tenant_exists = await request.app.state.pool.fetchval(
        "SELECT EXISTS (SELECT 1 FROM tenants WHERE tenant_key=$1)", tenant_key
    )
    if not tenant_exists:
        raise HTTPException(404, "Tenant not found")
    for item in body.products:
        catalog = await request.app.state.pool.fetchrow(
            "SELECT integration_status FROM product_catalog WHERE product_key=$1",
            item.product_key,
        )
        if catalog is None:
            raise HTTPException(422, f"Unknown product: {item.product_key}")
        if item.enabled and (
            item.product_key not in READY_PRODUCTS or catalog["integration_status"] != "ready"
        ):
            raise HTTPException(409, f"{item.product_key} integration is not ready")
        if item.enabled and item.product_key in READY_PRODUCTS:
            try:
                await request.app.state.keycloak.group_id(
                    f"pulse/tenants/{tenant_key}/{item.product_key}"
                )
            except KeycloakGroupMissingError as error:
                raise HTTPException(
                    409,
                    f"Provision the tenant Keycloak groups for {item.product_key} first",
                ) from error
            except KeycloakError as error:
                raise HTTPException(503, "Keycloak is unavailable; entitlement was not changed") from error
    async with request.app.state.pool.acquire() as connection:
        async with connection.transaction():
            exists = await connection.fetchval(
                "SELECT EXISTS (SELECT 1 FROM tenants WHERE tenant_key=$1)", tenant_key
            )
            if not exists:
                raise HTTPException(404, "Tenant not found")
            removed = await connection.fetch(
                "SELECT product_key FROM tenant_products WHERE tenant_key=$1", tenant_key
            )
            removed_keys = {row["product_key"] for row in removed} - set(keys)
            for key in removed_keys:
                used = await connection.fetchval(
                    """SELECT EXISTS (SELECT 1 FROM membership_products
                       WHERE tenant_key=$1 AND product_key=$2)""",
                    tenant_key,
                    key,
                )
                if used:
                    raise HTTPException(409, f"Revoke {key} memberships before removing its entitlement")
                await connection.execute(
                    "DELETE FROM tenant_products WHERE tenant_key=$1 AND product_key=$2",
                    tenant_key,
                    key,
                )
            for item in body.products:
                known = await connection.fetchval(
                    "SELECT EXISTS (SELECT 1 FROM product_catalog WHERE product_key=$1)",
                    item.product_key,
                )
                if not known:
                    raise HTTPException(422, f"Unknown product: {item.product_key}")
                if item.enabled and item.product_key not in READY_PRODUCTS:
                    raise HTTPException(409, f"{item.product_key} integration is not ready")
                existing = await connection.fetchrow(
                    "SELECT enabled FROM tenant_products WHERE tenant_key=$1 AND product_key=$2 FOR UPDATE",
                    tenant_key,
                    item.product_key,
                )
                if existing and existing["enabled"] and not item.enabled:
                    used = await connection.fetchval(
                        """SELECT EXISTS (SELECT 1 FROM membership_products
                           WHERE tenant_key=$1 AND product_key=$2)""",
                        tenant_key,
                        item.product_key,
                    )
                    if used:
                        raise HTTPException(409, f"Revoke {item.product_key} memberships before disabling its entitlement")
                await connection.execute(
                    """INSERT INTO tenant_products
                       (tenant_key,product_key,enabled,seat_limit,access_mode)
                       VALUES ($1,$2,$3,$4,$5)
                       ON CONFLICT (tenant_key,product_key) DO UPDATE
                         SET enabled=EXCLUDED.enabled,seat_limit=EXCLUDED.seat_limit,
                             access_mode=EXCLUDED.access_mode""",
                    tenant_key,
                    item.product_key,
                    item.enabled,
                    item.seat_limit,
                    item.access_mode,
                )
                if item.enabled and item.seat_limit is not None:
                    assigned = await connection.fetchval(
                        """SELECT count(*) FROM membership_products mp
                           JOIN tenant_memberships tm USING (tenant_key,keycloak_subject)
                           WHERE mp.tenant_key=$1 AND mp.product_key=$2
                             AND tm.membership_status!='suspended'""",
                        tenant_key,
                        item.product_key,
                    )
                    if assigned > item.seat_limit:
                        raise HTTPException(409, f"Current assignments exceed the {item.product_key} seat limit")
                ready = await connection.fetchval(
                    "SELECT integration_status='ready' FROM product_catalog WHERE product_key=$1",
                    item.product_key,
                )
                if item.enabled and item.access_mode == "all_members" and ready:
                    members = await connection.fetchval(
                        """SELECT count(*) FROM tenant_memberships
                           WHERE tenant_key=$1 AND membership_status!='suspended'""",
                        tenant_key,
                    )
                    if item.seat_limit is not None and members > item.seat_limit:
                        raise HTTPException(409, f"Current members exceed the {item.product_key} seat limit")
                    await connection.execute(
                        """INSERT INTO membership_products
                           (tenant_key,keycloak_subject,product_key,product_role)
                           SELECT tenant_key,keycloak_subject,$2,'user'
                           FROM tenant_memberships
                           WHERE tenant_key=$1 AND membership_status!='suspended'
                           ON CONFLICT (tenant_key,keycloak_subject,product_key) DO NOTHING""",
                        tenant_key,
                        item.product_key,
                    )
    await sync_tenant_members(request, tenant_key)
    await record_audit(
        request,
        identity,
        tenant_key,
        "tenant.entitlements_updated",
        {"products": [item.model_dump(exclude_none=True) for item in body.products]},
    )
    return {"tenant_key": tenant_key, "products": await entitlement_rows(request, tenant_key)}


@app.get("/v1/tenants/{tenant_key}/members")
async def list_members(
    tenant_key: str,
    request: Request,
    identity: Identity = Depends(current_identity),
) -> list[dict]:
    await require_tenant_admin(request, identity, tenant_key)
    members = await request.app.state.pool.fetch(
        """SELECT keycloak_subject,email,display_name,tenant_role,membership_status,created_at
           FROM tenant_memberships WHERE tenant_key=$1 ORDER BY display_name""",
        tenant_key,
    )
    result = []
    for member in members:
        products = await request.app.state.pool.fetch(
            """SELECT product_key,product_role FROM membership_products
               WHERE tenant_key=$1 AND keycloak_subject=$2 ORDER BY product_key""",
            tenant_key,
            member["keycloak_subject"],
        )
        result.append({**dict(member), "products": [dict(row) for row in products]})
    return result


async def validate_products(
    request: Request, tenant_key: str, requested: list[str]
) -> tuple[list[str], dict[str, asyncpg.Record]]:
    if len(set(requested)) != len(requested):
        raise HTTPException(422, "Product list contains duplicates")
    rows = await request.app.state.pool.fetch(
        """SELECT tp.product_key,tp.seat_limit,tp.access_mode,pc.integration_status
           FROM tenant_products tp JOIN product_catalog pc USING (product_key)
           WHERE tp.tenant_key=$1 AND tp.enabled""",
        tenant_key,
    )
    entitlements = {row["product_key"]: row for row in rows}
    all_members = {
        product for product, row in entitlements.items() if row["access_mode"] == "all_members"
    }
    products = sorted(set(requested) | all_members)
    for product_key in products:
        row = entitlements.get(product_key)
        if row is None:
            raise HTTPException(403, f"Tenant does not have the {product_key} entitlement")
        if product_key not in READY_PRODUCTS or row["integration_status"] != "ready":
            raise HTTPException(409, f"{product_key} membership provisioning is not available yet")
    return products, entitlements


async def save_product_access(
    connection: asyncpg.Connection,
    tenant_key: str,
    subject: str,
    products: list[str],
    crm_role: str,
) -> None:
    for product_key in sorted(products):
        entitlement = await connection.fetchrow(
            """SELECT seat_limit FROM tenant_products
               WHERE tenant_key=$1 AND product_key=$2 AND enabled FOR UPDATE""",
            tenant_key,
            product_key,
        )
        if entitlement is None:
            raise HTTPException(403, f"Tenant does not have the {product_key} entitlement")
        limit = entitlement["seat_limit"]
        if limit is not None:
            used = await connection.fetchval(
                """SELECT count(*) FROM membership_products mp
                   JOIN tenant_memberships tm USING (tenant_key,keycloak_subject)
                   WHERE mp.tenant_key=$1 AND mp.product_key=$2
                     AND tm.membership_status!='suspended'""",
                tenant_key,
                product_key,
            )
            if used >= limit:
                raise HTTPException(409, f"No {product_key} seats are available")
        await connection.execute(
            """INSERT INTO membership_products
               (tenant_key,keycloak_subject,product_key,product_role)
               VALUES ($1,$2,$3,$4)""",
            tenant_key,
            subject,
            product_key,
            crm_role if product_key == "crm" else "user",
        )


def authorize_member_role_change(actor_role: str, current_role: str, target_role: str) -> None:
    if current_role == target_role:
        return
    if current_role == "owner" or target_role == "owner":
        if actor_role != "platform_admin":
            raise HTTPException(403, "Only a platform administrator can change the tenant owner")
    if (current_role == "admin" or target_role == "admin") and actor_role not in {
        "owner",
        "platform_admin",
    }:
        raise HTTPException(403, "Only the tenant owner can appoint or change tenant administrators")


@app.post("/v1/tenants/{tenant_key}/members", status_code=201)
async def invite_member(
    tenant_key: str,
    body: MemberCreate,
    request: Request,
    identity: Identity = Depends(current_identity),
) -> dict:
    actor_role = await require_tenant_admin(request, identity, tenant_key)
    if body.tenant_role == "owner" and actor_role != "platform_admin":
        raise HTTPException(403, "Only a platform admin can appoint the tenant owner")
    if body.tenant_role == "admin" and actor_role not in {"owner", "platform_admin"}:
        raise HTTPException(403, "Only the tenant owner can appoint tenant admins")
    email = body.email.strip().lower()
    if len(email) > 320 or not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", email):
        raise HTTPException(422, "A valid work email is required")
    display_name = body.display_name.strip()
    if len(display_name) < 2:
        raise HTTPException(422, "A display name with at least 2 characters is required")
    tenant = await request.app.state.pool.fetchrow(
        "SELECT tenant_status FROM tenants WHERE tenant_key=$1", tenant_key
    )
    if tenant is None:
        raise HTTPException(404, "Tenant not found")
    if tenant["tenant_status"] != "active":
        raise HTTPException(409, "Tenant is not active yet")
    products, _entitlements = await validate_products(request, tenant_key, body.products)

    keycloak: KeycloakAdmin = request.app.state.keycloak
    keycloak_user = None
    created_user = False
    added_groups: list[str] = []
    subject: str | None = None
    membership_inserted = False
    try:
        keycloak_user = await keycloak.user_by_email(email)
        if keycloak_user is None:
            keycloak_user = await keycloak.create_user(email, display_name)
            created_user = True
        subject = keycloak_user["id"]

        async with request.app.state.pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    """INSERT INTO tenant_memberships
                       (tenant_key,keycloak_subject,email,display_name,tenant_role,membership_status,invited_by)
                       VALUES ($1,$2,$3,$4,$5,$6,$7)""",
                    tenant_key,
                    subject,
                    email,
                    display_name,
                    body.tenant_role,
                    "active" if keycloak_user.get("emailVerified") else "invited",
                    identity.subject,
                )
                membership_inserted = True
                await save_product_access(
                    connection, tenant_key, subject, products, body.crm_role
                )

        added_groups = await sync_subject_groups(request, subject)
        if not keycloak_user.get("emailVerified"):
            await keycloak.send_invitation(subject)
    except HTTPException:
        if subject and membership_inserted:
            await request.app.state.pool.execute(
                "DELETE FROM tenant_memberships WHERE tenant_key=$1 AND keycloak_subject=$2",
                tenant_key,
                subject,
            )
            await request.app.state.pool.execute(
                "DELETE FROM membership_products WHERE tenant_key=$1 AND keycloak_subject=$2",
                tenant_key,
                subject,
            )
        if subject and (created_user or added_groups):
            await request.app.state.keycloak.rollback_user_groups(subject, added_groups, created_user)
        raise
    except asyncpg.UniqueViolationError as error:
        if subject and membership_inserted:
            await request.app.state.pool.execute(
                "DELETE FROM tenant_memberships WHERE tenant_key=$1 AND keycloak_subject=$2",
                tenant_key,
                subject,
            )
            await request.app.state.pool.execute(
                "DELETE FROM membership_products WHERE tenant_key=$1 AND keycloak_subject=$2",
                tenant_key,
                subject,
            )
        if subject and (created_user or added_groups):
            await request.app.state.keycloak.rollback_user_groups(subject, added_groups, created_user)
        raise HTTPException(409, "This person is already a tenant member") from error
    except KeycloakError as error:
        if subject and membership_inserted:
            await request.app.state.pool.execute(
                "DELETE FROM tenant_memberships WHERE tenant_key=$1 AND keycloak_subject=$2",
                tenant_key,
                subject,
            )
            await request.app.state.pool.execute(
                "DELETE FROM membership_products WHERE tenant_key=$1 AND keycloak_subject=$2",
                tenant_key,
                subject,
            )
        if subject and (created_user or added_groups):
            await request.app.state.keycloak.rollback_user_groups(subject, added_groups, created_user)
        raise HTTPException(
            503,
            "Keycloak could not create the identity or send its verification email; check tenant-control permissions and SMTP.",
        ) from error
    except Exception:
        if subject and membership_inserted:
            await request.app.state.pool.execute(
                "DELETE FROM tenant_memberships WHERE tenant_key=$1 AND keycloak_subject=$2",
                tenant_key,
                subject,
            )
        if subject and (created_user or added_groups):
            await request.app.state.keycloak.rollback_user_groups(subject, added_groups, created_user)
        raise

    await record_audit(
        request,
        identity,
        tenant_key,
        "member.invited",
        {"subject": subject, "tenant_role": body.tenant_role, "products": products},
    )
    return {
        "keycloak_subject": subject,
        "email": email,
        "membership_status": "active" if keycloak_user.get("emailVerified") else "invited",
    }


@app.put("/v1/tenants/{tenant_key}/members/{subject}/products")
async def update_member_products(
    tenant_key: str,
    subject: str,
    body: MemberProductsUpdate,
    request: Request,
    identity: Identity = Depends(current_identity),
) -> dict:
    actor_role = await require_tenant_admin(request, identity, tenant_key)
    products, _entitlements = await validate_products(request, tenant_key, body.products)
    previous_role: str
    async with request.app.state.pool.acquire() as connection:
        async with connection.transaction():
            tenant = await connection.fetchrow(
                "SELECT tenant_key FROM tenants WHERE tenant_key=$1 FOR UPDATE", tenant_key
            )
            if tenant is None:
                raise HTTPException(404, "Tenant not found")
            locked_membership = await connection.fetchrow(
                """SELECT tenant_role,membership_status FROM tenant_memberships
                   WHERE tenant_key=$1 AND keycloak_subject=$2 FOR UPDATE""",
                tenant_key,
                subject,
            )
            if locked_membership is None or locked_membership["membership_status"] == "suspended":
                raise HTTPException(404, "Tenant membership not found")
            target_role = body.tenant_role or locked_membership["tenant_role"]
            authorize_member_role_change(actor_role, locked_membership["tenant_role"], target_role)
            previous_role = locked_membership["tenant_role"]
            previous_products = await connection.fetch(
                """SELECT product_key,product_role FROM membership_products
                   WHERE tenant_key=$1 AND keycloak_subject=$2""",
                tenant_key,
                subject,
            )
            if locked_membership["tenant_role"] == "owner" and target_role != "owner":
                other_owners = await connection.fetchval(
                    """SELECT count(*) FROM tenant_memberships
                       WHERE tenant_key=$1 AND tenant_role='owner'
                         AND membership_status='active' AND keycloak_subject<>$2""",
                    tenant_key,
                    subject,
                )
                if not other_owners:
                    raise HTTPException(409, "Assign another active owner before changing the current owner")
            await connection.execute(
                "DELETE FROM membership_products WHERE tenant_key=$1 AND keycloak_subject=$2",
                tenant_key,
                subject,
            )
            await save_product_access(connection, tenant_key, subject, products, body.crm_role)
            if target_role != locked_membership["tenant_role"]:
                await connection.execute(
                    "UPDATE tenant_memberships SET tenant_role=$3 WHERE tenant_key=$1 AND keycloak_subject=$2",
                    tenant_key,
                    subject,
                    target_role,
                )
    try:
        added_groups = await sync_subject_groups(request, subject)
    except KeycloakError as error:
        async with request.app.state.pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    "UPDATE tenant_memberships SET tenant_role=$3 WHERE tenant_key=$1 AND keycloak_subject=$2",
                    tenant_key,
                    subject,
                    previous_role,
                )
                await connection.execute(
                    "DELETE FROM membership_products WHERE tenant_key=$1 AND keycloak_subject=$2",
                    tenant_key,
                    subject,
                )
                for product in previous_products:
                    await connection.execute(
                        """INSERT INTO membership_products
                           (tenant_key,keycloak_subject,product_key,product_role)
                           VALUES ($1,$2,$3,$4)""",
                        tenant_key,
                        subject,
                        product["product_key"],
                        product["product_role"],
                    )
        try:
            await sync_subject_groups(request, subject)
        except KeycloakError:
            pass
        raise HTTPException(
            503,
            "Membership changes were rolled back because Keycloak access synchronization failed; retry the operation.",
        ) from error
    await record_audit(
        request,
        identity,
        tenant_key,
        "member.access_updated",
        {"subject": subject, "tenant_role": target_role, "products": products},
    )
    return {"keycloak_subject": subject, "products": products, "sync_groups_added": len(added_groups)}


@app.delete("/v1/tenants/{tenant_key}/members/{subject}", status_code=204)
async def suspend_member(
    tenant_key: str,
    subject: str,
    request: Request,
    identity: Identity = Depends(current_identity),
) -> None:
    actor_role = await require_tenant_admin(request, identity, tenant_key)
    async with request.app.state.pool.acquire() as connection:
        async with connection.transaction():
            tenant = await connection.fetchrow(
                "SELECT tenant_key FROM tenants WHERE tenant_key=$1 FOR UPDATE", tenant_key
            )
            if tenant is None:
                raise HTTPException(404, "Tenant not found")
            member = await connection.fetchrow(
                """SELECT tenant_role,membership_status FROM tenant_memberships
                   WHERE tenant_key=$1 AND keycloak_subject=$2 FOR UPDATE""",
                tenant_key,
                subject,
            )
            if member is None:
                raise HTTPException(404, "Tenant membership not found")
            if member["membership_status"] == "suspended":
                return
            if member["tenant_role"] == "owner" and actor_role != "platform_admin":
                raise HTTPException(403, "Only a platform administrator can suspend the tenant owner")
            if member["tenant_role"] == "admin" and actor_role not in {"owner", "platform_admin"}:
                raise HTTPException(403, "Only the tenant owner can suspend tenant administrators")
            if member["tenant_role"] == "owner":
                active_owners = await connection.fetchval(
                    """SELECT count(*) FROM tenant_memberships
                       WHERE tenant_key=$1 AND tenant_role='owner'
                         AND membership_status='active' AND keycloak_subject<>$2""",
                    tenant_key,
                    subject,
                )
                if not active_owners:
                    raise HTTPException(409, "Assign another active owner before suspending the current owner")
            previous_products = await connection.fetch(
                """SELECT product_key,product_role FROM membership_products
                   WHERE tenant_key=$1 AND keycloak_subject=$2""",
                tenant_key,
                subject,
            )
            await connection.execute(
                "UPDATE tenant_memberships SET membership_status='suspended' WHERE tenant_key=$1 AND keycloak_subject=$2",
                tenant_key,
                subject,
            )
            await connection.execute(
                "DELETE FROM membership_products WHERE tenant_key=$1 AND keycloak_subject=$2",
                tenant_key,
                subject,
            )
    try:
        await sync_subject_groups(request, subject)
    except KeycloakError as error:
        async with request.app.state.pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    "UPDATE tenant_memberships SET membership_status=$3 WHERE tenant_key=$1 AND keycloak_subject=$2",
                    tenant_key,
                    subject,
                    member["membership_status"],
                )
                for product in previous_products:
                    await connection.execute(
                        """INSERT INTO membership_products
                           (tenant_key,keycloak_subject,product_key,product_role)
                           VALUES ($1,$2,$3,$4)""",
                        tenant_key,
                        subject,
                        product["product_key"],
                        product["product_role"],
                    )
        try:
            await sync_subject_groups(request, subject)
        except KeycloakError:
            pass
        raise HTTPException(
            503,
            "Membership suspension was rolled back because Keycloak access synchronization failed; retry the operation.",
        ) from error
    await record_audit(request, identity, tenant_key, "member.suspended", {"subject": subject})


@app.post("/v1/tenants/{tenant_key}/members/{subject}/reactivate")
async def reactivate_member(
    tenant_key: str,
    subject: str,
    body: MemberReactivate,
    request: Request,
    identity: Identity = Depends(current_identity),
) -> dict:
    actor_role = await require_tenant_admin(request, identity, tenant_key)
    tenant = await request.app.state.pool.fetchrow(
        "SELECT tenant_status FROM tenants WHERE tenant_key=$1", tenant_key
    )
    if tenant is None:
        raise HTTPException(404, "Tenant not found")
    if tenant["tenant_status"] != "active":
        raise HTTPException(409, "Activate the tenant before reactivating its members")

    products, _entitlements = await validate_products(request, tenant_key, body.products)
    async with request.app.state.pool.acquire() as connection:
        async with connection.transaction():
            locked_tenant = await connection.fetchrow(
                "SELECT tenant_status FROM tenants WHERE tenant_key=$1 FOR UPDATE", tenant_key
            )
            if locked_tenant is None:
                raise HTTPException(404, "Tenant not found")
            if locked_tenant["tenant_status"] != "active":
                raise HTTPException(409, "Activate the tenant before reactivating its members")
            member = await connection.fetchrow(
                """SELECT tenant_role,membership_status FROM tenant_memberships
                   WHERE tenant_key=$1 AND keycloak_subject=$2 FOR UPDATE""",
                tenant_key,
                subject,
            )
            if member is None:
                raise HTTPException(404, "Tenant membership not found")
            if member["membership_status"] != "suspended":
                raise HTTPException(409, "Only suspended memberships can be reactivated")
            if member["tenant_role"] == "owner" and actor_role != "platform_admin":
                raise HTTPException(403, "Only a platform administrator can reactivate the tenant owner")
            if member["tenant_role"] == "admin" and actor_role not in {"owner", "platform_admin"}:
                raise HTTPException(403, "Only the tenant owner can reactivate tenant administrators")
            await connection.execute(
                "UPDATE tenant_memberships SET membership_status='active' WHERE tenant_key=$1 AND keycloak_subject=$2",
                tenant_key,
                subject,
            )
            await save_product_access(connection, tenant_key, subject, products, body.crm_role)

    try:
        await sync_subject_groups(request, subject)
    except KeycloakError as error:
        await request.app.state.pool.execute(
            "UPDATE tenant_memberships SET membership_status='suspended' WHERE tenant_key=$1 AND keycloak_subject=$2",
            tenant_key,
            subject,
        )
        await request.app.state.pool.execute(
            "DELETE FROM membership_products WHERE tenant_key=$1 AND keycloak_subject=$2",
            tenant_key,
            subject,
        )
        try:
            await sync_subject_groups(request, subject)
        except KeycloakError:
            pass
        raise HTTPException(
            503,
            "Membership was rolled back because Keycloak access synchronization failed; retry the operation.",
        ) from error

    await record_audit(
        request,
        identity,
        tenant_key,
        "member.reactivated",
        {"subject": subject, "products": products},
    )
    return {"keycloak_subject": subject, "membership_status": "active", "products": products}
