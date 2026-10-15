import asyncio
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from app import main
from app.auth import Identity
from app.keycloak import KeycloakError


def identity() -> Identity:
    return Identity("platform-user", "pulse-oauth2-proxy", ("/pulse/tenant-control-admin",))


def request_for(pool, keycloak):
    state = SimpleNamespace(pool=pool, keycloak=keycloak)
    return SimpleNamespace(app=SimpleNamespace(state=state))


class PreflightPool:
    def __init__(self, *, crm_enabled=True, crm_host="acme.crm.izatacore.com", members=()):
        self.crm_enabled = crm_enabled
        self.crm_host = crm_host
        self.members = list(members)

    async def fetchrow(self, query, *_args):
        if "AS crm_enabled" in query:
            return {"crm_host": self.crm_host, "crm_enabled": self.crm_enabled}
        raise AssertionError(f"Unexpected fetchrow query: {query}")

    async def fetch(self, query, *_args):
        if "FROM tenant_memberships tm" in query:
            return self.members
        raise AssertionError(f"Unexpected fetch query: {query}")


class FakeKeycloak:
    def __init__(self, groups=()):
        self.groups = set(groups)

    async def group_id(self, group_path):
        if group_path not in self.groups:
            raise KeycloakError("missing group")
        return f"id:{group_path}"


def test_activation_preflight_passes_when_host_and_required_groups_exist():
    pool = PreflightPool(members=[{"tenant_role": "owner", "product_role": "manager"}])
    keycloak = FakeKeycloak(
        {
            "pulse/tenants/acme/crm",
            "pulse/tenants/acme/crm-admin",
            "pulse/tenants/acme/admin",
        }
    )
    result = asyncio.run(
        main.tenant_activation_preflight(
            request_for(pool, keycloak),
            {"tenant_key": "acme", "tenant_status": "provisioning"},
        )
    )
    assert result["ready"] is True
    assert {check["status"] for check in result["checks"]} == {"ready"}
    assert result["manual_checks"]


def test_activation_preflight_blocks_wrong_crm_host_and_missing_keycloak_group():
    pool = PreflightPool(crm_host="other.crm.izatacore.com")
    result = asyncio.run(
        main.tenant_activation_preflight(
            request_for(pool, FakeKeycloak()),
            {"tenant_key": "acme", "tenant_status": "provisioning"},
        )
    )
    assert result["ready"] is False
    assert {check["key"] for check in result["checks"] if check["status"] == "blocked"} == {
        "crm_host",
        "group:pulse/tenants/acme/crm",
        "group:pulse/tenants/acme/admin",
    }


def test_activation_preflight_does_not_require_crm_but_requires_tenant_admin_group():
    result = asyncio.run(
        main.tenant_activation_preflight(
            request_for(
                PreflightPool(crm_enabled=False, crm_host=None),
                FakeKeycloak({"pulse/tenants/acme/admin"}),
            ),
            {"tenant_key": "acme", "tenant_status": "provisioning"},
        )
    )
    assert result["ready"] is True
    assert {check["key"] for check in result["checks"]} == {
        "crm_host",
        "group:pulse/tenants/acme/admin",
    }
    assert next(check for check in result["checks"] if check["key"] == "crm_host")["status"] == "not_required"
    assert result["manual_checks"] == []


class StatusUpdatePool:
    def __init__(self):
        self.status = "active"
        self.status_updates = []

    async def fetchrow(self, query, *_args):
        if "AS crm_enabled" in query:
            return {
                "tenant_key": "acme",
                "display_name": "Acme",
                "crm_host": "acme.crm.izatacore.com",
                "tenant_status": self.status,
                "crm_enabled": True,
            }
        if query.startswith("UPDATE tenants SET"):
            self.status = "suspended"
            return {
                "tenant_key": "acme",
                "display_name": "Acme",
                "tenant_status": self.status,
                "crm_host": "acme.crm.izatacore.com",
            }
        raise AssertionError(f"Unexpected fetchrow query: {query}")

    async def fetch(self, query, *_args):
        if "SELECT keycloak_subject FROM tenant_memberships" in query:
            return [{"keycloak_subject": "member-1"}]
        raise AssertionError(f"Unexpected fetch query: {query}")

    async def execute(self, query, *_args):
        self.status_updates.append(query)
        self.status = "active"


def test_status_rolls_back_when_keycloak_sync_fails(monkeypatch):
    pool = StatusUpdatePool()
    keycloak = FakeKeycloak()
    calls = 0

    async def fail_sync(_request, _subject):
        nonlocal calls
        calls += 1
        raise KeycloakError("Keycloak unavailable")

    monkeypatch.setattr(main, "sync_subject_groups", fail_sync)

    async def run_update():
        with pytest.raises(HTTPException) as error:
            await main.update_tenant(
                "acme",
                main.TenantUpdate(tenant_status="suspended"),
                request_for(pool, keycloak),
                identity(),
            )
        return error.value

    error = asyncio.run(run_update())
    assert error.status_code == 503
    assert pool.status == "active"
    assert pool.status_updates
    assert calls == 2  # failed transition plus best-effort restoration


class TenantEditPool:
    def __init__(self, *, crm_enabled=False):
        self.crm_enabled = crm_enabled
        self.update = None

    async def fetchrow(self, query, *args):
        if "AS crm_enabled" in query:
            return {
                "tenant_key": "acme",
                "display_name": "Acme",
                "crm_host": "acme.crm.izatacore.com" if self.crm_enabled else None,
                "tenant_status": "provisioning",
                "crm_enabled": self.crm_enabled,
            }
        if query.startswith("UPDATE tenants SET"):
            self.update = (query, args)
            return {
                "tenant_key": "acme",
                "display_name": args[1] if "display_name" in query else "Acme",
                "tenant_status": "provisioning",
                "crm_host": "acme.crm.izatacore.com" if self.crm_enabled else None,
            }
        raise AssertionError(f"Unexpected fetchrow query: {query}")


def test_partial_tenant_edit_only_writes_the_supplied_fields():
    pool = TenantEditPool()
    result = asyncio.run(
        main.update_tenant(
            "acme",
            main.TenantUpdate(display_name="Acme Incorporated"),
            request_for(pool, FakeKeycloak()),
            identity(),
        )
    )
    query, args = pool.update
    assignments = query.split(" WHERE ", 1)[0]
    assert "display_name=$2" in assignments
    assert "crm_host" not in assignments
    assert "tenant_status" not in assignments
    assert args == ("acme", "Acme Incorporated")
    assert result["display_name"] == "Acme Incorporated"


def test_activation_endpoint_rejects_incomplete_preflight_before_updating_tenant():
    pool = TenantEditPool(crm_enabled=True)
    pool.crm_host = "other.crm.izatacore.com"
    pool.members = []

    async def fetchrow(query, *args):
        if "AS crm_enabled" in query:
            return {
                "tenant_key": "acme",
                "display_name": "Acme",
                "crm_host": pool.crm_host,
                "tenant_status": "provisioning",
                "crm_enabled": True,
            }
        if query.startswith("UPDATE tenants SET"):
            pool.update = (query, args)
            raise AssertionError("Tenant must not be updated when preflight is blocked")
        raise AssertionError(f"Unexpected fetchrow query: {query}")

    pool.fetchrow = fetchrow

    async def fetch(query, *_args):
        if "FROM tenant_memberships tm" in query:
            return pool.members
        raise AssertionError(f"Unexpected fetch query: {query}")

    pool.fetch = fetch

    async def run_update():
        with pytest.raises(HTTPException) as error:
            await main.update_tenant(
                "acme",
                main.TenantUpdate(tenant_status="active"),
                request_for(pool, FakeKeycloak()),
                identity(),
            )
        return error.value

    error = asyncio.run(run_update())
    assert error.status_code == 409
    assert error.detail["preflight"]["ready"] is False
    assert pool.update is None


class AsyncNoopContext:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False


class ReactivationConnection:
    def __init__(self):
        self.statements = []

    def transaction(self):
        return AsyncNoopContext()

    async def fetchrow(self, query, *_args):
        assert "FOR UPDATE" in query
        return {"tenant_role": "member", "membership_status": "suspended"}

    async def execute(self, query, *_args):
        self.statements.append(query)


class ReactivationPool:
    def __init__(self):
        self.connection = ReactivationConnection()
        self.rollback_statements = []

    def acquire(self):
        class AcquireContext(AsyncNoopContext):
            async def __aenter__(inner_self):
                return self.connection

        return AcquireContext()

    async def fetchrow(self, query, *_args):
        assert "tenant_status" in query
        return {"tenant_status": "active"}

    async def execute(self, query, *_args):
        self.rollback_statements.append(query)


def test_reactivate_member_restores_membership_and_requested_product_groups(monkeypatch):
    pool = ReactivationPool()
    sync_calls = []
    saved_products = []

    async def validate(_request, _tenant_key, products):
        return products, {}

    async def save(_connection, _tenant_key, _subject, products, _crm_role):
        saved_products.extend(products)

    async def sync(_request, subject):
        sync_calls.append(subject)
        return ["/pulse/tenants/acme/crm"]

    monkeypatch.setattr(main, "validate_products", validate)
    monkeypatch.setattr(main, "save_product_access", save)
    monkeypatch.setattr(main, "sync_subject_groups", sync)
    result = asyncio.run(
        main.reactivate_member(
            "acme",
            "member-1",
            main.MemberReactivate(products=["crm"], crm_role="manager"),
            request_for(pool, FakeKeycloak()),
            identity(),
        )
    )
    assert result == {
        "keycloak_subject": "member-1",
        "membership_status": "active",
        "products": ["crm"],
    }
    assert "membership_status='active'" in pool.connection.statements[0]
    assert saved_products == ["crm"]
    assert sync_calls == ["member-1"]


def test_reactivate_member_rolls_back_when_keycloak_sync_fails(monkeypatch):
    pool = ReactivationPool()
    sync_calls = 0

    async def validate(_request, _tenant_key, products):
        return products, {}

    async def save(*_args):
        return None

    async def sync(_request, _subject):
        nonlocal sync_calls
        sync_calls += 1
        raise KeycloakError("Keycloak unavailable")

    monkeypatch.setattr(main, "validate_products", validate)
    monkeypatch.setattr(main, "save_product_access", save)
    monkeypatch.setattr(main, "sync_subject_groups", sync)

    async def run_reactivation():
        with pytest.raises(HTTPException) as error:
            await main.reactivate_member(
                "acme",
                "member-1",
                main.MemberReactivate(products=["crm"]),
                request_for(pool, FakeKeycloak()),
                identity(),
            )
        return error.value

    error = asyncio.run(run_reactivation())
    assert error.status_code == 503
    assert len(pool.rollback_statements) == 2
    assert sync_calls == 2


def test_member_role_changes_keep_platform_admin_boundaries():
    with pytest.raises(HTTPException) as error:
        main.authorize_member_role_change("admin", "member", "admin")
    assert error.value.status_code == 403

    main.authorize_member_role_change("owner", "admin", "member")
    main.authorize_member_role_change("platform_admin", "owner", "member")


class SeatLimitConnection:
    def __init__(self, *, used, limit=1):
        self.used = used
        self.limit = limit
        self.inserted = False

    async def fetchrow(self, _query, *_args):
        return {"seat_limit": self.limit}

    async def fetchval(self, _query, *_args):
        return self.used

    async def execute(self, _query, *_args):
        self.inserted = True


def test_seat_limit_blocks_extra_assignment_and_allows_available_seat():
    full = SeatLimitConnection(used=1)
    with pytest.raises(HTTPException) as error:
        asyncio.run(main.save_product_access(full, "acme", "member-2", ["crm"], "user"))
    assert error.value.status_code == 409
    assert full.inserted is False

    available = SeatLimitConnection(used=0)
    asyncio.run(main.save_product_access(available, "acme", "member-2", ["crm"], "user"))
    assert available.inserted is True
