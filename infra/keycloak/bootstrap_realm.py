#!/usr/bin/env python3
"""Idempotently create the shared Pulse realm's service clients and groups.

Reads client secrets from the private `.env.auth` on the VPS. Secret values and
tokens are never printed. The existing Keycloak instance is the shared IdP;
this script does not create or migrate a second Keycloak database.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


ENV_PATH = Path(os.getenv("AUTH_ENV_FILE", "/srv/data-stack/infra/.env.auth"))
PUBLIC_BASE = "https://portal.izatacore.com/keylogin"
ADMIN_BASE = "http://127.0.0.1:18083/keylogin"
REALM = "pulse"


def read_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


class KeycloakAdmin:
    def __init__(self, access_token: str):
        self.access_token = access_token

    def request(self, method: str, path: str, payload=None, expected=(200, 201, 204)):
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Host": "portal.izatacore.com",
            "X-Forwarded-Host": "portal.izatacore.com",
            "X-Forwarded-Proto": "https",
            "X-Forwarded-Port": "443",
        }
        if body is not None:
            headers["Content-Type"] = "application/json"
        request = Request(ADMIN_BASE + path, data=body, headers=headers, method=method)
        try:
            with urlopen(request, timeout=30) as response:
                result = response.read()
                if response.status not in expected:
                    raise RuntimeError(f"Keycloak {method} {path}: HTTP {response.status}")
                return response.status, result, response.headers
        except HTTPError as error:
            if error.code in expected:
                return error.code, error.read(), error.headers
            raise RuntimeError(f"Keycloak {method} {path}: HTTP {error.code}") from None

    def get_json(self, path: str):
        _, body, _ = self.request("GET", path)
        return json.loads(body) if body else None


def get_admin_token(env: dict[str, str]) -> str:
    data = urlencode(
        {
            "grant_type": "password",
            "client_id": "admin-cli",
            "username": env["KC_BOOTSTRAP_ADMIN_USERNAME"],
            "password": env["KC_BOOTSTRAP_ADMIN_PASSWORD"],
        }
    ).encode("ascii")
    request = Request(
        f"{PUBLIC_BASE}/realms/master/protocol/openid-connect/token",
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        with urlopen(request, timeout=30) as response:
            payload = json.load(response)
    except HTTPError as error:
        raise RuntimeError(f"Keycloak bootstrap token request: HTTP {error.code}") from None
    token = payload.get("access_token")
    if not token:
        raise RuntimeError("Keycloak did not return an admin token")
    return token


def ensure_realm(admin: KeycloakAdmin) -> None:
    try:
        realm = admin.get_json(f"/admin/realms/{REALM}")
        if realm.get("loginTheme") != "pulse":
            realm["loginTheme"] = "pulse"
            admin.request("PUT", f"/admin/realms/{REALM}", realm)
        return
    except RuntimeError as error:
        if "HTTP 404" not in str(error):
            raise
    admin.request(
        "POST",
        "/admin/realms",
        {
            "realm": REALM,
            "loginTheme": "pulse",
            "enabled": True,
            "sslRequired": "external",
            "registrationAllowed": False,
            "loginWithEmailAllowed": True,
            "duplicateEmailsAllowed": False,
            "resetPasswordAllowed": False,
            "verifyEmail": True,
            "bruteForceProtected": True,
        },
    )


def group_children(admin: KeycloakAdmin, parent_id: str | None) -> list[dict]:
    path = f"/admin/realms/{REALM}/groups"
    if parent_id:
        path += f"/{parent_id}/children"
    path += "?briefRepresentation=false&first=0&max=1000"
    return admin.get_json(path) or []


def ensure_group(admin: KeycloakAdmin, group_path: str) -> list[str]:
    parent_id = None
    group_ids = []
    for name in group_path.strip("/").split("/"):
        children = group_children(admin, parent_id)
        match = next((group for group in children if group.get("name") == name), None)
        if match is None:
            path = f"/admin/realms/{REALM}/groups"
            if parent_id:
                path += f"/{parent_id}/children"
            admin.request("POST", path, {"name": name})
            children = group_children(admin, parent_id)
            match = next((group for group in children if group.get("name") == name), None)
        if match is None or not match.get("id"):
            raise RuntimeError(f"Keycloak group could not be created: {group_path}")
        parent_id = match["id"]
        group_ids.append(parent_id)
    return group_ids


def ensure_groups_scope(admin: KeycloakAdmin) -> str:
    path = f"/admin/realms/{REALM}/client-scopes"
    scopes = admin.get_json(f"{path}?search=groups") or []
    existing = next((scope for scope in scopes if scope.get("name") == "groups"), None)
    scope = {
        "name": "groups",
        "description": "Full Keycloak group paths for service and tenant authorization",
        "protocol": "openid-connect",
        "attributes": {"display.on.consent.screen": "false"},
    }
    if existing:
        admin.request("PUT", f"{path}/{existing['id']}", {**existing, **scope})
        scope_id = existing["id"]
    else:
        admin.request("POST", path, scope)
        scopes = admin.get_json(f"{path}?search=groups") or []
        existing = next((item for item in scopes if item.get("name") == "groups"), None)
        if not existing:
            raise RuntimeError("Keycloak groups client scope was not created")
        scope_id = existing["id"]

    mapper_path = f"{path}/{scope_id}/protocol-mappers/models"
    mappers = admin.get_json(mapper_path) or []
    existing = next((mapper for mapper in mappers if mapper.get("name") == "groups"), None)
    mapper = {
        "name": "groups",
        "protocol": "openid-connect",
        "protocolMapper": "oidc-group-membership-mapper",
        "consentRequired": False,
        "config": {
            "full.path": "true",
            "id.token.claim": "true",
            "access.token.claim": "true",
            "userinfo.token.claim": "true",
            "claim.name": "groups",
        },
    }
    if existing:
        admin.request("PUT", f"{mapper_path}/{existing['id']}", {**existing, **mapper})
    else:
        admin.request("POST", mapper_path, mapper)
    return scope_id


def ensure_client(admin: KeycloakAdmin, spec: dict, groups_scope_id: str) -> None:
    path = f"/admin/realms/{REALM}/clients"
    found = admin.get_json(f"{path}?clientId={spec['clientId']}") or []
    representation = {
        "clientId": spec["clientId"],
        "name": spec["name"],
        "enabled": True,
        "protocol": "openid-connect",
        "publicClient": False,
        "standardFlowEnabled": True,
        "implicitFlowEnabled": False,
        "directAccessGrantsEnabled": False,
        "serviceAccountsEnabled": False,
        "authorizationServicesEnabled": False,
        "clientAuthenticatorType": "client-secret",
        "secret": spec["secret"],
        "redirectUris": spec["redirectUris"],
        "webOrigins": spec["webOrigins"],
    }
    if found:
        client = found[0]
        representation = {**client, **representation}
        admin.request("PUT", f"{path}/{client['id']}", representation)
        client_uuid = client["id"]
    else:
        admin.request("POST", path, representation)
        found = admin.get_json(f"{path}?clientId={spec['clientId']}") or []
        if not found:
            raise RuntimeError(f"Keycloak client was not created: {spec['clientId']}")
        client_uuid = found[0]["id"]
    default_scopes_path = f"/admin/realms/{REALM}/clients/{client_uuid}/default-client-scopes"
    default_scopes = admin.get_json(default_scopes_path) or []
    if not any(scope.get("id") == groups_scope_id for scope in default_scopes):
        admin.request("PUT", f"{default_scopes_path}/{groups_scope_id}")

    # Older runs used a client-local mapper before the shared `groups` scope
    # was introduced. Remove it so the groups claim is emitted exactly once.
    client_mapper_path = f"/admin/realms/{REALM}/clients/{client_uuid}/protocol-mappers/models"
    for mapper in admin.get_json(client_mapper_path) or []:
        if mapper.get("name") == "groups":
            admin.request("DELETE", f"{client_mapper_path}/{mapper['id']}")


def ensure_tenant_control_client(admin: KeycloakAdmin, client_id: str, secret: str) -> None:
    """Create the private user/group-management client with limited realm roles."""
    clients_path = f"/admin/realms/{REALM}/clients"
    found = admin.get_json(f"{clients_path}?clientId={client_id}") or []
    representation = {
        "clientId": client_id,
        "name": "Pulse Tenant Control Plane",
        "enabled": True,
        "protocol": "openid-connect",
        "publicClient": False,
        "standardFlowEnabled": False,
        "implicitFlowEnabled": False,
        "directAccessGrantsEnabled": False,
        "serviceAccountsEnabled": True,
        "authorizationServicesEnabled": False,
        "clientAuthenticatorType": "client-secret",
        "secret": secret,
    }
    if found:
        client = found[0]
        client_uuid = client["id"]
        admin.request("PUT", f"{clients_path}/{client_uuid}", {**client, **representation})
    else:
        admin.request("POST", clients_path, representation)
        found = admin.get_json(f"{clients_path}?clientId={client_id}") or []
        if not found:
            raise RuntimeError("Tenant-control Keycloak client was not created")
        client_uuid = found[0]["id"]

    service_user = admin.get_json(
        f"{clients_path}/{client_uuid}/service-account-user"
    )
    if not service_user or not service_user.get("id"):
        raise RuntimeError("Tenant-control service account was not created")

    realm_management = admin.get_json(f"{clients_path}?clientId=realm-management") or []
    if not realm_management:
        raise RuntimeError("Keycloak realm-management client was not found")
    management_uuid = realm_management[0]["id"]
    roles_path = f"{clients_path}/{management_uuid}/roles"
    role_defs = {role["name"]: role for role in admin.get_json(roles_path) or []}
    role_names = ("manage-users", "view-users", "query-users", "query-groups")
    missing_roles = [name for name in role_names if name not in role_defs]
    if missing_roles:
        raise RuntimeError("Keycloak management roles unavailable: " + ", ".join(missing_roles))

    mappings_path = (
        f"/admin/realms/{REALM}/users/{service_user['id']}"
        f"/role-mappings/clients/{management_uuid}"
    )
    existing_roles = {
        role["name"] for role in admin.get_json(mappings_path) or [] if role.get("name")
    }
    assign = [role_defs[name] for name in role_names if name not in existing_roles]
    if assign:
        admin.request("POST", mappings_path, assign)


def configure_realm_email(admin: KeycloakAdmin, env: dict[str, str]) -> bool:
    """Configure verified-email delivery only when a complete SMTP tuple exists."""
    names = (
        "KC_SMTP_HOST",
        "KC_SMTP_PORT",
        "KC_SMTP_FROM",
        "KC_SMTP_USER",
        "KC_SMTP_PASSWORD",
    )
    if any(not env.get(name) or env[name].startswith("replace_") for name in names):
        return False

    realm_path = f"/admin/realms/{REALM}"
    realm = admin.get_json(realm_path)
    realm["verifyEmail"] = True
    realm["smtpServer"] = {
        "host": env["KC_SMTP_HOST"],
        "port": env["KC_SMTP_PORT"],
        "from": env["KC_SMTP_FROM"],
        "auth": "true",
        "user": env["KC_SMTP_USER"],
        "password": env["KC_SMTP_PASSWORD"],
        "starttls": env.get("KC_SMTP_STARTTLS", "true").lower(),
        "ssl": env.get("KC_SMTP_SSL", "false").lower(),
    }
    admin.request("PUT", realm_path, realm)
    return True


def main() -> int:
    env = read_env(ENV_PATH)
    required = (
        "KC_BOOTSTRAP_ADMIN_USERNAME",
        "KC_BOOTSTRAP_ADMIN_PASSWORD",
        "OAUTH2_PROXY_CLIENT_ID",
        "OAUTH2_PROXY_CLIENT_SECRET",
        "PAPERLESS_OIDC_CLIENT_ID",
        "PAPERLESS_OIDC_CLIENT_SECRET",
        "GRAFANA_OIDC_CLIENT_ID",
        "GRAFANA_OIDC_CLIENT_SECRET",
        "SAMIA_OIDC_CLIENT_ID",
        "SAMIA_OIDC_CLIENT_SECRET",
        "AIRFLOW_OIDC_CLIENT_ID",
        "AIRFLOW_OIDC_CLIENT_SECRET",
        "TENANT_CONTROL_CLIENT_ID",
        "TENANT_CONTROL_CLIENT_SECRET",
    )
    missing = [key for key in required if not env.get(key) or env[key].startswith("replace_")]
    if missing:
        raise SystemExit("Missing private Keycloak settings: " + ", ".join(missing))

    admin = KeycloakAdmin(get_admin_token(env))
    ensure_realm(admin)
    groups_scope_id = ensure_groups_scope(admin)
    clients = (
        {
            "clientId": env["OAUTH2_PROXY_CLIENT_ID"],
            "name": "Pulse Shared SSO",
            "secret": env["OAUTH2_PROXY_CLIENT_SECRET"],
            "redirectUris": ["https://portal.izatacore.com/oauth2/callback"],
            "webOrigins": ["https://portal.izatacore.com"],
        },
        {
            "clientId": env["PAPERLESS_OIDC_CLIENT_ID"],
            "name": "Pulse Archive",
            "secret": env["PAPERLESS_OIDC_CLIENT_SECRET"],
            "redirectUris": [f"https://archive.izatacore.com/accounts/oidc/{env['PAPERLESS_OIDC_CLIENT_ID']}/login/callback/"],
            "webOrigins": ["https://archive.izatacore.com"],
        },
        {
            "clientId": env["GRAFANA_OIDC_CLIENT_ID"],
            "name": "Pulse Analytics",
            "secret": env["GRAFANA_OIDC_CLIENT_SECRET"],
            "redirectUris": ["https://analytics.izatacore.com/login/generic_oauth"],
            "webOrigins": ["https://analytics.izatacore.com"],
        },
        {
            "clientId": env["SAMIA_OIDC_CLIENT_ID"],
            "name": "Pulse Samia",
            "secret": env["SAMIA_OIDC_CLIENT_SECRET"],
            "redirectUris": ["https://knowledge.izatacore.com/api/v1/auth/oidc/callback"],
            "webOrigins": ["https://knowledge.izatacore.com"],
        },
        {
            "clientId": env["AIRFLOW_OIDC_CLIENT_ID"],
            "name": "Pulse Automation",
            "secret": env["AIRFLOW_OIDC_CLIENT_SECRET"],
            "redirectUris": ["https://automation.izatacore.com/auth/oauth-authorized/keycloak"],
            "webOrigins": ["https://automation.izatacore.com"],
        },
    )
    for client in clients:
        ensure_client(admin, client, groups_scope_id)
    ensure_tenant_control_client(
        admin,
        env["TENANT_CONTROL_CLIENT_ID"],
        env["TENANT_CONTROL_CLIENT_SECRET"],
    )
    configure_realm_email(admin, env)

    groups = (
        "pulse/home",
        "pulse/admin",
        "pulse/tenant-control-admin",
        "pulse/home/airflow",
        "pulse/home/airflow-admin",
        "pulse/home/cloudbeaver",
        "pulse/home/fileupload/admin",
        "pulse/home/guacamole",
        "pulse/home/smartetl",
        "pulse/home/datalogger",
        "pulse/home/analytics",
        "pulse/home/analytics-admin",
        "pulse/home/archive",
        "pulse/home/crm",
        "pulse/home/tracking",
        "pulse/home/tracking-admin",
        "pulse/tenants",
    )
    for group in groups:
        ensure_group(admin, group)
    print("Provisioned shared realm pulse, 5 application clients, and the tenant-control service client.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
