#!/usr/bin/env python3
"""Provision one isolated Odoo CRM tenant and its Keycloak OIDC client."""

from __future__ import annotations

import argparse
import os
import re
import secrets
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bootstrap_realm as keycloak  # noqa: E402


REPO_ROOT = Path(__file__).resolve().parents[2]
CRM_ROOT = REPO_ROOT / "domains" / "crm"
TENANTS_ROOT = CRM_ROOT / "tenants"
TENANT_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9-]{1,29}[a-z0-9]$")


def validate_tenant_key(tenant_key: str) -> str:
    if not TENANT_KEY_RE.fullmatch(tenant_key):
        raise ValueError("Tenant key must use 2-30 lowercase letters, digits, or hyphens.")
    return tenant_key


def tenant_public_host(tenant_key: str) -> str:
    validate_tenant_key(tenant_key)
    return "crm.izatacore.com" if tenant_key == "demo" else f"{tenant_key}.crm.izatacore.com"


def load_optional_env(path: Path) -> dict[str, str]:
    return keycloak.read_env(path) if path.exists() else {}


def write_private_env(path: Path, values: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path.parent, 0o700)
    content = "# Private tenant settings. Do not commit or print values.\n"
    content += "".join(f"{name}={value}\n" for name, value in sorted(values.items()))
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(content)


def create_odoo_config(tenant_dir: Path, values: dict[str, str]) -> None:
    config_dir = tenant_dir / "config"
    config_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(config_dir, 0o700)
    config_path = config_dir / "odoo.conf"
    if config_path.is_symlink():
        raise SystemExit("Tenant Odoo configuration must not be a symlink.")
    if config_path.exists():
        subprocess.run(["sudo", "chown", "100:101", str(config_path)], check=True)
        subprocess.run(["sudo", "chmod", "400", str(config_path)], check=True)
        return

    contents = (CRM_ROOT / "config" / "odoo.conf.example").read_text(encoding="utf-8")
    replacements = {
        "__CRM_MASTER_PASSWORD__": values["CRM_MASTER_PASSWORD"],
        "__CRM_DB_USER__": values["CRM_DB_USER"],
        "__CRM_DB_PASSWORD__": values["CRM_DB_PASSWORD"],
        "__CRM_DB_NAME__": values["CRM_DB_NAME"],
    }
    for marker, value in replacements.items():
        contents = contents.replace(marker, value)
    descriptor = os.open(config_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(contents)
    subprocess.run(["sudo", "chown", "100:101", str(config_path)], check=True)
    subprocess.run(["sudo", "chmod", "400", str(config_path)], check=True)


def create_nginx_vhost(tenant_dir: Path, tenant_key: str, public_host: str, odoo_port: int) -> None:
    template = (CRM_ROOT / "nginx" / "tenant.conf.template").read_text(encoding="utf-8")
    contents = template.replace("__CRM_PUBLIC_HOST__", public_host)
    contents = contents.replace("__CRM_TENANT_KEY__", tenant_key)
    contents = contents.replace("__CRM_ODOO_PORT__", str(odoo_port))
    output = tenant_dir / "nginx.conf"
    temporary = output.with_name(".nginx.conf.tmp")
    temporary.write_text(contents, encoding="utf-8")
    os.chmod(temporary, 0o644)
    temporary.replace(output)


def tenant_values(
    tenant_key: str,
    public_host: str,
    odoo_port: int,
    existing: dict[str, str],
) -> dict[str, str]:
    db_suffix = tenant_key.replace("-", "_")
    client_id = f"pulse-crm-{tenant_key}"
    if existing.get("CRM_OIDC_CLIENT_ID") not in (None, client_id):
        raise SystemExit("Existing tenant .env has a non-matching OIDC client ID.")
    client_secret = existing.get("CRM_OIDC_CLIENT_SECRET") or secrets.token_urlsafe(48)

    return {
        "CRM_TENANT_KEY": tenant_key,
        "CRM_PUBLIC_HOST": public_host,
        "CRM_ODOO_PORT": str(odoo_port),
        "CRM_DB_NAME": existing.get("CRM_DB_NAME", f"pulse_crm_{db_suffix}"),
        "CRM_DB_USER": existing.get("CRM_DB_USER", f"pulse_crm_{db_suffix}"),
        "CRM_DB_PASSWORD": existing.get("CRM_DB_PASSWORD", secrets.token_urlsafe(32)),
        "CRM_MASTER_PASSWORD": existing.get("CRM_MASTER_PASSWORD", secrets.token_urlsafe(32)),
        "CRM_INITIAL_ADMIN_PASSWORD": existing.get(
            "CRM_INITIAL_ADMIN_PASSWORD", secrets.token_urlsafe(32)
        ),
        "CRM_OIDC_CLIENT_ID": client_id,
        "CRM_OIDC_CLIENT_SECRET": client_secret,
        "CRM_ODOO_CONFIG": f"./tenants/{tenant_key}/config/odoo.conf",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tenant_key", help="Stable tenant slug, e.g. demo or acme.")
    parser.add_argument("--odoo-port", type=int, required=True, help="Unused loopback port for this tenant's Odoo service.")
    parser.add_argument("--public-host", help="Defaults to crm.izatacore.com for demo, otherwise <tenant>.crm.izatacore.com.")
    args = parser.parse_args()

    try:
        tenant_key = validate_tenant_key(args.tenant_key)
    except ValueError as error:
        raise SystemExit(str(error)) from None
    if not 1024 <= args.odoo_port <= 65535:
        raise SystemExit("Choose an unused TCP port between 1024 and 65535.")

    default_host = tenant_public_host(tenant_key)
    public_host = args.public_host or default_host
    if public_host != default_host:
        raise SystemExit(f"Tenant host must be exactly {default_host}.")

    auth_env = keycloak.read_env(keycloak.ENV_PATH)
    if any(
        not auth_env.get(name) or auth_env[name].startswith("replace_")
        for name in ("KC_BOOTSTRAP_ADMIN_USERNAME", "KC_BOOTSTRAP_ADMIN_PASSWORD")
    ):
        raise SystemExit("Set Keycloak admin credentials in the private AUTH_ENV_FILE first.")

    tenant_dir = TENANTS_ROOT / tenant_key
    if tenant_dir.is_symlink():
        raise SystemExit("Tenant directory must not be a symlink.")
    env_path = tenant_dir / ".env"
    if env_path.is_symlink():
        raise SystemExit("Tenant environment file must not be a symlink.")
    existing = load_optional_env(env_path)
    if existing:
        required_tenant_values = (
            "CRM_TENANT_KEY",
            "CRM_PUBLIC_HOST",
            "CRM_ODOO_PORT",
            "CRM_ODOO_CONFIG",
            "CRM_DB_NAME",
            "CRM_DB_USER",
            "CRM_DB_PASSWORD",
            "CRM_MASTER_PASSWORD",
            "CRM_INITIAL_ADMIN_PASSWORD",
            "CRM_OIDC_CLIENT_ID",
            "CRM_OIDC_CLIENT_SECRET",
        )
        missing = [name for name in required_tenant_values if not existing.get(name)]
        if missing:
            raise SystemExit(
                "Existing tenant .env is incomplete; repair it from a secure backup before provisioning."
            )
        os.chmod(tenant_dir, 0o700)
        os.chmod(env_path, 0o600)
        if existing.get("CRM_TENANT_KEY") != tenant_key:
            raise SystemExit("Existing tenant .env has a different tenant key.")
        if existing.get("CRM_PUBLIC_HOST") != public_host:
            raise SystemExit("Changing a tenant hostname requires an explicit OIDC redirect migration.")
        if existing.get("CRM_ODOO_PORT") != str(args.odoo_port):
            raise SystemExit("Changing a tenant port requires an explicit proxy migration.")
    for other_env_path in TENANTS_ROOT.glob("*/.env"):
        if other_env_path.parent.name == tenant_key:
            continue
        other = load_optional_env(other_env_path)
        if other.get("CRM_ODOO_PORT") == str(args.odoo_port):
            raise SystemExit(f"Loopback port is already allocated to tenant {other_env_path.parent.name}.")

    values = tenant_values(tenant_key, public_host, args.odoo_port, existing)
    if not existing:
        write_private_env(env_path, values)
    create_odoo_config(tenant_dir, values)
    create_nginx_vhost(tenant_dir, tenant_key, public_host, args.odoo_port)

    admin = keycloak.KeycloakAdmin(keycloak.get_admin_token(auth_env))
    groups_scope_id = keycloak.ensure_groups_scope(admin)
    keycloak.ensure_group(admin, "pulse/home/crm")
    keycloak.ensure_group(admin, f"pulse/tenants/{tenant_key}/crm")
    keycloak.ensure_client(
        admin,
        {
            "clientId": values["CRM_OIDC_CLIENT_ID"],
            "name": f"Pulse CRM — {tenant_key}",
            "secret": values["CRM_OIDC_CLIENT_SECRET"],
            "redirectUris": [f"https://{public_host}/auth_oauth/signin"],
            "webOrigins": [f"https://{public_host}"],
        },
        groups_scope_id,
    )

    print(f"Prepared isolated CRM tenant '{tenant_key}' at https://{public_host}.")
    print(f"Tenant authorization group: /pulse/tenants/{tenant_key}/crm.")
    print(f"Private .env, Odoo config, and Nginx vhost are in {tenant_dir} (values not printed).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
