#!/usr/bin/env python3
"""Interactively create a user in the shared Pulse Keycloak realm."""

from __future__ import annotations

import getpass
import sys
from pathlib import Path
from urllib.parse import urlencode

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bootstrap_realm as keycloak  # noqa: E402


GROUPS = {
    "pulse/home": "Portal (required)",
    "pulse/home/smartetl": "SmartETL",
    "pulse/home/datalogger": "DataLogger",
    "pulse/home/analytics": "Analytics viewer",
    "pulse/home/analytics-admin": "Analytics editor",
    "pulse/admin": "Pulse/Grafana administrator",
    "pulse/home/archive": "Archive UI (document ACL is separate)",
    "pulse/home/crm": "Pulse CRM (Odoo)",
}


def prompt(input_stream, output_stream, label: str) -> str:
    output_stream.write(label)
    output_stream.flush()
    value = input_stream.readline()
    if not value:
        raise SystemExit("No terminal input. Run this script from an interactive SSH session.")
    return value.rstrip("\r\n").strip()


def find_user(admin: keycloak.KeycloakAdmin, username: str) -> list[dict]:
    path = f"/admin/realms/{keycloak.REALM}/users?{urlencode({'username': username, 'exact': 'true'})}"
    return admin.get_json(path) or []


def main() -> int:
    try:
        terminal_input = open("/dev/tty", "r", encoding="utf-8")
        terminal_output = open("/dev/tty", "w", encoding="utf-8")
    except OSError:
        raise SystemExit(
            "No interactive TTY. Exit this shell, reconnect from your computer with "
            "`ssh -tt sam@179.198.221.68`, then run this script."
        ) from None

    with terminal_input, terminal_output:
        terminal_output.write("Pulse account setup\n")
        terminal_output.write("Groups: " + ", ".join(f"/{path} ({label})" for path, label in GROUPS.items()) + "\n")
        terminal_output.write("Archive group grants UI group sync only; document ACLs are assigned separately.\n\n")
        username = prompt(terminal_input, terminal_output, "Username: ")
        email = prompt(terminal_input, terminal_output, "Email you control: ")
        first_name = prompt(terminal_input, terminal_output, "First name: ")
        last_name = prompt(terminal_input, terminal_output, "Last name: ")
        raw_groups = prompt(terminal_input, terminal_output, "Additional groups, comma-separated [none]: ")

        selected = [item.strip().strip("/") for item in raw_groups.split(",") if item.strip()]
        if "pulse/home" not in selected:
            selected.insert(0, "pulse/home")
        selected = list(dict.fromkeys(selected))
        unknown = sorted(set(selected) - GROUPS.keys())
        if unknown:
            raise SystemExit("Unknown group path(s): " + ", ".join("/" + path for path in unknown))
        if not username or "@" not in email:
            raise SystemExit("Enter a username and a valid email address.")
        if prompt(terminal_input, terminal_output, "Confirm this is your verified email? [y/N]: ").lower() not in {"y", "yes", "s", "sim"}:
            raise SystemExit("Confirm email ownership before creating the account.")

        password = getpass.getpass("Temporary password (minimum 16 characters): ", stream=terminal_output)
        confirmation = getpass.getpass("Repeat temporary password: ", stream=terminal_output)
        if len(password) < 16 or password != confirmation:
            raise SystemExit("Passwords must match and be at least 16 characters long.")

        env = keycloak.read_env(keycloak.ENV_PATH)
        admin = keycloak.KeycloakAdmin(keycloak.get_admin_token(env))
        if find_user(admin, username):
            raise SystemExit("That username already exists in the Pulse realm.")

        users_path = f"/admin/realms/{keycloak.REALM}/users"
        admin.request(
            "POST",
            users_path,
            {
                "username": username,
                "email": email,
                "emailVerified": True,
                "enabled": True,
                "firstName": first_name,
                "lastName": last_name,
                "credentials": [{"type": "password", "value": password, "temporary": True}],
            },
        )

        created = find_user(admin, username)
        if not created:
            raise SystemExit("Keycloak accepted the request but the new account could not be found.")
        user_id = created[0]["id"]

        try:
            for group_path in selected:
                group_ids = keycloak.ensure_group(admin, group_path)
                admin.request(
                    "PUT",
                    f"/admin/realms/{keycloak.REALM}/users/{user_id}/groups/{group_ids[-1]}",
                )
        except Exception:
            admin.request("DELETE", f"{users_path}/{user_id}", expected=(204, 404))
            raise

        terminal_output.write("\nAccount created in the Pulse realm.\n")
        terminal_output.write("Groups: " + ", ".join("/" + path for path in selected) + "\n")
        terminal_output.write("Open https://portal.izatacore.com/ and sign in with the temporary password.\n")
        terminal_output.write("Keycloak will ask you to choose a new password at first sign-in.\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
