#!/usr/bin/env python3
"""Interactively reset the Keycloak master bootstrap administrator password."""

from __future__ import annotations

import argparse
import getpass
import os
import re
import stat
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bootstrap_realm as keycloak  # noqa: E402


PASSWORD_PATTERN = re.compile(r"[A-Za-z0-9_-]{24,}")


def admin_user(admin: keycloak.KeycloakAdmin, username: str) -> dict:
    from urllib.parse import urlencode

    users = admin.get_json(
        "/admin/realms/master/users?"
        + urlencode({"username": username, "exact": "true"})
    ) or []
    if not users:
        raise RuntimeError("Configured Keycloak bootstrap admin user was not found in master realm.")
    return users[0]


def set_password(admin: keycloak.KeycloakAdmin, user_id: str, value: str) -> None:
    admin.request(
        "PUT",
        f"/admin/realms/master/users/{user_id}/reset-password",
        {"type": "password", "value": value, "temporary": False},
    )


def update_private_env(password: str) -> None:
    path = keycloak.ENV_PATH
    current = path.stat()
    lines = path.read_text(encoding="utf-8").splitlines()
    found = False
    for index, line in enumerate(lines):
        if line.startswith("KC_BOOTSTRAP_ADMIN_PASSWORD="):
            lines[index] = "KC_BOOTSTRAP_ADMIN_PASSWORD=" + password
            found = True
    if not found:
        raise RuntimeError("KC_BOOTSTRAP_ADMIN_PASSWORD is missing from the private auth env file.")

    fd, temp_name = tempfile.mkstemp(prefix=".env.auth.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as output:
            output.write("\n".join(lines) + "\n")
            output.flush()
            os.fsync(output.fileno())
        os.chmod(temp_name, stat.S_IMODE(current.st_mode))
        os.chown(temp_name, current.st_uid, current.st_gid)
        os.replace(temp_name, path)
    except Exception:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate the admin credential stored in the private env without changing it",
    )
    args = parser.parse_args()

    env = keycloak.read_env(keycloak.ENV_PATH)
    if args.check:
        admin = keycloak.KeycloakAdmin(keycloak.get_admin_token(env))
        admin_user(admin, env["KC_BOOTSTRAP_ADMIN_USERNAME"])
        print("Stored Keycloak master admin credentials are valid; no changes made.")
        return 0

    try:
        terminal_input = open("/dev/tty", "r", encoding="utf-8")
        terminal_output = open("/dev/tty", "w", encoding="utf-8")
    except OSError:
        raise SystemExit(
            "Interactive TTY required. From your computer run: "
            "ssh -tt sam@179.198.221.68 'sudo python3 /srv/data-stack/infra/keycloak/reset_admin_password.py'"
        ) from None

    with terminal_input, terminal_output:
        terminal_output.write("Reset the Keycloak master administrator password.\n")
        terminal_output.write("Use 24 or more letters, digits, underscores, or hyphens.\n")
        terminal_output.flush()
        password = getpass.getpass("New admin password: ", stream=terminal_output)
        confirmation = getpass.getpass("Repeat new admin password: ", stream=terminal_output)
        if not PASSWORD_PATTERN.fullmatch(password) or password != confirmation:
            raise SystemExit("Passwords must match and contain at least 24 allowed characters.")

    # Acquire the token only after prompting, so time spent typing cannot expire it.
    admin = keycloak.KeycloakAdmin(keycloak.get_admin_token(env))
    user = admin_user(admin, env["KC_BOOTSTRAP_ADMIN_USERNAME"])
    user_id = user["id"]
    old_password = env["KC_BOOTSTRAP_ADMIN_PASSWORD"]
    try:
        set_password(admin, user_id, password)
    except RuntimeError as error:
        if "HTTP 401" not in str(error):
            raise
        # A short-lived admin token can expire between the prompt and API request.
        admin = keycloak.KeycloakAdmin(keycloak.get_admin_token(env))
        user = admin_user(admin, env["KC_BOOTSTRAP_ADMIN_USERNAME"])
        user_id = user["id"]
        set_password(admin, user_id, password)
    try:
        update_private_env(password)
        keycloak.get_admin_token({**env, "KC_BOOTSTRAP_ADMIN_PASSWORD": password})
    except Exception:
        try:
            set_password(admin, user_id, old_password)
            update_private_env(old_password)
        except Exception:
            raise RuntimeError(
                "Password update verification failed and automatic rollback also failed; "
                "use the active Keycloak Admin API recovery procedure."
            ) from None
        raise RuntimeError("Password update verification failed; previous password was restored.") from None

    print("Keycloak master admin password updated and verified.")
    print("The new value is stored in the private auth env file with its existing permissions.")
    print("Use this admin credential through the SSH-tunneled console; it is separate from your Pulse account.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
