"""End-to-end synthetic Keycloak -> Paperless OIDC/group-sync smoke test.

This intentionally provisions temporary test identities in the shared
development Keycloak and logs them into the empty Archive. It creates no
documents and removes the synthetic Keycloak/Paperless users and groups in a
finally block. Run only after the central OIDC clients have been configured.
"""

from __future__ import annotations

import base64
import json
import os
import shlex
import subprocess
import sys
import uuid
from html.parser import HTMLParser
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

import requests


ARCHIVE_URL = os.getenv("PAPERLESS_TEST_URL", "https://archive.izatacore.com").rstrip("/")
SSH_TARGET = os.getenv("PULSE_AUTH_SSH_TARGET", "sam@179.198.221.68")
PROVIDER_ID = "pulse-archive"

REMOTE_ADMIN_SCRIPT = r'''import base64, json, os, subprocess, sys
from pathlib import Path
sys.path.insert(0, "/srv/data-stack/infra/keycloak")
import bootstrap_realm as kc

payload = json.load(sys.stdin)
admin = kc.KeycloakAdmin(kc.get_admin_token(kc.read_env(kc.ENV_PATH)))
realm = kc.REALM
action = payload["action"]

def lookup_user(username):
    from urllib.parse import urlencode
    return admin.get_json(f"/admin/realms/{realm}/users?{urlencode({'username': username, 'exact': 'true'})}") or []

if action == "seed":
    results = []
    group_names = sorted({"/pulse/home/archive"} | {
        "/" + group_path.strip("/")
        for user in payload["users"]
        for group_path in user["group_paths"]
        if "/tenants/oidc-test-" in "/" + group_path.strip("/")
    })
    paperless_code = "import json,os; from django.contrib.auth.models import Group,Permission; " \
        "names=json.loads(os.environ[\"OIDC_TEST_GROUPS\"]); " \
        "[(lambda g,perms:g.permissions.set(perms))(Group.objects.get_or_create(name=name)[0],Permission.objects.filter(codename__in=([\"view_document\",\"view_uisettings\"] if name==\"/pulse/home/archive\" else [\"view_document\",\"change_document\",\"view_uisettings\"]))) for name in names]; " \
        "print(\"OIDC_TEST_GROUPS_READY\")"
    seeded_groups = subprocess.run([
        "docker", "exec", "-e", "OIDC_TEST_GROUPS=" + json.dumps(group_names),
        "pulse-archive", "python3", "manage.py", "shell", "-c", paperless_code,
    ], check=True, capture_output=True, text=True)
    if "OIDC_TEST_GROUPS_READY" not in seeded_groups.stdout:
        raise RuntimeError("Paperless tenant groups were not prepared")
    for user in payload["users"]:
        group_ids_by_path = {group_path: kc.ensure_group(admin, group_path) for group_path in user["group_paths"]}
        admin.request("POST", f"/admin/realms/{realm}/users", {
            "username": user["username"],
            "email": user["email"],
            "emailVerified": True,
            "enabled": True,
            "firstName": "Synthetic",
            "lastName": user["suffix"],
            "credentials": [{"type": "password", "value": user["password"], "temporary": False}],
        })
        matches = lookup_user(user["username"])
        if not matches:
            raise RuntimeError("Keycloak test identity was not created")
        user_id = matches[0]["id"]
        for group_ids in group_ids_by_path.values():
            admin.request("PUT", f"/admin/realms/{realm}/users/{user_id}/groups/{group_ids[-1]}")
        cleanup_group_ids = [
            group_id
            for group_path, group_ids in group_ids_by_path.items()
            if group_path.startswith("pulse/tenants/oidc-test-")
            for group_id in group_ids[2:]
        ]
        results.append({
            "username": user["username"],
            "email": user["email"],
            "user_id": user_id,
            "group_paths": user["group_paths"],
            "group_ids": cleanup_group_ids,
        })
    print("OIDC_TEST_SEED=" + json.dumps(results))
elif action == "check-paperless":
    emails = payload["emails"]
    code = "import json,os; from django.contrib.auth.models import User; from allauth.socialaccount.models import SocialAccount; emails=json.loads(os.environ[\"OIDC_TEST_EMAILS\"]); result={}; " \
        "[(lambda u,e: (lambda extra: result.update({e:{\"exists\":bool(u),\"groups\":list(u.groups.values_list(\"name\",flat=True)) if u else [],\"oidc_groups\":extra.get(\"groups\",[]),\"userinfo_keys\":list(extra.get(\"userinfo\",{}).keys()) if isinstance(extra.get(\"userinfo\"),dict) else [],\"userinfo_groups\":extra.get(\"userinfo\",{}).get(\"groups\",[]) if isinstance(extra.get(\"userinfo\"),dict) else [],\"id_token_groups\":extra.get(\"id_token\",{}).get(\"groups\",[]) if isinstance(extra.get(\"id_token\"),dict) else []}}))(SocialAccount.objects.filter(user=u).first().extra_data or {} if u and SocialAccount.objects.filter(user=u).exists() else {}))(User.objects.filter(email=e).first(),e) for e in emails]; " \
        "print(\"OIDC_TEST_GROUP_SYNC=\"+json.dumps(result))"
    result = subprocess.run([
        "docker", "exec", "-e", "OIDC_TEST_EMAILS=" + json.dumps(emails),
        "pulse-archive", "python3", "manage.py", "shell", "-c", code,
    ], check=True, capture_output=True, text=True)
    marker = next((line for line in result.stdout.splitlines() if line.startswith("OIDC_TEST_GROUP_SYNC=")), None)
    if not marker:
        raise RuntimeError("Paperless did not return test group-sync state")
    print(marker)
elif action == "cleanup":
    for user in payload.get("users", []):
        admin.request("DELETE", f"/admin/realms/{realm}/users/{user['user_id']}", expected=(204, 404))
    for user in payload.get("users", []):
        for group_id in reversed(user.get("group_ids", [])):
            admin.request("DELETE", f"/admin/realms/{realm}/groups/{group_id}", expected=(204, 404))
    nonce = payload["nonce"]
    code = "import os; from django.contrib.auth.models import User,Group; " \
        "User.objects.filter(username__startswith=\"oidc-test-\"+os.environ[\"OIDC_TEST_NONCE\"]).delete(); " \
        "Group.objects.filter(name__contains=os.environ[\"OIDC_TEST_NONCE\"]).delete(); print(\"OIDC_TEST_CLEANUP=ok\")"
    result = subprocess.run([
        "docker", "exec", "-e", "OIDC_TEST_NONCE=" + nonce,
        "pulse-archive", "python3", "manage.py", "shell", "-c", code,
    ], check=True, capture_output=True, text=True)
    if "OIDC_TEST_CLEANUP=ok" not in result.stdout:
        raise RuntimeError("Paperless synthetic identity cleanup failed")
    print("OIDC_TEST_CLEANUP=ok")
elif action == "cleanup-grafana":
    values = {}
    for line in Path("/srv/data-stack/domains/data-platform/.env").read_text().splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            key, value = line.split("=", 1)
            values[key] = value.strip(chr(34) + chr(39))
    credentials = base64.b64encode((values["GRAFANA_ADMIN_USER"] + ":" + values["GRAFANA_ADMIN_PASSWORD"]).encode()).decode()
    from urllib.error import HTTPError
    from urllib.parse import urlencode
    from urllib.request import Request, urlopen
    lookup = "http://127.0.0.1:13001/api/users/lookup?" + urlencode({"loginOrEmail": payload["username"]})
    request = Request(lookup, headers={"Authorization": "Basic " + credentials})
    try:
        with urlopen(request, timeout=15) as response:
            grafana_user = json.load(response)
    except HTTPError as error:
        if error.code == 404:
            print("OIDC_TEST_GRAFANA_CLEANUP=already-absent")
            raise SystemExit(0)
        raise RuntimeError("Grafana synthetic user lookup failed") from None
    request = Request(
        "http://127.0.0.1:13001/api/admin/users/" + str(grafana_user["id"]),
        headers={"Authorization": "Basic " + credentials},
        method="DELETE",
    )
    try:
        with urlopen(request, timeout=15) as response:
            if response.status not in (200, 204):
                raise RuntimeError("Grafana synthetic user cleanup failed")
    except HTTPError as error:
        if error.code != 404:
            raise RuntimeError("Grafana synthetic user cleanup failed") from None
    print("OIDC_TEST_GRAFANA_CLEANUP=ok")
elif action == "check-airflow-user":
    result = subprocess.run([
        "docker", "exec", "data-platform-airflow-api",
        "airflow", "users", "list", "--output", "json",
    ], check=True, capture_output=True, text=True)
    matches = [
        user for user in json.loads(result.stdout)
        if user.get("username") == payload["username"]
    ]
    if not matches:
        raise RuntimeError("Airflow did not provision the temporary OIDC identity")
    print("OIDC_TEST_AIRFLOW_USER=" + json.dumps({"roles": matches[0].get("roles", [])}))
elif action == "cleanup-airflow":
    result = subprocess.run([
        "docker", "exec", "data-platform-airflow-api",
        "airflow", "users", "list", "--output", "json",
    ], check=True, capture_output=True, text=True)
    matches = [
        user for user in json.loads(result.stdout)
        if user.get("username") == payload["username"]
    ]
    if matches:
        subprocess.run([
            "docker", "exec", "data-platform-airflow-api",
            "airflow", "users", "delete", "--username", payload["username"],
        ], check=True, capture_output=True, text=True)
    print("OIDC_TEST_AIRFLOW_CLEANUP=ok")
else:
    raise RuntimeError("Unknown test action")
'''


class FormParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.forms = []
        self.current = None

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "form":
            self.current = {"action": attrs.get("action", ""), "method": attrs.get("method", "get"), "inputs": []}
            self.forms.append(self.current)
        elif tag == "input" and self.current is not None:
            self.current["inputs"].append({
                "name": attrs.get("name"),
                "type": attrs.get("type", "text"),
                "value": attrs.get("value", ""),
            })

    def handle_endtag(self, tag):
        if tag == "form":
            self.current = None


def remote_admin(payload: dict) -> str:
    encoded_script = base64.b64encode(REMOTE_ADMIN_SCRIPT.encode("utf-8")).decode("ascii")
    wrapper = f"import base64; exec(base64.b64decode('{encoded_script}'))"
    remote_command = "python3 -c " + shlex.quote(wrapper)
    result = subprocess.run(
        ["ssh", "-o", "BatchMode=yes", SSH_TARGET, remote_command],
        input=json.dumps(payload).encode("utf-8"),
        capture_output=True,
        timeout=60,
    )
    if result.returncode:
        # Only expose a generic failure; stderr can include server details.
        raise RuntimeError("Remote Keycloak/Paperless test fixture operation failed")
    return result.stdout.decode("utf-8", errors="replace").strip()


def form_parser(response: requests.Response) -> FormParser:
    parser = FormParser()
    parser.feed(response.text)
    return parser


def keycloak_login_form(response: requests.Response):
    return next(
        (
            item for item in form_parser(response).forms
            if {field["name"] for field in item["inputs"]} >= {"username", "password"}
        ),
        None,
    )


def login_to_oauth2_proxy(
    username: str,
    password: str,
) -> requests.Session:
    session = requests.Session()
    response = session.get(
        "https://portal.izatacore.com/oauth2/start",
        # Use a public Pulse asset as callback target so synthetic identities
        # without the Portal group can still establish a Proxy session for a
        # negative group-authorization check.
        params={"rd": "https://portal.izatacore.com/pulse-logo.svg"},
        allow_redirects=True,
        timeout=60,
    )
    form = keycloak_login_form(response)
    if not form:
        raise AssertionError("Portal sign-in did not present the Pulse Keycloak login form")
    data = {item["name"]: item["value"] for item in form["inputs"] if item["name"]}
    data.update({"username": username, "password": password})
    authenticated = session.post(
        urljoin(response.url, form["action"]),
        data=data,
        allow_redirects=True,
        timeout=60,
    )
    if authenticated.status_code != 200 or urlparse(authenticated.url).hostname != "portal.izatacore.com":
        details = {"status": authenticated.status_code, "host": urlparse(authenticated.url).hostname, "path": urlparse(authenticated.url).path}
        raise AssertionError("OAuth2 Proxy sign-in did not establish a session: " + json.dumps(details))
    return session


def login_to_archive(session: requests.Session, username: str, password: str) -> requests.Session:
    response = session.get(f"{ARCHIVE_URL}/accounts/login/", timeout=30)
    response.raise_for_status()
    parser = form_parser(response)
    form = next(
        (item for item in parser.forms if f"/accounts/oidc/{PROVIDER_ID}/login/" in item["action"]),
        None,
    )
    if not form:
        raise AssertionError("Paperless login page has no Keycloak OIDC provider form")
    action = urljoin(response.url, form["action"])
    parts = urlparse(action)
    query = parse_qs(parts.query)
    query["process"] = ["login"]
    action = urlunparse(parts._replace(query=urlencode(query, doseq=True)))
    form_data = {item["name"]: item["value"] for item in form["inputs"] if item["name"]}
    start = session.post(
        action,
        data=form_data,
        headers={"Referer": response.url, "Origin": ARCHIVE_URL},
        allow_redirects=True,
        timeout=40,
    )
    if keycloak_login_form(start):
        raise AssertionError("Archive prompted for Keycloak credentials after Portal sign-in")
    if start.status_code >= 400 or urlparse(start.url).hostname != urlparse(ARCHIVE_URL).hostname:
        details = {"status": start.status_code, "host": urlparse(start.url).hostname, "path": urlparse(start.url).path}
        raise AssertionError("Portal SSO did not complete the Archive callback: " + json.dumps(details))
    profile = session.get(f"{ARCHIVE_URL}/api/profile/", allow_redirects=False, timeout=30)
    if profile.status_code != 200:
        details = {
            "status": profile.status_code,
            "location": urlparse(profile.headers.get("Location", "")).path,
            "session_cookie_present": any(cookie.name.startswith("sessionid") for cookie in session.cookies),
            "final_path": urlparse(start.url).path,
        }
        raise AssertionError("OIDC session did not authenticate to the Paperless profile API: " + json.dumps(details))
    return session


def login_to_grafana(session: requests.Session, username: str) -> None:
    response = session.get(
        "https://analytics.izatacore.com/login/generic_oauth",
        allow_redirects=True,
        timeout=60,
    )
    if keycloak_login_form(response):
        raise AssertionError("Analytics prompted for Keycloak credentials after Portal sign-in")
    if response.status_code >= 400 or urlparse(response.url).hostname != "analytics.izatacore.com":
        details = {"status": response.status_code, "host": urlparse(response.url).hostname, "path": urlparse(response.url).path}
        raise AssertionError("Portal SSO did not complete the Analytics callback: " + json.dumps(details))
    profile = session.get("https://analytics.izatacore.com/api/user", allow_redirects=False, timeout=30)
    if profile.status_code != 200 or profile.json().get("login") != username:
        raise AssertionError("Analytics did not establish the expected synthetic user session")


def login_to_airflow(session: requests.Session) -> None:
    response = session.get(
        "https://automation.izatacore.com/auth/login/keycloak",
        allow_redirects=True,
        timeout=90,
    )
    if keycloak_login_form(response):
        raise AssertionError("Automation prompted for Keycloak credentials after Portal sign-in")
    fragment = urlparse(response.url).fragment
    if response.status_code >= 400 or urlparse(response.url).hostname != "automation.izatacore.com" or "error=" in fragment:
        details = {"status": response.status_code, "host": urlparse(response.url).hostname, "path": urlparse(response.url).path}
        raise AssertionError("Portal SSO did not complete the Automation callback: " + json.dumps(details))


def assert_site_group_access(session: requests.Session, url: str, allowed: bool) -> None:
    response = session.get(url, allow_redirects=False, timeout=25)
    if allowed and not (200 <= response.status_code < 400):
        raise AssertionError(f"Keycloak group did not reach {urlparse(url).hostname}: HTTP {response.status_code}")
    if not allowed and 200 <= response.status_code < 300:
        raise AssertionError(f"A tenant without the service group reached {urlparse(url).hostname}")


def main() -> int:
    nonce = uuid.uuid4().hex[:8]
    users = [
        {
            "suffix": "a",
            "username": f"oidc-test-{nonce}-a",
            "email": f"oidc-test-{nonce}-a@example.invalid",
            "password": "Test-" + uuid.uuid4().hex + "!",
            "group_paths": [
                "pulse/home",
                "pulse/home/smartetl",
                "pulse/home/datalogger",
                "pulse/home/analytics",
                "pulse/home/airflow",
                "pulse/home/archive",
                f"pulse/tenants/oidc-test-{nonce}-a/invoice",
            ],
            "tenant_group": f"pulse/tenants/oidc-test-{nonce}-a/invoice",
        },
        {
            "suffix": "b",
            "username": f"oidc-test-{nonce}-b",
            "email": f"oidc-test-{nonce}-b@example.invalid",
            "password": "Test-" + uuid.uuid4().hex + "!",
            "group_paths": ["pulse/home/archive", f"pulse/tenants/oidc-test-{nonce}-b/invoice"],
            "tenant_group": f"pulse/tenants/oidc-test-{nonce}-b/invoice",
        },
    ]
    seeded = []
    grafana_user = users[0]["username"]
    airflow_user = users[0]["username"]
    try:
        output = remote_admin({"action": "seed", "users": users})
        marker = next((line.removeprefix("OIDC_TEST_SEED=") for line in output.splitlines() if line.startswith("OIDC_TEST_SEED=")), None)
        if not marker:
            raise AssertionError("Synthetic Keycloak identities were not seeded")
        seeded = json.loads(marker)
        sessions = [login_to_oauth2_proxy(user["username"], user["password"]) for user in users]
        for session, user in zip(sessions, users):
            login_to_archive(session, user["username"], user["password"])
        login_to_grafana(sessions[0], grafana_user)
        login_to_airflow(sessions[0])
        airflow_output = remote_admin({"action": "check-airflow-user", "username": airflow_user})
        marker = next((line.removeprefix("OIDC_TEST_AIRFLOW_USER=") for line in airflow_output.splitlines() if line.startswith("OIDC_TEST_AIRFLOW_USER=")), None)
        if not marker or "User" not in json.loads(marker).get("roles", []):
            raise AssertionError("Keycloak /pulse/home/airflow group did not map to the Airflow User role")
        check_output = remote_admin({"action": "check-paperless", "emails": [user["email"] for user in users]})
        marker = next((line.removeprefix("OIDC_TEST_GROUP_SYNC=") for line in check_output.splitlines() if line.startswith("OIDC_TEST_GROUP_SYNC=")), None)
        if not marker:
            raise AssertionError("Paperless group-sync state was not returned")
        state = json.loads(marker)
        for user in users:
            result = state.get(user["email"], {})
            expected_group = "/" + user["tenant_group"]
            actual_groups = set(result.get("groups", []))
            other_tenant_group = "/" + users[1 - users.index(user)]["tenant_group"]
            if not result.get("exists") or expected_group not in actual_groups or other_tenant_group in actual_groups:
                raise AssertionError(
                    "OIDC group sync mismatch: "
                    + json.dumps({"exists": result.get("exists"), "expected": expected_group, "actual_groups": result.get("groups", []), "oidc_groups": result.get("oidc_groups", []), "userinfo_groups": result.get("userinfo_groups", []), "id_token_groups": result.get("id_token_groups", [])})
                )
        for url in (
            "https://portal.izatacore.com/",
            "https://smartetl.izatacore.com/",
            "https://datalogger.izatacore.com/",
            "https://automation.izatacore.com/",
        ):
            assert_site_group_access(sessions[0], url, allowed=True)
            assert_site_group_access(sessions[1], url, allowed=False)
        print("PASS: Portal-first SSO reused the Keycloak session in Archive, Analytics, and Automation; Paperless groups synced; application group access stayed isolated.")
    finally:
        remote_admin({"action": "cleanup-airflow", "username": airflow_user})
        remote_admin({"action": "cleanup-grafana", "username": grafana_user})
        if seeded:
            remote_admin({"action": "cleanup", "nonce": nonce, "users": seeded})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
