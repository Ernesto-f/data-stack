"""Disposable Paperless-ngx object-permission test with synthetic documents.

Run the test compose stack first. The script seeds two synthetic users/groups,
uploads two generated PDFs, grants per-document group permissions, then checks
list/search/detail/download isolation through the Paperless REST API.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid
from io import BytesIO
from pathlib import Path
from urllib.parse import urlparse

import requests
from reportlab.pdfgen import canvas


ROOT = Path(__file__).resolve().parents[3]
COMPOSE_FILE = ROOT / "domains/archive/tests/tenant-acl.compose.yml"
PROJECT = "pulse-archive-tenant-acl-test"
BASE_URL = os.getenv("PAPERLESS_TEST_URL", "http://127.0.0.1:18995").rstrip("/")
TEST_PASSWORD = "tenant-acl-disposable-password"
TEST_ADMIN_PASSWORD = "disposable-admin-password"

parsed_base_url = urlparse(BASE_URL)
if parsed_base_url.scheme != "http" or parsed_base_url.hostname not in {"127.0.0.1", "localhost"}:
    raise SystemExit("Refusing to run: Paperless ACL test is restricted to localhost")


def make_pdf(marker: str) -> bytes:
    output = BytesIO()
    page = canvas.Canvas(output, pagesize=(612, 792))
    page.drawString(72, 720, f"Synthetic invoice access test {marker}")
    page.save()
    return output.getvalue()


def request_json(
    session: requests.Session,
    method: str,
    path: str,
    *,
    expected: tuple[int, ...] = (200,),
    **kwargs,
) -> requests.Response:
    response = session.request(method, f"{BASE_URL}{path}", timeout=30, **kwargs)
    if response.status_code not in expected:
        raise AssertionError(f"{method} {path} returned HTTP {response.status_code}")
    return response


def wait_for_document(admin: requests.Session, title: str) -> dict:
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        response = request_json(
            admin,
            "GET",
            "/api/documents/",
            params={"page_size": 100},
        )
        result = next(
            (item for item in response.json().get("results", []) if item.get("title") == title),
            None,
        )
        if result:
            detail = request_json(admin, "GET", f"/api/documents/{result['id']}/").json()
            if detail.get("content"):
                return detail
        time.sleep(2)
    raise AssertionError("Paperless did not finish consuming a synthetic test document")


def seed_test_users(nonce: str) -> dict[str, int]:
    seed_code = f'''\
import json
from django.contrib.auth.models import Group, Permission, User
from django.contrib.contenttypes.models import ContentType
content_type = ContentType.objects.get(app_label="documents", model="document")
perms = Permission.objects.filter(content_type=content_type, codename__in=["view_document", "change_document"])
if set(perms.values_list("codename", flat=True)) != {{"view_document", "change_document"}}:
    raise RuntimeError("Paperless document permissions are missing")
out = {{}}
for suffix in ("a", "b"):
    group, _ = Group.objects.get_or_create(name="tenant-acl-{nonce}-" + suffix)
    group.permissions.set(perms)
    user, _ = User.objects.get_or_create(username="acl-{nonce}-" + suffix)
    user.email = "acl-" + suffix + "@example.invalid"
    user.is_active = True
    user.is_staff = False
    user.is_superuser = False
    user.set_password("{TEST_PASSWORD}")
    user.groups.set([group])
    user.save()
    out[suffix] = {{"user_id": user.id, "group_id": group.id}}
print("TENANT_ACL_SEED=" + json.dumps(out))
'''
    result = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(COMPOSE_FILE),
            "-p",
            PROJECT,
            "exec",
            "-T",
            "paperless",
            "python3",
            "manage.py",
            "shell",
            "-c",
            seed_code,
        ],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    marker = next(
        (line.removeprefix("TENANT_ACL_SEED=") for line in result.stdout.splitlines() if line.startswith("TENANT_ACL_SEED=")),
        None,
    )
    if not marker:
        raise AssertionError("Paperless test identities were not created")
    return json.loads(marker)


def create_document(admin: requests.Session, title: str, marker: str) -> dict:
    response = admin.post(
        f"{BASE_URL}/api/documents/post_document/",
        files={"document": (f"{title}.pdf", make_pdf(marker), "application/pdf")},
        data={"title": title},
        timeout=30,
    )
    if response.status_code != 200:
        raise AssertionError(f"Synthetic document upload returned HTTP {response.status_code}")
    return wait_for_document(admin, title)


def assign_group_permissions(
    admin: requests.Session,
    document_id: int,
    group_id: int,
) -> None:
    permissions = {
        "view": {"users": [], "groups": [group_id]},
        "change": {"users": [], "groups": [group_id]},
    }
    request_json(
        admin,
        "PATCH",
        f"/api/documents/{document_id}/",
        json={"set_permissions": permissions},
    )


def list_document_ids(session: requests.Session) -> set[int]:
    response = request_json(
        session,
        "GET",
        "/api/documents/",
        params={"page_size": 100},
    )
    return {int(item["id"]) for item in response.json().get("results", [])}


def wait_for_paperless(session: requests.Session) -> None:
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        try:
            response = session.get(f"{BASE_URL}/api/documents/", timeout=5)
            if response.status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    raise AssertionError("Disposable Paperless service did not become ready")


def assert_user_access(
    session: requests.Session,
    own_document: dict,
    other_document: dict,
    own_marker: str,
    other_marker: str,
) -> None:
    own_id = int(own_document["id"])
    other_id = int(other_document["id"])
    visible_ids = list_document_ids(session)
    assert own_id in visible_ids, "user cannot list their tenant's document"
    assert other_id not in visible_ids, "document list crossed tenant boundary"

    own_detail = request_json(session, "GET", f"/api/documents/{own_id}/").json()
    assert own_marker in (own_detail.get("content") or ""), "own document content is not visible"
    request_json(session, "GET", f"/api/documents/{own_id}/download/")
    request_json(session, "PATCH", f"/api/documents/{own_id}/", json={"title": own_detail["title"] + " reviewed"})

    request_json(
        session,
        "GET",
        f"/api/documents/{other_id}/",
        expected=(403, 404),
    )
    request_json(
        session,
        "GET",
        f"/api/documents/{other_id}/download/",
        expected=(403, 404),
    )
    request_json(
        session,
        "PATCH",
        f"/api/documents/{other_id}/",
        json={"title": "cross-tenant edit"},
        expected=(403, 404),
    )
    request_json(
        session,
        "POST",
        "/api/documents/post_document/",
        files={"document": ("forbidden.pdf", make_pdf("forbidden-upload"), "application/pdf")},
        expected=(403, 404),
    )

    own_search = request_json(
        session,
        "GET",
        "/api/documents/",
        params={"query": own_marker},
    ).json()
    assert any(int(item["id"]) == own_id for item in own_search.get("results", [])), "tenant search missed its own document"
    other_search = request_json(
        session,
        "GET",
        "/api/documents/",
        params={"query": other_marker},
    ).json()
    assert all(int(item["id"]) != other_id for item in other_search.get("results", [])), "full-text search crossed tenant boundary"


def main() -> int:
    nonce = uuid.uuid4().hex[:10]
    admin = requests.Session()
    admin.auth = ("admin", TEST_ADMIN_PASSWORD)

    # Readiness check fails closed without returning any response content.
    wait_for_paperless(admin)
    identities = seed_test_users(nonce)
    marker_a = f"tenant-a-marker-{nonce}"
    marker_b = f"tenant-b-marker-{nonce}"
    document_a = create_document(admin, f"Synthetic invoice A {nonce}", marker_a)
    document_b = create_document(admin, f"Synthetic invoice B {nonce}", marker_b)
    assign_group_permissions(admin, int(document_a["id"]), identities["a"]["group_id"])
    assign_group_permissions(admin, int(document_b["id"]), identities["b"]["group_id"])

    user_a = requests.Session()
    user_a.auth = (f"acl-{nonce}-a", TEST_PASSWORD)
    user_b = requests.Session()
    user_b.auth = (f"acl-{nonce}-b", TEST_PASSWORD)

    assert_user_access(user_a, document_a, document_b, marker_a, marker_b)
    assert_user_access(user_b, document_b, document_a, marker_b, marker_a)
    print("PASS: Paperless object ACL isolates synthetic tenant list/search/detail/download/edit and blocks direct upload.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
