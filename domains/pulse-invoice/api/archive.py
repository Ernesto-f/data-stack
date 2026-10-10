"""Restricted Paperless API client for storing invoice originals."""

from __future__ import annotations

import asyncio
import json
import re
from pathlib import PurePath

import httpx

from auth import Identity


class ArchiveIntegrationError(RuntimeError):
    pass


class PaperlessClient:
    def __init__(self, base_url: str, api_token: str) -> None:
        self.base_url = base_url.rstrip("/") + "/"
        self.headers = {"Authorization": f"Token {api_token}"}

    async def find_owner_id(self, identity: Identity) -> int:
        if not identity.username or not identity.email or not identity.email_verified:
            raise ArchiveIntegrationError("A verified OIDC email and username are required for Archive upload")

        async with httpx.AsyncClient(base_url=self.base_url, headers=self.headers, timeout=20) as client:
            response = await client.get(
                "users/",
                params={"username__iexact": identity.username, "page_size": 100},
            )
            if response.status_code != 200:
                raise ArchiveIntegrationError("Archive user lookup failed")
            payload = response.json()
            results = payload.get("results", []) if isinstance(payload, dict) else payload
            matches = [
                user for user in results
                if str(user.get("username", "")).casefold() == identity.username.casefold()
                and str(user.get("email", "")).casefold() == identity.email.casefold()
            ]
            if len(matches) != 1:
                raise ArchiveIntegrationError("No unique Paperless account matches this verified Pulse identity")
            return int(matches[0]["id"])

    async def upload_original(
        self,
        content: bytes,
        filename: str,
        title: str,
        owner_id: int,
        *,
        timeout_seconds: int = 180,
    ) -> int:
        safe_filename = PurePath(filename).name[:180] or "invoice.pdf"
        async with httpx.AsyncClient(base_url=self.base_url, headers=self.headers, timeout=30) as client:
            stage = "uploading original"
            try:
                response = await client.post(
                    "documents/post_document/",
                    data={"title": title[:128]},
                    files={"document": (safe_filename, content, "application/pdf")},
                )
                response.raise_for_status()
                body = response.json()
                task_id = body if isinstance(body, str) else body.get("task_id")
                if not task_id:
                    raise ArchiveIntegrationError("Paperless did not return an upload task id")

                deadline = asyncio.get_running_loop().time() + timeout_seconds
                task = None
                while asyncio.get_running_loop().time() < deadline:
                    await asyncio.sleep(1)
                    stage = "checking upload task"
                    status_response = await client.get("tasks/", params={"task_id": task_id})
                    status_response.raise_for_status()
                    tasks = status_response.json()
                    tasks = tasks.get("results", []) if isinstance(tasks, dict) else tasks
                    task = next((candidate for candidate in tasks if candidate.get("task_id") == task_id), None)
                    if task is None:
                        continue
                    state = str(task.get("status", "")).upper()
                    if state == "FAILURE":
                        raise ArchiveIntegrationError("Paperless failed to consume the uploaded invoice")
                    if state == "SUCCESS":
                        break
                else:
                    raise ArchiveIntegrationError("Paperless upload did not finish before the timeout")

                document_id = self._extract_document_id(task or {})
                if document_id is None:
                    raise ArchiveIntegrationError("Paperless completed the upload without returning a document id")

                # Keep the integration account as the owner. Grant only the
                # authenticated uploader access to this document instead of
                # transferring ownership or granting archive-wide view access.
                stage = "granting uploader access"
                owner_response = await client.patch(
                    f"documents/{document_id}/",
                    json={"set_permissions": {"view": {"users": [owner_id], "groups": []}}},
                )
                owner_response.raise_for_status()
                return document_id
            except ArchiveIntegrationError:
                raise
            except httpx.HTTPStatusError as error:
                raise ArchiveIntegrationError(
                    f"Paperless {stage} failed (HTTP {error.response.status_code})",
                ) from error
            except httpx.HTTPError as error:
                # Do not return Paperless response bodies or uploaded text to callers/logs.
                raise ArchiveIntegrationError(f"Paperless {stage} request failed") from error

    @staticmethod
    def _extract_document_id(task: dict) -> int | None:
        candidate = task.get("related_document")
        if isinstance(candidate, int):
            return candidate
        if isinstance(candidate, dict):
            candidate = candidate.get("id") or candidate.get("document_id")
            if isinstance(candidate, int):
                return candidate
        if isinstance(candidate, str):
            match = re.search(r"(?:documents/)?(\d+)", candidate)
            if match:
                return int(match.group(1))
        result = task.get("result")
        if isinstance(result, str):
            try:
                result = json.loads(result)
            except json.JSONDecodeError:
                pass
        if isinstance(result, dict):
            for key in ("document_id", "id", "related_document"):
                value = result.get(key)
                if isinstance(value, int):
                    return value
                if isinstance(value, str) and value.isdigit():
                    return int(value)
        return None
