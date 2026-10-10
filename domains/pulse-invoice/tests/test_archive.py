from __future__ import annotations

import asyncio
import json

import httpx

import archive
from archive import PaperlessClient


def test_upload_grants_document_level_view_without_transferring_owner(monkeypatch):
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "POST" and request.url.path.endswith("/documents/post_document/"):
            return httpx.Response(200, json="synthetic-task")
        if request.method == "GET" and request.url.path.endswith("/tasks/"):
            return httpx.Response(
                200,
                json={
                    "results": [{
                        "task_id": "synthetic-task",
                        "status": "SUCCESS",
                        "related_document": 987,
                    }],
                },
            )
        if request.method == "PATCH" and request.url.path.endswith("/documents/987/"):
            return httpx.Response(200, json={"id": 987})
        return httpx.Response(404)

    transport = httpx.MockTransport(handler)
    real_async_client = httpx.AsyncClient
    monkeypatch.setattr(
        archive.httpx,
        "AsyncClient",
        lambda *args, **kwargs: real_async_client(transport=transport, **kwargs),
    )

    async def no_wait(_seconds: float) -> None:
        return None

    monkeypatch.setattr(archive.asyncio, "sleep", no_wait)

    document_id = asyncio.run(
        PaperlessClient("https://archive.example/api", "synthetic-token").upload_original(
            b"%PDF-synthetic",
            "invoice.pdf",
            "Synthetic invoice",
            42,
        ),
    )

    assert document_id == 987
    patch = next(request for request in requests if request.method == "PATCH")
    assert json.loads(patch.content) == {
        "set_permissions": {"view": {"users": [42], "groups": []}},
    }
    assert "owner" not in json.loads(patch.content)
