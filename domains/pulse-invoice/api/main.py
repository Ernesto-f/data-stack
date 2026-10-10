"""Pulse Invoice API foundation: authenticated, transaction-scoped tenant access."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import AsyncIterator

import asyncpg
from fastapi import FastAPI

from archive import PaperlessClient
from auth import build_token_verifier
from extraction import build_invoice_extractor
from routes import router


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL must be configured")

    app.state.oidc_token_verifier = build_token_verifier()
    app.state.database_pool = await asyncpg.create_pool(
        dsn=database_url,
        min_size=int(os.getenv("DB_POOL_MIN_SIZE", "1")),
        max_size=int(os.getenv("DB_POOL_MAX_SIZE", "10")),
        command_timeout=15,
    )
    paperless_token = os.getenv("PAPERLESS_API_TOKEN")
    if not paperless_token:
        await app.state.database_pool.close()
        raise RuntimeError("PAPERLESS_API_TOKEN must be configured")
    app.state.paperless_client = PaperlessClient(
        base_url=os.getenv("PAPERLESS_API_URL", "http://pulse-archive:8000/api"),
        api_token=paperless_token,
    )
    app.state.invoice_extractor = build_invoice_extractor()
    try:
        yield
    finally:
        await app.state.database_pool.close()


app = FastAPI(title="Pulse Invoice API", version="0.1.0", lifespan=lifespan)
app.include_router(router)


@app.get("/health", tags=["system"])
async def health() -> dict[str, str]:
    return {"status": "ok"}
