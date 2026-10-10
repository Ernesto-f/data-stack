"""Transaction-local tenant context and RLS-enforced query dependency."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator

import asyncpg
from fastapi import Depends, HTTPException, Request, status

from auth import Identity, get_current_identity


@dataclass(frozen=True)
class TenantContext:
    connection: asyncpg.Connection
    keycloak_subject: str
    tenant_key: str
    tenant_role: str
    display_name: str


@asynccontextmanager
async def tenant_transaction(
    pool: asyncpg.Pool,
    identity: Identity,
) -> AsyncIterator[TenantContext]:
    async with pool.acquire() as connection:
        async with connection.transaction():
            # The API login is only a member of this restricted, non-owner role.
            # SET LOCAL and set_config(..., true) are cleared at transaction end.
            await connection.execute("SET LOCAL ROLE pulse_invoice_runtime")
            await connection.execute(
                "SELECT set_config('pulse_invoice.keycloak_subject', $1, true)",
                identity.subject,
            )
            membership = await connection.fetchrow(
                """
                SELECT tenant_key, tenant_role
                FROM pulse_invoice.tenant_memberships
                WHERE keycloak_subject = $1 AND is_active
                """,
                identity.subject,
            )
            if membership is None:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="No active Pulse Invoice tenant membership",
                )

            tenant_key = membership["tenant_key"]
            await connection.execute(
                "SELECT set_config('pulse_invoice.tenant_key', $1, true)",
                tenant_key,
            )
            tenant = await connection.fetchrow(
                """
                SELECT tenant_key, display_name, tenant_status
                FROM pulse_invoice.tenant_registry
                WHERE tenant_key = $1
                """,
                tenant_key,
            )
            if tenant is None or tenant["tenant_status"] != "active":
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Pulse Invoice tenant is unavailable",
                )

            yield TenantContext(
                connection=connection,
                keycloak_subject=identity.subject,
                tenant_key=tenant_key,
                tenant_role=membership["tenant_role"],
                display_name=tenant["display_name"],
            )


async def get_tenant_context(
    request: Request,
    identity: Identity = Depends(get_current_identity),
) -> AsyncIterator[TenantContext]:
    pool: asyncpg.Pool = request.app.state.database_pool
    async with tenant_transaction(pool, identity) as context:
        yield context
