# app/modules/pedidos/repository.py

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.modules.pedidos.models import Pedido


async def listar_pedidos(
    session: AsyncSession,
    *,
    limit: int,
    offset: int,
    status_id: int | None = None,
) -> list[Pedido]:
    query = select(Pedido)

    if status_id is not None:
        query = query.where(
            Pedido.status_id == status_id,
        )

    query = (
        query
        .order_by(Pedido.data_criacao.desc())
        .limit(limit)
        .offset(offset)
    )

    result = await session.execute(query)

    return list(result.scalars().all())


async def contar_pedidos(
    session: AsyncSession,
    *,
    status_id: int | None = None,
) -> int:
    query = select(
        func.count(Pedido.id)
    )

    if status_id is not None:
        query = query.where(
            Pedido.status_id == status_id,
        )

    total = await session.scalar(query)

    return total or 0