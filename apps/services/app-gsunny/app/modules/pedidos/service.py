# app/modules/pedidos/service.py

from sqlalchemy.ext.asyncio import AsyncSession

from app.modules.pedidos import repository
from app.modules.pedidos.schemas import (
    PedidoListResponse,
    PedidoResponse,
)


async def listar_pedidos(
    session: AsyncSession,
    *,
    limit: int,
    offset: int,
    status_id: int | None = None,
) -> PedidoListResponse:
    pedidos = await repository.listar_pedidos(
        session,
        limit=limit,
        offset=offset,
        status_id=status_id,
    )

    total = await repository.contar_pedidos(
        session,
        status_id=status_id,
    )

    items = [
        PedidoResponse.model_validate(pedido)
        for pedido in pedidos
    ]

    return PedidoListResponse(
        items=items,
        total=total,
        limit=limit,
        offset=offset,
    )