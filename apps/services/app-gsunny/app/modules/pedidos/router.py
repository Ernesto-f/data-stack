from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_session
from app.modules.pedidos import service
from app.modules.pedidos.schemas import PedidoListResponse


router = APIRouter()


@router.get(
    "/listarpedidos",
    response_model=PedidoListResponse,
    summary="Listar pedidos",
)
async def listar_pedidos(
    session: Annotated[
        AsyncSession,
        Depends(get_session),
    ],
    limit: Annotated[
        int,
        Query(ge=1, le=100),
    ] = 20,
    offset: Annotated[
        int,
        Query(ge=0),
    ] = 0,
    status_id: Annotated[
        int | None,
        Query(gt=0),
    ] = None,
) -> PedidoListResponse:
    return await service.listar_pedidos(
        session,
        limit=limit,
        offset=offset,
        status_id=status_id,
    )