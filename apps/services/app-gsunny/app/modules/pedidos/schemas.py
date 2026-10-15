# app/modules/pedidos/schemas.py

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict


class PedidoResponse(BaseModel):
    id: int
    titulo: str
    descricao: str | None
    valor: Decimal
    status_id: int
    data_criacao: datetime
    data_atualizacao: datetime

    model_config = ConfigDict(
        from_attributes=True,
    )
    
    #model_config = ConfigDict(from_attributes=True)
    # Permite ao Pydantic ler atributos de objetos SQLAlchemy.
    # Ex.: pedido.id e pedido.titulo → campos do PedidoResponse.
    


class PedidoListResponse(BaseModel):
    items: list[PedidoResponse]
    total: int
    limit: int
    offset: int