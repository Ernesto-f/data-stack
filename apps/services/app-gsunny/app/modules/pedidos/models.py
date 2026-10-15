# app/modules/pedidos/models.py

from datetime import datetime
from decimal import Decimal
from uuid import UUID
from app.db.base import Base

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Identity,
    Numeric,
    SmallInteger,
    String,
    Text,
    Uuid,
    func,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class Pedido(Base):
    __tablename__ = "tb_pedidos"
    __table_args__ = {"schema": "app_gsunny"}

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    titulo: Mapped[str] = mapped_column(
        String(200),
        nullable=False,
    )

    descricao: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )

    valor: Mapped[Decimal] = mapped_column(
        Numeric(14, 2),
        nullable=False,
        default=Decimal("0.00"),
    )

    status_id: Mapped[int] = mapped_column(
        SmallInteger,
        nullable=False,
    )

    usuario_criacao_id: Mapped[UUID] = mapped_column(
        nullable=False,
    )

    usuario_atualizacao_id: Mapped[UUID] = mapped_column(
        nullable=False,
    )

    data_criacao: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )

    data_atualizacao: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )