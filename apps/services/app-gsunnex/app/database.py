import os
import re
from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


# ============================================================
# 1. LÊ AS CONFIGURAÇÕES DO DOCKER / .ENV
# ============================================================

# Exemplo:
#
# postgresql+psycopg://usuario:senha@10.0.0.10:5432/appdb
#
DATABASE_URL = os.getenv("DATABASE_URL_GSUNNEX")

if not DATABASE_URL:
    raise RuntimeError(
        "A variável DATABASE_URL_GSUNNEX não foi configurada."
    )

# Schema onde estão as tabelas.
#
# Pode ser:
#
# app_gsunnex
#
# ou:
#
# public
#
DB_SCHEMA = os.getenv(
    "DB_SCHEMA_GSUNNEX",
    "app_gsunnex",
)

# Uma pequena validação para evitar que um nome completamente
# inválido de schema gere SQL quebrado.
if not re.fullmatch(
    r"[A-Za-z_][A-Za-z0-9_]*",
    DB_SCHEMA,
):
    raise RuntimeError(
        f"Schema PostgreSQL inválido: {DB_SCHEMA}"
    )

# ============================================================
# 2. CRIA O ENGINE
# ============================================================

# O engine representa a comunicação da aplicação
# com o PostgreSQL.
#
# Não significa que uma conexão será aberta imediatamente.
#
engine = create_async_engine(
    DATABASE_URL,

    # Antes de reutilizar uma conexão existente,
    # verifica se ela ainda está viva.
    pool_pre_ping=True,

    # Deixe False normalmente.
    # Se colocar True, o SQLAlchemy imprimirá as queries nos logs.
    echo=False,
)

# ============================================================
# 3. FÁBRICA DE SESSÕES
# ============================================================

# Cada requisição HTTP poderá pegar uma sessão.
#
# Essa sessão será utilizada para executar SELECT,
# INSERT, UPDATE, DELETE etc.
#
SessionFactory = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

# ============================================================
# 4. DEPENDÊNCIA DO FASTAPI
# ============================================================

async def get_session() -> AsyncIterator[AsyncSession]:

    # Cria uma sessão.
    async with SessionFactory() as session:

        try:
            # Entrega a sessão para o endpoint.
            yield session

        except Exception:

            # Caso alguma operação tenha dado erro,
            # desfaz a transação.
            await session.rollback()

            raise

# ============================================================
# 5. FUNÇÃO PARA MONTAR O NOME DA TABELA
# ============================================================

# Essas são as únicas tabelas que nossa primeira API
# terá permissão lógica para consultar.
#
ALLOWED_TABLES = {
    "users",
    "projects",
    "activities",
    "project_members",
    "project_comments",
    "project_success_metrics",
}


def db_table(table_name: str) -> str:

    if table_name not in ALLOWED_TABLES:
        raise ValueError(
            f"Tabela não permitida: {table_name}"
        )

    # Exemplo do resultado:
    #
    # "app_gsunnex"."projects"
    #
    return f'"{DB_SCHEMA}"."{table_name}"'