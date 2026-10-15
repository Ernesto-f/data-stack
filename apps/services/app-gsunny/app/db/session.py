from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import (
    DATABASE_ECHO_GSUNNY,
    DATABASE_URL_GSUNNY,
)


engine = create_async_engine(
    DATABASE_URL_GSUNNY,
    echo=DATABASE_ECHO_GSUNNY,
    pool_pre_ping=True,
    pool_size=3,
    max_overflow=2,
)


SessionFactory = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def get_session() -> AsyncIterator[AsyncSession]:
    async with SessionFactory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise