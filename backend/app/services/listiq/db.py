"""SQLAlchemy async engine and session factory for ListIQ."""
from pathlib import Path
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from app.models.listiq_models import Base

DB_PATH = Path(__file__).parent.parent.parent / "db" / "platform2.db"

engine = create_async_engine(
    f"sqlite+aiosqlite:///{DB_PATH}",
    echo=False,
    connect_args={"check_same_thread": False},
)

SessionLocal = async_sessionmaker(engine, expire_on_commit=False)


async def init_listiq_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    # Seed default config if empty
    async with SessionLocal() as session:
        from sqlalchemy import select
        from app.models.listiq_models import ListIQConfig
        result = await session.execute(select(ListIQConfig))
        if not result.scalars().first():
            defaults = [
                ListIQConfig(key="sync_hour", value="6"),
                ListIQConfig(key="sync_minute", value="0"),
                ListIQConfig(key="sync_enabled", value="true"),
            ]
            session.add_all(defaults)
            await session.commit()


async def get_listiq_db() -> AsyncSession:
    async with SessionLocal() as session:
        yield session
