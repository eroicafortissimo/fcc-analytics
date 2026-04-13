from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from pathlib import Path
import dotenv
import logging

logger = logging.getLogger(__name__)

# Load .env from the backend root (parent of app/)
dotenv.load_dotenv(Path(__file__).parent.parent / ".env")

from app.routers import lists, testcases, results, transactiq
from app.routers.listiq import sync as listiq_sync, changes as listiq_changes, records as listiq_records
from app.routers import reconciliation
from app.routers import threshold as threshold_router
from app.routers import btl as btl_router
from app.db.database import init_db
from app.services.listiq.db import init_listiq_db
from app.services.listiq.scheduler import start_scheduler, stop_scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await init_listiq_db()

    # Load schedule config and start scheduler
    from app.services.listiq.db import SessionLocal
    from app.models.listiq_models import ListIQConfig
    from sqlalchemy import select
    async with SessionLocal() as db:
        result = await db.execute(select(ListIQConfig))
        config = {row.key: row.value for row in result.scalars()}
        enabled = config.get("sync_enabled", "true") == "true"
        if enabled:
            hour = int(config.get("sync_hour", 6))
            minute = int(config.get("sync_minute", 0))
            start_scheduler(hour, minute)

        # Trigger sync on startup if today's snapshot is missing (runs in background)
        from app.models.listiq_models import ListIQSnapshot
        from datetime import date
        import asyncio
        snap = await db.execute(
            select(ListIQSnapshot).where(
                ListIQSnapshot.list_name == "OFAC_SDN",
                ListIQSnapshot.snapshot_date == date.today(),
            )
        )
        if not snap.scalars().first():
            logger.info("No snapshot for today — scheduling ListIQ sync in background")
            from app.services.listiq.downloader import run_sync
            async def _bg_sync():
                from app.services.listiq.db import SessionLocal
                try:
                    async with SessionLocal() as bg_db:
                        result = await run_sync(bg_db)
                    logger.info("Background ListIQ sync complete: %s", result)
                except Exception as exc:
                    logger.warning("Background ListIQ sync failed: %s", exc)
            asyncio.create_task(_bg_sync())

    yield
    stop_scheduler()


app = FastAPI(
    title="FCC Analytics API",
    description="AI-powered AFC compliance platform — ScreenIQ + AMLIQ",
    version="0.3.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000", "https://fcc-analytics-frontend.netlify.app"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ScreenIQ routes
app.include_router(lists.router, prefix="/api/lists", tags=["screeniq-lists"])
app.include_router(testcases.router, prefix="/api/testcases", tags=["screeniq-testcases"])
app.include_router(results.router, prefix="/api/results", tags=["screeniq-results"])
app.include_router(reconciliation.router, prefix="/api/reconciliation", tags=["screeniq-reconciliation"])

# TransactIQ routes
app.include_router(transactiq.router, prefix="/api/transactiq", tags=["transactiq"])

# List Update Manager routes
app.include_router(listiq_sync.router, prefix="/api/listiq/sync", tags=["listiq-sync"])
app.include_router(listiq_changes.router, prefix="/api/listiq/changes", tags=["listiq-changes"])
app.include_router(listiq_records.router, prefix="/api/listiq/records", tags=["listiq-records"])

# Threshold Setting routes
app.include_router(threshold_router.router, prefix="/api/threshold", tags=["threshold"])

# BTL standalone module
app.include_router(btl_router.router, prefix="/api/btl", tags=["btl"])


@app.get("/api/health")
async def health():
    return {"status": "ok", "version": "0.3.0"}
