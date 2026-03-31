"""APScheduler setup for ListIQ daily sync."""
import asyncio
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger(__name__)
_scheduler = BackgroundScheduler()


def _run_sync_job():
    """Run the async sync inside a new event loop (called from scheduler thread)."""
    from app.services.listiq.db import SessionLocal
    from app.services.listiq.downloader import run_sync

    async def _inner():
        async with SessionLocal() as db:
            result = await run_sync(db)
            logger.info("Scheduled ListIQ sync result: %s", result)

    asyncio.run(_inner())


def start_scheduler(hour: int = 6, minute: int = 0):
    if _scheduler.running:
        _scheduler.remove_all_jobs()
    _scheduler.add_job(
        _run_sync_job,
        CronTrigger(hour=hour, minute=minute),
        id="listiq_daily_sync",
        replace_existing=True,
    )
    if not _scheduler.running:
        _scheduler.start()
    logger.info("ListIQ scheduler started: daily at %02d:%02d", hour, minute)


def update_schedule(hour: int, minute: int):
    _scheduler.reschedule_job(
        "listiq_daily_sync",
        trigger=CronTrigger(hour=hour, minute=minute),
    )
    logger.info("ListIQ schedule updated to %02d:%02d", hour, minute)


def stop_scheduler():
    if _scheduler.running:
        _scheduler.shutdown(wait=False)
