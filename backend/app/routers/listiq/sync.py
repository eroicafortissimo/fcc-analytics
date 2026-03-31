from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import date

from app.services.listiq.db import get_listiq_db
from app.services.listiq.downloader import run_sync
from app.services.listiq.scheduler import update_schedule
from app.models.listiq_models import ListIQSnapshot, ListIQChange, ListIQConfig

router = APIRouter()


@router.post("/trigger")
async def trigger_sync(db: AsyncSession = Depends(get_listiq_db)):
    """Manually trigger a sync now."""
    result = await run_sync(db)
    return result


@router.get("/status")
async def sync_status(db: AsyncSession = Depends(get_listiq_db)):
    """Last sync time, status, record count, change counts."""
    snap = await db.execute(
        select(ListIQSnapshot)
        .where(ListIQSnapshot.list_name == "OFAC_SDN")
        .order_by(ListIQSnapshot.snapshot_date.desc())
        .limit(1)
    )
    snap = snap.scalars().first()
    if not snap:
        return {"synced": False, "last_sync": None, "record_count": 0}

    # Count changes for the latest snapshot date
    changes = await db.execute(
        select(
            ListIQChange.change_type,
            func.count(ListIQChange.id).label("count")
        )
        .where(
            ListIQChange.list_name == "OFAC_SDN",
            ListIQChange.change_date == snap.snapshot_date,
        )
        .group_by(ListIQChange.change_type)
    )
    change_counts = {row.change_type: row.count for row in changes}

    return {
        "synced": True,
        "last_sync": snap.created_at.isoformat() if snap.created_at else None,
        "snapshot_date": snap.snapshot_date.isoformat(),
        "record_count": snap.record_count,
        "additions": change_counts.get("ADDITION", 0),
        "deletions": change_counts.get("DELETION", 0),
        "modifications": change_counts.get("MODIFICATION", 0),
    }


@router.get("/schedule")
async def get_schedule(db: AsyncSession = Depends(get_listiq_db)):
    result = await db.execute(select(ListIQConfig))
    config = {row.key: row.value for row in result.scalars()}
    return {
        "sync_hour": int(config.get("sync_hour", 6)),
        "sync_minute": int(config.get("sync_minute", 0)),
        "sync_enabled": config.get("sync_enabled", "true") == "true",
    }


@router.put("/schedule")
async def update_schedule_endpoint(body: dict, db: AsyncSession = Depends(get_listiq_db)):
    hour = int(body.get("sync_hour", 6))
    minute = int(body.get("sync_minute", 0))
    enabled = body.get("sync_enabled", True)

    # Persist config
    for key, value in [("sync_hour", str(hour)), ("sync_minute", str(minute)), ("sync_enabled", str(enabled).lower())]:
        result = await db.execute(select(ListIQConfig).where(ListIQConfig.key == key))
        cfg = result.scalars().first()
        if cfg:
            cfg.value = value
        else:
            db.add(ListIQConfig(key=key, value=value))
    await db.commit()

    if enabled:
        update_schedule(hour, minute)

    return {"sync_hour": hour, "sync_minute": minute, "sync_enabled": enabled}


@router.get("/history")
async def sync_history(db: AsyncSession = Depends(get_listiq_db)):
    """Last 10 syncs."""
    snaps = await db.execute(
        select(ListIQSnapshot)
        .where(ListIQSnapshot.list_name == "OFAC_SDN")
        .order_by(ListIQSnapshot.snapshot_date.desc())
        .limit(10)
    )
    rows = []
    for snap in snaps.scalars():
        changes = await db.execute(
            select(ListIQChange.change_type, func.count(ListIQChange.id).label("count"))
            .where(ListIQChange.list_name == "OFAC_SDN", ListIQChange.change_date == snap.snapshot_date)
            .group_by(ListIQChange.change_type)
        )
        cc = {r.change_type: r.count for r in changes}
        rows.append({
            "snapshot_date": snap.snapshot_date.isoformat(),
            "created_at": snap.created_at.isoformat() if snap.created_at else None,
            "record_count": snap.record_count,
            "additions": cc.get("ADDITION", 0),
            "deletions": cc.get("DELETION", 0),
            "modifications": cc.get("MODIFICATION", 0),
        })
    return rows
