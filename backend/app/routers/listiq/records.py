import json
from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.listiq.db import get_listiq_db
from app.models.listiq_models import ListIQSnapshot, ListIQRecord, ListIQChange

router = APIRouter()


@router.get("/snapshots")
async def list_snapshots(db: AsyncSession = Depends(get_listiq_db)):
    rows = await db.execute(
        select(ListIQSnapshot)
        .where(ListIQSnapshot.list_name == "OFAC_SDN")
        .order_by(ListIQSnapshot.snapshot_date.desc())
    )
    return [
        {
            "id": s.id,
            "list_name": s.list_name,
            "snapshot_date": s.snapshot_date.isoformat(),
            "record_count": s.record_count,
            "raw_file_hash": s.raw_file_hash,
            "created_at": s.created_at.isoformat() if s.created_at else None,
        }
        for s in rows.scalars()
    ]


@router.get("/{record_uid}/history")
async def record_history(record_uid: str, db: AsyncSession = Depends(get_listiq_db)):
    rows = await db.execute(
        select(ListIQChange)
        .where(ListIQChange.record_uid == record_uid)
        .order_by(ListIQChange.change_date.desc())
    )
    return [
        {
            "id": c.id,
            "change_date": c.change_date.isoformat(),
            "change_type": c.change_type,
            "modification_fields": json.loads(c.modification_fields or "[]"),
            "before_data": json.loads(c.before_data) if c.before_data else None,
            "after_data": json.loads(c.after_data) if c.after_data else None,
        }
        for c in rows.scalars()
    ]
