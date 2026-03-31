import json
from datetime import date
from typing import Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.listiq.db import get_listiq_db
from app.models.listiq_models import ListIQChange

router = APIRouter()


def _serialize(c: ListIQChange) -> dict:
    return {
        "id": c.id,
        "list_name": c.list_name,
        "change_date": c.change_date.isoformat() if c.change_date else None,
        "record_uid": c.record_uid,
        "change_type": c.change_type,
        "modification_fields": json.loads(c.modification_fields or "[]"),
        "before_data": json.loads(c.before_data) if c.before_data else None,
        "after_data": json.loads(c.after_data) if c.after_data else None,
        "created_at": c.created_at.isoformat() if c.created_at else None,
    }


@router.get("")
async def list_changes(
    change_date: Optional[date] = Query(default=None),
    change_type: Optional[str] = Query(default=None),
    list_name: str = Query(default="OFAC_SDN"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    db: AsyncSession = Depends(get_listiq_db),
):
    q = select(ListIQChange).where(ListIQChange.list_name == list_name)
    if change_date:
        q = q.where(ListIQChange.change_date == change_date)
    if change_type:
        q = q.where(ListIQChange.change_type == change_type)

    count_q = select(func.count()).select_from(q.subquery())
    total = (await db.execute(count_q)).scalar()

    q = q.order_by(ListIQChange.change_date.desc(), ListIQChange.id.desc())
    q = q.offset((page - 1) * page_size).limit(page_size)
    rows = (await db.execute(q)).scalars().all()

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": [_serialize(c) for c in rows],
    }


@router.get("/summary/{change_date}")
async def changes_summary(
    change_date: date,
    list_name: str = Query(default="OFAC_SDN"),
    db: AsyncSession = Depends(get_listiq_db),
):
    rows = await db.execute(
        select(ListIQChange.change_type, func.count(ListIQChange.id).label("count"))
        .where(ListIQChange.list_name == list_name, ListIQChange.change_date == change_date)
        .group_by(ListIQChange.change_type)
    )
    counts = {r.change_type: r.count for r in rows}
    return {
        "change_date": change_date.isoformat(),
        "additions": counts.get("ADDITION", 0),
        "deletions": counts.get("DELETION", 0),
        "modifications": counts.get("MODIFICATION", 0),
        "total": sum(counts.values()),
    }


@router.get("/dates")
async def available_dates(
    list_name: str = Query(default="OFAC_SDN"),
    db: AsyncSession = Depends(get_listiq_db),
):
    """Return all dates that have change records, most recent first."""
    rows = await db.execute(
        select(ListIQChange.change_date)
        .where(ListIQChange.list_name == list_name)
        .distinct()
        .order_by(ListIQChange.change_date.desc())
    )
    return [r[0].isoformat() for r in rows]


@router.get("/{change_id}")
async def get_change(change_id: int, db: AsyncSession = Depends(get_listiq_db)):
    c = await db.get(ListIQChange, change_id)
    if not c:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Change not found")
    return _serialize(c)
