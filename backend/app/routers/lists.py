from fastapi import APIRouter, Depends, Query, BackgroundTasks
from typing import Optional
import aiosqlite

from app.db.database import get_db
from app.models.schemas import WatchlistEntry, WatchlistSummary, DownloadStatus, ListFilters
from app.services.list_downloader import download_all_lists, WATCHLIST_SOURCES
from app.services.list_cleaner import get_entries_from_db, get_summary

router = APIRouter()


@router.post("/download", response_model=list[DownloadStatus])
async def trigger_download(
    watchlists: list[str] = Query(default=list(WATCHLIST_SOURCES.keys())),
    db: aiosqlite.Connection = Depends(get_db),
):
    """
    Download and process one or more watchlists. Returns per-list status.
    Cached in SQLite — won't re-download if data is fresh (< 24 h).
    """
    results = await download_all_lists(watchlists, db)
    return results


@router.get("/entries", response_model=dict)
async def get_entries(
    watchlists: list[str] = Query(default=[]),
    entity_types: list[str] = Query(default=[]),
    nationalities: list[str] = Query(default=[]),
    search: Optional[str] = Query(default=None),
    recently_modified_only: bool = Query(default=False),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=500),
    db: aiosqlite.Connection = Depends(get_db),
):
    """Return paginated, filtered watchlist entries."""
    filters = ListFilters(
        watchlists=watchlists,
        entity_types=entity_types,
        nationalities=nationalities,
        search=search,
        recently_modified_only=recently_modified_only,
        page=page,
        page_size=page_size,
    )
    return await get_entries_from_db(filters, db)


@router.get("/summary", response_model=WatchlistSummary)
async def get_list_summary(db: aiosqlite.Connection = Depends(get_db)):
    """Aggregate counts for charts."""
    return await get_summary(db)


@router.get("/filters")
async def get_filter_options(db: aiosqlite.Connection = Depends(get_db)):
    """Return distinct values for each dropdown filter."""
    result = {}
    for col, key in [
        ("watchlist", "watchlists"),
        ("entity_type", "entity_types"),
        ("nationality", "nationalities"),
        ("sub_watchlist_1", "sub_watchlists"),
    ]:
        async with db.execute(
            f"SELECT DISTINCT {col} FROM watchlist_entries WHERE {col} IS NOT NULL ORDER BY {col}"
        ) as cur:
            rows = await cur.fetchall()
            result[key] = [r[0] for r in rows if r[0]]
    return result
