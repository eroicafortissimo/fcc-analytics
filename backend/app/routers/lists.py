from fastapi import APIRouter, Depends, Query, BackgroundTasks
from typing import Optional
import aiosqlite

from app.db.database import get_db
from app.models.schemas import WatchlistEntry, WatchlistSummary, DownloadStatus, ListFilters
from app.services.list_downloader import download_all_lists, WATCHLIST_SOURCES
from app.services.list_cleaner import get_entries_from_db, get_summary

router = APIRouter()


@router.post("/infer-nationalities")
async def infer_nationalities(
    watchlists: list[str] = Query(default=[]),
    batch_size: int = Query(default=500, ge=1, le=5000),
    llm_enabled: bool = Query(default=True),
    db: aiosqlite.Connection = Depends(get_db),
):
    """
    Run the LangGraph 3-tier nationality inference on entries that lack a nationality.
    Processes up to `batch_size` entries per call; call repeatedly to process the full list.
    """
    from app.services.nationality_chain import run_batch_inference
    result = await run_batch_inference(
        db=db,
        watchlists=watchlists or None,
        batch_size=batch_size,
        llm_enabled=llm_enabled,
    )
    return result


@router.get("/infer-nationalities/status")
async def infer_nationalities_status(db: aiosqlite.Connection = Depends(get_db)):
    """Show how many entries have nationality vs still pending inference."""
    async with db.execute(
        """SELECT
               COUNT(*) AS total,
               SUM(CASE WHEN nationality IS NOT NULL THEN 1 ELSE 0 END) AS inferred,
               SUM(CASE WHEN nationality IS NULL     THEN 1 ELSE 0 END) AS pending,
               SUM(CASE WHEN nationality_method = 'data_lookup' THEN 1 ELSE 0 END) AS via_data,
               SUM(CASE WHEN nationality_method = 'heuristic'   THEN 1 ELSE 0 END) AS via_heuristic,
               SUM(CASE WHEN nationality_method = 'llm'         THEN 1 ELSE 0 END) AS via_llm
           FROM watchlist_entries"""
    ) as cur:
        row = await cur.fetchone()
    return dict(row)


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
