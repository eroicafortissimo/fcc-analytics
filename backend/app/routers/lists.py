from fastapi import APIRouter, Depends, Query, BackgroundTasks, HTTPException
from typing import Optional
import logging
import aiosqlite

logger = logging.getLogger(__name__)

from app.db.database import get_db
from app.models.schemas import WatchlistEntry, WatchlistSummary, DownloadStatus, ListFilters
from app.services.list_downloader import download_all_lists, WATCHLIST_SOURCES
from app.services.list_cleaner import get_entries_from_db, get_summary, get_chart_data
from app.services.culture_cache import infer_cultures_batch, run_full_classification

router = APIRouter()


@router.post("/download", response_model=list[DownloadStatus])
async def trigger_download(
    background_tasks: BackgroundTasks,
    watchlists: list[str] = Query(default=list(WATCHLIST_SOURCES.keys())),
    db: aiosqlite.Connection = Depends(get_db),
):
    """
    Download and process one or more watchlists. Returns per-list status.
    Cached in SQLite — won't re-download if data is fresh (< 24 h).
    After ingestion, schedules a background task that applies the culture cache
    and then classifies all remaining entries to completion.
    """
    results = await download_all_lists(watchlists, db)
    background_tasks.add_task(run_full_classification)
    return results


@router.get("/entries", response_model=dict)
async def get_entries(
    watchlists: list[str] = Query(default=[]),
    entity_types: list[str] = Query(default=[]),
    search: Optional[str] = Query(default=None),
    recently_modified_only: bool = Query(default=False),
    min_tokens: Optional[int] = Query(default=None),
    max_tokens: Optional[int] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=500),
    db: aiosqlite.Connection = Depends(get_db),
):
    """Return paginated, filtered watchlist entries."""
    filters = ListFilters(
        watchlists=watchlists,
        entity_types=entity_types,
        search=search,
        recently_modified_only=recently_modified_only,
        min_tokens=min_tokens,
        max_tokens=max_tokens,
        page=page,
        page_size=page_size,
    )
    return await get_entries_from_db(filters, db)


@router.get("/summary", response_model=WatchlistSummary)
async def get_list_summary(db: aiosqlite.Connection = Depends(get_db)):
    """Aggregate counts for charts."""
    return await get_summary(db)


@router.get("/cultures")
async def get_cultures(db: aiosqlite.Connection = Depends(get_db)):
    """Return distinct name_culture values present in the watchlist."""
    async with db.execute(
        "SELECT DISTINCT name_culture FROM watchlist_entries "
        "WHERE name_culture IS NOT NULL AND name_culture != '' "
        "ORDER BY name_culture"
    ) as cur:
        rows = await cur.fetchall()
    return [r[0] for r in rows]


@router.get("/chart-data")
async def get_chart_data_endpoint(
    watchlists: list[str] = Query(default=[]),
    entity_types: list[str] = Query(default=[]),
    search: Optional[str] = Query(default=None),
    recently_modified_only: bool = Query(default=False),
    min_tokens: Optional[int] = Query(default=None),
    max_tokens: Optional[int] = Query(default=None),
    db: aiosqlite.Connection = Depends(get_db),
):
    """Return chart data aggregations that respect the current active filters."""
    filters = ListFilters(
        watchlists=watchlists,
        entity_types=entity_types,
        search=search,
        recently_modified_only=recently_modified_only,
        min_tokens=min_tokens,
        max_tokens=max_tokens,
        page=1,
        page_size=1,
    )
    return await get_chart_data(filters, db)


@router.post("/nl-filter")
async def nl_filter(body: dict):
    """
    Parse a natural language search query into filter parameters using Claude.
    Returns: { filters: {...}, explanation: str }
    """
    import os, json, re
    from anthropic import Anthropic

    query = (body.get("query") or "").strip()
    if not query:
        return {"filters": {}, "explanation": ""}

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise HTTPException(status_code=500, detail="ANTHROPIC_API_KEY not configured")

    client = Anthropic(api_key=api_key)
    prompt = f"""You are a sanctions list search assistant. Parse this search query into filter parameters.

Query: "{query}"

Available filters (use only what applies):
- watchlists: array, values from [OFAC_SDN, OFAC_NON_SDN, EU, HMT, BIS, JAPAN]
- entity_types: array, values from [individual, entity, vessel, aircraft]
- nationalities: array of nationality/region strings (e.g. "Russian", "Iranian", "Chinese", "African", "Middle Eastern / North African")
- search: keyword string for name search (use for specific names or terms)
- recently_modified_only: boolean
- min_tokens: integer — minimum number of space-separated tokens in the name (e.g. "two or more tokens" → 2, "at least three words" → 3)
- max_tokens: integer — maximum number of tokens (e.g. "at most four tokens" → 4, "up to two words" → 2)
  Use both min_tokens and max_tokens together for an exact count (e.g. "exactly two tokens" → min_tokens: 2, max_tokens: 2).

Respond with ONLY a JSON object. Include "explanation" key (1-2 sentences describing the search).
Example: {{"entity_types": ["vessel"], "nationalities": ["Iranian"], "explanation": "Searching for Iranian vessels."}}
Example: {{"entity_types": ["individual"], "min_tokens": 2, "max_tokens": 2, "explanation": "Searching for individuals with exactly two-token names."}}
Example: {{"entity_types": ["individual"], "min_tokens": 2, "explanation": "Searching for individuals with two or more tokens."}}
If nothing maps, return {{"explanation": "Could not parse query into known filters."}}"""

    try:
        message = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}],
        )
        text = message.content[0].text.strip()
        json_match = re.search(r'\{.*\}', text, re.DOTALL)
        parsed = json.loads(json_match.group() if json_match else text)
        explanation = parsed.pop("explanation", "")
        return {"filters": parsed, "explanation": explanation}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"NL filter error: {exc}")


@router.post("/infer-cultures")
async def infer_cultures(
    batch_size: int = Query(default=500, ge=1, le=2000),
    db: aiosqlite.Connection = Depends(get_db),
):
    """Classify region/name_culture for one batch of entries with NULL culture."""
    return await infer_cultures_batch(db, batch_size)


@router.get("/infer-cultures/status")
async def infer_cultures_status(db: aiosqlite.Connection = Depends(get_db)):
    async with db.execute(
        """SELECT COUNT(*) as total,
                  SUM(CASE WHEN region IS NOT NULL THEN 1 ELSE 0 END) as classified,
                  SUM(CASE WHEN region IS NULL THEN 1 ELSE 0 END) as pending
           FROM watchlist_entries"""
    ) as cur:
        row = await cur.fetchone()
    return dict(row)


@router.get("/overlap")
async def get_watchlist_overlap(db: aiosqlite.Connection = Depends(get_db)):
    """Cross-watchlist name overlap: count of exact cleaned_name matches between each pair."""
    from collections import defaultdict
    from itertools import combinations

    async with db.execute(
        "SELECT cleaned_name, watchlist FROM watchlist_entries WHERE primary_aka = 'primary'"
    ) as cur:
        rows = await cur.fetchall()

    name_to_lists: dict[str, set] = defaultdict(set)
    for name, wl in rows:
        name_to_lists[name].add(wl)

    overlap: dict[tuple, int] = defaultdict(int)
    for lists in name_to_lists.values():
        if len(lists) > 1:
            for wl1, wl2 in combinations(sorted(lists), 2):
                overlap[(wl1, wl2)] += 1

    return [
        {"wl1": k[0], "wl2": k[1], "count": v}
        for k, v in sorted(overlap.items(), key=lambda x: -x[1])
    ]


@router.delete("/clear")
async def clear_database(db: aiosqlite.Connection = Depends(get_db)):
    """Delete all watchlist entries and download cache from the database."""
    await db.execute("DELETE FROM watchlist_entries")
    await db.execute("DELETE FROM download_log")
    await db.commit()
    return {"cleared": True}


@router.get("/filters")
async def get_filter_options(db: aiosqlite.Connection = Depends(get_db)):
    """Return distinct values for each dropdown filter."""
    result = {}
    for col, key in [
        ("watchlist", "watchlists"),
        ("entity_type", "entity_types"),
        ("sub_watchlist_1", "sub_watchlists"),
    ]:
        async with db.execute(
            f"SELECT DISTINCT {col} FROM watchlist_entries WHERE {col} IS NOT NULL ORDER BY {col}"
        ) as cur:
            rows = await cur.fetchall()
            result[key] = [r[0] for r in rows if r[0]]
    return result
