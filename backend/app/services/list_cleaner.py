"""
list_cleaner.py
Normalizes raw entry dicts from the parsers, then upserts into SQLite.
Also provides read helpers for the /entries and /summary endpoints.
"""
from __future__ import annotations

import re
import unicodedata
from datetime import date, datetime, timezone, timedelta
from typing import Any
import aiosqlite

from app.models.schemas import ListFilters, WatchlistSummary


# ── Cleaning Pipeline ──────────────────────────────────────────────────────────

def clean_name(raw: str) -> str:
    """
    Normalize a raw name string:
    1. Unicode NFC normalization
    2. Remove control characters
    3. Collapse multiple spaces
    4. Strip leading/trailing whitespace
    5. Title-case only if the name is entirely uppercase (common in sanctions lists)
    """
    if not raw:
        return ""

    # NFC normalize (handles composed vs decomposed Unicode)
    name = unicodedata.normalize("NFC", raw)

    # Remove control characters except regular spaces
    name = "".join(ch for ch in name if unicodedata.category(ch) != "Cc" or ch == " ")

    # Collapse whitespace
    name = re.sub(r"\s+", " ", name).strip()

    # Convert ALL-CAPS to title case (but preserve mixed-case like "McDonald")
    if name == name.upper() and any(c.isalpha() for c in name):
        name = _smart_title(name)

    return name


def _smart_title(s: str) -> str:
    """
    Title-case a string while preserving common particles and known acronyms.
    """
    lowercase_particles = {"al", "el", "bin", "bint", "abu", "um", "ibn", "van", "de", "der", "von", "and", "of"}
    words = s.split()
    result: list[str] = []
    for i, word in enumerate(words):
        # Preserve numbers and acronyms already in good form
        stripped = word.strip(".,;:-()\"'")
        if stripped.lower() in lowercase_particles and i != 0:
            result.append(word.lower())
        else:
            result.append(word.capitalize())
    return " ".join(result)


def count_tokens(name: str) -> int:
    return len(name.split()) if name else 0


def detect_recently_modified(date_str: str | None, days: int = 90) -> bool:
    if not date_str:
        return False
    try:
        listed = datetime.strptime(date_str[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - listed).days <= days
    except ValueError:
        return False


def normalize_entity_type(raw: str) -> str:
    mapping = {
        "individual": "individual",
        "person": "individual",
        "entity": "entity",
        "organisation": "entity",
        "organization": "entity",
        "company": "entity",
        "vessel": "vessel",
        "ship": "vessel",
        "aircraft": "aircraft",
        "plane": "aircraft",
        "country": "country",
    }
    return mapping.get((raw or "").lower().strip(), "unknown")


# ── Upsert ─────────────────────────────────────────────────────────────────────

async def clean_and_upsert(
    entries: list[dict[str, Any]],
    watchlist_key: str,
    db: aiosqlite.Connection,
) -> int:
    """
    Clean each raw entry dict and upsert into watchlist_entries table.
    Returns count of inserted/updated rows.
    """
    if not entries:
        return 0

    rows: list[tuple] = []
    for e in entries:
        uid = e.get("uid", "")
        original_name = (e.get("original_name") or "").strip()
        if not original_name or not uid:
            continue

        cleaned = clean_name(original_name)
        if not cleaned:
            continue

        entity_type = normalize_entity_type(e.get("entity_type", "unknown"))
        date_listed = e.get("date_listed")
        recently_mod = 1 if detect_recently_modified(date_listed) else 0

        rows.append((
            uid,
            watchlist_key,
            e.get("sub_watchlist_1"),
            e.get("sub_watchlist_2"),
            cleaned,
            original_name,
            e.get("primary_aka", "primary"),
            entity_type,
            count_tokens(cleaned),
            len(cleaned),
            None,   # nationality — filled later by nationality_chain
            None,   # nationality_confidence
            None,   # nationality_method
            date_listed,
            recently_mod,
            e.get("sanctions_program"),
        ))

    if not rows:
        return 0

    await db.executemany(
        """INSERT INTO watchlist_entries
           (uid, watchlist, sub_watchlist_1, sub_watchlist_2,
            cleaned_name, original_name, primary_aka, entity_type,
            num_tokens, name_length,
            nationality, nationality_confidence, nationality_method,
            date_listed, recently_modified, sanctions_program)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(uid) DO UPDATE SET
             cleaned_name       = excluded.cleaned_name,
             original_name      = excluded.original_name,
             sub_watchlist_1    = excluded.sub_watchlist_1,
             sub_watchlist_2    = excluded.sub_watchlist_2,
             primary_aka        = excluded.primary_aka,
             entity_type        = excluded.entity_type,
             num_tokens         = excluded.num_tokens,
             name_length        = excluded.name_length,
             date_listed        = excluded.date_listed,
             recently_modified  = excluded.recently_modified,
             sanctions_program  = excluded.sanctions_program
        """,
        rows,
    )
    await db.commit()
    return len(rows)


# ── Read Helpers ───────────────────────────────────────────────────────────────

def _build_where(filters: ListFilters) -> tuple[str, list[Any]]:
    """Return (WHERE clause string, params list) for the given filters."""
    conditions: list[str] = []
    params: list[Any] = []

    if filters.watchlists:
        placeholders = ", ".join("?" for _ in filters.watchlists)
        conditions.append(f"watchlist IN ({placeholders})")
        params.extend(filters.watchlists)

    if filters.entity_types:
        placeholders = ", ".join("?" for _ in filters.entity_types)
        conditions.append(f"entity_type IN ({placeholders})")
        params.extend(filters.entity_types)

    if filters.nationalities:
        placeholders = ", ".join("?" for _ in filters.nationalities)
        conditions.append(f"nationality IN ({placeholders})")
        params.extend(filters.nationalities)

    if filters.search:
        conditions.append("(cleaned_name LIKE ? OR original_name LIKE ?)")
        like = f"%{filters.search}%"
        params.extend([like, like])

    if filters.recently_modified_only:
        conditions.append("recently_modified = 1")

    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    return where, params


async def get_chart_data(filters: ListFilters, db: aiosqlite.Connection) -> dict:
    """Return all chart data for the current filter state."""
    where, params = _build_where(filters)

    async def query(sql: str, p: list = None) -> list:
        async with db.execute(sql, p if p is not None else params) as cur:
            return await cur.fetchall()

    # By watchlist
    rows = await query(
        f"SELECT watchlist, COUNT(*) as count FROM watchlist_entries {where} GROUP BY watchlist ORDER BY count DESC"
    )
    by_watchlist = [{"name": r[0], "count": r[1]} for r in rows]

    # By entity type
    rows = await query(
        f"SELECT entity_type, COUNT(*) as count FROM watchlist_entries {where} GROUP BY entity_type ORDER BY count DESC"
    )
    by_entity_type = [{"name": r[0], "value": r[1]} for r in rows]

    # By nationality (top 20, excluding NULL/Unknown)
    nat_where = where + (" AND " if where else "WHERE ") + "nationality IS NOT NULL AND nationality != 'Unknown'"
    rows = await query(
        f"SELECT nationality, COUNT(*) as count FROM watchlist_entries {nat_where} GROUP BY nationality ORDER BY count DESC LIMIT 20",
        params,
    )
    by_nationality = [{"name": r[0], "count": r[1]} for r in rows]

    # Name length histogram (bucketed)
    length_buckets = [
        ("1-5",   "name_length BETWEEN 1 AND 5"),
        ("6-10",  "name_length BETWEEN 6 AND 10"),
        ("11-15", "name_length BETWEEN 11 AND 15"),
        ("16-20", "name_length BETWEEN 16 AND 20"),
        ("21-30", "name_length BETWEEN 21 AND 30"),
        ("31-40", "name_length BETWEEN 31 AND 40"),
        ("41-50", "name_length BETWEEN 41 AND 50"),
        ("51+",   "name_length > 50"),
    ]
    name_length_hist = []
    for label, cond in length_buckets:
        extra = f" AND {cond}" if where else f"WHERE {cond}"
        rows = await query(
            f"SELECT COUNT(*) FROM watchlist_entries {where}{extra}",
            params,
        )
        name_length_hist.append({"bucket": label, "count": rows[0][0]})

    # Token count histogram (1-10, then 11+)
    token_hist = []
    for t in range(1, 11):
        extra = f" AND num_tokens = {t}" if where else f"WHERE num_tokens = {t}"
        rows = await query(f"SELECT COUNT(*) FROM watchlist_entries {where}{extra}", params)
        token_hist.append({"tokens": str(t), "count": rows[0][0]})
    extra = f" AND num_tokens > 10" if where else "WHERE num_tokens > 10"
    rows = await query(f"SELECT COUNT(*) FROM watchlist_entries {where}{extra}", params)
    token_hist.append({"tokens": "11+", "count": rows[0][0]})

    # Recently modified count
    rm_extra = f" AND recently_modified = 1" if where else "WHERE recently_modified = 1"
    rows = await query(f"SELECT COUNT(*) FROM watchlist_entries {where}{rm_extra}", params)
    recently_modified_count = rows[0][0]

    # Total
    rows = await query(f"SELECT COUNT(*) FROM watchlist_entries {where}")
    total = rows[0][0]

    return {
        "total": total,
        "by_watchlist": by_watchlist,
        "by_entity_type": by_entity_type,
        "by_nationality": by_nationality,
        "name_length_hist": name_length_hist,
        "token_count_hist": token_hist,
        "recently_modified_count": recently_modified_count,
    }


async def get_entries_from_db(filters: ListFilters, db: aiosqlite.Connection) -> dict:
    where, params = _build_where(filters)

    count_sql = f"SELECT COUNT(*) FROM watchlist_entries {where}"
    async with db.execute(count_sql, params) as cur:
        total = (await cur.fetchone())[0]

    # Paginated data
    offset = (filters.page - 1) * filters.page_size
    data_sql = f"""
        SELECT uid, watchlist, sub_watchlist_1, sub_watchlist_2,
               cleaned_name, original_name, primary_aka, entity_type,
               num_tokens, name_length, nationality, nationality_confidence,
               nationality_method, date_listed, recently_modified, sanctions_program
        FROM watchlist_entries
        {where}
        ORDER BY cleaned_name
        LIMIT ? OFFSET ?
    """
    async with db.execute(data_sql, params + [filters.page_size, offset]) as cur:
        rows = await cur.fetchall()

    return {
        "total": total,
        "page": filters.page,
        "page_size": filters.page_size,
        "items": [_row_to_dict(r) for r in rows],
    }


def _row_to_dict(row: aiosqlite.Row) -> dict:
    d = dict(row)
    d["recently_modified"] = bool(d.get("recently_modified"))
    return d


async def get_summary(db: aiosqlite.Connection) -> WatchlistSummary:
    async with db.execute("SELECT COUNT(*) FROM watchlist_entries") as cur:
        total = (await cur.fetchone())[0]

    async with db.execute(
        "SELECT watchlist, COUNT(*) FROM watchlist_entries GROUP BY watchlist"
    ) as cur:
        by_watchlist = {r[0]: r[1] for r in await cur.fetchall()}

    async with db.execute(
        "SELECT entity_type, COUNT(*) FROM watchlist_entries GROUP BY entity_type"
    ) as cur:
        by_entity = {r[0]: r[1] for r in await cur.fetchall()}

    async with db.execute(
        "SELECT timestamp FROM download_log ORDER BY timestamp DESC LIMIT 1"
    ) as cur:
        row = await cur.fetchone()
    last_updated = datetime.fromisoformat(row[0]) if row else None

    return WatchlistSummary(
        total=total,
        by_watchlist=by_watchlist,
        by_entity_type=by_entity,
        last_updated=last_updated,
    )
