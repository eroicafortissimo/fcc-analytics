"""
Persistent culture cache service.

Maintains a `culture_cache` table (watchlist + uid → region / name_culture / confidence)
so that re-downloads don't lose previously inferred culture classifications.

Public API
----------
apply_culture_cache(db)      — copy cache → watchlist_entries for newly ingested rows
save_culture_cache(db)       — copy watchlist_entries → cache for all classified rows
infer_cultures_batch(db, n)  — classify one batch (heuristic then LLM)
run_full_classification()    — background task: cache → classify loop → cache
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re

import aiosqlite

logger = logging.getLogger(__name__)

_REGIONS = [
    "East Asian", "South & Southeast Asian", "Middle Eastern & North African",
    "Sub-Saharan African", "Western", "Other",
]
_CULTURES = [
    "Chinese", "Japanese", "Korean", "Vietnamese",
    "Indian/South Asian", "Pakistani/Bangladeshi", "Indonesian/Malay", "Filipino",
    "Arabic", "Persian/Farsi", "Turkish", "Hebrew/Israeli",
    "West African", "East African", "Southern African",
    "Anglo/Germanic", "Hispanic/Latino", "Romance", "Slavic/Eastern European",
    "South American Indigenous", "Central Asian", "Nordic",
]


async def apply_culture_cache(db: aiosqlite.Connection) -> int:
    """Update watchlist_entries rows that have NULL region using the culture cache.

    Returns the number of rows updated.
    """
    await db.execute("""
        UPDATE watchlist_entries
        SET
            region             = (SELECT cc.region       FROM culture_cache cc
                                   WHERE cc.watchlist = watchlist_entries.watchlist
                                     AND cc.uid       = watchlist_entries.uid
                                     AND cc.region IS NOT NULL),
            name_culture       = (SELECT cc.name_culture FROM culture_cache cc
                                   WHERE cc.watchlist = watchlist_entries.watchlist
                                     AND cc.uid       = watchlist_entries.uid
                                     AND cc.region IS NOT NULL),
            culture_confidence = (SELECT cc.confidence   FROM culture_cache cc
                                   WHERE cc.watchlist = watchlist_entries.watchlist
                                     AND cc.uid       = watchlist_entries.uid
                                     AND cc.region IS NOT NULL)
        WHERE region IS NULL
          AND EXISTS (
              SELECT 1 FROM culture_cache cc
              WHERE cc.watchlist = watchlist_entries.watchlist
                AND cc.uid       = watchlist_entries.uid
                AND cc.region IS NOT NULL
          )
    """)
    async with db.execute("SELECT changes()") as cur:
        row = await cur.fetchone()
    await db.commit()
    return row[0] if row else 0


async def save_culture_cache(db: aiosqlite.Connection) -> None:
    """Upsert all classified watchlist_entries rows into culture_cache."""
    await db.execute("""
        INSERT OR REPLACE INTO culture_cache
            (watchlist, uid, cleaned_name, name_culture, region, confidence)
        SELECT watchlist, uid, cleaned_name, name_culture, region, culture_confidence
        FROM watchlist_entries
        WHERE region IS NOT NULL
    """)
    await db.commit()


async def infer_cultures_batch(db: aiosqlite.Connection, batch_size: int = 500) -> dict:
    """Classify one batch of unclassified entries (heuristic first, then LLM).

    Returns dict: {processed, heuristic, llm, remaining}
    """
    from app.services.list_cleaner import get_culture

    async with db.execute(
        "SELECT uid, cleaned_name, nationality, sanctions_program "
        "FROM watchlist_entries WHERE region IS NULL LIMIT ?",
        (batch_size,),
    ) as cur:
        rows = await cur.fetchall()

    if not rows:
        async with db.execute(
            "SELECT COUNT(*) FROM watchlist_entries WHERE region IS NULL"
        ) as cur:
            remaining = (await cur.fetchone())[0]
        return {"processed": 0, "heuristic": 0, "llm": 0, "remaining": remaining}

    heuristic_updates: list[tuple] = []
    llm_candidates: list[dict] = []

    for uid, name, nationality, program in rows:
        region, name_culture, confidence = get_culture(nationality, name, program)
        if region:
            heuristic_updates.append((region, name_culture, confidence, uid))
        else:
            llm_candidates.append({
                "uid": uid, "name": name,
                "nationality": nationality or "", "program": program or "",
            })

    if heuristic_updates:
        await db.executemany(
            "UPDATE watchlist_entries SET region=?, name_culture=?, culture_confidence=? WHERE uid=?",
            heuristic_updates,
        )
        await db.commit()

    llm_updates: list[tuple] = []
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if llm_candidates and api_key:
        from anthropic import Anthropic
        client = Anthropic(api_key=api_key)
        chunk_size = 50
        for i in range(0, len(llm_candidates), chunk_size):
            chunk = llm_candidates[i: i + chunk_size]
            entries_text = "\n".join(
                f'{j + 1}. name="{c["name"]}" nationality="{c["nationality"]}" program="{c["program"]}"'
                for j, c in enumerate(chunk)
            )
            prompt = f"""Classify each sanctions list entity by region and name_culture.

Regions: {_REGIONS}
Name cultures: {_CULTURES}

Rules:
- Use nationality field when provided; otherwise use name patterns, scripts, or program code
- Program codes like IRAN→Persian/Farsi, DPRK→Korean, RUSSIA→Slavic, SYRIA→Arabic, etc.
- Company suffixes: OOO/ZAO→Slavic, SARL→Romance, GmbH→Anglo/Germanic, FZE/FZC→Arabic (UAE)
- Every entry MUST get a classification — pick the closest match; never leave blank
- Return JSON array: [{{"idx":1,"region":"...","name_culture":"...","confidence":"Low"}}]
- confidence: "High"=nationality obvious, "Medium"=name patterns clear, "Low"=best guess

Entries:
{entries_text}

Return ONLY the JSON array, no other text."""

            for attempt in range(3):
                try:
                    msg = client.messages.create(
                        model="claude-haiku-4-5-20251001",
                        max_tokens=2048,
                        messages=[{"role": "user", "content": prompt}],
                    )
                    text = msg.content[0].text.strip()
                    json_match = re.search(r'\[.*\]', text, re.DOTALL)
                    results = json.loads(json_match.group() if json_match else text)
                    for r in results:
                        idx = r.get("idx", 0) - 1
                        if 0 <= idx < len(chunk):
                            llm_updates.append((
                                r.get("region"), r.get("name_culture"),
                                r.get("confidence", "Low"), chunk[idx]["uid"],
                            ))
                    break
                except Exception as exc:
                    if attempt < 2 and "overload" in str(exc).lower():
                        wait = 2 ** attempt
                        logger.warning("Claude overloaded, retrying in %ds (attempt %d)", wait, attempt + 1)
                        await asyncio.sleep(wait)
                    else:
                        logger.warning("Culture LLM batch failed: %s", exc)
                        break

    if llm_updates:
        await db.executemany(
            "UPDATE watchlist_entries SET region=?, name_culture=?, culture_confidence=? WHERE uid=?",
            llm_updates,
        )
        await db.commit()

    async with db.execute(
        "SELECT COUNT(*) FROM watchlist_entries WHERE region IS NULL"
    ) as cur:
        remaining = (await cur.fetchone())[0]

    return {
        "processed": len(heuristic_updates) + len(llm_updates),
        "heuristic": len(heuristic_updates),
        "llm": len(llm_updates),
        "remaining": remaining,
    }


async def run_full_classification(batch_size: int = 500) -> None:
    """Background task: apply cache → classify loop → save cache.

    Designed to be called via FastAPI BackgroundTasks after a download/refresh.
    Opens its own DB connection so the HTTP response can be returned immediately.
    """
    from app.db.database import DB_PATH

    logger.info("Background culture classification started")
    try:
        async with aiosqlite.connect(str(DB_PATH), timeout=120) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("PRAGMA synchronous=NORMAL")

            # Step 1: Apply previously cached cultures to any newly ingested records
            applied = await apply_culture_cache(db)
            logger.info("Culture cache: applied %d cached entries", applied)

            # Step 2: Classify remaining unknowns until fully done
            no_progress_streak = 0
            while True:
                result = await infer_cultures_batch(db, batch_size)
                logger.info("Culture batch result: %s", result)
                if result["remaining"] == 0:
                    break
                if result["processed"] == 0:
                    no_progress_streak += 1
                    if no_progress_streak >= 3:
                        logger.warning(
                            "Culture classification stalled — %d entries remain unclassified",
                            result["remaining"],
                        )
                        break
                    await asyncio.sleep(3)
                else:
                    no_progress_streak = 0

            # Step 3: Persist all classified entries back to the cache
            await save_culture_cache(db)
            logger.info("Culture cache saved")

    except Exception as exc:
        logger.error("Background culture classification failed: %s", exc, exc_info=True)
