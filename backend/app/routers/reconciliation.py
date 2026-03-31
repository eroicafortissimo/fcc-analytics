"""
List Reconciliation router.

POST /run          — upload private list + select watchlists → job_id
GET  /status/{id} — poll job progress
GET  /results/{id} — retrieve paginated results
"""

from fastapi import APIRouter, UploadFile, File, Form, BackgroundTasks, HTTPException, Query
from fastapi.responses import StreamingResponse
from typing import List, Optional
from pathlib import Path
import uuid
import traceback
import csv
import io
import aiosqlite

from app.services.reconciliation_service import (
    parse_private_list,
    run_reconciliation,
)

router = APIRouter()

# In-memory job store  {job_id → job_dict}
_jobs: dict = {}

DB_PATH = Path(__file__).parent.parent / "db" / "platform2.db"


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/run")
async def start_reconciliation(
    background_tasks: BackgroundTasks,
    files: List[UploadFile] = File(...),
    watchlists: List[str] = Form(...),
    use_ai: bool = Form(True),
):
    file_payloads = []
    for f in files:
        file_payloads.append((await f.read(), f.filename or "upload.csv"))

    job_id = str(uuid.uuid4())
    _jobs[job_id] = {
        "status": "running",
        "progress": 0,
        "message": "Queued…",
        "result": None,
        "error": None,
    }
    background_tasks.add_task(
        _run_job, job_id, file_payloads, watchlists, use_ai
    )
    return {"job_id": job_id}


@router.get("/status/{job_id}")
async def get_status(job_id: str):
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    return {
        "status": job["status"],
        "progress": job["progress"],
        "message": job["message"],
        "error": job.get("error"),
    }


def _apply_filters(data: list, section: str, watchlist, entity_type, search) -> list:
    if section != "private_list":
        if watchlist:
            data = [e for e in data if (e.get("watchlist") or "") == watchlist]
        if entity_type:
            data = [e for e in data if (e.get("entity_type") or "").lower() == entity_type.lower()]
    if search:
        q = search.strip().lower()
        data = [
            e for e in data
            if q in (e.get("name") or "").lower()
            or any(q in (a or "").lower() for a in (e.get("akas") or []))
        ]
    return data


@router.get("/results/{job_id}")
async def get_results(
    job_id: str,
    section: str = "public_not_on_private",
    page: int = 1,
    page_size: int = 50,
    watchlist: Optional[str] = Query(None),
    entity_type: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
):
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    if job["status"] == "error":
        raise HTTPException(400, job.get("error", "Job failed"))
    if job["status"] != "done":
        raise HTTPException(400, "Job not complete yet")

    result = job["result"]
    data = _apply_filters(result.get(section, []), section, watchlist, entity_type, search)

    total = len(data)
    start = (page - 1) * page_size

    return {
        "stats": result["stats"],
        "name_col": result.get("name_col"),
        "aka_col": result.get("aka_col"),
        "section": section,
        "page": page,
        "page_size": page_size,
        "total": total,
        "entries": data[start: start + page_size],
    }


# Column definitions per section for CSV export
_EXPORT_COLS = {
    "full_public":           ["name", "is_primary", "watchlist", "entity_type", "sanctions_program", "date_listed", "uid", "match_tier", "matched_to", "matched_key", "akas"],
    "public_not_on_private": ["name", "watchlist", "entity_type", "sanctions_program", "date_listed", "uid", "akas"],
    "private_not_on_public": ["name", "akas"],
    "matches":               ["name", "watchlist", "entity_type", "match_tier", "uid", "matched_to", "matched_key"],
    "public_list":           ["name", "watchlist", "entity_type", "sanctions_program", "date_listed", "uid", "akas"],
    "private_list":          ["key", "name", "akas"],
}


@router.get("/export/{job_id}")
async def export_results(
    job_id: str,
    section: str = "public_not_on_private",
    watchlist: Optional[str] = Query(None),
    entity_type: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
):
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    if job["status"] != "done":
        raise HTTPException(400, "Job not complete yet")

    result = job["result"]
    # public_list is an alias for full_public on the backend
    backend_section = "full_public" if section == "public_list" else section
    data = _apply_filters(result.get(backend_section, []), section, watchlist, entity_type, search)

    cols = _EXPORT_COLS.get(section, list(data[0].keys()) if data else [])

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=cols, extrasaction="ignore")
    writer.writeheader()
    for entry in data:
        row = {k: v for k, v in entry.items() if k in cols}
        if "akas" in row and isinstance(row["akas"], list):
            row["akas"] = " | ".join(row["akas"])
        writer.writerow(row)

    buf.seek(0)
    filename = f"reconciliation_{section}_{job_id[:8]}.csv"
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


# ---------------------------------------------------------------------------
# Background job
# ---------------------------------------------------------------------------

async def _run_job(
    job_id: str,
    file_payloads: List[tuple],   # [(bytes, filename), ...]
    watchlists: List[str],
    use_ai: bool,
) -> None:
    job = _jobs[job_id]

    async def progress(pct: int, msg: str) -> None:
        job["progress"] = pct
        job["message"] = msg

    try:
        await progress(2, f"Parsing {len(file_payloads)} private list file(s)…")

        all_private_entries = []
        name_col = None
        aka_col = None
        seen_names: set = set()

        for file_bytes, filename in file_payloads:
            entries, nc, ac = parse_private_list(file_bytes, filename)
            if name_col is None:
                name_col = nc
            if aka_col is None and ac:
                aka_col = ac
            for e in entries:
                key = e['name'].strip().lower()
                if key not in seen_names:
                    seen_names.add(key)
                    all_private_entries.append(e)

        private_entries = all_private_entries
        n_priv = len(private_entries)
        aka_info = f", aka col: '{aka_col}'" if aka_col else ""
        await progress(8, f"Loaded {n_priv} entries from {len(file_payloads)} file(s) (name col: '{name_col}'{aka_info})…")

        if not watchlists:
            raise ValueError("No watchlists selected.")

        await progress(10, f"Loading public entries from {', '.join(watchlists)}…")

        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            placeholders = ",".join("?" * len(watchlists))
            rows = await db.execute_fetchall(
                f"""
                SELECT uid, cleaned_name, original_name, watchlist, primary_aka,
                       sanctions_program, entity_type, date_listed, parent_uid
                FROM watchlist_entries
                WHERE watchlist IN ({placeholders})
                ORDER BY watchlist, parent_uid NULLS LAST, primary_aka
                """,
                watchlists,
            )

        # Group AKAs under their primary entries
        primary_map: dict = {}
        orphan_akas: list = []

        for row in rows:
            uid = row["uid"]
            name = row["cleaned_name"] or row["original_name"] or ""
            if not name:
                continue

            is_primary = (row["primary_aka"] or "primary") == "primary"

            if is_primary:
                primary_map[uid] = {
                    "uid": uid,
                    "name": name,
                    "akas": [],
                    "watchlist": row["watchlist"],
                    "entity_type": row["entity_type"] or "unknown",
                    "sanctions_program": row["sanctions_program"],
                    "date_listed": row["date_listed"],
                    "is_primary": True,
                }
            else:
                parent = primary_map.get(row["parent_uid"])
                if parent:
                    parent["akas"].append(name)
                else:
                    orphan_akas.append({
                        "uid": uid,
                        "name": name,
                        "akas": [],
                        "watchlist": row["watchlist"],
                        "entity_type": row["entity_type"] or "unknown",
                        "sanctions_program": row["sanctions_program"],
                        "date_listed": row["date_listed"],
                        "is_primary": False,
                    })

        public_entities = list(primary_map.values()) + orphan_akas

        # Assign sequential keys to private entries for cross-reference
        for i, e in enumerate(private_entries, 1):
            e['key'] = str(i)

        if not public_entities:
            raise ValueError(
                f"No entries found for the selected watchlist(s): {', '.join(watchlists)}. "
                "Make sure you have downloaded the watchlists first."
            )

        await progress(20, f"Loaded {len(public_entities)} public entities…")

        result = await run_reconciliation(
            public_entities,
            private_entries,
            use_ai=use_ai,
            progress_cb=progress,
        )
        result["name_col"] = name_col
        result["aka_col"] = aka_col

        job["result"] = result
        job["status"] = "done"
        job["progress"] = 100
        stats = result["stats"]
        job["message"] = (
            f"Done — {stats['unmatched_public']} public gaps, "
            f"{stats['unmatched_private']} private extras"
        )

    except Exception as exc:
        job["status"] = "error"
        job["error"] = str(exc)
        job["message"] = f"Error: {exc}"
        print(traceback.format_exc())
