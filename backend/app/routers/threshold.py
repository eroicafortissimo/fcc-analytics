"""
threshold.py — API router for the Threshold Setting module.
Routes: /api/threshold/...
"""
from __future__ import annotations
import json
from datetime import timedelta
from typing import Optional

import pandas as pd
import aiosqlite
from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from pydantic import BaseModel

from app.db.database import get_db
from app.services import threshold_service as svc

router = APIRouter()


# ── Pydantic models ────────────────────────────────────────────────────────────

class FilterCondition(BaseModel):
    column: str
    operator: str
    value: object = None

class FilterGroup(BaseModel):
    operator: str = "AND"
    conditions: list[FilterCondition] = []

class FilterRules(BaseModel):
    group_operator: str = "AND"
    groups: list[FilterGroup] = []

class ScenarioCreate(BaseModel):
    dataset_id: int
    name: str
    description: str = ""
    filter_rules: dict = {}
    analysis_type: str = "single"  # "single" or "aggregate"
    aggregation_key: str = ""
    aggregation_amount: str = ""
    aggregation_date: str = ""
    aggregation_period: str = "none"
    aggregation_days: int = 30
    aggregation_function: str = "SUM"

class AnalysisRequest(BaseModel):
    dataset_id: int
    scenario_id: Optional[int] = None
    filter_rules: dict = {}
    analysis_type: str = "single"
    aggregation_key: str = ""
    aggregation_amount: str = ""
    aggregation_date: str = ""
    aggregation_period: str = "none"
    aggregation_days: int = 30
    aggregation_function: str = "SUM"
    parameter_column: str = ""
    boundaries: Optional[list[float]] = None

class SimulateRequest(BaseModel):
    dataset_id: int
    analysis_id: Optional[int] = None
    filter_rules: dict = {}
    analysis_type: str = "single"
    aggregation_key: str = ""
    aggregation_amount: str = ""
    aggregation_date: str = ""
    aggregation_period: str = "none"
    aggregation_days: int = 30
    aggregation_function: str = "SUM"
    parameter_column: str = ""
    thresholds: list[float] = []
    target_monthly_alerts: Optional[int] = None

class AIScenarioRequest(BaseModel):
    dataset_id: int
    prompt: str

class ReportRequest(BaseModel):
    analysis_id: int

class AIPromptScenario(BaseModel):
    dataset_id: int
    prompt: str


# ── Auto-reload helper ─────────────────────────────────────────────────────────

async def _get_mem(dataset_id: int, db: aiosqlite.Connection) -> dict:
    """Return in-memory dataset, reloading from stored bytes if needed. Never returns None."""
    mem = svc.get_dataset(dataset_id)
    if mem:
        return mem
    # Not in memory — try to reload from stored file bytes
    async with db.execute(
        "SELECT name, file_name, file_data FROM threshold_datasets WHERE id = ?", (dataset_id,)
    ) as cur:
        row = await cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"Dataset {dataset_id} not found")
    if not row["file_data"]:
        raise HTTPException(status_code=404, detail="Dataset file not stored — please re-upload your file once to enable auto-reload")
    try:
        parsed = svc.parse_upload(bytes(row["file_data"]), row["file_name"] or "upload.csv")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to reload dataset: {e}")
    svc.store_dataset(dataset_id, parsed["df"], {
        "name": row["name"], "file_name": row["file_name"],
        "columns": parsed["columns"], "sample_values": parsed["sample_values"],
        "row_count": parsed["row_count"],
        "date_range_start": parsed["date_range_start"],
        "date_range_end": parsed["date_range_end"],
    })
    return svc.get_dataset(dataset_id)


# ── Datasets ───────────────────────────────────────────────────────────────────

@router.post("/datasets/upload")
async def upload_dataset(
    file: UploadFile = File(...),
    name: str = Form(""),
    db: aiosqlite.Connection = Depends(get_db),
):
    """Upload a CSV or Excel transaction file."""
    raw = await file.read()
    try:
        parsed = svc.parse_upload(raw, file.filename or "upload.csv")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Persist metadata + raw bytes to DB
    async with db.execute(
        """INSERT INTO threshold_datasets (name, file_name, row_count, column_list,
           date_range_start, date_range_end, file_data)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            name or file.filename,
            file.filename,
            parsed["row_count"],
            json.dumps([c["name"] for c in parsed["columns"]]),
            parsed["date_range_start"],
            parsed["date_range_end"],
            raw,
        ),
    ) as cur:
        dataset_id = cur.lastrowid
    await db.commit()

    # Store in memory
    svc.store_dataset(dataset_id, parsed["df"], {
        "name": name or file.filename,
        "file_name": file.filename,
        "columns": parsed["columns"],
        "sample_values": parsed["sample_values"],
        "row_count": parsed["row_count"],
        "date_range_start": parsed["date_range_start"],
        "date_range_end": parsed["date_range_end"],
    })

    return {
        "id": dataset_id,
        "name": name or file.filename,
        "file_name": file.filename,
        "row_count": parsed["row_count"],
        "columns": parsed["columns"],
        "sample_values": parsed["sample_values"],
        "date_range_start": parsed["date_range_start"],
        "date_range_end": parsed["date_range_end"],
    }


@router.get("/datasets")
async def list_datasets(db: aiosqlite.Connection = Depends(get_db)):
    async with db.execute(
        "SELECT id, name, file_name, row_count, column_list, date_range_start, date_range_end, uploaded_at, (file_data IS NOT NULL) as has_file FROM threshold_datasets ORDER BY uploaded_at DESC"
    ) as cur:
        rows = await cur.fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d["columns"] = json.loads(d["column_list"] or "[]")
        d["in_memory"] = svc.get_dataset(d["id"]) is not None
        d["has_file"] = bool(d["has_file"])
        result.append(d)
    return result


@router.get("/datasets/{dataset_id}")
async def get_dataset(dataset_id: int, db: aiosqlite.Connection = Depends(get_db)):
    async with db.execute(
        "SELECT id, name, file_name, row_count, column_list, date_range_start, date_range_end, uploaded_at FROM threshold_datasets WHERE id = ?",
        (dataset_id,),
    ) as cur:
        row = await cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Dataset not found")
    d = dict(row)
    d["columns"] = json.loads(d["column_list"] or "[]")
    mem = svc.get_dataset(dataset_id)
    d["in_memory"] = mem is not None
    if mem:
        d["sample_values"] = mem.get("sample_values", {})
        d["columns"] = mem.get("columns", d["columns"])
    return d


@router.get("/datasets/{dataset_id}/preview")
async def preview_dataset(dataset_id: int, rows: int = Query(default=100, le=500), db: aiosqlite.Connection = Depends(get_db)):
    mem = await _get_mem(dataset_id, db)
    df = mem["df"].head(rows)
    return {
        "columns": [c["name"] for c in mem["columns"]],
        "rows": df.fillna("").to_dict(orient="records"),
    }


@router.delete("/datasets/{dataset_id}")
async def delete_dataset(dataset_id: int, db: aiosqlite.Connection = Depends(get_db)):
    svc.delete_dataset(dataset_id)
    await db.execute("DELETE FROM threshold_datasets WHERE id = ?", (dataset_id,))
    await db.commit()
    return {"deleted": True}


@router.post("/datasets/{dataset_id}/reupload")
async def reupload_dataset(
    dataset_id: int,
    file: UploadFile = File(...),
    db: aiosqlite.Connection = Depends(get_db),
):
    """Replace the stored file bytes for an existing dataset record."""
    async with db.execute("SELECT id FROM threshold_datasets WHERE id = ?", (dataset_id,)) as cur:
        if not await cur.fetchone():
            raise HTTPException(status_code=404, detail="Dataset not found")
    raw = await file.read()
    try:
        parsed = svc.parse_upload(raw, file.filename or "upload.csv")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    await db.execute(
        """UPDATE threshold_datasets
           SET file_name=?, file_data=?, row_count=?, column_list=?,
               date_range_start=?, date_range_end=?
           WHERE id=?""",
        (
            file.filename, raw, parsed["row_count"],
            json.dumps([c["name"] for c in parsed["columns"]]),
            parsed["date_range_start"], parsed["date_range_end"],
            dataset_id,
        ),
    )
    await db.commit()
    svc.store_dataset(dataset_id, parsed["df"], {
        "name": file.filename,
        "file_name": file.filename,
        "columns": parsed["columns"],
        "sample_values": parsed["sample_values"],
        "row_count": parsed["row_count"],
        "date_range_start": parsed["date_range_start"],
        "date_range_end": parsed["date_range_end"],
    })
    return {
        "status": "ok", "row_count": parsed["row_count"],
        "columns": parsed["columns"],
        "date_range_start": parsed["date_range_start"],
        "date_range_end": parsed["date_range_end"],
    }


@router.post("/datasets/{dataset_id}/reload")
async def reload_dataset(dataset_id: int, db: aiosqlite.Connection = Depends(get_db)):
    """Re-parse stored file bytes back into the in-memory store (survives server restarts)."""
    if svc.get_dataset(dataset_id):
        return {"status": "already_in_memory"}
    async with db.execute(
        "SELECT name, file_name, file_data FROM threshold_datasets WHERE id = ?", (dataset_id,)
    ) as cur:
        row = await cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Dataset not found")
    if not row["file_data"]:
        raise HTTPException(status_code=404, detail="No stored file data — please re-upload the file")
    try:
        parsed = svc.parse_upload(bytes(row["file_data"]), row["file_name"] or "upload.csv")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to re-parse file: {e}")
    svc.store_dataset(dataset_id, parsed["df"], {
        "name": row["name"],
        "file_name": row["file_name"],
        "columns": parsed["columns"],
        "sample_values": parsed["sample_values"],
        "row_count": parsed["row_count"],
        "date_range_start": parsed["date_range_start"],
        "date_range_end": parsed["date_range_end"],
    })
    return {"status": "loaded", "row_count": parsed["row_count"], "columns": parsed["columns"]}


# ── Scenarios ──────────────────────────────────────────────────────────────────

@router.post("/scenarios")
async def create_scenario(body: ScenarioCreate, db: aiosqlite.Connection = Depends(get_db)):
    async with db.execute(
        """INSERT INTO threshold_scenarios
           (dataset_id, name, description, filter_rules, analysis_type,
            aggregation_key, aggregation_amount, aggregation_date,
            aggregation_period, aggregation_days, aggregation_function)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            body.dataset_id, body.name, body.description,
            json.dumps(body.filter_rules), body.analysis_type,
            body.aggregation_key, body.aggregation_amount, body.aggregation_date,
            body.aggregation_period, body.aggregation_days, body.aggregation_function,
        ),
    ) as cur:
        sid = cur.lastrowid
    await db.commit()
    return {"id": sid, "name": body.name}


@router.post("/scenarios/ai")
async def ai_create_scenario(body: AIScenarioRequest, db: aiosqlite.Connection = Depends(get_db)):
    """Generate a scenario from natural language using Claude."""
    mem = await _get_mem(body.dataset_id, db)
    try:
        result = await svc.ai_generate_scenario(
            body.prompt, mem["columns"], mem["sample_values"]
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return result


@router.get("/scenarios")
async def list_scenarios(
    dataset_id: Optional[int] = Query(default=None),
    db: aiosqlite.Connection = Depends(get_db),
):
    q = "SELECT * FROM threshold_scenarios"
    params = []
    if dataset_id:
        q += " WHERE dataset_id = ?"
        params.append(dataset_id)
    q += " ORDER BY created_at DESC"
    async with db.execute(q, params) as cur:
        rows = await cur.fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d["filter_rules"] = json.loads(d.get("filter_rules") or "{}")
        result.append(d)
    return result


@router.get("/scenarios/{scenario_id}")
async def get_scenario(scenario_id: int, db: aiosqlite.Connection = Depends(get_db)):
    async with db.execute("SELECT * FROM threshold_scenarios WHERE id = ?", (scenario_id,)) as cur:
        row = await cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Scenario not found")
    d = dict(row)
    d["filter_rules"] = json.loads(d.get("filter_rules") or "{}")
    return d


@router.delete("/scenarios/{scenario_id}")
async def delete_scenario(scenario_id: int, db: aiosqlite.Connection = Depends(get_db)):
    await db.execute("DELETE FROM threshold_scenarios WHERE id = ?", (scenario_id,))
    await db.commit()
    return {"deleted": True}


@router.post("/scenarios/{scenario_id}/preview")
async def preview_scenario(scenario_id: int, db: aiosqlite.Connection = Depends(get_db)):
    """Apply scenario filters and return match count + first 20 rows."""
    async with db.execute("SELECT * FROM threshold_scenarios WHERE id = ?", (scenario_id,)) as cur:
        row = await cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Scenario not found")
    sc = dict(row)
    mem = await _get_mem(sc["dataset_id"], db)
    filter_rules = json.loads(sc.get("filter_rules") or "{}")
    df = svc.apply_filters(mem["df"], filter_rules)
    preview = df.head(20).fillna("").to_dict(orient="records")
    return {"total": len(df), "original_total": len(mem["df"]), "preview": preview}


# ── Analysis ───────────────────────────────────────────────────────────────────

@router.post("/analysis")
async def run_analysis(body: AnalysisRequest, db: aiosqlite.Connection = Depends(get_db)):
    """Run statistical analysis on filtered/aggregated data for a chosen column."""
    mem = await _get_mem(body.dataset_id, db)

    raw_df = svc.apply_filters(mem["df"], body.filter_rules)
    if len(raw_df) == 0:
        raise HTTPException(status_code=400, detail="No rows match the current filters")

    # Aggregation
    is_aggregate = body.analysis_type == "aggregate" and body.aggregation_key and body.aggregation_amount
    if is_aggregate:
        try:
            df = svc.aggregate_transactions(
                raw_df,
                key_column=body.aggregation_key,
                amount_column=body.aggregation_amount,
                period=body.aggregation_period,
                agg_function=body.aggregation_function,
                date_column=body.aggregation_date or None,
            )
            analysis_column = "agg_value"
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Aggregation error: {e}")
    else:
        df = raw_df
        analysis_column = body.parameter_column

    if not analysis_column or analysis_column not in df.columns:
        raise HTTPException(status_code=400, detail=f"Column '{analysis_column}' not found in data")

    series = df[analysis_column]
    col_meta = next((c for c in mem["columns"] if c["name"] == body.parameter_column), None)
    is_categorical = col_meta and col_meta.get("kind") == "categorical"

    if is_categorical:
        dist = svc.categorical_distribution(series)
        stats = {}
        boundaries = []
        tranches = []
        trim_tranches = []
    else:
        stats = svc.compute_statistics(series)
        s_trim_mild = stats.pop("_s_trim_mild", None)
        boundaries = body.boundaries or svc.auto_tranches(series)
        tranches = svc.tranche_distribution(series, boundaries)
        trim_tranches = svc.tranche_distribution(s_trim_mild, boundaries) if s_trim_mild is not None else []
        dist = {}

    # Save to DB
    param_col = body.parameter_column or analysis_column
    series_values = pd.to_numeric(series, errors="coerce").dropna().tolist()
    key_values = df[body.aggregation_key].tolist() if body.aggregation_key and body.aggregation_key in df.columns else []
    date_values = df[body.aggregation_date].astype(str).tolist() if body.aggregation_date and body.aggregation_date in df.columns else []

    async with db.execute(
        """INSERT INTO threshold_analyses
           (scenario_id, parameter_columns, statistics, threshold_values, threshold_results,
            recommended_threshold, recommendation_reason, report_text, series_data)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            body.scenario_id, json.dumps([param_col]),
            json.dumps(stats), json.dumps([]),
            json.dumps({}), None, None, None,
            json.dumps({"values": series_values, "keys": key_values, "dates": date_values}),
        ),
    ) as cur:
        analysis_id = cur.lastrowid
    await db.commit()

    # Sample up to 800 values for scatter plot
    s_num_all = pd.to_numeric(series, errors="coerce").dropna()
    sample = s_num_all.sample(min(800, len(s_num_all)), random_state=42).tolist() if len(s_num_all) else []

    # ── Applicable events (tab 2) ───────────────────────────────────────────
    tid_col = next((c for c in ["transaction_id", "txn_id", "id", "reference_id"] if c in raw_df.columns), None)

    # Detect rolling window mode
    period = body.aggregation_period or "none"
    is_rolling = period.startswith("rolling_") or period.startswith("custom_")
    rolling_days = None
    if is_rolling:
        try:
            rolling_days = int(period.split("_")[1])
        except (IndexError, ValueError):
            is_rolling = False

    if is_aggregate:
        events_out = []
        for _, row in df.iterrows():
            ekey = row.get(body.aggregation_key)
            date_start = date_end = days = None

            if is_rolling and rolling_days and body.aggregation_date and body.aggregation_date in raw_df.columns:
                # Rolling window: each df row is one transaction's window anchored at row[date_column]
                window_end = pd.Timestamp(row[body.aggregation_date])
                window_start = window_end - timedelta(days=rolling_days)
                raw_dates = pd.to_datetime(raw_df[body.aggregation_date], errors="coerce")
                entity_rows = raw_df[
                    (raw_df[body.aggregation_key] == ekey) &
                    (raw_dates >= window_start) &
                    (raw_dates <= window_end)
                ]
                date_start = str(window_start.date())
                date_end = str(window_end.date())
                days = rolling_days
            else:
                entity_rows = raw_df[raw_df[body.aggregation_key] == ekey]
                if body.aggregation_date and body.aggregation_date in entity_rows.columns:
                    dates = pd.to_datetime(entity_rows[body.aggregation_date], errors="coerce").dropna()
                    if len(dates):
                        date_start = str(dates.min().date())
                        date_end = str(dates.max().date())
                        days = int((dates.max() - dates.min()).days + 1)

            tids = entity_rows[tid_col].astype(str).tolist() if tid_col else [str(i) for i in entity_rows.index.tolist()]
            events_out.append({
                "key":             str(ekey) if ekey is not None else "",
                "date_start":      date_start,
                "date_end":        date_end,
                "days":            days,
                "sum":             round(float(row["agg_value"]), 2),
                "count":           int(len(entity_rows)),
                "transaction_ids": tids,
            })
        events_out.sort(key=lambda x: x["sum"], reverse=True)
        events_out = events_out[:500]
    else:
        raw_sample = raw_df.head(500).copy()
        raw_sample.insert(0, "_row", range(1, len(raw_sample) + 1))
        events_out = raw_sample.fillna("").astype(str).to_dict("records")

    # Raw transactions for tab 3 (all filtered, not aggregated)
    raw_tx = raw_df.head(2000).copy()
    raw_tx.insert(0, "_row", range(1, len(raw_tx) + 1))
    raw_transactions = raw_tx.fillna("").astype(str).to_dict("records")
    raw_columns = ["_row"] + list(raw_df.columns)

    return {
        "analysis_id":      analysis_id,
        "matched_rows":     len(raw_df),
        "original_rows":    len(mem["df"]),
        "column":           param_col,
        "is_categorical":   is_categorical,
        "statistics":       stats,
        "boundaries":       boundaries,
        "tranches":         tranches,
        "trim_tranches":    trim_tranches,
        "categorical_dist": dist,
        "analysis_type":    body.analysis_type,
        "sample_values":    sample,
        "events":           events_out,
        "raw_transactions": raw_transactions,
        "raw_columns":      raw_columns,
        "agg_key_col":      body.aggregation_key if is_aggregate else None,
        "tid_col":          tid_col,
    }


@router.post("/analysis/simulate")
async def simulate(body: SimulateRequest, db: aiosqlite.Connection = Depends(get_db)):
    """Simulate alert volumes at multiple threshold values."""
    mem = await _get_mem(body.dataset_id, db)

    df = svc.apply_filters(mem["df"], body.filter_rules)
    if len(df) == 0:
        raise HTTPException(status_code=400, detail="No rows match the current filters")
    if body.analysis_type == "aggregate" and body.aggregation_key and body.aggregation_amount:
        df = svc.aggregate_transactions(
            df,
            key_column=body.aggregation_key,
            amount_column=body.aggregation_amount,
            period=body.aggregation_period,
            agg_function=body.aggregation_function,
            date_column=body.aggregation_date or None,
        )
        value_series = df["agg_value"]
        key_series = df.get(body.aggregation_key)
        date_col = body.aggregation_date if body.aggregation_date in df.columns else None
        date_series = df[date_col] if date_col else None
    else:
        col = body.parameter_column
        if not col or col not in df.columns:
            raise HTTPException(status_code=400, detail=f"Column '{col}' not found")
        value_series = df[col]
        key_series = df.get(body.aggregation_key) if body.aggregation_key else None
        date_col = body.aggregation_date if body.aggregation_date in df.columns else None
        date_series = df[date_col] if date_col else None

    results = svc.simulate_thresholds(
        value_series,
        body.thresholds,
        key_series=key_series,
        date_series=date_series,
    )
    recommendation = svc.recommend_threshold(
        results, target_monthly_alerts=body.target_monthly_alerts
    )
    return {"results": results, "recommendation": recommendation}


@router.post("/analysis/auto-thresholds")
async def auto_thresholds(body: AnalysisRequest, db: aiosqlite.Connection = Depends(get_db)):
    """Suggest thresholds at key percentile breakpoints."""
    mem = await _get_mem(body.dataset_id, db)
    df = svc.apply_filters(mem["df"], body.filter_rules)
    if body.analysis_type == "aggregate" and body.aggregation_key and body.aggregation_amount:
        df = svc.aggregate_transactions(
            df, body.aggregation_key, body.aggregation_amount,
            body.aggregation_period, body.aggregation_function,
            body.aggregation_date or None,
        )
        col = "agg_value"
    else:
        col = body.parameter_column
    if col not in df.columns:
        raise HTTPException(status_code=400, detail=f"Column '{col}' not found")
    s = df[col].dropna()
    pcts = [50, 75, 85, 90, 95, 99]
    return {"thresholds": [round(float(s.quantile(p / 100)), 2) for p in pcts], "percentiles": pcts}


@router.get("/analysis/{analysis_id}")
async def get_analysis(analysis_id: int, db: aiosqlite.Connection = Depends(get_db)):
    async with db.execute("SELECT * FROM threshold_analyses WHERE id = ?", (analysis_id,)) as cur:
        row = await cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Analysis not found")
    d = dict(row)
    for key in ("parameter_columns", "statistics", "threshold_values", "threshold_results"):
        d[key] = json.loads(d.get(key) or "null")
    return d


# ── Report generation ──────────────────────────────────────────────────────────

@router.post("/report/generate")
async def generate_report(body: ReportRequest, db: aiosqlite.Connection = Depends(get_db)):
    async with db.execute("SELECT * FROM threshold_analyses WHERE id = ?", (body.analysis_id,)) as cur:
        row = await cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Analysis not found")
    d = dict(row)
    for key in ("parameter_columns", "statistics", "threshold_values", "threshold_results"):
        d[key] = json.loads(d.get(key) or "null")
    try:
        report_text = await svc.ai_generate_report(d)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    await db.execute(
        "UPDATE threshold_analyses SET report_text = ? WHERE id = ?",
        (report_text, body.analysis_id),
    )
    await db.commit()
    return {"report_text": report_text}
