"""
btl.py — Standalone BTL (Below the Line) analysis router.
Routes: /api/btl/...

Supports two data sources:
  1. User uploads a CSV/Excel file directly (upload-preview → analyze)
  2. Data passed from the Threshold Setting module (uses existing /api/threshold/analysis/atl-btl)
"""
from __future__ import annotations
import io
import uuid

import pandas as pd
from fastapi import APIRouter, File, HTTPException, UploadFile
from pydantic import BaseModel

from app.services import threshold_service as svc

router = APIRouter()

# ── In-memory upload cache (upload_id → DataFrame) ─────────────────────────────
_btl_uploads: dict[str, pd.DataFrame] = {}


# ── Step 1: Upload file and preview columns ────────────────────────────────────

@router.post("/upload-preview")
async def btl_upload_preview(file: UploadFile = File(...)):
    """
    Upload a CSV or Excel file. Returns column metadata and preview rows.
    The returned upload_id is required for the /analyze endpoint.
    """
    raw = await file.read()
    fname = (file.filename or "").lower()

    try:
        if fname.endswith((".xlsx", ".xls")):
            df = pd.read_excel(io.BytesIO(raw))
        else:
            df = pd.read_csv(io.BytesIO(raw))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not parse file: {e}")

    if len(df) == 0:
        raise HTTPException(status_code=400, detail="File contains no rows")

    upload_id = str(uuid.uuid4())
    _btl_uploads[upload_id] = df

    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    preview = df.head(8).fillna("").astype(str).to_dict(orient="records")

    return {
        "upload_id": upload_id,
        "row_count": len(df),
        "columns": df.columns.tolist(),
        "numeric_columns": numeric_cols,
        "preview_rows": preview,
    }


# ── Step 2: Run BTL k-means analysis ──────────────────────────────────────────

class BtlAnalyzeRequest(BaseModel):
    upload_id: str
    value_column: str
    candidate_threshold: float


@router.post("/analyze")
async def btl_analyze(body: BtlAnalyzeRequest):
    """
    Run k-means BTL analysis on a previously uploaded file.
    Returns the same structure as /api/threshold/analysis/atl-btl.
    """
    df = _btl_uploads.get(body.upload_id)
    if df is None:
        raise HTTPException(
            status_code=404,
            detail="Upload not found. Please re-upload the file.",
        )

    if body.value_column not in df.columns:
        raise HTTPException(
            status_code=400,
            detail=f"Column '{body.value_column}' not found in uploaded file.",
        )

    series = pd.to_numeric(df[body.value_column], errors="coerce").dropna()
    if len(series) < 4:
        raise HTTPException(
            status_code=400,
            detail=f"Column '{body.value_column}' has fewer than 4 numeric values — cannot run k-means.",
        )

    try:
        result = svc.suggest_btl_kmeans(series, body.candidate_threshold)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"K-means error: {e}")

    result["p95"] = float(series.quantile(0.95))
    return result
