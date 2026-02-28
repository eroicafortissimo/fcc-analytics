from fastapi import APIRouter, Depends, UploadFile, File
import aiosqlite

from app.db.database import get_db
from app.models.schemas import ConfusionMatrix

router = APIRouter()


@router.post("/upload")
async def upload_results(
    file: UploadFile = File(...),
    db: aiosqlite.Connection = Depends(get_db),
):
    """Upload CSV/Excel screening results and join with test cases."""
    from app.services.results_analyzer import ingest_results
    return await ingest_results(file, db)


@router.get("/summary")
async def get_results_summary(db: aiosqlite.Connection = Depends(get_db)):
    """Return confusion matrix, detection rate, FP rate, precision, recall."""
    from app.services.results_analyzer import compute_summary
    return await compute_summary(db)


@router.get("/breakdown")
async def get_breakdown(
    by: str = "test_case_type",
    db: aiosqlite.Connection = Depends(get_db),
):
    """Return detection rates broken down by a dimension."""
    from app.services.results_analyzer import compute_breakdown
    return await compute_breakdown(by, db)


@router.post("/analyze-misses")
async def analyze_misses(db: aiosqlite.Connection = Depends(get_db)):
    """Run LangGraph miss analysis engine over all false negatives."""
    from app.services.miss_analyzer import run_miss_analysis
    return await run_miss_analysis(db)
