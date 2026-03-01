from fastapi import APIRouter, Depends, Query, UploadFile, File
from fastapi.responses import StreamingResponse
import aiosqlite
import io
import csv

from app.db.database import get_db

router = APIRouter()

_TEMPLATE_HEADERS = [
    'test_case_id', 'actual_result', 'match_score',
    'matched_list_entry', 'alert_details',
]
_TEMPLATE_EXAMPLE = [
    'TC001_abc12345', 'HIT', '0.92', 'Ahmad Al-Rashid', 'Fuzzy match score 0.92',
]


@router.get("/template")
async def download_template():
    """Download a blank CSV template showing the expected upload columns."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(_TEMPLATE_HEADERS)
    writer.writerow(_TEMPLATE_EXAMPLE)
    writer.writerow(['TC002_def67890', 'MISS', '', '', ''])
    buf.seek(0)
    return StreamingResponse(
        io.BytesIO(buf.getvalue().encode()),
        media_type='text/csv',
        headers={'Content-Disposition': 'attachment; filename=results_template.csv'},
    )


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
    """Return confusion matrix, detection rate, FP rate, precision, recall, F1."""
    from app.services.results_analyzer import compute_summary
    return await compute_summary(db)


@router.get("/breakdown")
async def get_breakdown(
    by: str = Query(default='entity_type'),
    db: aiosqlite.Connection = Depends(get_db),
):
    """Return detection rates broken down by a dimension.

    by: entity_type | watchlist | culture_nationality | num_tokens |
        name_length_bucket | test_case_type
    """
    from app.services.results_analyzer import compute_breakdown
    return await compute_breakdown(by, db)


@router.get("/")
async def list_results(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=500),
    outcome: str = Query(default=None),
    entity_type: str = Query(default=None),
    search: str = Query(default=None),
    db: aiosqlite.Connection = Depends(get_db),
):
    """Return paginated screening results joined with test case details."""
    from app.services.results_analyzer import get_results_table
    return await get_results_table(db, page, page_size, outcome, entity_type, search)


@router.delete("/clear")
async def clear_results(db: aiosqlite.Connection = Depends(get_db)):
    """Delete all uploaded screening results."""
    await db.execute("DELETE FROM screening_results")
    await db.commit()
    return {'cleared': True}


@router.get("/export/excel")
async def export_excel(db: aiosqlite.Connection = Depends(get_db)):
    """Export full results table as a styled Excel workbook."""
    from app.services.results_analyzer import export_results_excel
    data = await export_results_excel(db)
    return StreamingResponse(
        io.BytesIO(data),
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers={'Content-Disposition': 'attachment; filename=screening_results.xlsx'},
    )


@router.post("/analyze-misses")
async def analyze_misses(db: aiosqlite.Connection = Depends(get_db)):
    """Run LangGraph miss analysis engine over all false negatives."""
    from app.services.miss_analyzer import run_miss_analysis
    return await run_miss_analysis(db)


@router.get("/miss-analyses")
async def get_miss_analyses(db: aiosqlite.Connection = Depends(get_db)):
    """Return all previously saved miss analyses, newest first."""
    from app.services.miss_analyzer import get_saved_analyses
    return await get_saved_analyses(db)
