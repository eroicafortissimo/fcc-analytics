from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
import aiosqlite
import csv
import io

from app.db.database import get_db
from app.models.schemas import GenerationRequest, TestCase, TestCaseType

router = APIRouter()


@router.get("/types", response_model=list[TestCaseType])
async def get_test_case_types():
    """Return all pre-defined test case types from CSV."""
    from app.services.test_generator import load_test_case_types
    return load_test_case_types()


@router.post("/generate", response_model=dict)
async def generate_test_cases(
    request: GenerationRequest,
    db: aiosqlite.Connection = Depends(get_db),
):
    """Generate test cases for the requested types and count."""
    from app.services.test_generator import generate_test_cases
    result = await generate_test_cases(request, db)
    return result


@router.get("/stats")
async def get_stats(db: aiosqlite.Connection = Depends(get_db)):
    """Aggregate counts for the generated test cases table."""
    async with db.execute("SELECT COUNT(*) FROM test_cases") as cur:
        total = (await cur.fetchone())[0]

    async with db.execute(
        "SELECT expected_result, COUNT(*) FROM test_cases GROUP BY expected_result"
    ) as cur:
        by_result = {row[0]: row[1] for row in await cur.fetchall()}

    async with db.execute(
        "SELECT entity_type, COUNT(*) FROM test_cases GROUP BY entity_type ORDER BY COUNT(*) DESC"
    ) as cur:
        by_entity_type = {row[0]: row[1] for row in await cur.fetchall()}

    async with db.execute(
        "SELECT watchlist, COUNT(*) FROM test_cases GROUP BY watchlist ORDER BY COUNT(*) DESC"
    ) as cur:
        by_watchlist = {row[0]: row[1] for row in await cur.fetchall()}

    return {
        "total": total,
        "by_result": by_result,
        "by_entity_type": by_entity_type,
        "by_watchlist": by_watchlist,
    }


@router.delete("/clear")
async def clear_test_cases(db: aiosqlite.Connection = Depends(get_db)):
    """Delete all generated test cases."""
    await db.execute("DELETE FROM test_cases")
    await db.commit()
    return {"cleared": True}


@router.get("/", response_model=dict)
async def list_test_cases(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=500),
    expected_result: str = Query(default=None),
    entity_type: str = Query(default=None),
    search: str = Query(default=None),
    db: aiosqlite.Connection = Depends(get_db),
):
    """Return paginated generated test cases with optional filters."""
    conditions = []
    params: list = []

    if expected_result:
        conditions.append("expected_result = ?")
        params.append(expected_result)
    if entity_type:
        conditions.append("entity_type = ?")
        params.append(entity_type)
    if search:
        conditions.append(
            "(cleaned_original_name LIKE ? OR test_name LIKE ? OR test_case_type LIKE ?)"
        )
        like = f"%{search}%"
        params.extend([like, like, like])

    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    offset = (page - 1) * page_size

    async with db.execute(f"SELECT COUNT(*) FROM test_cases {where}", params) as cur:
        total = (await cur.fetchone())[0]

    async with db.execute(
        f"""SELECT * FROM test_cases {where}
            ORDER BY test_case_id
            LIMIT ? OFFSET ?""",
        params + [page_size, offset],
    ) as cur:
        rows = await cur.fetchall()

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": [dict(r) for r in rows],
    }


@router.get("/export/csv")
async def export_csv(
    expected_result: str = Query(default=None),
    entity_type: str = Query(default=None),
    db: aiosqlite.Connection = Depends(get_db),
):
    """Export test cases as CSV (names-only format)."""
    conditions = []
    params: list = []
    if expected_result:
        conditions.append("expected_result = ?")
        params.append(expected_result)
    if entity_type:
        conditions.append("entity_type = ?")
        params.append(entity_type)
    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    async with db.execute(
        f"""SELECT test_case_id, test_case_type, watchlist, cleaned_original_name,
                   test_name, entity_type, expected_result, expected_result_rationale
            FROM test_cases {where} ORDER BY test_case_id""",
        params,
    ) as cur:
        rows = await cur.fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "test_case_id", "test_case_type", "watchlist",
        "original_name", "test_name", "entity_type",
        "expected_result", "rationale",
    ])
    for row in rows:
        writer.writerow(list(row))

    output.seek(0)
    return StreamingResponse(
        io.BytesIO(output.getvalue().encode()),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=test_cases.csv"},
    )


@router.post("/chatbot/message")
async def chatbot_message(
    message: dict,
    db: aiosqlite.Connection = Depends(get_db),
):
    """Handle a chat message for the new test case type chatbot (LangGraph)."""
    from app.services.chatbot_agent import handle_message
    return await handle_message(message, db)
