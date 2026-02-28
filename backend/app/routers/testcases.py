from fastapi import APIRouter, Depends, Query
import aiosqlite

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


@router.get("/", response_model=dict)
async def list_test_cases(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=500),
    db: aiosqlite.Connection = Depends(get_db),
):
    """Return paginated generated test cases."""
    offset = (page - 1) * page_size
    async with db.execute("SELECT COUNT(*) FROM test_cases") as cur:
        total = (await cur.fetchone())[0]
    async with db.execute(
        "SELECT * FROM test_cases ORDER BY test_case_id LIMIT ? OFFSET ?",
        (page_size, offset),
    ) as cur:
        rows = await cur.fetchall()
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": [dict(r) for r in rows],
    }


@router.post("/chatbot/message")
async def chatbot_message(
    message: dict,
    db: aiosqlite.Connection = Depends(get_db),
):
    """Handle a chat message for the new test case type chatbot (LangGraph)."""
    from app.services.chatbot_agent import handle_message
    return await handle_message(message, db)
