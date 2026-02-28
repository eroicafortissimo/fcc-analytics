"""
test_generator.py — Test case generation logic.
Stub — implementation follows in Step 4 of the build plan.
"""
from __future__ import annotations
from pathlib import Path
import csv

from app.models.schemas import TestCaseType, GenerationRequest

TEST_CASE_TYPES_CSV = Path(__file__).parent.parent / "data" / "test_case_types.csv"


def load_test_case_types() -> list[TestCaseType]:
    if not TEST_CASE_TYPES_CSV.exists():
        return []
    with open(TEST_CASE_TYPES_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        types = []
        for row in reader:
            try:
                types.append(
                    TestCaseType(
                        type_id=row["type_id"],
                        type_name=row["type_name"],
                        description=row["description"],
                        applicable_entity_types=row["applicable_entity_types"].split("|"),
                        applicable_min_tokens=int(row.get("applicable_min_tokens", 1)),
                        applicable_min_name_length=int(row.get("applicable_min_name_length", 1)),
                        variation_logic=row["variation_logic"],
                    )
                )
            except (KeyError, ValueError):
                continue
    return types


async def generate_test_cases(request: GenerationRequest, db) -> dict:
    # TODO: implement full generation logic
    return {"status": "not_implemented", "message": "Test case generation coming in step 4"}
