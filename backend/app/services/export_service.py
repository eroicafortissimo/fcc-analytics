"""
export_service.py — Excel, pacs.008, pacs.009, and FUF export formatters.
Stub — implementation follows in Step 9 of the build plan.
"""
from __future__ import annotations


async def export_names_only(test_case_ids: list[str], db) -> bytes:
    raise NotImplementedError


async def export_pacs008(test_case_ids: list[str], db) -> bytes:
    raise NotImplementedError


async def export_pacs009(test_case_ids: list[str], db) -> bytes:
    raise NotImplementedError


async def export_fuf(test_case_ids: list[str], db) -> bytes:
    raise NotImplementedError
