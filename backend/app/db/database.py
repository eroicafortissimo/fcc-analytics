import aiosqlite
import os
from pathlib import Path

DB_PATH = Path(__file__).parent / "platform.db"


async def get_db():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        yield db


async def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS watchlist_entries (
                uid TEXT PRIMARY KEY,
                watchlist TEXT NOT NULL,
                sub_watchlist_1 TEXT,
                sub_watchlist_2 TEXT,
                cleaned_name TEXT NOT NULL,
                original_name TEXT NOT NULL,
                primary_aka TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                num_tokens INTEGER NOT NULL,
                name_length INTEGER NOT NULL,
                nationality TEXT,
                nationality_confidence TEXT,
                nationality_method TEXT,
                date_listed TEXT,
                recently_modified INTEGER DEFAULT 0,
                sanctions_program TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_watchlist ON watchlist_entries(watchlist);
            CREATE INDEX IF NOT EXISTS idx_entity_type ON watchlist_entries(entity_type);
            CREATE INDEX IF NOT EXISTS idx_cleaned_name ON watchlist_entries(cleaned_name);

            CREATE TABLE IF NOT EXISTS download_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                watchlist TEXT NOT NULL,
                status TEXT NOT NULL,
                count INTEGER DEFAULT 0,
                error TEXT,
                timestamp TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS nationality_cache (
                name_key TEXT PRIMARY KEY,
                nationality TEXT,
                confidence TEXT,
                method TEXT,
                cached_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS test_cases (
                test_case_id TEXT PRIMARY KEY,
                test_case_type TEXT NOT NULL,
                watchlist TEXT,
                sub_watchlist TEXT,
                cleaned_original_name TEXT,
                original_original_name TEXT,
                culture_nationality TEXT,
                test_name TEXT NOT NULL,
                primary_aka TEXT,
                entity_type TEXT,
                num_tokens INTEGER,
                name_length INTEGER,
                expected_result TEXT NOT NULL,
                expected_result_rationale TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS screening_results (
                test_case_id TEXT PRIMARY KEY,
                test_name TEXT,
                expected_result TEXT,
                actual_result TEXT,
                match_score REAL,
                matched_list_entry TEXT,
                alert_details TEXT,
                miss_explanation TEXT,
                uploaded_at TEXT DEFAULT (datetime('now'))
            );
        """)
        await db.commit()
