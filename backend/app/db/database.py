import aiosqlite
import logging
from pathlib import Path

DB_PATH = Path(__file__).parent / "platform2.db"
logger = logging.getLogger(__name__)


async def get_db():
    async with aiosqlite.connect(DB_PATH, timeout=30) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=NORMAL")
        await db.execute("PRAGMA cache_size=-65536")
        await db.execute("PRAGMA temp_store=MEMORY")
        yield db


async def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(DB_PATH, timeout=30) as db:
        try:
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

                CREATE TABLE IF NOT EXISTS chatbot_sessions (
                    session_id TEXT PRIMARY KEY,
                    stage TEXT DEFAULT 'new',
                    proposed_type TEXT,
                    examples TEXT DEFAULT '[]',
                    iteration INTEGER DEFAULT 0,
                    messages TEXT DEFAULT '[]',
                    updated_at TEXT DEFAULT (datetime('now'))
                );

                CREATE TABLE IF NOT EXISTS custom_test_types (
                    type_id TEXT PRIMARY KEY,
                    theme TEXT DEFAULT 'Custom',
                    category TEXT DEFAULT 'User-Defined',
                    type_name TEXT NOT NULL,
                    description TEXT NOT NULL,
                    applicable_entity_types TEXT NOT NULL,
                    applicable_min_tokens INTEGER DEFAULT 1,
                    applicable_min_name_length INTEGER DEFAULT 1,
                    expected_outcome TEXT DEFAULT 'Should Hit',
                    variation_logic TEXT NOT NULL,
                    python_lambda TEXT NOT NULL,
                    created_at TEXT DEFAULT (datetime('now'))
                );

                CREATE TABLE IF NOT EXISTS miss_analyses (
                    test_case_id TEXT PRIMARY KEY,
                    test_name TEXT,
                    original_name TEXT,
                    test_case_type TEXT,
                    entity_type TEXT,
                    miss_category TEXT,
                    explanation TEXT,
                    recommendation TEXT,
                    confidence TEXT DEFAULT 'medium',
                    analyzed_at TEXT DEFAULT (datetime('now'))
                );

                CREATE TABLE IF NOT EXISTS threshold_datasets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    file_name TEXT NOT NULL,
                    row_count INTEGER,
                    column_list TEXT,
                    date_range_start TEXT,
                    date_range_end TEXT,
                    uploaded_at TEXT DEFAULT (datetime('now'))
                );

                CREATE TABLE IF NOT EXISTS threshold_scenarios (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dataset_id INTEGER REFERENCES threshold_datasets(id),
                    name TEXT NOT NULL,
                    description TEXT,
                    filter_rules TEXT,
                    analysis_type TEXT DEFAULT 'single',
                    aggregation_key TEXT,
                    aggregation_amount TEXT,
                    aggregation_date TEXT,
                    aggregation_period TEXT DEFAULT 'none',
                    aggregation_days INTEGER DEFAULT 30,
                    aggregation_function TEXT DEFAULT 'SUM',
                    created_at TEXT DEFAULT (datetime('now')),
                    created_by_ai INTEGER DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS threshold_analyses (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scenario_id INTEGER,
                    parameter_columns TEXT,
                    statistics TEXT,
                    threshold_values TEXT,
                    threshold_results TEXT,
                    recommended_threshold REAL,
                    recommendation_reason TEXT,
                    report_text TEXT,
                    created_at TEXT DEFAULT (datetime('now'))
                );
            """)
        except Exception as exc:
            logger.warning("init_db executescript skipped (DB may be busy): %s", exc)

        # Performance PRAGMAs — applied on every connection
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=NORMAL")
        await db.execute("PRAGMA cache_size=-65536")  # 64 MB page cache
        await db.execute("PRAGMA temp_store=MEMORY")

        # Migrations — add columns/indexes that may not exist in older DBs.
        # Each is individually wrapped so one failure doesn't block the rest.
        for migration in [
            "ALTER TABLE threshold_analyses ADD COLUMN series_data TEXT",
            "ALTER TABLE threshold_datasets ADD COLUMN file_data BLOB",
            "ALTER TABLE watchlist_entries ADD COLUMN parent_uid TEXT",
            "ALTER TABLE watchlist_entries ADD COLUMN region TEXT",
            "ALTER TABLE watchlist_entries ADD COLUMN name_culture TEXT",
            "ALTER TABLE watchlist_entries ADD COLUMN culture_confidence TEXT",
            "CREATE INDEX IF NOT EXISTS idx_parent_uid       ON watchlist_entries(parent_uid)",
            "CREATE INDEX IF NOT EXISTS idx_recently_modified ON watchlist_entries(recently_modified)",
            "CREATE INDEX IF NOT EXISTS idx_primary_aka      ON watchlist_entries(primary_aka)",
            "CREATE INDEX IF NOT EXISTS idx_name_length      ON watchlist_entries(name_length)",
            "CREATE INDEX IF NOT EXISTS idx_num_tokens       ON watchlist_entries(num_tokens)",
            # Composite index for the most common query pattern
            "CREATE INDEX IF NOT EXISTS idx_watchlist_primary ON watchlist_entries(watchlist, primary_aka)",
            # Culture cache — persists name_culture across downloads
            """CREATE TABLE IF NOT EXISTS culture_cache (
                watchlist TEXT NOT NULL,
                uid TEXT NOT NULL,
                cleaned_name TEXT,
                name_culture TEXT,
                region TEXT,
                confidence TEXT,
                cached_at TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (watchlist, uid)
            )""",
        ]:
            try:
                await db.execute(migration)
            except Exception:
                pass  # Column/index already exists, or DB locked — safe to skip
        try:
            await db.commit()
        except Exception as exc:
            logger.warning("init_db commit skipped: %s", exc)

        # Backfill culture_nationality in test_cases from watchlist_entries.name_culture
        try:
            await db.execute("""
                UPDATE test_cases
                SET culture_nationality = (
                    SELECT we.name_culture
                    FROM watchlist_entries we
                    WHERE we.watchlist = test_cases.watchlist
                      AND we.cleaned_name = test_cases.cleaned_original_name
                    LIMIT 1
                )
                WHERE culture_nationality IS NULL
            """)
            await db.commit()
        except Exception as exc:
            logger.warning("culture_nationality backfill skipped: %s", exc)
