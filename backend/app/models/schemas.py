from pydantic import BaseModel, Field
from typing import Optional, Literal
from datetime import date, datetime


# ── Watchlist Entry ────────────────────────────────────────────────────────────

class WatchlistEntry(BaseModel):
    uid: str
    watchlist: Literal["OFAC_SDN", "OFAC_NON_SDN", "EU", "HMT", "BIS", "JAPAN"]
    sub_watchlist_1: Optional[str] = None
    sub_watchlist_2: Optional[str] = None
    cleaned_name: str
    original_name: str
    primary_aka: Literal["primary", "aka"]
    entity_type: Literal["individual", "entity", "country", "vessel", "aircraft", "unknown"]
    num_tokens: int
    name_length: int
    date_listed: Optional[date] = None
    recently_modified: bool = False
    sanctions_program: Optional[str] = None


class WatchlistSummary(BaseModel):
    total: int
    by_watchlist: dict[str, int]
    by_entity_type: dict[str, int]
    last_updated: Optional[datetime] = None


class DownloadStatus(BaseModel):
    watchlist: str
    status: Literal["success", "failed", "cached"]
    count: int = 0
    error: Optional[str] = None
    timestamp: datetime


# ── Filters ───────────────────────────────────────────────────────────────────

class ListFilters(BaseModel):
    watchlists: list[str] = []
    sub_watchlists: list[str] = []
    entity_types: list[str] = []
    search: Optional[str] = None
    recently_modified_only: bool = False
    min_tokens: Optional[int] = None
    max_tokens: Optional[int] = None
    page: int = 1
    page_size: int = 100


# ── Test Case ─────────────────────────────────────────────────────────────────

class TestCaseType(BaseModel):
    type_id: str
    theme: str
    category: str
    type_name: str
    description: str
    applicable_entity_types: list[str]
    applicable_min_tokens: int = 1
    applicable_min_name_length: int = 1
    expected_outcome: str
    variation_logic: str


class TestCase(BaseModel):
    test_case_id: str
    test_case_type: str
    watchlist: str
    sub_watchlist: Optional[str] = None
    cleaned_original_name: str
    original_original_name: str
    culture_nationality: Optional[str] = None
    test_name: str
    primary_aka: str
    entity_type: str
    num_tokens: int
    name_length: int
    expected_result: Literal["HIT", "MISS"]
    expected_result_rationale: str


class GenerationRequest(BaseModel):
    type_ids: list[str]
    count_per_type: int = 250
    culture_distribution: Literal["balanced", "weighted", "custom"] = "balanced"
    custom_distribution: Optional[dict[str, float]] = None
    export_format: Literal["names_only", "pacs008", "pacs009", "fuf"] = "names_only"
    outcome_overrides: Optional[dict[str, dict[str, str]]] = None  # type_id → entity_type → outcome
    watchlists: list[str] = []  # empty = all watchlists


# ── Results Interpreter ───────────────────────────────────────────────────────

class ScreeningResult(BaseModel):
    test_case_id: str
    test_name: str
    expected_result: Literal["HIT", "MISS"]
    actual_result: Literal["HIT", "MISS"]
    match_score: Optional[float] = None
    matched_list_entry: Optional[str] = None
    alert_details: Optional[str] = None


class ConfusionMatrix(BaseModel):
    tp: int
    fp: int
    tn: int
    fn: int
    detection_rate: float   # TP / (TP + FN)
    false_positive_rate: float  # FP / (FP + TN)
    precision: float
    recall: float
    f1: float


class MissAnalysis(BaseModel):
    test_case_id: str
    test_name: str
    original_name: str
    test_case_type: str
    failure_explanation: str
    char_similarity: float
    phonetic_distance: Optional[float] = None
    token_overlap: float
