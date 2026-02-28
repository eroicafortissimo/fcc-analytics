"""
nationality_chain.py

LangGraph 3-tier nationality inference workflow.

Graph:
  data_lookup → output            (nationality found in record data, confidence=HIGH)
  data_lookup → heuristic         (no explicit nationality)
  heuristic   → output            (script/program gives MEDIUM confidence)
  heuristic   → llm_inference     (inconclusive)
  llm_inference → output

All results are cached in the `nationality_cache` SQLite table.
"""
from __future__ import annotations

import os
import re
import unicodedata
import logging
from typing import Optional, Literal
from functools import lru_cache

import aiosqlite
from langgraph.graph import StateGraph, END
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage, SystemMessage
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


# ── State ──────────────────────────────────────────────────────────────────────

class NationalityState(BaseModel):
    """Immutable-ish state passed through LangGraph nodes."""
    name: str
    record_context: dict = {}  # sanctions_program, entity_type, sub_watchlist_1, watchlist

    # Outputs (filled as graph progresses)
    nationality: Optional[str] = None
    confidence: Optional[Literal["HIGH", "MEDIUM", "LOW"]] = None
    method_used: Optional[Literal["data_lookup", "heuristic", "llm"]] = None

    def done(self) -> bool:
        return self.nationality is not None and self.confidence in ("HIGH", "MEDIUM")


# ── LLM structured output schema ──────────────────────────────────────────────

class NationalityResult(BaseModel):
    nationality: str = Field(
        description="Most likely nationality, country, or world region. "
                    "Use ISO country name or region (e.g. 'Iranian', 'Russian', 'Middle Eastern', 'East Asian'). "
                    "Return 'Unknown' if cannot determine."
    )
    confidence: Literal["HIGH", "MEDIUM", "LOW"] = Field(
        description="Confidence level of the inference."
    )
    reasoning: str = Field(
        description="One sentence explaining the basis for the inference."
    )


# ── Program → Nationality mapping ─────────────────────────────────────────────

PROGRAM_NATIONALITY: dict[str, tuple[str, str]] = {
    # OFAC program codes → (nationality, confidence)
    "IRAN": ("Iranian", "HIGH"),
    "IRGC": ("Iranian", "HIGH"),
    "IFSR": ("Iranian", "HIGH"),
    "NPWMD": ("Iranian", "MEDIUM"),  # nonproliferation — often Iran/DPRK
    "RUSSIA": ("Russian", "HIGH"),
    "UKRAINE-EO13685": ("Russian", "HIGH"),
    "UKRAINE-EO13662": ("Russian", "HIGH"),
    "UKRAINE-EO13661": ("Russian", "HIGH"),
    "UKRAINE-EO13660": ("Ukrainian", "MEDIUM"),
    "DPRK": ("North Korean", "HIGH"),
    "DPRK2": ("North Korean", "HIGH"),
    "DPRK3": ("North Korean", "HIGH"),
    "DPRK4": ("North Korean", "HIGH"),
    "CUBA": ("Cuban", "HIGH"),
    "SYRIA": ("Syrian", "HIGH"),
    "IRAQ2": ("Iraqi", "HIGH"),
    "IRAQ3": ("Iraqi", "HIGH"),
    "SOMALIA": ("Somali", "HIGH"),
    "SUDAN": ("Sudanese", "HIGH"),
    "GLOMAG": ("Unknown", "LOW"),     # Global Magnitsky — many nationalities
    "SDGT": ("Unknown", "LOW"),       # Terrorism — diverse
    "TCO": ("Unknown", "LOW"),        # Transnational crime — diverse
    "CYBER2": ("Unknown", "LOW"),
    "VENEZUELA": ("Venezuelan", "HIGH"),
    "NICARAGUA": ("Nicaraguan", "HIGH"),
    "BELARUS": ("Belarusian", "HIGH"),
    "MYANMAR": ("Burmese", "HIGH"),
    "LIBYA": ("Libyan", "HIGH"),
    "MALI": ("Malian", "HIGH"),
    "CAR": ("Central African", "HIGH"),
    "DRC": ("Congolese", "HIGH"),
    "YEMEN": ("Yemeni", "HIGH"),
    "IRAQ": ("Iraqi", "HIGH"),
    "AFGHANISTAN": ("Afghan", "HIGH"),
    "HAMAS": ("Palestinian", "HIGH"),
    "HIZBALLAH": ("Lebanese", "HIGH"),
}


# ── Script → Region heuristic ──────────────────────────────────────────────────

def detect_script_region(name: str) -> tuple[str, str] | None:
    """
    Detect the dominant Unicode script in `name` and map to a nationality/region.
    Returns (region, confidence) or None if inconclusive (Latin-only).
    """
    script_counts: dict[str, int] = {}
    for ch in name:
        if not ch.isalpha():
            continue
        block = _unicode_block(ch)
        script_counts[block] = script_counts.get(block, 0) + 1

    if not script_counts:
        return None

    dominant = max(script_counts, key=script_counts.__getitem__)
    total_alpha = sum(script_counts.values())
    dominant_frac = script_counts[dominant] / total_alpha

    # Only commit if dominant script is at least 40% of alpha chars
    if dominant_frac < 0.4:
        return None

    return SCRIPT_REGION.get(dominant)


SCRIPT_REGION: dict[str, tuple[str, str]] = {
    "ARABIC": ("Middle Eastern / North African", "MEDIUM"),
    "CYRILLIC": ("Russian / Eastern European", "MEDIUM"),
    "CJK_UNIFIED": ("East Asian", "MEDIUM"),
    "CJK_EXTENSION": ("East Asian", "MEDIUM"),
    "HANGUL": ("Korean", "HIGH"),
    "HIRAGANA": ("Japanese", "HIGH"),
    "KATAKANA": ("Japanese", "HIGH"),
    "HEBREW": ("Israeli / Middle Eastern", "MEDIUM"),
    "DEVANAGARI": ("South Asian (Indian subcontinent)", "MEDIUM"),
    "THAI": ("Thai", "HIGH"),
    "GEORGIAN": ("Georgian", "HIGH"),
    "ARMENIAN": ("Armenian", "HIGH"),
    "ETHIOPIC": ("East African (Ethiopian/Eritrean)", "HIGH"),
    "GREEK": ("Greek", "HIGH"),
    "TAMIL": ("South Asian (Tamil)", "HIGH"),
    "BENGALI": ("South Asian (Bengali/Bangladeshi)", "HIGH"),
    "GUJARATI": ("South Asian (Gujarati)", "HIGH"),
    "GURMUKHI": ("South Asian (Punjabi/Sikh)", "HIGH"),
    "KANNADA": ("South Asian (Kannada)", "HIGH"),
    "MALAYALAM": ("South Asian (Malayalam)", "HIGH"),
    "TELUGU": ("South Asian (Telugu)", "HIGH"),
    "SINHALA": ("Sri Lankan", "HIGH"),
    "MYANMAR_SCRIPT": ("Burmese", "HIGH"),
    "KHMER": ("Cambodian", "HIGH"),
    "LAO": ("Laotian", "HIGH"),
    "TIBETAN": ("Tibetan / Chinese", "MEDIUM"),
}


def _unicode_block(ch: str) -> str:
    """Map a character to a named Unicode block category."""
    cp = ord(ch)
    if 0x0600 <= cp <= 0x06FF or 0x0750 <= cp <= 0x077F or 0xFB50 <= cp <= 0xFDFF:
        return "ARABIC"
    if 0x0400 <= cp <= 0x04FF or 0x0500 <= cp <= 0x052F:
        return "CYRILLIC"
    if 0x4E00 <= cp <= 0x9FFF or 0x3400 <= cp <= 0x4DBF or 0x20000 <= cp <= 0x2A6DF:
        return "CJK_UNIFIED"
    if 0xAC00 <= cp <= 0xD7AF or 0x1100 <= cp <= 0x11FF:
        return "HANGUL"
    if 0x3040 <= cp <= 0x309F:
        return "HIRAGANA"
    if 0x30A0 <= cp <= 0x30FF or 0xFF65 <= cp <= 0xFF9F:
        return "KATAKANA"
    if 0x0590 <= cp <= 0x05FF or 0xFB1D <= cp <= 0xFB4F:
        return "HEBREW"
    if 0x0900 <= cp <= 0x097F:
        return "DEVANAGARI"
    if 0x0E00 <= cp <= 0x0E7F:
        return "THAI"
    if 0x10A0 <= cp <= 0x10FF:
        return "GEORGIAN"
    if 0x0530 <= cp <= 0x058F:
        return "ARMENIAN"
    if 0x1200 <= cp <= 0x137F or 0x1380 <= cp <= 0x139F:
        return "ETHIOPIC"
    if 0x0370 <= cp <= 0x03FF:
        return "GREEK"
    if 0x0B80 <= cp <= 0x0BFF:
        return "TAMIL"
    if 0x0980 <= cp <= 0x09FF:
        return "BENGALI"
    if 0x0A80 <= cp <= 0x0AFF:
        return "GUJARATI"
    if 0x0A00 <= cp <= 0x0A7F:
        return "GURMUKHI"
    if 0x0C80 <= cp <= 0x0CFF:
        return "KANNADA"
    if 0x0D00 <= cp <= 0x0D7F:
        return "MALAYALAM"
    if 0x0C00 <= cp <= 0x0C7F:
        return "TELUGU"
    if 0x0D80 <= cp <= 0x0DFF:
        return "SINHALA"
    if 0x1000 <= cp <= 0x109F:
        return "MYANMAR_SCRIPT"
    if 0x1780 <= cp <= 0x17FF:
        return "KHMER"
    if 0x0E80 <= cp <= 0x0EFF:
        return "LAO"
    if 0x0F00 <= cp <= 0x0FFF:
        return "TIBETAN"
    return "LATIN_OR_OTHER"


# ── Phonetic / name pattern heuristics ────────────────────────────────────────

# Patterns strongly associated with specific origins (Latin-script names)
_PHONETIC_PATTERNS: list[tuple[re.Pattern, str, str]] = [
    # East Asian (transliterated)
    (re.compile(r"\b(kim|lee|park|choi|jung|shin|han|yoon|lim|oh)\b", re.I), "Korean", "MEDIUM"),
    (re.compile(r"\b(wang|zhang|li|liu|chen|yang|huang|zhao|wu|zhou|xu|sun|ma|hu|zhu|guo|he|lin)\b", re.I), "Chinese", "MEDIUM"),
    (re.compile(r"\b(tanaka|suzuki|sato|watanabe|ito|yamamoto|nakamura|hayashi|kobayashi|kato)\b", re.I), "Japanese", "MEDIUM"),
    # Arabic/Middle Eastern
    (re.compile(r"\b(al|abu|ibn|bin|bint|abd|abdul|mohammed|muhammad|mohammad|hassan|hussein|ali|omar|ahmed|mustafa|ibrahim|khalid|suleiman|ismail|yusuf|nasser)\b", re.I), "Middle Eastern / North African", "LOW"),
    # Russian/Slavic
    (re.compile(r"(ovich|evich|ovna|evna|enko|sky|ski|vich|iev|ova|eva|kin|ina)\b", re.I), "Russian / Eastern European", "MEDIUM"),
    # South Asian
    (re.compile(r"\b(singh|kumar|sharma|patel|khan|ali|begum|rao|reddy|nair|menon|gupta|verma|joshi|mishra)\b", re.I), "South Asian (Indian subcontinent)", "LOW"),
    # Iranian / Persian
    (re.compile(r"(zadeh|pour|nia|far|doost|nejad|zad|ian)\b", re.I), "Iranian", "MEDIUM"),
    (re.compile(r"\b(reza|javad|mehdi|morteza|hossein|mostafa|alireza|mohsen|amir|farhad|shahram|babak|kamran)\b", re.I), "Iranian", "LOW"),
    # Turkish
    (re.compile(r"(oglu|oğlu|zade)\b", re.I), "Turkish / Central Asian", "MEDIUM"),
    # Korean suffixes
    (re.compile(r"\b(il|su|jin|ho|jun|hyun|seok|woo|jae|min)(?: |$)", re.I), "Korean", "LOW"),
    # North Korean typical names
    (re.compile(r"\b(choe|ri |jang|pak |jon |sin |nam |ryu )\b", re.I), "North Korean", "LOW"),
]


def detect_phonetic_nationality(name: str) -> tuple[str, str] | None:
    """
    Apply regex patterns to a Latin-script name to guess nationality.
    Returns the first (nationality, confidence) match or None.
    Prefers the highest-confidence match if multiple fire.
    """
    normalized = unicodedata.normalize("NFKD", name).lower()
    results: list[tuple[str, str]] = []
    for pattern, nationality, confidence in _PHONETIC_PATTERNS:
        if pattern.search(normalized):
            results.append((nationality, confidence))

    if not results:
        return None

    # Prefer HIGH > MEDIUM > LOW; if tied, return first
    for conf in ("HIGH", "MEDIUM", "LOW"):
        for r in results:
            if r[1] == conf:
                return r
    return results[0]


# ── LangGraph nodes ────────────────────────────────────────────────────────────

def node_data_lookup(state: dict) -> dict:
    """
    Tier 1: Check if nationality can be inferred from the record's structured data.
    - Explicit nationality field in record_context
    - Country/citizenship from document IDs
    - Sanctions program → strong nationality inference
    """
    ctx = state.get("record_context", {})

    # Direct nationality field (if record had one)
    for field in ("nationality", "citizenship", "country_of_birth", "country"):
        val = ctx.get(field, "")
        if val and val.lower() not in ("unknown", "none", "", "n/a"):
            return {**state, "nationality": val, "confidence": "HIGH", "method_used": "data_lookup"}

    # Program-based inference — strongest signal after explicit data
    program_raw = ctx.get("sanctions_program", "") or ""
    programs = [p.strip().upper() for p in re.split(r"[;,]", program_raw) if p.strip()]

    for prog in programs:
        for key, (nat, conf) in PROGRAM_NATIONALITY.items():
            if key in prog and conf in ("HIGH", "MEDIUM") and nat != "Unknown":
                return {**state, "nationality": nat, "confidence": conf, "method_used": "data_lookup"}

    # No data-level result — proceed to heuristic
    return state


def node_heuristic(state: dict) -> dict:
    """
    Tier 2: Analyze name script, phonetic patterns, and context clues.
    """
    name = state.get("name", "")

    # 1. Unicode script detection (highest signal for non-Latin names)
    script_result = detect_script_region(name)
    if script_result:
        nat, conf = script_result
        return {**state, "nationality": nat, "confidence": conf, "method_used": "heuristic"}

    # 2. Phonetic / pattern matching on Latin-script names
    phonetic_result = detect_phonetic_nationality(name)
    if phonetic_result and phonetic_result[1] in ("HIGH", "MEDIUM"):
        nat, conf = phonetic_result
        return {**state, "nationality": nat, "confidence": conf, "method_used": "heuristic"}

    # Inconclusive — escalate to LLM
    return state


async def node_llm_inference(state: dict) -> dict:
    """
    Tier 3: Claude LLM inference for cases where data + heuristic are inconclusive.
    """
    name = state.get("name", "")
    ctx = state.get("record_context", {})

    llm = _get_llm()

    context_str = ""
    if ctx.get("entity_type"):
        context_str += f"Entity type: {ctx['entity_type']}\n"
    if ctx.get("sanctions_program"):
        context_str += f"Sanctions program(s): {ctx['sanctions_program']}\n"
    if ctx.get("watchlist"):
        context_str += f"Watchlist: {ctx['watchlist']}\n"

    system = (
        "You are a sanctions analyst specializing in name origin and nationality inference. "
        "Given a name from a sanctions watchlist, determine the most likely nationality, country of origin, "
        "or cultural/geographic region. Be specific when confident, use regions when uncertain. "
        "Respond ONLY with valid JSON matching the schema: "
        '{"nationality": "...", "confidence": "HIGH|MEDIUM|LOW", "reasoning": "..."}'
    )
    user = f"Name: {name}\n{context_str}\nDetermine nationality/origin."

    try:
        resp = await llm.ainvoke([SystemMessage(content=system), HumanMessage(content=user)])
        import json
        text = resp.content.strip()
        # Strip markdown code fences if present
        text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text, flags=re.DOTALL).strip()
        data = json.loads(text)
        nat = data.get("nationality", "Unknown")
        conf = data.get("confidence", "LOW")
        if conf not in ("HIGH", "MEDIUM", "LOW"):
            conf = "LOW"
        return {**state, "nationality": nat, "confidence": conf, "method_used": "llm"}
    except Exception as exc:
        logger.warning(f"LLM inference failed for '{name}': {exc}")
        return {**state, "nationality": "Unknown", "confidence": "LOW", "method_used": "llm"}


def node_output(state: dict) -> dict:
    """Final node — ensures defaults if nothing was resolved."""
    if not state.get("nationality"):
        return {**state, "nationality": "Unknown", "confidence": "LOW", "method_used": state.get("method_used", "heuristic")}
    return state


# ── Routing logic ──────────────────────────────────────────────────────────────

def route_after_data_lookup(state: dict) -> str:
    if state.get("nationality") and state.get("confidence") in ("HIGH", "MEDIUM"):
        return "output"
    return "heuristic"


def route_after_heuristic(state: dict) -> str:
    if state.get("nationality") and state.get("confidence") in ("HIGH", "MEDIUM"):
        return "output"
    return "llm_inference"


# ── Graph construction ─────────────────────────────────────────────────────────

def build_nationality_graph():
    """Build and compile the LangGraph nationality inference workflow."""
    builder = StateGraph(dict)

    builder.add_node("data_lookup", node_data_lookup)
    builder.add_node("heuristic", node_heuristic)
    builder.add_node("llm_inference", node_llm_inference)
    builder.add_node("output", node_output)

    builder.set_entry_point("data_lookup")

    builder.add_conditional_edges(
        "data_lookup",
        route_after_data_lookup,
        {"output": "output", "heuristic": "heuristic"},
    )
    builder.add_conditional_edges(
        "heuristic",
        route_after_heuristic,
        {"output": "output", "llm_inference": "llm_inference"},
    )
    builder.add_edge("llm_inference", "output")
    builder.add_edge("output", END)

    return builder.compile()


# Lazy singleton — compiled once
_graph = None

def get_graph():
    global _graph
    if _graph is None:
        _graph = build_nationality_graph()
    return _graph


@lru_cache(maxsize=1)
def _get_llm() -> ChatAnthropic:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not set in environment")
    return ChatAnthropic(
        model="claude-haiku-4-5-20251001",  # Use Haiku for cost efficiency
        api_key=api_key,
        max_tokens=256,
        temperature=0,
    )


# ── Public API ─────────────────────────────────────────────────────────────────

async def infer_nationality(
    name: str,
    record_context: dict,
    db: aiosqlite.Connection,
) -> dict:
    """
    Run the 3-tier nationality inference for a single name.
    Checks SQLite cache first; writes result to cache after inference.
    Returns dict with nationality, confidence, method_used.
    """
    cache_key = _cache_key(name)

    # Check cache
    async with db.execute(
        "SELECT nationality, confidence, method FROM nationality_cache WHERE name_key = ?",
        (cache_key,),
    ) as cur:
        row = await cur.fetchone()
    if row:
        return {"nationality": row[0], "confidence": row[1], "method_used": row[2], "from_cache": True}

    # Run graph
    initial_state = {
        "name": name,
        "record_context": record_context,
        "nationality": None,
        "confidence": None,
        "method_used": None,
    }
    graph = get_graph()
    result = await graph.ainvoke(initial_state)

    nat = result.get("nationality", "Unknown")
    conf = result.get("confidence", "LOW")
    method = result.get("method_used", "heuristic")

    # Write to cache
    await db.execute(
        """INSERT INTO nationality_cache (name_key, nationality, confidence, method)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(name_key) DO UPDATE SET
             nationality = excluded.nationality,
             confidence  = excluded.confidence,
             method      = excluded.method,
             cached_at   = datetime('now')
        """,
        (cache_key, nat, conf, method),
    )
    await db.commit()

    return {"nationality": nat, "confidence": conf, "method_used": method, "from_cache": False}


async def run_batch_inference(
    db: aiosqlite.Connection,
    watchlists: list[str] | None = None,
    batch_size: int = 500,
    llm_enabled: bool = True,
) -> dict:
    """
    Run nationality inference on all entries that don't have a nationality yet.
    Returns a summary of processed counts by method.
    """
    conditions = ["nationality IS NULL"]
    params: list = []
    if watchlists:
        placeholders = ", ".join("?" for _ in watchlists)
        conditions.append(f"watchlist IN ({placeholders})")
        params.extend(watchlists)

    where = " AND ".join(conditions)
    async with db.execute(
        f"SELECT uid, cleaned_name, watchlist, sub_watchlist_1, entity_type, sanctions_program "
        f"FROM watchlist_entries WHERE {where} LIMIT ?",
        params + [batch_size],
    ) as cur:
        rows = await cur.fetchall()

    if not rows:
        return {"processed": 0, "by_method": {}}

    counts: dict[str, int] = {"data_lookup": 0, "heuristic": 0, "llm": 0, "cached": 0}

    for row in rows:
        uid, name, watchlist, sub_wl, entity_type, program = row
        ctx = {
            "watchlist": watchlist,
            "sub_watchlist_1": sub_wl,
            "entity_type": entity_type,
            "sanctions_program": program,
        }
        try:
            result = await infer_nationality(name, ctx, db)
        except Exception as exc:
            logger.warning(f"Inference failed for uid={uid}: {exc}")
            result = {"nationality": "Unknown", "confidence": "LOW", "method_used": "heuristic"}

        nat = result["nationality"]
        conf = result["confidence"]
        method = result["method_used"]

        if result.get("from_cache"):
            counts["cached"] = counts.get("cached", 0) + 1
        else:
            counts[method] = counts.get(method, 0) + 1

        await db.execute(
            """UPDATE watchlist_entries
               SET nationality = ?, nationality_confidence = ?, nationality_method = ?
               WHERE uid = ?""",
            (nat, conf, method, uid),
        )

    await db.commit()
    return {"processed": len(rows), "by_method": counts}


def _cache_key(name: str) -> str:
    """Normalize name to a stable cache key."""
    return unicodedata.normalize("NFC", name).lower().strip()
