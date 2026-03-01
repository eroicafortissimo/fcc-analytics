"""
miss_analyzer.py — LangGraph workflow to analyze false negatives from screening results.

For each FN case (expected=HIT, actual=MISS) the workflow:
  1. load_cases       — fetch all FN rows (capped at MAX_FNS) from the DB
  2. analyze_batches  — send batches of BATCH_SIZE to Claude Haiku for categorised analysis
  3. generate_summary — aggregate categories, deduplicate recommendations

Results are saved to the miss_analyses table and the miss_explanation column in
screening_results for quick access.
"""
from __future__ import annotations

import json
import os
import re
from typing import Any, Optional

import aiosqlite
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict


# ── Constants ──────────────────────────────────────────────────────────────────

MISS_CATEGORIES = [
    'Exact match noise',
    'Transliteration variant',
    'Abbreviation / initials',
    'Token omission',
    'Token insertion',
    'Token reorder / permutation',
    'Legal form variant',
    'Special characters / spacing',
    'Script / encoding issue',
    'Deliberate obfuscation',
    'Threshold gap',
    'Other',
]

BATCH_SIZE = 10
MAX_FNS = 100


# ── LLM ───────────────────────────────────────────────────────────────────────

def _llm() -> ChatAnthropic:
    return ChatAnthropic(
        model="claude-haiku-4-5-20251001",
        api_key=os.environ.get("ANTHROPIC_API_KEY", ""),
        temperature=0,
        max_tokens=2048,
    )


# ── State ─────────────────────────────────────────────────────────────────────

class MissAnalysisState(TypedDict):
    fn_cases: list[dict]    # Loaded from DB
    analyses: list[dict]    # One entry per FN case after Claude analysis
    summary: dict           # Aggregated output
    db: Any                 # aiosqlite.Connection (not serialised; in-memory only)


# ── Prompts ───────────────────────────────────────────────────────────────────

_SYSTEM = """\
You are a sanctions screening quality analyst. You are given a batch of false negatives
(test cases the screening system should have flagged but did not).

For each case you receive:
  - test_case_id:   unique identifier
  - test_name:      the name variation submitted to the screener
  - original_name:  the watchlisted name
  - test_case_type: the type of name variation applied
  - entity_type:    individual / entity / vessel / aircraft

Your job: explain why the screening system likely missed each case.

Return a JSON array with exactly one object per input case, in the same order:
[
  {
    "test_case_id": "...",
    "miss_category": "<one category from the list below>",
    "explanation": "<1-2 sentences explaining why the system missed this>",
    "recommendation": "<one specific, actionable recommendation for the screening team>",
    "confidence": "high" | "medium" | "low"
  },
  ...
]

Miss categories (use exactly one of these strings):
  "Exact match noise"            - small character-level edit exceeded edit-distance threshold
  "Transliteration variant"      - alternate romanisation of Arabic/Persian/Cyrillic names
  "Abbreviation / initials"      - name tokens abbreviated to initials or short forms
  "Token omission"               - one or more name tokens removed
  "Token insertion"              - extra tokens added around the core name
  "Token reorder / permutation"  - tokens appear in a different order
  "Legal form variant"           - legal designator changed, added, or removed
  "Special characters / spacing" - hyphens, spaces, or punctuation differences
  "Script / encoding issue"      - diacritics, Unicode variants, expanded characters
  "Deliberate obfuscation"       - zero-width chars, homoglyphs, or noise injection
  "Threshold gap"                - variation is close but just below the similarity threshold
  "Other"                        - does not fit any category above

Return ONLY the JSON array. No markdown fences, no text outside the array.
"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_json_array(text: str) -> list[dict]:
    """Strip markdown fences and parse a JSON array from the model response."""
    text = text.strip()
    text = re.sub(r'^```(?:json)?\s*', '', text)
    text = re.sub(r'\s*```$', '', text)
    result = json.loads(text)
    return result if isinstance(result, list) else []


def _chunks(lst: list, n: int):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def build_summary(analyses: list[dict], fn_cases: list[dict]) -> dict:
    """
    Pure aggregation: given per-case analyses and FN case metadata, return summary dict.
    Exported for unit testing.
    """
    categories: dict[str, int] = {}
    for a in analyses:
        cat = a.get('miss_category', 'Other')
        categories[cat] = categories.get(cat, 0) + 1

    # Deduplicate recommendations, highest-confidence first
    _conf_rank = {'high': 0, 'medium': 1, 'low': 2}
    sorted_analyses = sorted(
        analyses,
        key=lambda x: _conf_rank.get(x.get('confidence', 'low'), 2),
    )
    seen: set[str] = set()
    top_recommendations: list[str] = []
    for a in sorted_analyses:
        rec = a.get('recommendation', '').strip()
        if rec and rec not in seen:
            seen.add(rec)
            top_recommendations.append(rec)
        if len(top_recommendations) >= 10:
            break

    case_map = {c['test_case_id']: c for c in fn_cases}
    enriched = [
        {**case_map.get(a['test_case_id'], {}), **a}
        for a in analyses
    ]

    return {
        'total_fns': len(fn_cases),
        'analyzed': len(analyses),
        'categories': dict(sorted(categories.items(), key=lambda x: -x[1])),
        'top_recommendations': top_recommendations,
        'cases': enriched,
    }


# ── Graph nodes ────────────────────────────────────────────────────────────────

async def node_load_cases(state: MissAnalysisState) -> dict:
    """Fetch all false-negative rows from the DB (capped at MAX_FNS)."""
    db = state['db']
    async with db.execute(
        """
        SELECT sr.test_case_id, sr.test_name, tc.cleaned_original_name,
               tc.test_case_type, tc.entity_type
        FROM screening_results sr
        JOIN test_cases tc ON sr.test_case_id = tc.test_case_id
        WHERE sr.expected_result = 'HIT' AND sr.actual_result = 'MISS'
        ORDER BY sr.test_case_id
        LIMIT ?
        """,
        (MAX_FNS,),
    ) as cur:
        rows = await cur.fetchall()

    cases = [
        {
            'test_case_id': r[0],
            'test_name': r[1],
            'original_name': r[2],
            'test_case_type': r[3],
            'entity_type': r[4],
        }
        for r in rows
    ]
    return {'fn_cases': cases}


async def node_analyze_batches(state: MissAnalysisState) -> dict:
    """Send batches of FN cases to Claude Haiku for miss categorisation."""
    cases = state['fn_cases']
    if not cases:
        return {'analyses': []}

    llm = _llm()
    all_analyses: list[dict] = []

    for batch in _chunks(cases, BATCH_SIZE):
        prompt = json.dumps(batch, indent=2)
        try:
            response = llm.invoke([
                SystemMessage(content=_SYSTEM),
                HumanMessage(content=prompt),
            ])
            parsed = _parse_json_array(response.content)
            for item in parsed:
                if 'test_case_id' in item:
                    all_analyses.append({
                        'test_case_id': item.get('test_case_id', ''),
                        'miss_category': item.get('miss_category', 'Other'),
                        'explanation': item.get('explanation', ''),
                        'recommendation': item.get('recommendation', ''),
                        'confidence': item.get('confidence', 'medium'),
                    })
        except Exception as exc:
            # On batch failure add placeholder entries for every case in that batch
            for case in batch:
                all_analyses.append({
                    'test_case_id': case['test_case_id'],
                    'miss_category': 'Other',
                    'explanation': f'Analysis unavailable: {exc}',
                    'recommendation': 'Manual review required.',
                    'confidence': 'low',
                })

    return {'analyses': all_analyses}


async def node_generate_summary(state: MissAnalysisState) -> dict:
    """Aggregate per-case analyses into the summary dict."""
    return {'summary': build_summary(state['analyses'], state['fn_cases'])}


# ── Graph ──────────────────────────────────────────────────────────────────────

def _build_graph():
    g = StateGraph(MissAnalysisState)
    g.add_node('load_cases', node_load_cases)
    g.add_node('analyze_batches', node_analyze_batches)
    g.add_node('generate_summary', node_generate_summary)

    g.add_edge(START, 'load_cases')
    g.add_edge('load_cases', 'analyze_batches')
    g.add_edge('analyze_batches', 'generate_summary')
    g.add_edge('generate_summary', END)

    return g.compile()


_GRAPH: Optional[Any] = None


def _get_graph():
    global _GRAPH
    if _GRAPH is None:
        _GRAPH = _build_graph()
    return _GRAPH


# ── Persistence ────────────────────────────────────────────────────────────────

async def _save_analyses(
    analyses: list[dict],
    fn_cases: list[dict],
    db: aiosqlite.Connection,
) -> None:
    """Persist per-case analyses to miss_analyses and update screening_results."""
    case_map = {c['test_case_id']: c for c in fn_cases}

    for a in analyses:
        tc = case_map.get(a['test_case_id'], {})

        await db.execute(
            """
            INSERT INTO miss_analyses
                (test_case_id, test_name, original_name, test_case_type, entity_type,
                 miss_category, explanation, recommendation, confidence)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(test_case_id) DO UPDATE SET
                miss_category  = excluded.miss_category,
                explanation    = excluded.explanation,
                recommendation = excluded.recommendation,
                confidence     = excluded.confidence,
                analyzed_at    = datetime('now')
            """,
            (
                a['test_case_id'],
                tc.get('test_name', ''),
                tc.get('original_name', ''),
                tc.get('test_case_type', ''),
                tc.get('entity_type', ''),
                a['miss_category'],
                a['explanation'],
                a['recommendation'],
                a['confidence'],
            ),
        )

        # Mirror a compact label into the existing miss_explanation column
        await db.execute(
            "UPDATE screening_results SET miss_explanation = ? WHERE test_case_id = ?",
            (f"[{a['miss_category']}] {a['explanation']}", a['test_case_id']),
        )

    await db.commit()


# ── Public entry points ────────────────────────────────────────────────────────

async def run_miss_analysis(db: aiosqlite.Connection) -> dict:
    """
    Run the full miss-analysis pipeline over all current false negatives.

    Returns:
      {
        total_fns:           int,
        analyzed:            int,
        categories:          {category_name: count, ...},
        top_recommendations: [str, ...],
        cases:               [{test_case_id, test_name, miss_category, ...}, ...]
      }
    """
    graph = _get_graph()
    initial_state: MissAnalysisState = {
        'fn_cases': [],
        'analyses': [],
        'summary': {},
        'db': db,
    }
    result = await graph.ainvoke(initial_state)

    if result.get('analyses'):
        await _save_analyses(result['analyses'], result['fn_cases'], db)

    return result.get('summary', {})


async def get_saved_analyses(db: aiosqlite.Connection) -> list[dict]:
    """Return all previously saved miss analyses, newest first."""
    async with db.execute(
        """
        SELECT test_case_id, test_name, original_name, test_case_type, entity_type,
               miss_category, explanation, recommendation, confidence, analyzed_at
        FROM miss_analyses
        ORDER BY analyzed_at DESC
        """
    ) as cur:
        rows = await cur.fetchall()

    return [dict(r) for r in rows]
