"""
Tests for miss_analyzer.py — pure unit tests (no DB or LLM calls).

Covers:
  - _parse_json_array: markdown stripping, valid/invalid input
  - _chunks: correct batching
  - build_summary: category counts, recommendation deduplication, enrichment
  - node_analyze_batches: success path and per-batch failure fallback (mocked LLM)
  - node_load_cases: correct SQL output (mocked DB)
  - node_generate_summary: delegates to build_summary
  - MISS_CATEGORIES: completeness check
"""
import asyncio
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from app.services.miss_analyzer import (
    _parse_json_array,
    _chunks,
    build_summary,
    MISS_CATEGORIES,
    BATCH_SIZE,
    MAX_FNS,
    node_analyze_batches,
    node_load_cases,
    node_generate_summary,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

SAMPLE_CASES = [
    {
        'test_case_id': 'TC001_aabb',
        'test_name': 'AhmadAlRashid',
        'original_name': 'Ahmad Al-Rashid',
        'test_case_type': 'TC046 - Remove Punctuation',
        'entity_type': 'individual',
    },
    {
        'test_case_id': 'TC002_ccdd',
        'test_name': 'BankMeli',
        'original_name': 'Bank Melli Iran',
        'test_case_type': 'TC047 - Compress Name',
        'entity_type': 'entity',
    },
    {
        'test_case_id': 'TC003_eeff',
        'test_name': 'Q. Soleimani',
        'original_name': 'Qasem Soleimani',
        'test_case_type': 'TC028 - Initial First Name',
        'entity_type': 'individual',
    },
]

SAMPLE_ANALYSES = [
    {
        'test_case_id': 'TC001_aabb',
        'miss_category': 'Special characters / spacing',
        'explanation': 'Removing hyphens can drop below the fuzzy threshold.',
        'recommendation': 'Add a punctuation-stripped normalisation pass before fuzzy match.',
        'confidence': 'high',
    },
    {
        'test_case_id': 'TC002_ccdd',
        'miss_category': 'Special characters / spacing',
        'explanation': 'Compressing spaces creates a single token the system cannot match.',
        'recommendation': 'Add a punctuation-stripped normalisation pass before fuzzy match.',
        'confidence': 'medium',
    },
    {
        'test_case_id': 'TC003_eeff',
        'miss_category': 'Abbreviation / initials',
        'explanation': 'Initials for first name are not expanded before matching.',
        'recommendation': 'Implement initial-expansion lookup for common given names.',
        'confidence': 'high',
    },
]


# ── _parse_json_array ─────────────────────────────────────────────────────────

class TestParseJsonArray:
    def test_plain_array(self):
        text = json.dumps([{'a': 1}, {'b': 2}])
        result = _parse_json_array(text)
        assert result == [{'a': 1}, {'b': 2}]

    def test_strips_json_fence(self):
        text = '```json\n[{"x": 1}]\n```'
        result = _parse_json_array(text)
        assert result == [{'x': 1}]

    def test_strips_plain_fence(self):
        text = '```\n[{"y": 2}]\n```'
        result = _parse_json_array(text)
        assert result == [{'y': 2}]

    def test_leading_trailing_whitespace(self):
        text = '  \n[{"z": 3}]\n  '
        result = _parse_json_array(text)
        assert result == [{'z': 3}]

    def test_empty_array(self):
        assert _parse_json_array('[]') == []

    def test_non_array_returns_empty(self):
        # If Claude returns a dict instead of an array, return []
        result = _parse_json_array('{"key": "value"}')
        assert result == []

    def test_invalid_json_raises(self):
        with pytest.raises(Exception):
            _parse_json_array('not json at all')


# ── _chunks ───────────────────────────────────────────────────────────────────

class TestChunks:
    def test_even_split(self):
        chunks = list(_chunks([1, 2, 3, 4], 2))
        assert chunks == [[1, 2], [3, 4]]

    def test_uneven_split(self):
        chunks = list(_chunks([1, 2, 3, 4, 5], 2))
        assert chunks == [[1, 2], [3, 4], [5]]

    def test_larger_than_list(self):
        chunks = list(_chunks([1, 2], 10))
        assert chunks == [[1, 2]]

    def test_empty_list(self):
        assert list(_chunks([], 5)) == []

    def test_batch_size_one(self):
        chunks = list(_chunks([1, 2, 3], 1))
        assert chunks == [[1], [2], [3]]


# ── build_summary ─────────────────────────────────────────────────────────────

class TestBuildSummary:
    def test_total_fns(self):
        summary = build_summary(SAMPLE_ANALYSES, SAMPLE_CASES)
        assert summary['total_fns'] == len(SAMPLE_CASES)

    def test_analyzed_count(self):
        summary = build_summary(SAMPLE_ANALYSES, SAMPLE_CASES)
        assert summary['analyzed'] == len(SAMPLE_ANALYSES)

    def test_category_counts(self):
        summary = build_summary(SAMPLE_ANALYSES, SAMPLE_CASES)
        cats = summary['categories']
        assert cats['Special characters / spacing'] == 2
        assert cats['Abbreviation / initials'] == 1

    def test_categories_sorted_descending(self):
        summary = build_summary(SAMPLE_ANALYSES, SAMPLE_CASES)
        counts = list(summary['categories'].values())
        assert counts == sorted(counts, reverse=True)

    def test_recommendations_deduplicated(self):
        # Both TC001 and TC002 share the same recommendation
        summary = build_summary(SAMPLE_ANALYSES, SAMPLE_CASES)
        recs = summary['top_recommendations']
        assert len(recs) == len(set(recs)), "Recommendations should be deduplicated"

    def test_recommendations_high_confidence_first(self):
        summary = build_summary(SAMPLE_ANALYSES, SAMPLE_CASES)
        # 'high' confidence entries' recs should appear before 'medium'
        # TC001 (high) and TC003 (high) share recs; TC002 (medium) has a duplicate rec
        # The unique recs are: [TC001/TC002 normalisation pass, TC003 initial-expansion]
        # TC001 is high so the normalisation pass rec should appear first
        recs = summary['top_recommendations']
        assert 'normalisation' in recs[0].lower() or 'initial' in recs[0].lower()

    def test_cases_enriched_with_tc_metadata(self):
        summary = build_summary(SAMPLE_ANALYSES, SAMPLE_CASES)
        case = next(c for c in summary['cases'] if c['test_case_id'] == 'TC001_aabb')
        assert case['original_name'] == 'Ahmad Al-Rashid'
        assert case['miss_category'] == 'Special characters / spacing'

    def test_empty_analyses(self):
        summary = build_summary([], SAMPLE_CASES)
        assert summary['total_fns'] == 3
        assert summary['analyzed'] == 0
        assert summary['categories'] == {}
        assert summary['top_recommendations'] == []
        assert summary['cases'] == []

    def test_empty_cases_and_analyses(self):
        summary = build_summary([], [])
        assert summary['total_fns'] == 0
        assert summary['analyzed'] == 0

    def test_max_10_recommendations(self):
        many_analyses = [
            {
                'test_case_id': f'TC{i:03d}',
                'miss_category': 'Other',
                'explanation': 'x',
                'recommendation': f'Unique recommendation number {i}',
                'confidence': 'medium',
            }
            for i in range(20)
        ]
        summary = build_summary(many_analyses, [])
        assert len(summary['top_recommendations']) <= 10

    def test_unknown_case_id_handled(self):
        # Analysis references a test_case_id not in fn_cases — should not crash
        orphan = [{
            'test_case_id': 'ORPHAN_999',
            'miss_category': 'Other',
            'explanation': 'Mystery case',
            'recommendation': 'Check logs.',
            'confidence': 'low',
        }]
        summary = build_summary(orphan, [])
        assert summary['cases'][0]['test_case_id'] == 'ORPHAN_999'


# ── node_analyze_batches ──────────────────────────────────────────────────────

class TestNodeAnalyzeBatches:
    def test_empty_cases_returns_empty(self):
        state = {'fn_cases': [], 'analyses': [], 'summary': {}, 'db': None}
        result = asyncio.run(node_analyze_batches(state))
        assert result == {'analyses': []}

    def test_success_path(self):
        """LLM returns a valid JSON array → analyses populated."""
        llm_response = MagicMock()
        llm_response.content = json.dumps([
            {
                'test_case_id': 'TC001_aabb',
                'miss_category': 'Special characters / spacing',
                'explanation': 'Hyphens removed.',
                'recommendation': 'Normalise punctuation.',
                'confidence': 'high',
            }
        ])
        mock_llm = MagicMock()
        mock_llm.invoke.return_value = llm_response

        state = {
            'fn_cases': [SAMPLE_CASES[0]],
            'analyses': [],
            'summary': {},
            'db': None,
        }

        with patch('app.services.miss_analyzer._llm', return_value=mock_llm):
            result = asyncio.run(node_analyze_batches(state))

        assert len(result['analyses']) == 1
        assert result['analyses'][0]['test_case_id'] == 'TC001_aabb'
        assert result['analyses'][0]['miss_category'] == 'Special characters / spacing'

    def test_batch_failure_fallback(self):
        """LLM raises an exception → placeholder entries inserted for all cases in batch."""
        mock_llm = MagicMock()
        mock_llm.invoke.side_effect = RuntimeError("API timeout")

        state = {
            'fn_cases': SAMPLE_CASES[:2],
            'analyses': [],
            'summary': {},
            'db': None,
        }

        with patch('app.services.miss_analyzer._llm', return_value=mock_llm):
            result = asyncio.run(node_analyze_batches(state))

        assert len(result['analyses']) == 2
        for a in result['analyses']:
            assert a['miss_category'] == 'Other'
            assert a['confidence'] == 'low'
            assert 'API timeout' in a['explanation']

    def test_items_without_test_case_id_skipped(self):
        """Malformed items without test_case_id are silently dropped."""
        llm_response = MagicMock()
        llm_response.content = json.dumps([
            {'miss_category': 'Other', 'explanation': 'No ID here'},   # malformed
            {
                'test_case_id': 'TC001_aabb',
                'miss_category': 'Token omission',
                'explanation': 'Token missing.',
                'recommendation': 'Widen match window.',
                'confidence': 'medium',
            },
        ])
        mock_llm = MagicMock()
        mock_llm.invoke.return_value = llm_response

        state = {
            'fn_cases': [SAMPLE_CASES[0]],
            'analyses': [],
            'summary': {},
            'db': None,
        }

        with patch('app.services.miss_analyzer._llm', return_value=mock_llm):
            result = asyncio.run(node_analyze_batches(state))

        # Only the item with test_case_id should survive
        assert len(result['analyses']) == 1
        assert result['analyses'][0]['test_case_id'] == 'TC001_aabb'


# ── node_load_cases ───────────────────────────────────────────────────────────

class TestNodeLoadCases:
    def test_returns_fn_cases(self):
        """node_load_cases correctly maps DB rows to case dicts."""
        mock_rows = [
            ('TC001_aabb', 'AhmadAlRashid', 'Ahmad Al-Rashid', 'TC046', 'individual'),
            ('TC002_ccdd', 'BankMeli', 'Bank Melli Iran', 'TC047', 'entity'),
        ]

        mock_cursor = AsyncMock()
        mock_cursor.fetchall.return_value = mock_rows
        mock_cursor.__aenter__ = AsyncMock(return_value=mock_cursor)
        mock_cursor.__aexit__ = AsyncMock(return_value=False)

        mock_db = MagicMock()
        mock_db.execute = MagicMock(return_value=mock_cursor)

        state = {'fn_cases': [], 'analyses': [], 'summary': {}, 'db': mock_db}
        result = asyncio.run(node_load_cases(state))

        cases = result['fn_cases']
        assert len(cases) == 2
        assert cases[0]['test_case_id'] == 'TC001_aabb'
        assert cases[0]['test_name'] == 'AhmadAlRashid'
        assert cases[1]['entity_type'] == 'entity'

    def test_empty_db_returns_empty_list(self):
        mock_cursor = AsyncMock()
        mock_cursor.fetchall.return_value = []
        mock_cursor.__aenter__ = AsyncMock(return_value=mock_cursor)
        mock_cursor.__aexit__ = AsyncMock(return_value=False)

        mock_db = MagicMock()
        mock_db.execute = MagicMock(return_value=mock_cursor)

        state = {'fn_cases': [], 'analyses': [], 'summary': {}, 'db': mock_db}
        result = asyncio.run(node_load_cases(state))
        assert result == {'fn_cases': []}


# ── node_generate_summary ─────────────────────────────────────────────────────

class TestNodeGenerateSummary:
    def test_delegates_to_build_summary(self):
        state = {
            'fn_cases': SAMPLE_CASES,
            'analyses': SAMPLE_ANALYSES,
            'summary': {},
            'db': None,
        }
        result = asyncio.run(node_generate_summary(state))
        assert 'summary' in result
        assert result['summary']['total_fns'] == len(SAMPLE_CASES)
        assert result['summary']['analyzed'] == len(SAMPLE_ANALYSES)


# ── MISS_CATEGORIES constant ──────────────────────────────────────────────────

class TestMissCategories:
    def test_has_12_categories(self):
        assert len(MISS_CATEGORIES) == 12

    def test_includes_other(self):
        assert 'Other' in MISS_CATEGORIES

    def test_no_duplicates(self):
        assert len(MISS_CATEGORIES) == len(set(MISS_CATEGORIES))

    def test_all_strings(self):
        assert all(isinstance(c, str) for c in MISS_CATEGORIES)


# ── Constants sanity checks ───────────────────────────────────────────────────

class TestConstants:
    def test_batch_size_positive(self):
        assert BATCH_SIZE > 0

    def test_max_fns_positive(self):
        assert MAX_FNS > 0

    def test_batch_size_leq_max_fns(self):
        assert BATCH_SIZE <= MAX_FNS
