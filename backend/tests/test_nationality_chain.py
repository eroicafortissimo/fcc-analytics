"""
Tests for the 3-tier nationality inference chain.
Focuses on the deterministic tiers (data_lookup, heuristic) — no LLM calls.
Run: PYTHONPATH=backend python -m pytest tests/test_nationality_chain.py -v
"""
import pytest
from app.services.nationality_chain import (
    node_data_lookup,
    node_heuristic,
    detect_script_region,
    detect_phonetic_nationality,
    route_after_data_lookup,
    route_after_heuristic,
    _cache_key,
)


# ── Helpers ────────────────────────────────────────────────────────────────────

def make_state(name: str, context: dict = None) -> dict:
    return {
        "name": name,
        "record_context": context or {},
        "nationality": None,
        "confidence": None,
        "method_used": None,
    }


# ── Tier 1: data_lookup ────────────────────────────────────────────────────────

class TestDataLookup:
    def test_explicit_nationality_field(self):
        s = make_state("Test Name", {"nationality": "Iranian"})
        result = node_data_lookup(s)
        assert result["nationality"] == "Iranian"
        assert result["confidence"] == "HIGH"
        assert result["method_used"] == "data_lookup"

    def test_explicit_citizenship_field(self):
        s = make_state("Test Name", {"citizenship": "Russian"})
        result = node_data_lookup(s)
        assert result["nationality"] == "Russian"
        assert result["confidence"] == "HIGH"

    def test_unknown_nationality_field_passes_through(self):
        s = make_state("Test Name", {"nationality": "unknown"})
        result = node_data_lookup(s)
        assert result["nationality"] is None  # 'unknown' is ignored

    def test_iran_program(self):
        s = make_state("Ali Hosseini", {"sanctions_program": "IRAN"})
        result = node_data_lookup(s)
        assert result["nationality"] == "Iranian"
        assert result["confidence"] == "HIGH"
        assert result["method_used"] == "data_lookup"

    def test_russia_program(self):
        s = make_state("Ivan Petrov", {"sanctions_program": "RUSSIA"})
        result = node_data_lookup(s)
        assert result["nationality"] == "Russian"
        assert result["confidence"] == "HIGH"

    def test_dprk_program(self):
        s = make_state("Kim Jong", {"sanctions_program": "DPRK3"})
        result = node_data_lookup(s)
        assert result["nationality"] == "North Korean"
        assert result["confidence"] == "HIGH"

    def test_cuba_program(self):
        s = make_state("José Martinez", {"sanctions_program": "CUBA"})
        result = node_data_lookup(s)
        assert result["nationality"] == "Cuban"

    def test_multiple_programs_first_wins(self):
        s = make_state("Name", {"sanctions_program": "IRAN; SDGT"})
        result = node_data_lookup(s)
        assert result["nationality"] == "Iranian"

    def test_sdgt_alone_does_not_resolve(self):
        """SDGT alone is LOW confidence / Unknown — should not resolve at data_lookup."""
        s = make_state("Name", {"sanctions_program": "SDGT"})
        result = node_data_lookup(s)
        assert result["nationality"] is None  # LOW confidence Unknown is skipped

    def test_no_context_passes_through(self):
        s = make_state("Name")
        result = node_data_lookup(s)
        assert result["nationality"] is None

    def test_venezuela_program(self):
        s = make_state("Carlos Rodriguez", {"sanctions_program": "VENEZUELA"})
        result = node_data_lookup(s)
        assert result["nationality"] == "Venezuelan"


# ── Tier 2: heuristic ──────────────────────────────────────────────────────────

class TestHeuristic:
    # -- Script detection --
    def test_arabic_script(self):
        s = make_state("أسامة بن لادن")
        result = node_heuristic(s)
        assert result["nationality"] == "Middle Eastern / North African"
        assert result["confidence"] == "MEDIUM"
        assert result["method_used"] == "heuristic"

    def test_cyrillic_script(self):
        s = make_state("Владимир Путин")
        result = node_heuristic(s)
        assert "Russian" in result["nationality"] or "Eastern European" in result["nationality"]
        assert result["method_used"] == "heuristic"

    def test_korean_hangul(self):
        s = make_state("김정은")
        result = node_heuristic(s)
        assert result["nationality"] == "Korean"
        assert result["confidence"] == "HIGH"

    def test_japanese_katakana(self):
        s = make_state("タナカ ヒロシ")
        result = node_heuristic(s)
        assert result["nationality"] == "Japanese"

    def test_hebrew_script(self):
        s = make_state("יצחק רבין")
        result = node_heuristic(s)
        assert "Israeli" in result["nationality"] or "Middle Eastern" in result["nationality"]

    def test_devanagari_script(self):
        s = make_state("नरेंद्र मोदी")
        result = node_heuristic(s)
        assert "South Asian" in result["nationality"] or "Indian" in result["nationality"]

    def test_thai_script(self):
        s = make_state("ประยุทธ์ จันทร์โอชา")
        result = node_heuristic(s)
        assert result["nationality"] == "Thai"

    def test_georgian_script(self):
        s = make_state("მიხეილ სააკაშვილი")
        result = node_heuristic(s)
        assert result["nationality"] == "Georgian"

    # -- Phonetic patterns --
    def test_russian_suffix_ovich(self):
        s = make_state("Dmitri Alexandrovich")
        result = node_heuristic(s)
        assert "Russian" in result["nationality"] or "Eastern European" in result["nationality"]

    def test_iranian_suffix_zadeh(self):
        s = make_state("Mir Hossein Mousavi Khamenehzadeh")
        result = node_heuristic(s)
        assert result["nationality"] == "Iranian"
        assert result["confidence"] == "MEDIUM"

    def test_korean_family_name(self):
        s = make_state("Kim Jong Un")
        result = node_heuristic(s)
        # kim should trigger Korean LOW but that's not HIGH/MEDIUM so goes to LLM
        # This is expected to be inconclusive at heuristic level
        # (low confidence patterns don't resolve)
        assert result.get("method_used") in (None, "heuristic")

    def test_turkish_oglu_suffix(self):
        s = make_state("Mehmet Yilmazoglu")
        result = node_heuristic(s)
        assert "Turkish" in result["nationality"] or "Central Asian" in result["nationality"]

    def test_latin_only_no_match(self):
        """Pure Latin name with no identifiable pattern → should not resolve."""
        s = make_state("Smith Johnson")
        result = node_heuristic(s)
        # Should not set a HIGH or MEDIUM nationality for ambiguous Latin names
        assert result.get("confidence") not in ("HIGH", "MEDIUM") or result.get("nationality") is None

    def test_mixed_script_arabic_dominant(self):
        """Name with mostly Arabic plus a few Latin chars."""
        s = make_state("محمد ABC")
        result = node_heuristic(s)
        # Arabic is dominant
        assert result["nationality"] == "Middle Eastern / North African"


# ── Script detection unit tests ────────────────────────────────────────────────

class TestScriptDetection:
    def test_arabic(self):
        result = detect_script_region("عبد الله")
        assert result is not None
        assert result[0] == "Middle Eastern / North African"

    def test_cyrillic(self):
        result = detect_script_region("Иванов")
        assert result is not None
        assert "Russian" in result[0]

    def test_hangul(self):
        result = detect_script_region("한국어")
        assert result == ("Korean", "HIGH")

    def test_hiragana(self):
        result = detect_script_region("ひらがな")
        assert result == ("Japanese", "HIGH")

    def test_latin_returns_none(self):
        result = detect_script_region("John Smith")
        assert result is None

    def test_empty_string(self):
        result = detect_script_region("")
        assert result is None

    def test_numbers_only(self):
        result = detect_script_region("12345")
        assert result is None


# ── Phonetic detection unit tests ─────────────────────────────────────────────

class TestPhoneticDetection:
    def test_ovich_suffix(self):
        result = detect_phonetic_nationality("Dmitrievich")
        assert result is not None
        assert "Russian" in result[0]

    def test_zadeh_suffix(self):
        result = detect_phonetic_nationality("Ahmadinezadeh")
        assert result is not None
        assert result[0] == "Iranian"

    def test_oglu_suffix(self):
        result = detect_phonetic_nationality("Aliyoglu")
        assert result is not None
        assert "Turkish" in result[0]

    def test_no_match(self):
        result = detect_phonetic_nationality("Smith")
        assert result is None

    def test_russian_ova_suffix(self):
        result = detect_phonetic_nationality("Ivanova")
        assert result is not None
        assert "Russian" in result[0]


# ── Routing logic ──────────────────────────────────────────────────────────────

class TestRouting:
    def test_route_after_data_lookup_resolved(self):
        s = {"nationality": "Iranian", "confidence": "HIGH"}
        assert route_after_data_lookup(s) == "output"

    def test_route_after_data_lookup_not_resolved(self):
        s = {"nationality": None, "confidence": None}
        assert route_after_data_lookup(s) == "heuristic"

    def test_route_after_heuristic_resolved(self):
        s = {"nationality": "Russian", "confidence": "MEDIUM"}
        assert route_after_heuristic(s) == "output"

    def test_route_after_heuristic_not_resolved(self):
        s = {"nationality": None, "confidence": None}
        assert route_after_heuristic(s) == "llm_inference"

    def test_route_low_confidence_goes_to_llm(self):
        s = {"nationality": "Unknown", "confidence": "LOW"}
        assert route_after_heuristic(s) == "llm_inference"


# ── Cache key ──────────────────────────────────────────────────────────────────

class TestCacheKey:
    def test_normalized(self):
        assert _cache_key("  JOHN SMITH  ") == "john smith"

    def test_nfc(self):
        # Same logical string, different Unicode normalization → same cache key
        a = "Re\u0301mi"   # decomposed
        b = "R\u00e9mi"    # composed
        assert _cache_key(a) == _cache_key(b)

    def test_arabic_preserved(self):
        key = _cache_key("أسامة بن لادن")
        assert len(key) > 0
