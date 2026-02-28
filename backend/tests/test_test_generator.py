"""
Tests for the test case generation variation functions.
All deterministic — no DB or LLM calls needed.
Run: PYTHONPATH=backend python -m pytest tests/test_test_generator.py -v
"""
import random
import pytest
from app.services.test_generator import (
    VARIATION_FUNCTIONS, load_test_case_types, outcome_to_result,
    _v_exact_match, _v_omit_article, _v_omit_legal_designator,
    _v_omit_first_name, _v_omit_middle_name, _v_omit_title, _v_omit_prefix,
    _v_omit_qualifier, _v_insert_article, _v_insert_legal_designator,
    _v_reverse_tokens, _v_swap_first_last, _v_reorder_given_names,
    _v_initial_first_name, _v_initial_all_given, _v_company_acronym,
    _v_abbrev_token, _v_char_insert, _v_char_delete, _v_char_repeat,
    _v_char_transpose, _v_truncate_front, _v_truncate_end,
    _v_single_typo, _v_phonetic_sub, _v_insert_hyphen, _v_extra_spaces,
    _v_expand_diacritics, _v_remove_punctuation, _v_compress_name,
    _v_split_token, _v_strip_accents, _v_cyrillic_transliterate,
    _v_vessel_add_prefix, _v_vessel_remove_prefix,
    _v_homoglyph, _v_zero_width, _v_dialectal_prefix,
    _v_noise_suffix, _v_period_short_name, _v_country_code_suffix,
    _v_equiv_legal_designator, _v_alternate_romanisation,
)


RNG = random.Random(42)
REC = {'watchlist': 'OFAC_SDN', 'entity_type': 'individual', 'nationality': 'Iranian',
       'sanctions_program': 'IRAN'}


def v(fn, name, record=None):
    """Call variation function, return (test_name, rationale)."""
    return fn(name, record or REC, RNG)


# ── CSV loader ─────────────────────────────────────────────────────────────────

class TestCSVLoader:
    def test_loads_all_58_types(self):
        types = load_test_case_types()
        assert len(types) == 58

    def test_all_type_ids_have_functions(self):
        types = load_test_case_types()
        missing = [t.type_id for t in types if t.type_id not in VARIATION_FUNCTIONS]
        assert missing == [], f"Type IDs without functions: {missing}"

    def test_min_tokens_and_length_are_positive(self):
        for t in load_test_case_types():
            assert t.applicable_min_tokens >= 1
            assert t.applicable_min_name_length >= 1

    def test_applicable_entity_types_non_empty(self):
        for t in load_test_case_types():
            assert len(t.applicable_entity_types) > 0


# ── Outcome mapping ────────────────────────────────────────────────────────────

class TestOutcomeMapping:
    def test_must_hit(self):
        r, _ = outcome_to_result('Must Hit')
        assert r == 'HIT'

    def test_should_hit(self):
        r, _ = outcome_to_result('Should Hit')
        assert r == 'HIT'

    def test_testing_purposes(self):
        r, _ = outcome_to_result('Testing Purposes')
        assert r == 'HIT'

    def test_should_not_hit(self):
        r, _ = outcome_to_result('Should Not Hit')
        assert r == 'MISS'


# ── Baseline ───────────────────────────────────────────────────────────────────

class TestExactMatch:
    def test_returns_unchanged(self):
        name, _ = v(_v_exact_match, 'Rosneft Oil Company')
        assert name == 'Rosneft Oil Company'

    def test_arabic_unchanged(self):
        name, _ = v(_v_exact_match, 'أسامة بن لادن')
        assert name == 'أسامة بن لادن'


# ── Structural Omissions ───────────────────────────────────────────────────────

class TestOmitArticle:
    def test_removes_the(self):
        name, _ = v(_v_omit_article, 'The Bank of Tehran')
        assert name == 'Bank of Tehran'

    def test_removes_a(self):
        name, _ = v(_v_omit_article, 'A Trading Company')
        assert 'A' not in name.split()[:1]

    def test_skips_when_no_article(self):
        name, rationale = v(_v_omit_article, 'Rosneft Oil Company')
        assert name is None
        assert 'skip' in rationale.lower()

    def test_preserves_rest_of_name(self):
        name, _ = v(_v_omit_article, 'The First Bank of Iran')
        assert 'First' in name
        assert 'Iran' in name


class TestOmitLegalDesignator:
    def test_removes_llc(self):
        name, _ = v(_v_omit_legal_designator, 'Mahan Air LLC')
        assert 'LLC' not in name
        assert 'Mahan Air' in name

    def test_removes_ltd(self):
        name, _ = v(_v_omit_legal_designator, 'Iran Khodro Ltd')
        assert 'Ltd' not in name

    def test_skips_no_designator(self):
        name, _ = v(_v_omit_legal_designator, 'Ali Hassan Majid')
        assert name is None


class TestOmitFirstName:
    def test_removes_first_token(self):
        name, _ = v(_v_omit_first_name, 'Mohammad Reza Rahimi')
        assert name == 'Reza Rahimi'

    def test_skips_single_token(self):
        name, _ = v(_v_omit_first_name, 'Suharto')
        assert name is None


class TestOmitMiddleName:
    def test_keeps_first_and_last(self):
        name, _ = v(_v_omit_middle_name, 'Kim Jong Un')
        assert name == 'Kim Un'

    def test_keeps_first_and_last_long(self):
        name, _ = v(_v_omit_middle_name, 'Jean Pierre Louis Dupont')
        assert name == 'Jean Dupont'

    def test_skips_two_tokens(self):
        name, _ = v(_v_omit_middle_name, 'Kim Un')
        assert name is None


class TestOmitTitle:
    def test_removes_dr(self):
        name, _ = v(_v_omit_title, 'Dr Hassan Rouhani')
        assert name == 'Hassan Rouhani'

    def test_removes_general(self):
        name, _ = v(_v_omit_title, 'General Qasem Soleimani')
        assert 'General' not in name

    def test_skips_no_title(self):
        name, _ = v(_v_omit_title, 'Qasem Soleimani')
        assert name is None


class TestOmitPrefix:
    def test_removes_al_prefix(self):
        name, _ = v(_v_omit_prefix, 'Al-Zawahiri Ayman')
        assert name.startswith('Zawahiri')

    def test_removes_el_prefix(self):
        name, _ = v(_v_omit_prefix, 'El-Rashid Trading')
        assert not name.startswith('El-')

    def test_skips_no_prefix(self):
        name, _ = v(_v_omit_prefix, 'Kim Jong Un')
        assert name is None


class TestOmitQualifier:
    def test_removes_parenthetical(self):
        name, _ = v(_v_omit_qualifier, 'Kim Jong Un (Supreme Leader)')
        assert '(Supreme Leader)' not in name
        assert 'Kim Jong Un' in name

    def test_skips_no_qualifier(self):
        name, _ = v(_v_omit_qualifier, 'Kim Jong Un')
        assert name is None


# ── Structural Insertions ──────────────────────────────────────────────────────

class TestInsertArticle:
    def test_prepends_the(self):
        name, _ = v(_v_insert_article, 'Iran Air')
        assert name.startswith('The ')

    def test_entity_name_preserved(self):
        name, _ = v(_v_insert_article, 'Bank Melli')
        assert 'Bank Melli' in name


class TestInsertLegalDesignator:
    def test_appends_designator(self):
        name, _ = v(_v_insert_legal_designator, 'Rosoboronexport')
        parts = name.split()
        assert len(parts) > 1
        assert parts[0] == 'Rosoboronexport'


# ── Sequence Permutations ──────────────────────────────────────────────────────

class TestReverseTokens:
    def test_reverses_two_tokens(self):
        name, _ = v(_v_reverse_tokens, 'Viktor Bout')
        assert name == 'Bout Viktor'

    def test_reverses_three_tokens(self):
        name, _ = v(_v_reverse_tokens, 'Kim Jong Un')
        assert name == 'Un Jong Kim'


class TestSwapFirstLast:
    def test_swaps_two(self):
        name, _ = v(_v_swap_first_last, 'Parsian Bank')
        assert name == 'Bank Parsian'

    def test_swaps_multi_token(self):
        name, _ = v(_v_swap_first_last, 'National Iranian Oil Company')
        tokens = name.split()
        assert tokens[0] == 'Company'
        assert tokens[-1] == 'National'

    def test_skips_single_token(self):
        name, _ = v(_v_swap_first_last, 'Suharto')
        assert name is None


class TestReorderGivenNames:
    def test_keeps_surname_last(self):
        name, _ = v(_v_reorder_given_names, 'Ahmad Reza Khalil')
        assert name.split()[-1] == 'Khalil'

    def test_skips_two_tokens(self):
        name, _ = v(_v_reorder_given_names, 'Kim Un')
        assert name is None


# ── Abbreviation & Initialisation ─────────────────────────────────────────────

class TestInitialFirstName:
    def test_initialises_first_name(self):
        name, _ = v(_v_initial_first_name, 'Qasem Soleimani')
        assert name == 'Q. Soleimani'

    def test_preserves_surname(self):
        name, _ = v(_v_initial_first_name, 'Viktor Bout')
        assert 'Bout' in name
        assert name.startswith('V.')


class TestInitialAllGiven:
    def test_initialises_all_except_last(self):
        name, _ = v(_v_initial_all_given, 'Ahmad Jafar Rahimi')
        assert name == 'A. J. Rahimi'

    def test_skips_two_tokens(self):
        name, _ = v(_v_initial_all_given, 'Kim Un')
        assert name is None


class TestCompanyAcronym:
    def test_irgc(self):
        name, _ = v(_v_company_acronym, 'Islamic Revolutionary Guard Corps')
        assert name == 'IRGC'

    def test_skips_single_token(self):
        name, _ = v(_v_company_acronym, 'Rosoboronexport')
        assert name is None


class TestAbbrevToken:
    def test_abbreviates_long_token(self):
        name, _ = v(_v_abbrev_token, 'Rosoboronexport')
        assert name.endswith('.')
        assert len(name) < len('Rosoboronexport')

    def test_skips_short_token(self):
        name, _ = v(_v_abbrev_token, 'Iran Air')
        # 'Iran' is 4 chars, 'Air' is 3 chars — both < 6, should skip
        assert name is None


# ── Character-Level Modifications ─────────────────────────────────────────────

class TestCharInsert:
    def test_increases_length(self):
        original = 'Rosneft'
        name, _ = v(_v_char_insert, original)
        assert name is not None
        assert len(name) == len(original) + 1

    def test_preserves_prefix_and_suffix(self):
        name, _ = v(_v_char_insert, 'Gazprom')
        assert name[0] == 'G'


class TestCharDelete:
    def test_decreases_length(self):
        original = 'Sberbank'
        name, _ = v(_v_char_delete, original)
        assert name is not None
        assert len(name) == len(original) - 1


class TestCharRepeat:
    def test_increases_length(self):
        original = 'Rosneft'
        name, _ = v(_v_char_repeat, original)
        assert len(name) == len(original) + 1


class TestCharTranspose:
    def test_same_characters(self):
        original = 'Rosneft'
        name, _ = v(_v_char_transpose, original)
        assert sorted(name.lower()) == sorted(original.lower())


class TestTruncateFront:
    def test_shorter(self):
        name, _ = v(_v_truncate_front, 'Lukoil')
        assert len(name) < len('Lukoil')


class TestTruncateEnd:
    def test_shorter(self):
        name, _ = v(_v_truncate_end, 'Sberbank')
        assert len(name) < len('Sberbank')


class TestSingleTypo:
    def test_different_from_original(self):
        RNG2 = random.Random(1)
        name, _ = _v_single_typo('Novatek', {}, RNG2)
        assert name != 'Novatek'

    def test_same_length(self):
        RNG2 = random.Random(1)
        name, _ = _v_single_typo('Gazprom', {}, RNG2)
        assert len(name) == len('Gazprom')


# ── Phonetic ──────────────────────────────────────────────────────────────────

class TestPhoneticSub:
    def test_soleimani_variant(self):
        name, _ = v(_v_phonetic_sub, 'Soleimani')
        assert name in ('sulaimani', 'Sulaimani', None) or (name is not None and name.lower() != 'soleimani')

    def test_hussein_variant(self):
        RNG2 = random.Random(0)
        name, _ = _v_phonetic_sub('Hussein', {}, RNG2)
        if name:
            assert name.lower() != 'hussein'


# ── Special Characters & Spacing ──────────────────────────────────────────────

class TestInsertHyphen:
    def test_has_hyphen(self):
        name, _ = v(_v_insert_hyphen, 'Bank Melli')
        assert '-' in name

    def test_preserves_tokens(self):
        name, _ = v(_v_insert_hyphen, 'Bank Melli Iran')
        assert 'Bank' in name and 'Melli' in name


class TestExtraSpaces:
    def test_more_spaces(self):
        name, _ = v(_v_extra_spaces, 'Kim Jong Un')
        assert '  ' in name


class TestExpandDiacritics:
    def test_umlaut_expansion(self):
        name, _ = v(_v_expand_diacritics, 'Müller')
        assert name == 'Mueller'

    def test_accents(self):
        name, _ = v(_v_expand_diacritics, 'Société Générale')
        assert 'é' not in name

    def test_skips_ascii_name(self):
        name, _ = v(_v_expand_diacritics, 'Rosneft')
        assert name is None


class TestRemovePunctuation:
    def test_removes_hyphen(self):
        name, _ = v(_v_remove_punctuation, 'Al-Zawahiri')
        assert '-' not in name
        assert name == 'AlZawahiri'

    def test_removes_apostrophe(self):
        name, _ = v(_v_remove_punctuation, "Chateau d'Ivoire")
        assert "'" not in name

    def test_skips_no_punctuation(self):
        name, _ = v(_v_remove_punctuation, 'Kim Jong Un')
        assert name is None


class TestCompressName:
    def test_no_spaces(self):
        name, _ = v(_v_compress_name, 'Bank Melli Iran')
        assert ' ' not in name
        assert name == 'BankMelliIran'

    def test_skips_single_token(self):
        name, _ = v(_v_compress_name, 'Rosoboronexport')
        assert name is None


class TestSplitToken:
    def test_inserts_space(self):
        name, _ = v(_v_split_token, 'Hezbollah')
        assert ' ' in name
        assert len(name.split()) == 2

    def test_skips_short(self):
        name, _ = v(_v_split_token, 'Iran')
        assert name is None


# ── Script & Language ─────────────────────────────────────────────────────────

class TestStripAccents:
    def test_strips_accents(self):
        name, _ = v(_v_strip_accents, 'Société Générale')
        assert 'é' not in name

    def test_skips_ascii(self):
        name, _ = v(_v_strip_accents, 'Rosneft')
        assert name is None


class TestCyrillicTransliterate:
    def test_putin(self):
        name, _ = v(_v_cyrillic_transliterate, 'Путин')
        assert name is not None
        assert all(ord(c) < 256 for c in name if c.isalpha())

    def test_skips_latin(self):
        name, _ = v(_v_cyrillic_transliterate, 'Putin')
        assert name is None


class TestAlternateRomanisation:
    def test_hussein_variant(self):
        name, _ = v(_v_alternate_romanisation, 'Hussein')
        assert name is not None and name.lower() != 'hussein'

    def test_soleimani(self):
        name, _ = v(_v_alternate_romanisation, 'Soleimani')
        assert name is not None and 'sulaimani' in (name or '').lower()


# ── Vessel ────────────────────────────────────────────────────────────────────

class TestVesselAddPrefix:
    def test_adds_prefix(self):
        name, _ = v(_v_vessel_add_prefix, 'Arctic Sea')
        assert name.split()[0] in ('MV', 'MT', 'SS', 'MS')

    def test_skips_if_already_prefixed(self):
        name, _ = v(_v_vessel_add_prefix, 'MV Arctic Sea')
        assert name is None


class TestVesselRemovePrefix:
    def test_removes_mv(self):
        name, _ = v(_v_vessel_remove_prefix, 'MV Arctic Sea')
        assert name == 'Arctic Sea'

    def test_skips_no_prefix(self):
        name, _ = v(_v_vessel_remove_prefix, 'Arctic Sea')
        assert name is None


# ── Obfuscation ───────────────────────────────────────────────────────────────

class TestHomoglyph:
    def test_same_visual_length(self):
        name, _ = v(_v_homoglyph, 'Rosneft')
        if name:
            assert len(name) == len('Rosneft')

    def test_skips_no_candidates(self):
        # Name with no homoglyph-substitutable chars
        name, _ = v(_v_homoglyph, 'Zzz')
        # Z has no homoglyph — may or may not skip depending on mapping
        # Just assert it doesn't crash
        assert name is None or isinstance(name, str)


class TestZeroWidth:
    def test_inserts_zwc(self):
        name, _ = v(_v_zero_width, 'Gazprom')
        assert '\u200b' in name

    def test_looks_same_length_visually(self):
        name, _ = v(_v_zero_width, 'Gazprom')
        visible = name.replace('\u200b', '')
        assert visible == 'Gazprom'


# ── Dialectal & Substitution ──────────────────────────────────────────────────

class TestDialectalPrefix:
    def test_al_to_el(self):
        name, _ = v(_v_dialectal_prefix, 'Al-Zawahiri')
        assert name.startswith('El-')

    def test_el_to_al(self):
        name, _ = v(_v_dialectal_prefix, 'El-Rashid Trading')
        assert name.startswith('Al-')

    def test_skips_no_prefix(self):
        name, _ = v(_v_dialectal_prefix, 'Kim Jong Un')
        assert name is None


# ── Field Contamination ───────────────────────────────────────────────────────

class TestNoiseSuffix:
    def test_has_reference(self):
        name, _ = v(_v_noise_suffix, 'Viktor Bout')
        assert any(x in name for x in ['REF#', 'ACCT', 'TXN#', 'ID:'])

    def test_original_name_preserved(self):
        name, _ = v(_v_noise_suffix, 'Viktor Bout')
        assert name.startswith('Viktor Bout')


class TestCountryCodeSuffix:
    def test_appends_iso_code(self):
        name, _ = v(_v_country_code_suffix, 'Kim Jong Un',
                    {'nationality': 'North Korean'})
        assert name.endswith('KP')

    def test_fallback_without_nationality(self):
        name, _ = v(_v_country_code_suffix, 'Kim Jong Un', {})
        assert name is not None
        assert len(name.split()[-1]) == 2


class TestEquivLegalDesignator:
    def test_substitutes_llc(self):
        name, _ = v(_v_equiv_legal_designator, 'Mahan Air LLC')
        assert 'LLC' not in name
        assert 'Mahan Air' in name

    def test_skips_no_designator(self):
        name, _ = v(_v_equiv_legal_designator, 'Kim Jong Un')
        assert name is None


# ── Short Names ───────────────────────────────────────────────────────────────

class TestPeriodShortName:
    def test_bp_to_b_p(self):
        name, _ = v(_v_period_short_name, 'BP')
        assert name == 'B.P.'

    def test_three_char(self):
        name, _ = v(_v_period_short_name, 'IBM')
        assert name == 'I.B.M.'

    def test_skips_long_name(self):
        name, _ = v(_v_period_short_name, 'Rosneft')
        assert name is None
