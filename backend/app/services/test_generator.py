"""
test_generator.py

Deterministic test case generation engine.
Maps the 58 type_ids in test_case_types.csv to Python variation functions.
No LLM calls — all logic is rule-based.
"""
from __future__ import annotations

import csv
import random
import re
import string
import unicodedata
import uuid
from pathlib import Path
from typing import Callable, Optional
import aiosqlite

from app.models.schemas import GenerationRequest, TestCaseType

# ── Paths ──────────────────────────────────────────────────────────────────────

TYPES_CSV = Path(__file__).parent.parent / "data" / "test_case_types.csv"

# ── Lookup tables ──────────────────────────────────────────────────────────────

ARTICLES = {
    'the', 'a', 'an', 'of', 'de', 'la', 'le', 'les', 'du', 'des',
    'von', 'van', 'und', 'y', 'e', 'i', 'et',
}

LEGAL_DESIGNATORS = [
    'LLC', 'Ltd', 'Ltd.', 'Limited', 'Corp', 'Corp.', 'Corporation',
    'Inc', 'Inc.', 'Incorporated', 'SA', 'S.A.', 'AG', 'GmbH', 'PLC',
    'Plc', 'OJSC', 'PAO', 'OAO', 'ZAO', 'JSC', 'PJSC', 'Co.', 'Co',
    'Company', 'Group', 'Holdings', 'International', 'Industries',
    'Trading', 'Services', 'Enterprises', 'Foundation', 'Fund', 'Trust',
    'Association', 'Institute', 'Authority', 'Organisation', 'Organization',
    'Bureau', 'Agency', 'Office', 'Est.', 'Establishment', 'Brothers',
    'Bros.', 'Partners', 'LP', 'LLP', 'NV', 'BV', 'SARL', 'SpA',
]
LEGAL_DESIGNATORS_SET = {d.lower().rstrip('.') for d in LEGAL_DESIGNATORS}

LEGAL_EQUIV: dict[str, list[str]] = {
    'LLC': ['Ltd', 'Limited', 'Corp'], 'Ltd': ['LLC', 'Limited', 'Inc'],
    'Limited': ['LLC', 'Ltd', 'Inc'], 'Corp': ['Inc', 'Corporation', 'Ltd'],
    'Corporation': ['Corp', 'Inc', 'Ltd'], 'Inc': ['Corp', 'LLC', 'Ltd'],
    'SA': ['AG', 'PLC', 'LLC'], 'AG': ['SA', 'GmbH', 'PLC'],
    'GmbH': ['AG', 'SA', 'LLC'], 'PLC': ['SA', 'AG', 'Corp'],
    'OJSC': ['JSC', 'PJSC', 'PAO'], 'PAO': ['OJSC', 'OAO', 'ZAO'],
    'OAO': ['OJSC', 'PAO', 'JSC'], 'JSC': ['OJSC', 'PJSC', 'PAO'],
    'PJSC': ['JSC', 'OJSC', 'PAO'], 'NV': ['BV', 'SA', 'LLC'],
    'BV': ['NV', 'SA', 'GmbH'], 'SARL': ['LLC', 'Ltd', 'SA'],
}

TITLES = {
    'general', 'gen', 'admiral', 'adm', 'colonel', 'col', 'major',
    'captain', 'capt', 'commander', 'cmdr', 'lieutenant', 'lt',
    'dr', 'doctor', 'prof', 'professor', 'mr', 'mrs', 'ms', 'miss',
    'sheikh', 'shaikh', 'sheik', 'ayatollah', 'president', 'minister',
    'prime', 'ambassador', 'sir', 'lord', 'baron', 'count', 'prince',
    'princess', 'king', 'queen', 'h.e.', 'h.r.h.',
}

TITLE_EQUIV: dict[str, list[str]] = {
    'Dr': ['Prof', 'Dr.'], 'Dr.': ['Prof.', 'Dr'], 'Prof': ['Dr', 'Prof.'],
    'General': ['Commander', 'Admiral', 'Gen'], 'Gen': ['Commander', 'Adm', 'Col'],
    'President': ['Minister', 'H.E.'], 'Sheikh': ['Shaikh', 'Sheik'],
    'Ayatollah': ['Grand Ayatollah', 'Sheikh'], 'Mr': ['Mr.', 'Sir'],
    'Sir': ['Mr', 'H.E.'], 'Colonel': ['Col', 'General', 'Commander'],
}

NAME_PREFIXES = ['Al-', 'El-', 'Abu ', 'Bin ', 'Bint ', 'Ibn ', 'Um ', 'Om ']

CITY_NAMES = [
    'Tehran', 'Baghdad', 'Damascus', 'Beirut', 'Tripoli', 'Kabul', 'Pyongyang',
    'Moscow', 'Minsk', 'Beijing', 'Shanghai', 'Seoul', 'Caracas', 'Havana',
    'Dubai', 'Riyadh', 'Doha', 'Ankara', 'Istanbul', 'Cairo', 'Algiers',
    'Naypyidaw', 'Harare', 'Khartum', 'Mogadishu', 'Sanaa', 'Tripoli',
]

COUNTRY_ISO2: dict[str, str] = {
    'iran': 'IR', 'russia': 'RU', 'russian federation': 'RU',
    'north korea': 'KP', "democratic people's republic of korea": 'KP',
    'dprk': 'KP', 'china': 'CN', 'syria': 'SY', 'cuba': 'CU',
    'venezuela': 'VE', 'belarus': 'BY', 'myanmar': 'MM',
    'nicaragua': 'NI', 'yemen': 'YE', 'libya': 'LY', 'sudan': 'SD',
    'iraq': 'IQ', 'afghanistan': 'AF', 'lebanon': 'LB', 'somalia': 'SO',
}

NATIONALITY_ISO2: dict[str, str] = {
    'iranian': 'IR', 'russian': 'RU', 'north korean': 'KP', 'chinese': 'CN',
    'syrian': 'SY', 'cuban': 'CU', 'venezuelan': 'VE', 'belarusian': 'BY',
    'burmese': 'MM', 'nicaraguan': 'NI', 'yemeni': 'YE', 'libyan': 'LY',
    'sudanese': 'SD', 'iraqi': 'IQ', 'afghan': 'AF', 'lebanese': 'LB',
    'somali': 'SO', 'middle eastern / north african': 'IR',
}

VESSEL_PREFIXES = {'mv', 'mt', 'ss', 'ms', 'm/v', 'm/t', 'f/v', 'mv.'}

QUALIFIERS = [
    '(alias)', '(deceased)', '(also known as)', '(a.k.a.)',
    '(Supreme Leader)', '(Chairman)', '(Director General)',
    '(former)', '(acting)', '(General Secretary)', '(Commander)',
]

GIVEN_NAMES = [
    'Ahmed', 'Mohamed', 'Hassan', 'Ali', 'Omar', 'Ibrahim', 'Khalid',
    'Yusuf', 'Vladimir', 'Ivan', 'Nikolai', 'Dmitri', 'Alexei',
    'Reza', 'Mehdi', 'Javad', 'Amir', 'Jean', 'Pierre', 'Louis',
    'Ahmad', 'Jafar', 'Mahmoud', 'Kim', 'Park', 'Song', 'Yong',
]

PATRONYMICS = [
    'Al-Musawi', 'Al-Rashid', 'Al-Hussain', 'Al-Shaikh',
    'Ivanov', 'Petrov', 'Sidorov', 'Kozlov', 'Bin Laden', 'Bin Ali',
]

# QWERTY keyboard adjacency (lowercase)
KEYBOARD_ADJACENT: dict[str, str] = {
    'q': 'wa',  'w': 'qesa', 'e': 'wrds', 'r': 'etfd', 't': 'rygf',
    'y': 'tuhg', 'u': 'yijh', 'i': 'uokj', 'o': 'iplk', 'p': 'ol',
    'a': 'qwsz', 's': 'awedxz', 'd': 'serfcx', 'f': 'drtgvc',
    'g': 'ftyhbv', 'h': 'gyujnb', 'j': 'huikm', 'k': 'jilon',
    'l': 'kop',  'z': 'asx',  'x': 'zsdc', 'c': 'xdfv',
    'v': 'cfgb', 'b': 'vghn', 'n': 'bhjm', 'm': 'njk',
}

# Phonetic substitution patterns: (regex, replacement) — applied in order
# Use named patterns that work on the whole lowercased name string
PHONETIC_PATTERNS: list[tuple[str, str]] = [
    # Well-known name-level substitutions (try these first)
    ('soleimani', 'sulaimani'), ('sulaimani', 'soleimani'),
    ('hussein', 'hossein'),    ('hossein', 'hussein'),
    ('hassan', 'hasan'),       ('hasan', 'hassan'),
    ('hezbollah', 'hizbullah'),('hizbullah', 'hezbollah'),
    ('mohammad', 'mohammed'),  ('mohammed', 'muhammad'), ('muhammad', 'mohammad'),
    ('mustafa', 'mostafa'),    ('mostafa', 'mustafa'),
    ('mahmoud', 'mahmood'),    ('mahmood', 'mahmoud'),
    ('zawahiri', 'zawahri'),   ('zawahri', 'zawahiri'),
    ('qassem', 'kasem'),       ('kasem', 'qassem'),
    ('yusuf', 'yosef'),        ('yosef', 'yusuf'),
    ('khalil', 'halil'),       ('halil', 'khalil'),
    ('ismail', 'esmail'),      ('esmail', 'ismail'),
    # Sub-token phoneme pairs
    ('kh', 'h'), ('ou', 'u'), ('ei', 'i'), ('ai', 'ay'),
    ('oo', 'u'), ('ee', 'i'), ('ph', 'f'), ('gh', 'q'),
    ('dh', 'd'), ('sh', 's'),
]

DIACRITIC_EXPAND: dict[str, str] = {
    'ä': 'ae', 'ö': 'oe', 'ü': 'ue', 'ß': 'ss',
    'Ä': 'Ae', 'Ö': 'Oe', 'Ü': 'Ue',
    'à': 'a', 'á': 'a', 'â': 'a', 'ã': 'a', 'å': 'a',
    'è': 'e', 'é': 'e', 'ê': 'e', 'ë': 'e',
    'ì': 'i', 'í': 'i', 'î': 'i', 'ï': 'i',
    'ò': 'o', 'ó': 'o', 'ô': 'o', 'õ': 'o', 'ø': 'o',
    'ù': 'u', 'ú': 'u', 'û': 'u',
    'ñ': 'n', 'ç': 'c', 'ý': 'y', 'ÿ': 'y',
    'À': 'A', 'Á': 'A', 'Â': 'A', 'Ã': 'A', 'Å': 'A',
    'È': 'E', 'É': 'E', 'Ê': 'E', 'Ë': 'E',
    'Ì': 'I', 'Í': 'I', 'Î': 'I', 'Ï': 'I',
    'Ò': 'O', 'Ó': 'O', 'Ô': 'O', 'Õ': 'O', 'Ø': 'O',
    'Ù': 'U', 'Ú': 'U', 'Û': 'U',
    'Ñ': 'N', 'Ç': 'C', 'Ý': 'Y',
    'ō': 'o', 'ū': 'u', 'ā': 'a', 'ī': 'i',
    'Ō': 'O', 'Ū': 'U', 'Ā': 'A', 'Ī': 'I',
    'ě': 'e', 'š': 's', 'ž': 'z', 'č': 'c', 'ř': 'r',
    'ě': 'e', 'ğ': 'g', 'ı': 'i', 'ş': 's',
}

CYRILLIC_LATIN: dict[str, str] = {
    'А': 'A', 'Б': 'B', 'В': 'V', 'Г': 'G', 'Д': 'D', 'Е': 'E', 'Ё': 'Yo',
    'Ж': 'Zh', 'З': 'Z', 'И': 'I', 'Й': 'Y', 'К': 'K', 'Л': 'L', 'М': 'M',
    'Н': 'N', 'О': 'O', 'П': 'P', 'Р': 'R', 'С': 'S', 'Т': 'T', 'У': 'U',
    'Ф': 'F', 'Х': 'Kh', 'Ц': 'Ts', 'Ч': 'Ch', 'Ш': 'Sh', 'Щ': 'Shch',
    'Ъ': '', 'Ы': 'Y', 'Ь': '', 'Э': 'E', 'Ю': 'Yu', 'Я': 'Ya',
    'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo',
    'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
    'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
    'ф': 'f', 'х': 'kh', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'shch',
    'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya',
}

# Latin → visually identical Unicode (Cyrillic/Greek homoglyphs)
HOMOGLYPH_MAP: dict[str, str] = {
    'a': 'а', 'c': 'с', 'e': 'е', 'o': 'о', 'p': 'р', 'x': 'х',
    'A': 'А', 'B': 'В', 'E': 'Е', 'H': 'Н', 'K': 'К',
    'M': 'М', 'O': 'О', 'P': 'Р', 'T': 'Т', 'X': 'Х',
}


# ── Outcome → expected_result mapping ─────────────────────────────────────────

def outcome_to_result(expected_outcome: str) -> tuple[str, str]:
    """Map matrix outcome label → (expected_result, rationale_prefix)"""
    rationales = {
        'Must Hit':         'Must Hit — deterministic match; any miss is a critical system failure',
        'Should Hit':       'Should Hit — standard capability; a miss indicates a configuration or threshold gap',
        'Testing Purposes': 'Testing Purposes — capability benchmark; a miss is expected without the specific capability in place',
        'Should Not Hit':   'Should Not Hit — expected non-match; a hit would be a false positive',
    }
    return expected_outcome, rationales.get(expected_outcome, expected_outcome)


# ── CSV loader ─────────────────────────────────────────────────────────────────

def load_test_case_types() -> list[TestCaseType]:
    if not TYPES_CSV.exists():
        return []
    with open(TYPES_CSV, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        types: list[TestCaseType] = []
        for row in reader:
            try:
                types.append(TestCaseType(
                    type_id=row['type_id'],
                    theme=row.get('theme', ''),
                    category=row.get('category', ''),
                    type_name=row['type_name'],
                    description=row['description'],
                    applicable_entity_types=[
                        e.strip() for e in row['applicable_entity_types'].split('|')
                    ],
                    applicable_min_tokens=int(row.get('applicable_min_tokens', 1)),
                    applicable_min_name_length=int(row.get('applicable_min_name_length', 1)),
                    expected_outcome=row.get('expected_outcome', 'Should Hit'),
                    variation_logic=row['variation_logic'],
                ))
            except (KeyError, ValueError):
                continue
    return types


def _get_type_meta(type_id: str) -> dict:
    """Return the full CSV row for a type_id (cached in memory)."""
    if not hasattr(_get_type_meta, '_cache'):
        _get_type_meta._cache = {}
        if TYPES_CSV.exists():
            with open(TYPES_CSV, newline='', encoding='utf-8') as f:
                for row in csv.DictReader(f):
                    if row['type_id'] not in _get_type_meta._cache:
                        _get_type_meta._cache[row['type_id']] = row
    return _get_type_meta._cache.get(type_id, {})


# ── Variation helpers ──────────────────────────────────────────────────────────

def _longest_alpha_token(tokens: list[str]) -> int:
    """Return index of longest purely-alphabetic token."""
    best, best_len = 0, 0
    for i, t in enumerate(tokens):
        alpha_len = sum(1 for c in t if c.isalpha())
        if alpha_len > best_len:
            best, best_len = i, alpha_len
    return best


def _is_legal_designator(token: str) -> bool:
    return token.lower().rstrip('.') in LEGAL_DESIGNATORS_SET


def _is_article(token: str) -> bool:
    return token.lower().rstrip('.,') in ARTICLES


def _is_title(token: str) -> bool:
    return token.lower().rstrip('.') in TITLES


def _keyboard_typo(char: str, rng: random.Random) -> str:
    c = char.lower()
    neighbours = KEYBOARD_ADJACENT.get(c, '')
    if not neighbours:
        return char
    replacement = rng.choice(neighbours)
    return replacement.upper() if char.isupper() else replacement


# ── Variation functions ────────────────────────────────────────────────────────
# Signature: (name: str, record: dict, rng: Random) -> (test_name | None, rationale_suffix)
# Return None as test_name to signal "skip this name for this type"

def _v_exact_match(name, record, rng):
    return name, "exact string copy"

def _v_primary_name_match(name, record, rng):
    if record.get('primary_aka') != 'primary':
        return None, "skip: not a primary name"
    return name, "primary name exact copy"

def _v_aka_match(name, record, rng):
    if record.get('primary_aka') != 'aka':
        return None, "skip: not an AKA name"
    return name, "AKA exact copy"

def _v_omit_article(name, record, rng):
    tokens = name.split()
    new = [t for i, t in enumerate(tokens)
           if not (i == 0 and _is_article(t)) and not (i > 0 and _is_article(t) and i == 1)]
    # Remove first article anywhere in list
    removed = False
    new = []
    for t in tokens:
        if not removed and _is_article(t):
            removed = True
            continue
        new.append(t)
    if not removed:
        return None, "skip: no article token found"
    return ' '.join(new), f"article removed from '{name}'"

def _v_omit_legal_designator(name, record, rng):
    tokens = name.split()
    new = [t for t in tokens if not _is_legal_designator(t)]
    if len(new) == len(tokens):
        return None, "skip: no legal designator found"
    if not new:
        return None, "skip: would produce empty name"
    return ' '.join(new), "legal designator stripped"

def _v_omit_location_segment(name, record, rng):
    tokens = name.split()
    cities_lc = {c.lower() for c in CITY_NAMES}
    if tokens and tokens[-1].lower() in cities_lc:
        return ' '.join(tokens[:-1]), f"location token '{tokens[-1]}' removed"
    return None, "skip: last token not a recognised city"

def _v_omit_first_name(name, record, rng):
    tokens = name.split()
    if len(tokens) < 2:
        return None, "skip: insufficient tokens"
    return ' '.join(tokens[1:]), "first name token removed"

def _v_omit_all_given(name, record, rng):
    tokens = name.split()
    if len(tokens) < 2:
        return None, "skip: insufficient tokens"
    return tokens[-1], "all given names removed — surname only"

def _v_omit_middle_name(name, record, rng):
    tokens = name.split()
    if len(tokens) < 3:
        return None, "skip: need at least 3 tokens for middle-name removal"
    return f"{tokens[0]} {tokens[-1]}", "middle name(s) removed — first + last only"

def _v_omit_entity_segment(name, record, rng):
    tokens = name.split()
    if len(tokens) < 3:
        return None, "skip: need at least 3 tokens"
    interior = list(range(1, len(tokens) - 1))
    idx = rng.choice(interior)
    new = tokens[:idx] + tokens[idx+1:]
    return ' '.join(new), f"interior token '{tokens[idx]}' removed"

def _v_omit_title(name, record, rng):
    tokens = name.split()
    if tokens and _is_title(tokens[0]):
        return ' '.join(tokens[1:]), f"title '{tokens[0]}' removed"
    return None, "skip: no leading title found"

def _v_omit_prefix(name, record, rng):
    tokens = name.split()
    first = tokens[0]
    for prefix in ['Al-', 'El-', 'al-', 'el-']:
        if first.startswith(prefix):
            stripped = first[len(prefix):]
            return ' '.join([stripped] + tokens[1:]), f"prefix '{prefix}' removed"
    return None, "skip: no recognisable prefix found"

def _v_omit_qualifier(name, record, rng):
    # Remove parenthetical (...)
    cleaned = re.sub(r'\s*\([^)]*\)', '', name).strip()
    if cleaned == name:
        return None, "skip: no parenthetical qualifier found"
    return cleaned, "parenthetical qualifier removed"

def _v_insert_article(name, record, rng):
    return f"The {name}", "article 'The' prepended"

def _v_insert_legal_designator(name, record, rng):
    d = rng.choice(['LLC', 'Ltd', 'Corp', 'Inc', 'Co.', 'SA', 'OJSC'])
    return f"{name} {d}", f"legal designator '{d}' appended"

def _v_insert_location(name, record, rng):
    city = rng.choice(CITY_NAMES)
    return f"{name} {city}", f"location '{city}' appended"

def _v_insert_given_name(name, record, rng):
    tokens = name.split()
    if len(tokens) < 2:
        return None, "skip: insufficient tokens"
    gn = rng.choice(GIVEN_NAMES)
    new = tokens[:-1] + [gn] + [tokens[-1]]
    return ' '.join(new), f"given name '{gn}' inserted before surname"

def _v_insert_surname(name, record, rng):
    pat = rng.choice(PATRONYMICS)
    return f"{name} {pat}", f"additional surname '{pat}' appended"

def _v_insert_prefix(name, record, rng):
    prefix = rng.choice(['Al-', 'El-', 'Abu ', 'Bin '])
    return f"{prefix}{name}", f"name prefix '{prefix.strip()}' prepended"

def _v_insert_title(name, record, rng):
    title = rng.choice(['Dr', 'General', 'Sheikh', 'President', 'Mr', 'Colonel'])
    return f"{title} {name}", f"title '{title}' prepended"

def _v_insert_qualifier(name, record, rng):
    q = rng.choice(QUALIFIERS)
    return f"{name} {q}", f"qualifier '{q}' appended"

def _v_reverse_tokens(name, record, rng):
    tokens = name.split()
    return ' '.join(reversed(tokens)), "token order reversed"

def _v_reorder_given_names(name, record, rng):
    tokens = name.split()
    if len(tokens) < 3:
        return None, "skip: need at least 3 tokens"
    given = tokens[:-1]
    rng.shuffle(given)
    return ' '.join(given + [tokens[-1]]), "given names reordered"

def _v_swap_entity_segments(name, record, rng):
    tokens = name.split()
    if len(tokens) < 3:
        return None, "skip: need at least 3 tokens"
    i, j = rng.sample(range(len(tokens)), 2)
    tokens[i], tokens[j] = tokens[j], tokens[i]
    return ' '.join(tokens), f"tokens at positions {i} and {j} swapped"

def _v_swap_first_last(name, record, rng):
    tokens = name.split()
    if len(tokens) < 2:
        return None, "skip: need at least 2 tokens"
    tokens[0], tokens[-1] = tokens[-1], tokens[0]
    return ' '.join(tokens), "first and last tokens swapped"

def _v_equiv_legal_designator(name, record, rng):
    tokens = name.split()
    for i, t in enumerate(tokens):
        base = t.rstrip('.')
        if base in LEGAL_EQUIV:
            replacement = rng.choice(LEGAL_EQUIV[base])
            tokens[i] = replacement
            return ' '.join(tokens), f"designator '{base}' substituted with '{replacement}'"
    return None, "skip: no substitutable designator found"

def _v_dialectal_prefix(name, record, rng):
    if re.search(r'\bAl-', name, re.I):
        result = re.sub(r'\bAl-', 'El-', name, count=1, flags=re.I)
        return result, "Al- prefix variant: Al- → El-"
    if re.search(r'\bEl-', name, re.I):
        result = re.sub(r'\bEl-', 'Al-', name, count=1, flags=re.I)
        return result, "Al- prefix variant: El- → Al-"
    return None, "skip: no Al-/El- prefix found"

def _v_subst_title(name, record, rng):
    tokens = name.split()
    if not tokens:
        return None, "skip"
    t = tokens[0].rstrip('.')
    if t in TITLE_EQUIV:
        replacement = rng.choice(TITLE_EQUIV[t])
        return ' '.join([replacement] + tokens[1:]), f"title '{t}' → '{replacement}'"
    return None, "skip: no substitutable title found"

def _v_subst_qualifier(name, record, rng):
    if '(' not in name:
        return None, "skip: no qualifier found"
    new_q = rng.choice(QUALIFIERS)
    result = re.sub(r'\([^)]*\)', new_q, name, count=1)
    return result, f"qualifier replaced with '{new_q}'"

def _v_initial_first_name(name, record, rng):
    tokens = name.split()
    if len(tokens) < 2:
        return None, "skip: need at least 2 tokens"
    initial = tokens[0][0].upper() + '.'
    return ' '.join([initial] + tokens[1:]), f"first name initialised to '{initial}'"

def _v_initial_all_given(name, record, rng):
    tokens = name.split()
    if len(tokens) < 3:
        return None, "skip: need at least 3 tokens"
    initials = [t[0].upper() + '.' for t in tokens[:-1]]
    return ' '.join(initials + [tokens[-1]]), "all given names initialised"

def _v_company_acronym(name, record, rng):
    tokens = name.split()
    if len(tokens) < 2:
        return None, "skip: need at least 2 tokens"
    acronym = ''.join(t[0].upper() for t in tokens if t and t[0].isalpha())
    if not acronym or len(acronym) < 2:
        return None, "skip: acronym too short"
    return acronym, f"company name replaced with acronym '{acronym}'"

def _v_abbrev_token(name, record, rng):
    tokens = name.split()
    idx = _longest_alpha_token(tokens)
    t = tokens[idx]
    alpha_len = sum(1 for c in t if c.isalpha())
    if alpha_len < 6:
        return None, "skip: token too short to abbreviate meaningfully"
    cut = min(5, len(t) - 1)
    tokens[idx] = t[:cut] + '.'
    return ' '.join(tokens), f"token abbreviated to '{tokens[idx]}'"

def _v_city_typo(name, record, rng):
    tokens = name.split()
    cities_lc = {c.lower(): c for c in CITY_NAMES}
    for i, t in enumerate(tokens):
        if t.lower() in cities_lc:
            city = t
            pos = rng.randint(1, len(city) - 1)
            alpha_chars = [j for j, c in enumerate(city) if c.isalpha()]
            if not alpha_chars:
                continue
            j = rng.choice(alpha_chars[1:] if len(alpha_chars) > 1 else alpha_chars)
            replacement = rng.choice('abcdefghijklmnoprstuvwxyz'.replace(city[j].lower(), ''))
            typo = city[:j] + (replacement.upper() if city[j].isupper() else replacement) + city[j+1:]
            tokens[i] = typo
            return ' '.join(tokens), f"city '{city}' → '{typo}' (edit distance 1)"
    return None, "skip: no recognised city token found"

def _v_country_iso2(name, record, rng):
    tokens = name.split()
    for i, t in enumerate(tokens):
        iso = COUNTRY_ISO2.get(t.lower())
        if iso:
            tokens[i] = iso
            return ' '.join(tokens), f"country '{t}' → ISO code '{iso}'"
    return None, "skip: no recognised country token found"

def _v_char_insert(name, record, rng):
    tokens = name.split()
    idx = _longest_alpha_token(tokens)
    t = tokens[idx]
    if len(t) < 3:
        return None, "skip: token too short"
    pos = rng.randint(1, len(t) - 1)
    new_char = rng.choice(string.ascii_lowercase)
    tokens[idx] = t[:pos] + new_char + t[pos:]
    return ' '.join(tokens), f"character '{new_char}' inserted at position {pos}"

def _v_char_delete(name, record, rng):
    tokens = name.split()
    idx = _longest_alpha_token(tokens)
    t = tokens[idx]
    if len(t) < 4:
        return None, "skip: token too short"
    pos = rng.randint(1, len(t) - 1)
    tokens[idx] = t[:pos] + t[pos+1:]
    return ' '.join(tokens), f"character at position {pos} deleted"

def _v_char_repeat(name, record, rng):
    tokens = name.split()
    idx = _longest_alpha_token(tokens)
    t = tokens[idx]
    alpha_positions = [i for i, c in enumerate(t) if c.isalpha()]
    if not alpha_positions:
        return None, "skip: no alpha chars"
    pos = rng.choice(alpha_positions)
    tokens[idx] = t[:pos] + t[pos] + t[pos:]
    return ' '.join(tokens), f"character '{t[pos]}' doubled at position {pos}"

def _v_char_transpose(name, record, rng):
    tokens = name.split()
    idx = _longest_alpha_token(tokens)
    t = tokens[idx]
    if len(t) < 3:
        return None, "skip: token too short"
    pos = rng.randint(0, len(t) - 2)
    lst = list(t)
    lst[pos], lst[pos+1] = lst[pos+1], lst[pos]
    tokens[idx] = ''.join(lst)
    return ' '.join(tokens), f"characters transposed at positions {pos},{pos+1}"

def _v_truncate_front(name, record, rng):
    tokens = name.split()
    idx = _longest_alpha_token(tokens)
    t = tokens[idx]
    if len(t) < 4:
        return None, "skip: token too short"
    n = rng.choice([1, 2])
    tokens[idx] = t[n:]
    return ' '.join(tokens), f"first {n} character(s) removed from front"

def _v_truncate_end(name, record, rng):
    tokens = name.split()
    idx = _longest_alpha_token(tokens)
    t = tokens[idx]
    if len(t) < 4:
        return None, "skip: token too short"
    n = rng.choice([1, 2])
    tokens[idx] = t[:-n]
    return ' '.join(tokens), f"last {n} character(s) truncated from end"

def _v_single_typo(name, record, rng):
    tokens = name.split()
    idx = _longest_alpha_token(tokens)
    t = tokens[idx]
    alpha_positions = [i for i, c in enumerate(t) if c.isalpha()]
    if not alpha_positions:
        return None, "skip: no alpha chars"
    pos = rng.choice(alpha_positions)
    replacement = _keyboard_typo(t[pos], rng)
    tokens[idx] = t[:pos] + replacement + t[pos+1:]
    return ' '.join(tokens), f"keyboard typo: '{t[pos]}' → '{replacement}' at position {pos}"

def _v_double_typo(name, record, rng):
    tokens = name.split()
    idx = _longest_alpha_token(tokens)
    t = tokens[idx]
    alpha_positions = [i for i, c in enumerate(t) if c.isalpha()]
    if len(alpha_positions) < 4:
        return None, "skip: token too short for non-adjacent double typo"
    pos1, pos2 = sorted(rng.sample(alpha_positions, 2))
    if abs(pos1 - pos2) < 2:
        return None, "skip: positions too close"
    lst = list(t)
    lst[pos1] = _keyboard_typo(lst[pos1], rng)
    lst[pos2] = _keyboard_typo(lst[pos2], rng)
    tokens[idx] = ''.join(lst)
    return ' '.join(tokens), f"double typo at positions {pos1},{pos2}"

def _v_phonetic_sub(name, record, rng):
    lower = name.lower()
    for pattern, replacement in PHONETIC_SUBS if False else _shuffled_phonetic(PHONETIC_PATTERNS, rng):
        if pattern in lower:
            result = lower.replace(pattern, replacement, 1)
            # Restore capitalisation heuristically
            result = _restore_caps(name, result)
            if result.lower() != lower:
                return result, f"phonetic substitution: '{pattern}' → '{replacement}'"
    return None, "skip: no applicable phonetic pattern found"

def _shuffled_phonetic(patterns, rng):
    shuffled = patterns[:]
    rng.shuffle(shuffled)
    return shuffled

def _restore_caps(original: str, modified: str) -> str:
    """Restore title-case capitalisation to a modified lowercased string."""
    if original == original.upper():
        return modified.upper()
    if original[0].isupper() and modified:
        return modified[0].upper() + modified[1:]
    return modified

def _v_insert_hyphen(name, record, rng):
    tokens = name.split()
    if len(tokens) < 2:
        return None, "skip: single token"
    pos = rng.randint(0, len(tokens) - 2)
    result = ' '.join(tokens[:pos]) + (' ' if pos > 0 else '') + tokens[pos] + '-' + tokens[pos+1] + (' ' if pos + 2 < len(tokens) else '') + ' '.join(tokens[pos+2:])
    return result.strip(), "inter-token space replaced with hyphen"

def _v_extra_spaces(name, record, rng):
    tokens = name.split()
    if len(tokens) < 2:
        return None, "skip: single token"
    extra = rng.choice(['  ', '   '])
    pos = rng.randint(0, len(tokens) - 2)
    parts = tokens[:pos+1] + [''] + tokens[pos+1:]
    return extra.join(tokens[:pos+1]) + extra + ' '.join(tokens[pos+1:]), "extra spaces inserted"

def _v_expand_diacritics(name, record, rng):
    result = ''.join(DIACRITIC_EXPAND.get(c, c) for c in name)
    if result == name:
        return None, "skip: no diacritical characters found"
    return result, "diacritics expanded to ASCII equivalents"

def _v_remove_punctuation(name, record, rng):
    result = re.sub(r"[-']", '', name)
    if result == name:
        return None, "skip: no hyphens or apostrophes found"
    return result, "hyphens and apostrophes removed"

def _v_compress_name(name, record, rng):
    compressed = name.replace(' ', '')
    if compressed == name:
        return None, "skip: single token (no spaces to remove)"
    return compressed, "all spaces removed — name compressed"

def _v_split_token(name, record, rng):
    tokens = name.split()
    idx = _longest_alpha_token(tokens)
    t = tokens[idx]
    if len(t) < 6:
        return None, "skip: token too short to split meaningfully"
    mid = len(t) // 2
    tokens[idx] = t[:mid] + ' ' + t[mid:]
    return ' '.join(tokens), f"token split at midpoint: '{t}' → '{tokens[idx]}'"

def _v_strip_accents(name, record, rng):
    nfd = unicodedata.normalize('NFD', name)
    ascii_only = ''.join(c for c in nfd if unicodedata.category(c) != 'Mn' and ord(c) < 128)
    if ascii_only == name:
        return None, "skip: no diacritical marks to strip"
    return ascii_only, "diacritical marks stripped via NFD decomposition"

def _v_alternate_romanisation(name, record, rng):
    lower = name.lower()
    pairs = [
        ('qasem', 'kasem'), ('kasem', 'qasem'),
        ('hussein', 'hossein'), ('hossein', 'hussein'),
        ('mahmoud', 'mahmood'), ('mahmood', 'mahmoud'),
        ('mohammad', 'mohammed'), ('mohammed', 'muhammad'), ('muhammad', 'mohammad'),
        ('soleimani', 'sulaimani'), ('sulaimani', 'soleimani'),
        ('hassan', 'hasan'), ('hasan', 'hassan'),
        ('khalil', 'halil'), ('halil', 'khalil'),
        ('yusuf', 'yosef'), ('zawahiri', 'zawahri'),
        ('hezbollah', 'hizbullah'), ('hizbullah', 'hezbollah'),
        ('mustafa', 'mostafa'), ('mostafa', 'mustafa'),
    ]
    rng.shuffle(pairs)
    for src, dst in pairs:
        if src in lower:
            result = lower.replace(src, dst, 1)
            return _restore_caps(name, result), f"romanisation variant: '{src}' → '{dst}'"
    return None, "skip: no applicable romanisation pattern found"

def _v_cyrillic_transliterate(name, record, rng):
    if not any(c in CYRILLIC_LATIN for c in name):
        return None, "skip: no Cyrillic characters found"
    result = ''.join(CYRILLIC_LATIN.get(c, c) for c in name)
    return result, "Cyrillic transliterated to Latin"

def _v_vessel_add_prefix(name, record, rng):
    tokens = name.split()
    if tokens and tokens[0].lower() in VESSEL_PREFIXES:
        return None, "skip: vessel prefix already present"
    prefix = rng.choice(['MV', 'MT', 'SS', 'MS'])
    return f"{prefix} {name}", f"vessel prefix '{prefix}' prepended"

def _v_vessel_remove_prefix(name, record, rng):
    tokens = name.split()
    if tokens and tokens[0].lower() in VESSEL_PREFIXES:
        return ' '.join(tokens[1:]), f"vessel prefix '{tokens[0]}' removed"
    return None, "skip: no vessel prefix found"

def _v_homoglyph(name, record, rng):
    candidates = [i for i, c in enumerate(name) if c in HOMOGLYPH_MAP]
    if not candidates:
        return None, "skip: no homoglyph-substitutable characters"
    positions = rng.sample(candidates, min(2, len(candidates)))
    lst = list(name)
    for pos in positions:
        lst[pos] = HOMOGLYPH_MAP[lst[pos]]
    return ''.join(lst), f"homoglyph substitution at {len(positions)} position(s)"

def _v_zero_width(name, record, rng):
    tokens = name.split()
    idx = _longest_alpha_token(tokens)
    t = tokens[idx]
    if len(t) < 3:
        return None, "skip: token too short"
    pos = rng.randint(1, len(t) - 1)
    tokens[idx] = t[:pos] + '\u200b' + t[pos:]
    return ' '.join(tokens), "zero-width space (U+200B) inserted"

def _v_noise_suffix(name, record, rng):
    ref_type = rng.choice(['REF#', 'ACCT', 'TXN#', 'ID:'])
    digits = ''.join(rng.choices(string.digits, k=rng.randint(6, 8)))
    return f"{name} {ref_type}{digits}", f"noise suffix '{ref_type}{digits}' appended"

def _v_country_code_suffix(name, record, rng):
    nationality = (record.get('nationality') or '').lower()
    iso = NATIONALITY_ISO2.get(nationality)
    if not iso:
        # Fall back to a plausible sanctioned country
        iso = rng.choice(['IR', 'RU', 'KP', 'SY', 'CU', 'VE'])
    return f"{name} {iso}", f"country code '{iso}' appended"

def _v_period_short_name(name, record, rng):
    tokens = name.split()
    if not tokens:
        return None, "skip"
    t = tokens[0]
    alpha = [c for c in t if c.isalpha()]
    if len(alpha) > 3 or len(alpha) < 2:
        return None, "skip: name not a short acronym (2-3 chars)"
    dotted = '.'.join(c.upper() for c in alpha) + '.'
    return ' '.join([dotted] + tokens[1:]), f"period-separated acronym: '{t}' → '{dotted}'"


# ── Additional variation functions (TC059+) ────────────────────────────────────

def _v_omit_all_articles(name, record, rng):
    tokens = name.split()
    new = [t for t in tokens if not _is_article(t)]
    if len(new) == len(tokens):
        return None, "skip: no article tokens found"
    return ' '.join(new) if new else None, "all article tokens removed"

def _v_omit_all_legal_designators(name, record, rng):
    tokens = name.split()
    new = [t for t in tokens if not _is_legal_designator(t)]
    if len(new) == len(tokens) or not new:
        return None, "skip: no legal designators or would produce empty name"
    return ' '.join(new), "all legal designators removed"

def _v_omit_surname(name, record, rng):
    tokens = name.split()
    if len(tokens) < 2:
        return None, "skip: insufficient tokens"
    return ' '.join(tokens[:-1]), f"last token '{tokens[-1]}' (surname) removed"

def _v_omit_all_surnames_keep_first(name, record, rng):
    tokens = name.split()
    if len(tokens) < 2:
        return None, "skip: insufficient tokens"
    return tokens[0], "only first token retained"

def _v_omit_multiple_segments(name, record, rng):
    tokens = name.split()
    if len(tokens) < 4:
        return None, "skip: need at least 4 tokens"
    interior = list(range(1, len(tokens) - 1))
    if len(interior) < 2:
        return None, "skip: insufficient interior tokens"
    idxs = sorted(rng.sample(interior, 2), reverse=True)
    new = tokens[:]
    for i in idxs:
        new.pop(i)
    return ' '.join(new), f"segments at positions {idxs} removed"

def _v_insert_multiple_articles(name, record, rng):
    return f"The A {name}", "multiple articles 'The A' prepended"

def _v_insert_legal_designator_front(name, record, rng):
    d = rng.choice(['The', 'Joint', 'National', 'General'])
    return f"{d} {name}", f"prefix '{d}' inserted at front"

def _v_duplicate_legal_designator(name, record, rng):
    tokens = name.split()
    for i, t in enumerate(tokens):
        if _is_legal_designator(t):
            tokens.insert(i, t)
            return ' '.join(tokens), f"designator '{t}' duplicated"
    d = rng.choice(['LLC', 'Ltd', 'Corp'])
    return f"{name} {d} {d}", f"designator '{d}' appended twice"

def _v_insert_country_name(name, record, rng):
    countries = ['Iran', 'Russia', 'Syria', 'China', 'Cuba', 'Venezuela', 'Belarus', 'Myanmar']
    c = rng.choice(countries)
    return f"{name} {c}", f"country name '{c}' appended"

def _v_insert_middle_name(name, record, rng):
    tokens = name.split()
    if len(tokens) < 2:
        return None, "skip: need at least 2 tokens"
    mn = rng.choice(GIVEN_NAMES)
    new = [tokens[0]] + [mn] + tokens[1:]
    return ' '.join(new), f"middle name '{mn}' inserted"

def _v_insert_multiple_prefixes(name, record, rng):
    p1 = rng.choice(['Al-', 'El-', 'Abu '])
    p2 = rng.choice(['Bin ', 'Ibn '])
    return f"{p1}{p2}{name}", f"multiple prefixes '{p1}', '{p2}' prepended"

def _v_insert_title_twice(name, record, rng):
    title = rng.choice(['Dr', 'Sheikh', 'General', 'Mr'])
    return f"{title} {title} {name}", f"title '{title}' prepended twice"

def _v_insert_segment_middle(name, record, rng):
    tokens = name.split()
    if len(tokens) < 2:
        return None, "skip: need at least 2 tokens"
    seg = rng.choice(['International', 'Group', 'Global', 'National', 'General'])
    mid = len(tokens) // 2
    new = tokens[:mid] + [seg] + tokens[mid:]
    return ' '.join(new), f"segment '{seg}' inserted in middle"

def _v_reverse_comma(name, record, rng):
    tokens = name.split()
    if len(tokens) < 2:
        return None, "skip: need at least 2 tokens"
    last = tokens[-1]
    rest = ' '.join(tokens[:-1])
    return f"{last}, {rest}", "comma-delimited reversal: Last, First"

def _v_swap_first_middle(name, record, rng):
    tokens = name.split()
    if len(tokens) < 3:
        return None, "skip: need at least 3 tokens"
    tokens[0], tokens[1] = tokens[1], tokens[0]
    return ' '.join(tokens), "first and second tokens swapped"

def _v_swap_surnames(name, record, rng):
    tokens = name.split()
    if len(tokens) < 3:
        return None, "skip: need at least 3 tokens"
    tokens[-1], tokens[-2] = tokens[-2], tokens[-1]
    return ' '.join(tokens), "last two tokens (surnames) swapped"

def _v_swap_start_pair(name, record, rng):
    tokens = name.split()
    if len(tokens) < 3:
        return None, "skip: need at least 3 tokens"
    tokens[0], tokens[1] = tokens[1], tokens[0]
    return ' '.join(tokens), "first two tokens swapped"

def _v_swap_end_pair(name, record, rng):
    tokens = name.split()
    if len(tokens) < 3:
        return None, "skip: need at least 3 tokens"
    tokens[-1], tokens[-2] = tokens[-2], tokens[-1]
    return ' '.join(tokens), "last two tokens swapped"

def _v_subst_article(name, record, rng):
    tokens = name.split()
    articles = list(ARTICLES)
    for i, t in enumerate(tokens):
        if _is_article(t):
            alts = [a for a in articles if a != t.lower()]
            if alts:
                replacement = rng.choice(alts)
                tokens[i] = replacement.capitalize() if i == 0 else replacement
                return ' '.join(tokens), f"article '{t}' → '{tokens[i]}'"
    # No article found — insert different one
    arts = ['de', 'le', 'von', 'van']
    return f"{name} {rng.choice(arts)}", "different article appended"

def _v_subst_non_equiv_designator(name, record, rng):
    tokens = name.split()
    for i, t in enumerate(tokens):
        if _is_legal_designator(t):
            alts = ['Society', 'Cooperative', 'Syndicate', 'Consortium', 'Network']
            rep = rng.choice(alts)
            tokens[i] = rep
            return ' '.join(tokens), f"designator '{t}' → non-equivalent '{rep}'"
    return None, "skip: no legal designator found"

def _v_subst_city(name, record, rng):
    tokens = name.split()
    cities_lc = {c.lower(): c for c in CITY_NAMES}
    for i, t in enumerate(tokens):
        if t.lower() in cities_lc:
            alts = [c for c in CITY_NAMES if c.lower() != t.lower()]
            tokens[i] = rng.choice(alts)
            return ' '.join(tokens), f"city '{t}' replaced with different city"
    # Append a different city if none found
    city = rng.choice(CITY_NAMES)
    return f"{name} {city}", f"city '{city}' appended as segment"

def _v_subst_surname_variant(name, record, rng):
    tokens = name.split()
    if len(tokens) < 2:
        return None, "skip: need at least 2 tokens"
    last = tokens[-1]
    # Apply a minor phonetic variation to the surname
    lower = last.lower()
    pairs = [('ei', 'i'), ('ou', 'u'), ('kh', 'h'), ('ai', 'ay'), ('hassan', 'hasan'),
             ('hussein', 'hossein'), ('ali', 'aly'), ('ian', 'yan'), ('an', 'en')]
    rng.shuffle(pairs)
    for src, dst in pairs:
        if src in lower:
            new_last = lower.replace(src, dst, 1)
            new_last = _restore_caps(last, new_last)
            if new_last != last:
                tokens[-1] = new_last
                return ' '.join(tokens), f"surname variant: '{last}' → '{new_last}'"
    # Fallback: add suffix variation
    tokens[-1] = last + 'i'
    return ' '.join(tokens), f"surname variant: '{last}' → '{last}i'"

def _v_subst_all_surnames_snr(name, record, rng):
    tokens = name.split()
    if len(tokens) < 2:
        return None, "skip: need at least 2 tokens"
    new_last = rng.choice(PATRONYMICS)
    tokens[-1] = new_last
    return ' '.join(tokens), f"all surnames replaced: '{name}' → '{' '.join(tokens)}'"

def _v_subst_entity_segment(name, record, rng):
    tokens = name.split()
    if len(tokens) < 3:
        return None, "skip: need at least 3 tokens"
    interior = list(range(1, len(tokens) - 1))
    idx = rng.choice(interior)
    replacements = ['International', 'Global', 'National', 'General', 'United', 'Allied']
    tokens[idx] = rng.choice(replacements)
    return ' '.join(tokens), f"entity segment at position {idx} replaced"

def _v_subst_middle_name(name, record, rng):
    tokens = name.split()
    if len(tokens) < 3:
        return None, "skip: need at least 3 tokens"
    new_mid = rng.choice(GIVEN_NAMES)
    tokens[1] = new_mid
    return ' '.join(tokens), f"middle name replaced with '{new_mid}'"

def _v_initial_first_and_last(name, record, rng):
    tokens = name.split()
    if len(tokens) < 2:
        return None, "skip: need at least 2 tokens"
    tokens[0] = tokens[0][0].upper() + '.'
    tokens[-1] = tokens[-1][0].upper() + '.'
    return ' '.join(tokens), "first and last tokens initialised"

def _v_expand_acronym(name, record, rng):
    # If name looks like an acronym (all caps, 2-5 chars), expand each letter
    tokens = name.split()
    for i, t in enumerate(tokens):
        if len(t) >= 2 and t.isupper() and t.isalpha():
            expanded = ' '.join(rng.choice(GIVEN_NAMES[:5]) if j % 2 == 0 else rng.choice(['International', 'National', 'General'])[0] for j, c in enumerate(t))
            tokens[i] = ' '.join(c + '.' for c in t)
            return ' '.join(tokens), f"acronym '{t}' expanded to initials"
    # Fallback: just period-separate
    return _v_period_short_name(name, record, rng)

def _v_abbrev_multiple_tokens(name, record, rng):
    tokens = name.split()
    if len(tokens) < 2:
        return None, "skip: need at least 2 tokens"
    idxs = rng.sample(range(len(tokens)), min(2, len(tokens)))
    changed = False
    for i in idxs:
        t = tokens[i]
        if len(t) >= 5:
            tokens[i] = t[:3] + '.'
            changed = True
    if not changed:
        return None, "skip: tokens too short to abbreviate"
    return ' '.join(tokens), "multiple tokens abbreviated"

def _v_abbrev_first_middle(name, record, rng):
    tokens = name.split()
    if len(tokens) < 3:
        return None, "skip: need at least 3 tokens"
    tokens[0] = tokens[0][0].upper() + '.'
    tokens[1] = tokens[1][0].upper() + '.'
    return ' '.join(tokens), "first and middle tokens abbreviated to initials"

def _v_city_typo_end(name, record, rng):
    tokens = name.split()
    cities_lc = {c.lower(): c for c in CITY_NAMES}
    for i, t in enumerate(tokens):
        if t.lower() in cities_lc:
            if len(t) < 3:
                continue
            j = len(t) - 1
            replacement = rng.choice('abcdefghijklmnoprstuvwxyz'.replace(t[j].lower(), ''))
            tokens[i] = t[:j] + (replacement.upper() if t[j].isupper() else replacement)
            return ' '.join(tokens), f"city '{t}' typo at end position"
    return None, "skip: no recognised city token found"

def _v_city_typo_start(name, record, rng):
    tokens = name.split()
    cities_lc = {c.lower(): c for c in CITY_NAMES}
    for i, t in enumerate(tokens):
        if t.lower() in cities_lc:
            if len(t) < 3:
                continue
            replacement = rng.choice('abcdefghijklmnoprstuvwxyz'.replace(t[1].lower(), ''))
            tokens[i] = t[0] + (replacement.upper() if t[1].isupper() else replacement) + t[2:]
            return ' '.join(tokens), f"city '{t}' typo at start position"
    return None, "skip: no recognised city token found"

def _v_city_split_special(name, record, rng):
    tokens = name.split()
    cities_lc = {c.lower(): c for c in CITY_NAMES}
    for i, t in enumerate(tokens):
        if t.lower() in cities_lc and len(t) > 4:
            mid = len(t) // 2
            sep = rng.choice(['-', '.', '/'])
            tokens[i] = t[:mid] + sep + t[mid:]
            return ' '.join(tokens), f"city '{t}' split with '{sep}'"
    return None, "skip: no city or city too short"

def _v_city_add_char(name, record, rng):
    tokens = name.split()
    cities_lc = {c.lower(): c for c in CITY_NAMES}
    for i, t in enumerate(tokens):
        if t.lower() in cities_lc:
            pos = rng.randint(1, len(t) - 1)
            c = rng.choice(string.ascii_lowercase)
            tokens[i] = t[:pos] + c + t[pos:]
            return ' '.join(tokens), f"character inserted in city '{t}'"
    return None, "skip: no city found"

def _v_city_remove_char(name, record, rng):
    tokens = name.split()
    cities_lc = {c.lower(): c for c in CITY_NAMES}
    for i, t in enumerate(tokens):
        if t.lower() in cities_lc and len(t) > 3:
            pos = rng.randint(1, len(t) - 2)
            tokens[i] = t[:pos] + t[pos+1:]
            return ' '.join(tokens), f"character removed from city '{t}'"
    return None, "skip: no city found or too short"

COUNTRY_ISO3: dict[str, str] = {
    'iran': 'IRN', 'russia': 'RUS', 'north korea': 'PRK', 'china': 'CHN',
    'syria': 'SYR', 'cuba': 'CUB', 'venezuela': 'VEN', 'belarus': 'BLR',
    'myanmar': 'MMR', 'nicaragua': 'NIC', 'yemen': 'YEM', 'libya': 'LBY',
    'sudan': 'SDN', 'iraq': 'IRQ', 'afghanistan': 'AFG', 'lebanon': 'LBN',
    'somalia': 'SOM',
}

def _v_country_iso3(name, record, rng):
    tokens = name.split()
    for i, t in enumerate(tokens):
        iso3 = COUNTRY_ISO3.get(t.lower())
        if iso3:
            tokens[i] = iso3
            return ' '.join(tokens), f"country '{t}' → ISO-3 '{iso3}'"
    return None, "skip: no recognised country token"

def _v_country_city_embargo(name, record, rng):
    nationality = (record.get('nationality') or '').lower()
    country_cities = {
        'iranian': ('Tehran', 'Iran'), 'russian': ('Moscow', 'Russia'),
        'north korean': ('Pyongyang', 'North Korea'), 'syrian': ('Damascus', 'Syria'),
        'cuban': ('Havana', 'Cuba'),
    }
    pair = country_cities.get(nationality)
    if pair:
        return f"{name} {pair[0]} {pair[1]}", f"city+country '{pair[0]} {pair[1]}' appended"
    city = rng.choice(['Tehran', 'Moscow', 'Damascus', 'Pyongyang'])
    return f"{name} {city}", f"embargo city '{city}' appended"

def _v_country_abbreviation(name, record, rng):
    abbrevs = {
        'iran': 'Ir.', 'russia': 'Rus.', 'north korea': 'N. Korea',
        'china': 'Ch.', 'syria': 'Syr.', 'venezuela': 'Ven.',
    }
    tokens = name.split()
    for i, t in enumerate(tokens):
        abbr = abbrevs.get(t.lower())
        if abbr:
            tokens[i] = abbr
            return ' '.join(tokens), f"country '{t}' abbreviated to '{abbr}'"
    return None, "skip: no recognised country to abbreviate"

DEMONYMS: dict[str, str] = {
    'iran': 'Iranian', 'russia': 'Russian', 'north korea': 'North Korean',
    'china': 'Chinese', 'syria': 'Syrian', 'cuba': 'Cuban',
    'venezuela': 'Venezuelan', 'belarus': 'Belarusian', 'myanmar': 'Burmese',
}

def _v_nationality_descriptor(name, record, rng):
    nationality = (record.get('nationality') or '').lower()
    if nationality in NATIONALITY_ISO2:
        demonym = nationality.title()
        return f"{demonym} {name}", f"nationality descriptor '{demonym}' prepended"
    tokens = name.split()
    for t in tokens:
        dem = DEMONYMS.get(t.lower())
        if dem:
            return f"{dem} {name}", f"nationality descriptor '{dem}' prepended"
    dem = rng.choice(['Iranian', 'Russian', 'Syrian', 'Chinese'])
    return f"{dem} {name}", f"nationality descriptor '{dem}' prepended"

def _v_char_insert_start(name, record, rng):
    tokens = name.split()
    idx = _longest_alpha_token(tokens)
    t = tokens[idx]
    c = rng.choice(string.ascii_lowercase)
    tokens[idx] = c + t
    return ' '.join(tokens), f"character '{c}' inserted at start of token"

def _v_char_insert_end(name, record, rng):
    tokens = name.split()
    idx = _longest_alpha_token(tokens)
    t = tokens[idx]
    c = rng.choice(string.ascii_lowercase)
    tokens[idx] = t + c
    return ' '.join(tokens), f"character '{c}' appended at end of token"

def _v_char_insert_two(name, record, rng):
    tokens = name.split()
    idx = _longest_alpha_token(tokens)
    t = tokens[idx]
    if len(t) < 3:
        return None, "skip: token too short"
    c1 = rng.choice(string.ascii_lowercase)
    c2 = rng.choice(string.ascii_lowercase)
    p1 = rng.randint(0, len(t))
    p2 = rng.randint(0, len(t) + 1)
    t1 = t[:p1] + c1 + t[p1:]
    tokens[idx] = t1[:p2] + c2 + t1[p2:]
    return ' '.join(tokens), f"two characters '{c1}' and '{c2}' inserted"

def _v_char_delete_start(name, record, rng):
    tokens = name.split()
    idx = _longest_alpha_token(tokens)
    t = tokens[idx]
    if len(t) < 4:
        return None, "skip: token too short"
    tokens[idx] = t[1:]
    return ' '.join(tokens), "first character deleted"

def _v_char_delete_end(name, record, rng):
    tokens = name.split()
    idx = _longest_alpha_token(tokens)
    t = tokens[idx]
    if len(t) < 4:
        return None, "skip: token too short"
    tokens[idx] = t[:-1]
    return ' '.join(tokens), "last character deleted"

def _v_char_delete_two(name, record, rng):
    tokens = name.split()
    idx = _longest_alpha_token(tokens)
    t = tokens[idx]
    if len(t) < 5:
        return None, "skip: token too short"
    alpha = [i for i, c in enumerate(t) if c.isalpha()]
    if len(alpha) < 2:
        return None, "skip: insufficient alpha chars"
    p1, p2 = sorted(rng.sample(alpha, 2), reverse=True)
    t2 = t[:p1] + t[p1+1:]
    t2 = t2[:p2] + t2[p2+1:] if p2 < len(t2) else t2
    tokens[idx] = t2
    return ' '.join(tokens), f"two characters at positions {p1},{p2} deleted"

LETTER_TO_NUM: dict[str, str] = {
    'o': '0', 'O': '0', 'i': '1', 'I': '1', 'l': '1', 'z': '2', 'Z': '2',
    'e': '3', 'E': '3', 'a': '4', 'A': '4', 's': '5', 'S': '5',
    'g': '9', 'G': '9', 'b': '8', 'B': '8', 't': '7', 'T': '7',
}

def _v_numeric_substitute(name, record, rng):
    candidates = [(i, c) for i, c in enumerate(name) if c in LETTER_TO_NUM]
    if not candidates:
        return None, "skip: no substitutable letters"
    pos, char = rng.choice(candidates)
    result = name[:pos] + LETTER_TO_NUM[char] + name[pos+1:]
    return result, f"letter '{char}' → numeral '{LETTER_TO_NUM[char]}'"

def _v_numeric_multi(name, record, rng):
    candidates = [(i, c) for i, c in enumerate(name) if c in LETTER_TO_NUM]
    if len(candidates) < 2:
        return None, "skip: insufficient substitutable letters"
    chosen = rng.sample(candidates, min(3, len(candidates)))
    lst = list(name)
    for pos, char in chosen:
        lst[pos] = LETTER_TO_NUM[char]
    result = ''.join(lst)
    if result == name:
        return None, "skip: no change"
    return result, f"multiple numeric substitutions: {[(c, LETTER_TO_NUM[c]) for _, c in chosen]}"

def _v_numeric_segment(name, record, rng):
    tokens = name.split()
    idx = _longest_alpha_token(tokens)
    t = tokens[idx]
    result = ''.join(LETTER_TO_NUM.get(c, c) for c in t)
    if result == t:
        return None, "skip: no substitutable letters"
    tokens[idx] = result
    return ' '.join(tokens), f"token '{t}' → '{result}' (all letter→numeral)"

def _v_phonetic_multi(name, record, rng):
    lower = name.lower()
    applied = []
    result = lower
    for pattern, replacement in _shuffled_phonetic(PHONETIC_PATTERNS, rng)[:3]:
        if pattern in result and result != result.replace(pattern, replacement, 1):
            result = result.replace(pattern, replacement, 1)
            applied.append(f"'{pattern}'→'{replacement}'")
            if len(applied) >= 2:
                break
    if not applied or result == lower:
        return None, "skip: no applicable phonetic patterns"
    return _restore_caps(name, result), f"multiple phonetic subs: {', '.join(applied)}"

def _v_phonetic_cross_part(name, record, rng):
    tokens = name.split()
    if len(tokens) < 2:
        return None, "skip: single token"
    idx = rng.choice(range(len(tokens)))
    t = tokens[idx]
    lower = t.lower()
    for pattern, replacement in _shuffled_phonetic(PHONETIC_PATTERNS, rng):
        if pattern in lower:
            new_t = lower.replace(pattern, replacement, 1)
            if new_t != lower:
                tokens[idx] = _restore_caps(t, new_t)
                return ' '.join(tokens), f"phonetic sub in token {idx}: '{pattern}'→'{replacement}'"
    return None, "skip: no phonetic pattern found"

def _v_char_repeat_two_letters(name, record, rng):
    tokens = name.split()
    idx = _longest_alpha_token(tokens)
    t = tokens[idx]
    alpha = [i for i, c in enumerate(t) if c.isalpha()]
    if len(alpha) < 2:
        return None, "skip: insufficient alpha chars"
    p1, p2 = sorted(rng.sample(alpha, 2))
    lst = list(t)
    lst.insert(p2 + 1, lst[p2])
    lst.insert(p1 + 1, lst[p1])
    tokens[idx] = ''.join(lst)
    return ' '.join(tokens), f"two letters repeated at positions {p1},{p2}"

def _v_char_repeat_thrice(name, record, rng):
    tokens = name.split()
    idx = _longest_alpha_token(tokens)
    t = tokens[idx]
    alpha = [i for i, c in enumerate(t) if c.isalpha()]
    if not alpha:
        return None, "skip: no alpha chars"
    pos = rng.choice(alpha)
    tokens[idx] = t[:pos] + t[pos] * 2 + t[pos:]
    return ' '.join(tokens), f"character '{t[pos]}' at pos {pos} repeated twice extra"

def _v_char_transpose_non_adjacent(name, record, rng):
    tokens = name.split()
    idx = _longest_alpha_token(tokens)
    t = tokens[idx]
    alpha = [i for i, c in enumerate(t) if c.isalpha()]
    non_adj = [(i, j) for i in alpha for j in alpha if j - i > 2]
    if not non_adj:
        return None, "skip: no non-adjacent alpha pair"
    p1, p2 = rng.choice(non_adj)
    lst = list(t)
    lst[p1], lst[p2] = lst[p2], lst[p1]
    tokens[idx] = ''.join(lst)
    return ' '.join(tokens), f"non-adjacent chars at positions {p1},{p2} transposed"

def _v_char_transpose_multiple(name, record, rng):
    tokens = name.split()
    idx = _longest_alpha_token(tokens)
    t = tokens[idx]
    if len(t) < 5:
        return None, "skip: token too short"
    lst = list(t)
    n_swaps = rng.randint(2, min(4, len(lst) // 2))
    positions = rng.sample(range(len(lst) - 1), n_swaps)
    for pos in positions:
        lst[pos], lst[pos+1] = lst[pos+1], lst[pos]
    tokens[idx] = ''.join(lst)
    return ' '.join(tokens), f"{n_swaps} character transpositions applied"

def _v_truncate_front_two(name, record, rng):
    tokens = name.split()
    idx = _longest_alpha_token(tokens)
    t = tokens[idx]
    if len(t) < 5:
        return None, "skip: token too short"
    tokens[idx] = t[2:]
    return ' '.join(tokens), "first 2 characters truncated from front"

def _v_truncate_end_two(name, record, rng):
    tokens = name.split()
    idx = _longest_alpha_token(tokens)
    t = tokens[idx]
    if len(t) < 5:
        return None, "skip: token too short"
    tokens[idx] = t[:-2]
    return ' '.join(tokens), "last 2 characters truncated from end"

def _v_truncate_middle(name, record, rng):
    tokens = name.split()
    idx = _longest_alpha_token(tokens)
    t = tokens[idx]
    if len(t) < 6:
        return None, "skip: token too short"
    mid = len(t) // 2
    n = rng.choice([1, 2])
    tokens[idx] = t[:mid - n//2] + t[mid + (n - n//2):]
    return ' '.join(tokens), f"{n} character(s) removed from middle of token"

def _v_typo_first_letter(name, record, rng):
    tokens = name.split()
    idx = _longest_alpha_token(tokens)
    t = tokens[idx]
    if not t or not t[0].isalpha():
        return None, "skip: first char not alpha"
    replacement = _keyboard_typo(t[0], rng)
    tokens[idx] = replacement + t[1:]
    return ' '.join(tokens), f"typo on first letter: '{t[0]}' → '{replacement}'"

def _v_typo_adjacent(name, record, rng):
    tokens = name.split()
    idx = _longest_alpha_token(tokens)
    t = tokens[idx]
    alpha = [i for i, c in enumerate(t) if c.isalpha()]
    if len(alpha) < 3:
        return None, "skip: insufficient alpha chars"
    # Pick two adjacent alpha positions
    adj_pairs = [(alpha[i], alpha[i+1]) for i in range(len(alpha)-1) if alpha[i+1] - alpha[i] == 1]
    if not adj_pairs:
        return None, "skip: no adjacent alpha pair"
    p1, p2 = rng.choice(adj_pairs)
    lst = list(t)
    lst[p1] = _keyboard_typo(lst[p1], rng)
    lst[p2] = _keyboard_typo(lst[p2], rng)
    tokens[idx] = ''.join(lst)
    return ' '.join(tokens), f"two adjacent typos at positions {p1},{p2}"

def _v_typo_noise_parts(name, record, rng):
    tokens = name.split()
    # Find article or designator token to introduce typo in
    noise_idxs = [i for i, t in enumerate(tokens) if _is_article(t) or _is_legal_designator(t)]
    if not noise_idxs:
        return None, "skip: no noise/stop words found"
    idx = rng.choice(noise_idxs)
    t = tokens[idx]
    alpha = [i for i, c in enumerate(t) if c.isalpha()]
    if not alpha:
        return None, "skip: no alpha chars in noise token"
    pos = rng.choice(alpha)
    replacement = _keyboard_typo(t[pos], rng)
    tokens[idx] = t[:pos] + replacement + t[pos+1:]
    return ' '.join(tokens), f"typo in noise part '{t}' → '{tokens[idx]}'"

def _v_typo_across_parts(name, record, rng):
    tokens = name.split()
    if len(tokens) < 2:
        return None, "skip: single token"
    # Introduce typos in two different tokens
    idxs = rng.sample(range(len(tokens)), min(2, len(tokens)))
    for idx in idxs:
        t = tokens[idx]
        alpha = [i for i, c in enumerate(t) if c.isalpha()]
        if alpha:
            pos = rng.choice(alpha)
            tokens[idx] = t[:pos] + _keyboard_typo(t[pos], rng) + t[pos+1:]
    return ' '.join(tokens), "typos introduced across different name parts"

def _v_typo_stop_word(name, record, rng):
    tokens = name.split()
    stop = [i for i, t in enumerate(tokens) if _is_article(t)]
    if not stop:
        return None, "skip: no stop words found"
    idx = rng.choice(stop)
    t = tokens[idx]
    if not t:
        return None, "skip"
    pos = rng.randint(0, len(t) - 1)
    replacement = rng.choice(string.ascii_lowercase.replace(t[pos].lower(), ''))
    tokens[idx] = t[:pos] + replacement + t[pos+1:]
    return ' '.join(tokens), f"typo on stop word '{t}'"

SPECIAL_CHARS = ['@', '#', '$', '%', '&', '*', '+', '=', '~', '^']

def _v_special_char_add(name, record, rng):
    sc = rng.choice(SPECIAL_CHARS)
    pos = rng.choice(['start', 'end', 'middle'])
    tokens = name.split()
    idx = _longest_alpha_token(tokens)
    t = tokens[idx]
    if pos == 'start':
        tokens[idx] = sc + t
    elif pos == 'end':
        tokens[idx] = t + sc
    else:
        mid = len(t) // 2
        tokens[idx] = t[:mid] + sc + t[mid:]
    return ' '.join(tokens), f"special char '{sc}' added at {pos}"

def _v_special_char_add_multiple(name, record, rng):
    chars = rng.sample(SPECIAL_CHARS, min(3, len(SPECIAL_CHARS)))
    result = name
    for sc in chars:
        pos = rng.randint(1, len(result))
        result = result[:pos] + sc + result[pos:]
    return result, f"multiple special chars added: {''.join(chars)}"

def _v_special_between_letters(name, record, rng):
    tokens = name.split()
    idx = _longest_alpha_token(tokens)
    t = tokens[idx]
    if len(t) < 3:
        return None, "skip: token too short"
    sc = rng.choice(SPECIAL_CHARS)
    tokens[idx] = sc.join(list(t))
    return ' '.join(tokens), f"letters of '{t}' separated by '{sc}'"

def _v_special_between_parts(name, record, rng):
    tokens = name.split()
    if len(tokens) < 2:
        return None, "skip: single token"
    sc = rng.choice(SPECIAL_CHARS)
    return sc.join(tokens), f"name parts separated by '{sc}'"

def _v_special_surround_words(name, record, rng):
    sc = rng.choice(SPECIAL_CHARS)
    tokens = name.split()
    new = []
    for t in tokens:
        new.append(rng.choice(['', sc]) + t + rng.choice(['', sc]))
    return ' '.join(new), f"special chars '{sc}' inserted within name"

def _v_noise_surround(name, record, rng):
    noise = ''.join(rng.choices('!@#$%^&*+=~', k=rng.randint(2, 4)))
    return f"{noise}{name}{noise}", f"noise '{noise}' surrounding name"

def _v_noise_adjacent(name, record, rng):
    noise = ''.join(rng.choices('!@#$%', k=rng.randint(2, 3)))
    return f"{name}{noise}", f"noise '{noise}' appended adjacent"

def _v_space_very_large(name, record, rng):
    tokens = name.split()
    if len(tokens) < 2:
        return None, "skip: single token"
    n = rng.randint(5, 10)
    return (' ' * n).join(tokens), f"{n} spaces between tokens"

def _v_special_replace_equiv(name, record, rng):
    repls = {'-': '–', "'": '\u2019', '.': '·', '/': '\\'}
    for char, equiv in repls.items():
        if char in name:
            return name.replace(char, equiv, 1), f"'{char}' → Unicode equivalent '{equiv}'"
    return None, "skip: no replaceable special character"

def _v_special_remove_single(name, record, rng):
    specials = [i for i, c in enumerate(name) if not c.isalnum() and c != ' ']
    if not specials:
        return None, "skip: no special characters"
    pos = rng.choice(specials)
    return name[:pos] + name[pos+1:], f"single special char at position {pos} removed"

def _v_compress_partial(name, record, rng):
    tokens = name.split()
    if len(tokens) < 2:
        return None, "skip: single token"
    i = rng.randint(0, len(tokens) - 2)
    tokens[i] = tokens[i] + tokens[i+1]
    del tokens[i+1]
    return ' '.join(tokens), f"tokens {i} and {i+1} compressed together"

def _v_split_all_letters(name, record, rng):
    tokens = name.split()
    idx = _longest_alpha_token(tokens)
    t = tokens[idx]
    tokens[idx] = ' '.join(list(t))
    return ' '.join(tokens), f"token '{t}' split into individual letters"

def _v_split_newline(name, record, rng):
    tokens = name.split()
    if len(tokens) < 2:
        return None, "skip: single token"
    pos = rng.randint(1, len(tokens) - 1)
    return ' '.join(tokens[:pos]) + '\n' + ' '.join(tokens[pos:]), "newline used as token delimiter"

def _v_letter_to_special(name, record, rng):
    alpha = [(i, c) for i, c in enumerate(name) if c.isalpha()]
    if not alpha:
        return None, "skip: no alpha chars"
    pos, char = rng.choice(alpha)
    sc = rng.choice(SPECIAL_CHARS)
    return name[:pos] + sc + name[pos+1:], f"letter '{char}' → special char '{sc}'"

def _v_name_part_to_special_snr(name, record, rng):
    tokens = name.split()
    if len(tokens) < 2:
        return None, "skip: single token"
    idx = rng.choice(range(len(tokens)))
    sc = rng.choice(SPECIAL_CHARS)
    tokens[idx] = sc * len(tokens[idx])
    return ' '.join(tokens), f"token replaced with '{sc}' chars"

def _v_short_special(name, record, rng):
    tokens = name.split()
    if not tokens:
        return None, "skip"
    t = tokens[0]
    if len(t) < 2:
        return None, "skip: token too short"
    sc = rng.choice(SPECIAL_CHARS)
    mid = len(t) // 2
    tokens[0] = t[:mid] + sc + t[mid:]
    return ' '.join(tokens), f"special char '{sc}' inserted in short name"

def _v_accent_inverted_q(name, record, rng):
    tokens = name.split()
    idx = _longest_alpha_token(tokens)
    t = tokens[idx]
    if not t:
        return None, "skip"
    # Insert inverted question mark or exclamation
    char = rng.choice(['¿', '¡', 'ñ'])
    mid = len(t) // 2
    tokens[idx] = t[:mid] + char + t[mid:]
    return ' '.join(tokens), f"special accent char '{char}' inserted"

def _v_to_cyrillic(name, record, rng):
    # Reverse: put Cyrillic look-alikes where possible
    reverse_map = {v: k for k, v in CYRILLIC_LATIN.items() if len(v) == 1 and v.isalpha()}
    result = ''.join(reverse_map.get(c.upper(), reverse_map.get(c, c)) for c in name)
    if result == name:
        # Use homoglyph map instead
        return _v_homoglyph(name, record, rng)
    return result, "Latin characters replaced with Cyrillic look-alikes"

def _v_leet_speak_numbers(name, record, rng):
    return _v_numeric_multi(name, record, rng)

def _v_leet_speak_currency(name, record, rng):
    CURRENCY_MAP = {'s': '$', 'S': '$', 'e': '€', 'E': '€', 'a': '@', 'A': '@',
                    'l': '£', 'L': '£', 'o': '⊕', 'O': '⊕'}
    candidates = [(i, c) for i, c in enumerate(name) if c in CURRENCY_MAP]
    if not candidates:
        return None, "skip: no substitutable letters"
    chosen = rng.sample(candidates, min(2, len(candidates)))
    lst = list(name)
    for pos, char in chosen:
        lst[pos] = CURRENCY_MAP[char]
    return ''.join(lst), f"currency symbol substitutions applied"

def _v_name_fragment(name, record, rng):
    tokens = name.split()
    if len(tokens) < 2:
        return None, "skip: single token"
    sep = rng.choice([' | ', ' / ', ' + '])
    mid = len(tokens) // 2
    return ' '.join(tokens[:mid]) + sep + ' '.join(tokens[mid:]), f"name fragmented with '{sep.strip()}'"

def _v_transliteration_mismatch(name, record, rng):
    tokens = name.split()
    if len(tokens) < 2:
        return None, "skip: single token"
    # Apply different romanisation rules to different tokens
    idx1, idx2 = rng.sample(range(len(tokens)), min(2, len(tokens)))
    lower1 = tokens[idx1].lower()
    lower2 = tokens[idx2].lower()
    pairs1 = [('kh', 'h'), ('ou', 'u'), ('ei', 'i'), ('ai', 'ay')]
    pairs2 = [('h', 'kh'), ('u', 'ou'), ('i', 'ei'), ('ay', 'ai')]
    rng.shuffle(pairs1)
    for src, dst in pairs1:
        if src in lower1:
            tokens[idx1] = _restore_caps(tokens[idx1], lower1.replace(src, dst, 1))
            break
    rng.shuffle(pairs2)
    for src, dst in pairs2:
        if src in lower2:
            tokens[idx2] = _restore_caps(tokens[idx2], lower2.replace(src, dst, 1))
            break
    return ' '.join(tokens), "inconsistent romanisation applied across tokens"

def _v_junk_account_number(name, record, rng):
    prefix = rng.choice(['ACCT-', 'ACC/', 'A/C:', 'REF:'])
    num = ''.join(rng.choices(string.digits, k=rng.randint(8, 12)))
    return f"{name} {prefix}{num}", f"account number '{prefix}{num}' appended"

def _v_address_spillover(name, record, rng):
    addrs = ['123 Main St', '45 High Road', 'P.O. Box 1234', 'Suite 500']
    addr = rng.choice(addrs)
    return f"{name} {addr}", f"address fragment '{addr}' appended"

def _v_short_single_char(name, record, rng):
    tokens = name.split()
    # Find a token that can be reduced to a single char
    for i, t in enumerate(tokens):
        if len(t) >= 2 and t.isalpha():
            tokens[i] = t[0]
            return ' '.join(tokens), f"token '{t}' reduced to single char '{t[0]}'"
    return None, "skip: no suitable token"

def _v_first_word_only(name, record, rng):
    tokens = name.split()
    if len(tokens) < 2:
        return None, "skip: single token"
    return tokens[0], "only first token retained"

def _v_mononym(name, record, rng):
    tokens = name.split()
    if len(tokens) < 2:
        return None, "skip: already single token"
    return rng.choice(tokens), "single name token (mononym) selected"

def _v_lowercase(name, record, rng):
    return name.lower(), "name converted to lowercase"

def _v_leading_zero(name, record, rng):
    tokens = name.split()
    # Find a token with digits and prepend 0
    for i, t in enumerate(tokens):
        if any(c.isdigit() for c in t):
            tokens[i] = '0' + t
            return ' '.join(tokens), f"leading zero added to '{t}'"
    # Append a zero-prefixed number
    return f"{name} 0{rng.randint(100,999)}", "leading zero added to appended number"

def _v_remove_leading_zero(name, record, rng):
    tokens = name.split()
    for i, t in enumerate(tokens):
        if t.startswith('0') and len(t) > 1:
            tokens[i] = t[1:]
            return ' '.join(tokens), f"leading zero removed from '{t}'"
    return None, "skip: no token starts with 0"

def _v_append_suffix(name, record, rng):
    suffixes = ['Oblast', 'Region', 'Province', 'District', 'Prefecture']
    s = rng.choice(suffixes)
    return f"{name} {s}", f"administrative suffix '{s}' appended"

def _v_append_region(name, record, rng):
    regions = ['North', 'South', 'East', 'West', 'Central', 'Greater']
    r = rng.choice(regions)
    return f"{name} {r}", f"region qualifier '{r}' appended"

COUNTRY_COMMON_NAMES: dict[str, str] = {
    'russian federation': 'Russia',
    'russia': 'Russian Federation',
    "democratic people's republic of korea": 'North Korea',
    'north korea': "Democratic People's Republic of Korea",
    'dprk': 'North Korea',
    'syrian arab republic': 'Syria',
    'syria': 'Syrian Arab Republic',
    'union of myanmar': 'Myanmar',
    'myanmar': 'Burma',
    'burma': 'Myanmar',
    'republic of cuba': 'Cuba',
    "côte d'ivoire": 'Ivory Coast',
    'ivory coast': "Côte d'Ivoire",
    'kingdom of eswatini': 'Swaziland',
    'eswatini': 'Swaziland',
    'swaziland': 'Eswatini',
}

def _v_country_common_alt(name, record, rng):
    lower = name.lower()
    for src, dst in COUNTRY_COMMON_NAMES.items():
        if src in lower:
            result = lower.replace(src, dst.lower(), 1)
            return _restore_caps(name, result), f"country name '{src}' → '{dst}'"
    return None, "skip: no recognised country name variant"

def _v_demonym(name, record, rng):
    demonym_map = {
        'iran': 'Iranian', 'russia': 'Russian', 'china': 'Chinese',
        'syria': 'Syrian', 'cuba': 'Cuban', 'venezuela': 'Venezuelan',
        'belarus': 'Belarusian', 'myanmar': 'Burmese', 'iraq': 'Iraqi',
    }
    tokens = name.split()
    for i, t in enumerate(tokens):
        dem = demonym_map.get(t.lower())
        if dem:
            tokens[i] = dem
            return ' '.join(tokens), f"country '{t}' → demonym '{dem}'"
    return None, "skip: no substitutable country"


# ── Dispatch table: type_id → function ────────────────────────────────────────

# Types where test_name intentionally equals source name — skip the same-name guard
PASSTHROUGH_TYPES: set[str] = {'TC001', 'TC001B'}

# Types that require sampling only primary or AKA entries respectively
PRIMARY_AKA_FILTER_MAP: dict[str, str] = {
    'TC001':  'primary',
    'TC001B': 'aka',
}

VARIATION_FUNCTIONS: dict[str, Callable] = {
    'TC001':  _v_primary_name_match,
    'TC001B': _v_aka_match,
    'TC002': _v_omit_article,
    'TC003': _v_omit_legal_designator,
    'TC004': _v_omit_location_segment,
    'TC005': _v_omit_first_name,
    'TC006': _v_omit_all_given,
    'TC007': _v_omit_middle_name,
    'TC008': _v_omit_entity_segment,
    'TC009': _v_omit_title,
    'TC010': _v_omit_prefix,
    'TC011': _v_omit_qualifier,
    'TC012': _v_insert_article,
    'TC013': _v_insert_legal_designator,
    'TC014': _v_insert_location,
    'TC015': _v_insert_given_name,
    'TC016': _v_insert_surname,
    'TC017': _v_insert_prefix,
    'TC018': _v_insert_title,
    'TC019': _v_insert_qualifier,
    'TC020': _v_reverse_tokens,
    'TC021': _v_reorder_given_names,
    'TC022': _v_swap_entity_segments,
    'TC023': _v_swap_first_last,
    'TC024': _v_equiv_legal_designator,
    'TC025': _v_dialectal_prefix,
    'TC026': _v_subst_title,
    'TC027': _v_subst_qualifier,
    'TC028': _v_initial_first_name,
    'TC029': _v_initial_all_given,
    'TC030': _v_company_acronym,
    'TC031': _v_abbrev_token,
    'TC032': _v_city_typo,
    'TC033': _v_country_iso2,
    'TC034': _v_char_insert,
    'TC035': _v_char_delete,
    'TC036': _v_char_repeat,
    'TC037': _v_char_transpose,
    'TC038': _v_truncate_front,
    'TC039': _v_truncate_end,
    'TC040': _v_single_typo,
    'TC041': _v_double_typo,
    'TC042': _v_phonetic_sub,
    'TC043': _v_insert_hyphen,
    'TC044': _v_extra_spaces,
    'TC045': _v_expand_diacritics,
    'TC046': _v_remove_punctuation,
    'TC047': _v_compress_name,
    'TC048': _v_split_token,
    'TC049': _v_strip_accents,
    'TC050': _v_alternate_romanisation,
    'TC051': _v_cyrillic_transliterate,
    'TC052': _v_vessel_add_prefix,
    'TC053': _v_vessel_remove_prefix,
    'TC054': _v_homoglyph,
    'TC055': _v_zero_width,
    'TC056': _v_noise_suffix,
    'TC057': _v_country_code_suffix,
    'TC058': _v_period_short_name,
    # ── TC059+ new types from Screening Test Matrix ────────────────────────────
    'TC059': _v_omit_all_articles,
    'TC060': _v_omit_all_legal_designators,
    'TC061': _v_omit_middle_name,           # all middle names → same as single
    'TC062': _v_omit_surname,
    'TC063': _v_omit_all_surnames_keep_first,
    'TC064': _v_omit_multiple_segments,
    'TC065': _v_insert_multiple_articles,
    'TC066': _v_insert_legal_designator_front,
    'TC067': _v_duplicate_legal_designator,
    'TC068': _v_insert_country_name,
    'TC069': _v_insert_middle_name,
    'TC070': _v_insert_multiple_prefixes,
    'TC071': _v_insert_title_twice,
    'TC072': _v_insert_segment_middle,
    'TC073': _v_reverse_comma,
    'TC074': _v_swap_first_middle,
    'TC075': _v_swap_surnames,
    'TC076': _v_swap_start_pair,
    'TC077': _v_swap_end_pair,
    'TC078': _v_subst_article,
    'TC079': _v_subst_non_equiv_designator,
    'TC080': _v_subst_city,
    'TC081': _v_subst_surname_variant,
    'TC082': _v_subst_all_surnames_snr,
    'TC083': _v_subst_entity_segment,
    'TC084': _v_subst_middle_name,
    'TC085': _v_initial_first_and_last,
    'TC086': _v_expand_acronym,
    'TC087': _v_abbrev_multiple_tokens,
    'TC088': _v_abbrev_first_middle,
    'TC091': _v_city_typo_end,
    'TC092': _v_city_typo_start,
    'TC093': _v_city_split_special,
    'TC094': _v_city_add_char,
    'TC095': _v_city_remove_char,
    'TC096': _v_country_iso3,
    'TC097': _v_country_city_embargo,
    'TC098': _v_country_abbreviation,
    'TC099': _v_nationality_descriptor,
    'TC102': _v_char_insert_start,
    'TC103': _v_char_insert_end,
    'TC104': _v_char_insert_two,
    'TC105': _v_char_delete_start,
    'TC106': _v_char_delete_end,
    'TC107': _v_char_delete_two,
    'TC108': _v_numeric_substitute,
    'TC109': _v_numeric_multi,
    'TC110': _v_numeric_segment,
    'TC111': _v_phonetic_multi,
    'TC112': _v_phonetic_cross_part,
    'TC113': _v_char_repeat_two_letters,
    'TC114': _v_char_repeat_thrice,
    'TC115': _v_char_transpose_non_adjacent,
    'TC116': _v_char_transpose_multiple,
    'TC117': _v_truncate_front_two,
    'TC118': _v_truncate_end_two,
    'TC119': _v_truncate_middle,
    'TC120': _v_typo_first_letter,
    'TC121': _v_typo_adjacent,
    'TC122': _v_typo_noise_parts,
    'TC123': _v_typo_across_parts,
    'TC124': _v_typo_stop_word,
    'TC125': _v_special_char_add,
    'TC126': _v_special_char_add_multiple,
    'TC127': _v_special_between_letters,
    'TC128': _v_special_between_parts,
    'TC129': _v_special_surround_words,
    'TC130': _v_noise_surround,
    'TC131': _v_noise_adjacent,
    'TC132': _v_space_very_large,
    'TC133': _v_special_replace_equiv,
    'TC134': _v_special_remove_single,
    'TC135': _v_compress_partial,
    'TC136': _v_split_all_letters,
    'TC137': _v_split_newline,
    'TC138': _v_letter_to_special,
    'TC139': _v_name_part_to_special_snr,
    'TC140': _v_short_special,
    'TC141': _v_accent_inverted_q,
    'TC146': _v_to_cyrillic,
    'TC172': _v_to_cyrillic,
    'TC177': _v_leet_speak_numbers,
    'TC178': _v_leet_speak_currency,
    'TC179': _v_name_fragment,
    'TC182': _v_transliteration_mismatch,
    'TC184': _v_junk_account_number,
    'TC185': _v_address_spillover,
    'TC187': _v_short_single_char,
    'TC189': _v_mononym,
    'TC192': _v_leading_zero,
    'TC193': _v_lowercase,
    'TC198': _v_first_word_only,
    'TC202': _v_remove_leading_zero,
    'TC207': _v_first_word_only,
    'TC212': _v_append_suffix,
    'TC213': _v_append_region,
    'TC216': _v_append_suffix,
    'TC253': _v_country_common_alt,
    'TC255': _v_country_common_alt,
    'TC261': _v_demonym,
}


# ── Custom type support ────────────────────────────────────────────────────────

async def load_custom_types(db: aiosqlite.Connection) -> list[TestCaseType]:
    """Load user-created types from the custom_test_types DB table."""
    try:
        async with db.execute(
            """SELECT type_id, theme, category, type_name, description,
                      applicable_entity_types, applicable_min_tokens, applicable_min_name_length,
                      expected_outcome, variation_logic
               FROM custom_test_types ORDER BY created_at"""
        ) as cur:
            rows = await cur.fetchall()
        return [
            TestCaseType(
                type_id=r[0],
                theme=r[1],
                category=r[2],
                type_name=r[3],
                description=r[4],
                applicable_entity_types=[e.strip() for e in (r[5] or 'individual').split('|')],
                applicable_min_tokens=r[6] or 1,
                applicable_min_name_length=r[7] or 1,
                expected_outcome=r[8] or 'Should Hit',
                variation_logic=r[9] or '',
            )
            for r in rows
        ]
    except Exception:
        return []


async def get_custom_lambda(type_id: str, db: aiosqlite.Connection) -> Optional[str]:
    """Return the python_lambda string for a custom type."""
    try:
        async with db.execute(
            "SELECT python_lambda FROM custom_test_types WHERE type_id = ?", (type_id,)
        ) as cur:
            row = await cur.fetchone()
        return row[0] if row else None
    except Exception:
        return None


def _make_lambda_fn(lambda_str: str):
    """Compile a lambda string into a variation function matching the standard signature."""
    from app.services.chatbot_agent import _safe_apply
    def var_fn(name: str, record: dict, rng: random.Random):
        result = _safe_apply(lambda_str, name, rng)
        if result is None:
            return None, "skip: lambda returned None"
        return result, "custom lambda applied"
    return var_fn


# ── Sampling ───────────────────────────────────────────────────────────────────

async def _sample_names(
    applicable_entity_types: list[str],
    min_tokens: int,
    min_name_length: int,
    count: int,
    db: aiosqlite.Connection,
    distribution: str = 'balanced',
    custom_dist: dict | None = None,
    primary_aka_filter: str | None = None,
    watchlists: list[str] | None = None,
) -> list[dict]:
    """
    Stratified sample of watchlist entries for a given type's constraints.
    Returns up to `count * 2` rows to allow for skips.
    """
    conditions = []
    params: list = []

    if primary_aka_filter is not None:
        conditions.append("primary_aka = ?")
        params.append(primary_aka_filter)

    if applicable_entity_types and 'unknown' not in applicable_entity_types:
        # If "all" types listed, skip filter
        all_types = {'individual', 'entity', 'vessel', 'aircraft', 'country', 'unknown'}
        if set(applicable_entity_types) != all_types:
            ph = ', '.join('?' for _ in applicable_entity_types)
            conditions.append(f"entity_type IN ({ph})")
            params.extend(applicable_entity_types)

    if min_tokens > 1:
        conditions.append(f"num_tokens >= ?")
        params.append(min_tokens)

    if min_name_length > 1:
        conditions.append(f"name_length >= ?")
        params.append(min_name_length)

    if watchlists:
        ph = ', '.join('?' for _ in watchlists)
        conditions.append(f"watchlist IN ({ph})")
        params.extend(watchlists)

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    _SELECT = """SELECT uid, watchlist, sub_watchlist_1, cleaned_name, original_name,
                        primary_aka, entity_type, num_tokens, name_length,
                        name_culture, sanctions_program
                 FROM watchlist_entries"""

    # ── Custom distribution: stratified sample per culture ────────────────────
    if distribution == 'custom' and custom_dist:
        active = [(c, float(pct)) for c, pct in custom_dist.items() if (pct or 0) > 0]
        if active:
            total_pct = sum(pct for _, pct in active)
            all_rows: list[dict] = []
            for culture, pct in active:
                n = max(2, round(count * 10 * pct / total_pct))
                c_conditions = conditions + ["name_culture = ?"]
                c_params = params + [culture]
                c_where = "WHERE " + " AND ".join(c_conditions)
                async with db.execute(
                    f"{_SELECT} {c_where} ORDER BY RANDOM() LIMIT ?",
                    c_params + [n],
                ) as cur:
                    all_rows.extend([dict(r) for r in await cur.fetchall()])
            return all_rows

    # ── Balanced distribution: equal sample from each available culture ────────
    if distribution == 'balanced':
        culture_cond = "name_culture IS NOT NULL AND name_culture != ''"
        culture_where = (where + " AND " + culture_cond) if where else ("WHERE " + culture_cond)
        async with db.execute(
            f"SELECT DISTINCT name_culture FROM watchlist_entries {culture_where}",
            params,
        ) as cur:
            cultures = [r[0] for r in await cur.fetchall()]
        if cultures:
            per_culture = max(2, (count * 10) // len(cultures))
            all_rows = []
            for culture in cultures:
                c_conditions = conditions + ["name_culture = ?"]
                c_params = params + [culture]
                c_where = "WHERE " + " AND ".join(c_conditions)
                async with db.execute(
                    f"{_SELECT} {c_where} ORDER BY RANDOM() LIMIT ?",
                    c_params + [per_culture],
                ) as cur:
                    all_rows.extend([dict(r) for r in await cur.fetchall()])
            return all_rows

    # ── Weighted / fallback: random sample from full candidate pool ───────────
    async with db.execute(
        f"SELECT COUNT(*) FROM watchlist_entries {where}", params
    ) as cur:
        total = (await cur.fetchone())[0]

    if total == 0:
        return []

    sample_n = min(count * 10, total)
    async with db.execute(
        f"{_SELECT} {where} ORDER BY RANDOM() LIMIT ?",
        params + [sample_n],
    ) as cur:
        rows = await cur.fetchall()

    return [dict(r) for r in rows]


# ── Main generation entry point ────────────────────────────────────────────────

async def generate_test_cases(request: GenerationRequest, db: aiosqlite.Connection) -> dict:
    """
    Generate test cases for the requested type_ids (built-in and custom).
    Stores results in the test_cases table.
    Returns a summary dict.
    """
    # Merge built-in CSV types + custom DB types
    # Keep first occurrence of each type_id (CSV may have duplicate type_ids for multi-theme reuse)
    builtin_types = {}
    for t in load_test_case_types():
        if t.type_id not in builtin_types:
            builtin_types[t.type_id] = t
    custom_types = {t.type_id: t for t in await load_custom_types(db)}
    all_types = {**builtin_types, **custom_types}

    meta_by_id = {tid: _get_type_meta(tid) for tid in request.type_ids if not tid.startswith('USER')}

    rng = random.Random()  # Seeded per-run for reproducibility within session

    summary = {
        'generated': 0,
        'skipped': 0,
        'by_type': {},
        'skip_reasons': {},
    }

    rows_to_insert: list[tuple] = []

    for type_id in request.type_ids:
        if type_id not in all_types:
            continue

        type_def = all_types[type_id]

        # Resolve variation function: built-in dispatch table or custom lambda
        if type_id.startswith('USER'):
            lambda_str = await get_custom_lambda(type_id, db)
            if not lambda_str:
                _ets = ['individual', 'entity', 'vessel', 'aircraft', 'country', 'unknown']
                summary['by_type'][type_id] = {'generated': 0, 'skipped': 0, 'by_entity_type': {et: {'generated': 0, 'skipped': 0, 'reason': 'no_variation_function'} for et in _ets}}
                continue
            var_fn = _make_lambda_fn(lambda_str)
            expected_outcome = type_def.expected_outcome
            meta = {}
        else:
            var_fn = VARIATION_FUNCTIONS.get(type_id)
            if var_fn is None:
                _ets = ['individual', 'entity', 'vessel', 'aircraft', 'country', 'unknown']
                summary['by_type'][type_id] = {'generated': 0, 'skipped': 0, 'by_entity_type': {et: {'generated': 0, 'skipped': 0, 'reason': 'no_variation_function'} for et in _ets}}
                continue
            meta = meta_by_id.get(type_id, {})
            expected_outcome = type_def.expected_outcome

        entity_overrides = (request.outcome_overrides or {}).get(type_id, {})

        # Iterate each applicable entity type separately so count_per_type
        # applies per entity type, not across all entity types combined.
        _all_ets = ['individual', 'entity', 'vessel', 'aircraft', 'country', 'unknown']
        ets_to_sample = type_def.applicable_entity_types if type_def.applicable_entity_types else _all_ets
        applicable_set = set(ets_to_sample)

        type_count = 0
        type_skips = 0
        type_et_results = {}

        # Pre-populate non-applicable entity types in the log
        for et in _all_ets:
            if et not in applicable_set:
                type_et_results[et] = {'generated': 0, 'skipped': 0, 'reason': 'not_applicable'}

        for et in ets_to_sample:
            candidates = await _sample_names(
                applicable_entity_types=[et],
                min_tokens=type_def.applicable_min_tokens,
                min_name_length=type_def.applicable_min_name_length,
                count=request.count_per_type,
                db=db,
                distribution=request.culture_distribution,
                custom_dist=request.custom_distribution,
                primary_aka_filter=PRIMARY_AKA_FILTER_MAP.get(type_id),
                watchlists=request.watchlists or None,
            )

            if not candidates:
                type_et_results[et] = {'generated': 0, 'skipped': 0, 'reason': 'no_watchlist_data'}
                continue

            final_outcome = entity_overrides.get(et, expected_outcome) if entity_overrides else expected_outcome
            expected_result, rationale_prefix = outcome_to_result(final_outcome)

            et_count = 0
            et_skips = 0
            for record in candidates:
                if et_count >= request.count_per_type:
                    break

                name = record['cleaned_name']
                try:
                    test_name, rationale_suffix = var_fn(name, record, rng)
                except Exception as exc:
                    test_name = None
                    rationale_suffix = f"error: {exc}"

                same_name = test_name is not None and test_name.strip() == name.strip()
                if test_name is None or (same_name and type_id not in PASSTHROUGH_TYPES):
                    et_skips += 1
                    type_skips += 1
                    reason = rationale_suffix
                    summary['skip_reasons'][reason] = summary['skip_reasons'].get(reason, 0) + 1
                    continue

                test_name = test_name.strip()
                tc_id = f"{type_id}_{str(uuid.uuid4())[:8]}"
                full_rationale = f"{rationale_prefix} — {rationale_suffix}"

                rows_to_insert.append((
                    tc_id,
                    f"{meta.get('type_name', type_id)} ({type_id})",
                    record['watchlist'],
                    record.get('sub_watchlist_1'),
                    record['cleaned_name'],
                    record['original_name'],
                    record.get('name_culture'),
                    test_name,
                    record['primary_aka'],
                    record['entity_type'],
                    len(test_name.split()),
                    len(test_name),
                    expected_result,
                    full_rationale,
                ))
                et_count += 1
                type_count += 1

            if et_count == 0:
                type_et_results[et] = {
                    'generated': 0,
                    'skipped': et_skips,
                    'reason': 'all_names_skipped' if et_skips > 0 else 'no_watchlist_data',
                }
            else:
                type_et_results[et] = {'generated': et_count, 'skipped': et_skips}

        summary['by_type'][type_id] = {
            'generated': type_count,
            'skipped': type_skips,
            'by_entity_type': type_et_results,
        }
        summary['generated'] += type_count
        summary['skipped'] += type_skips

    if rows_to_insert:
        await db.executemany(
            """INSERT OR IGNORE INTO test_cases
               (test_case_id, test_case_type, watchlist, sub_watchlist,
                cleaned_original_name, original_original_name, culture_nationality,
                test_name, primary_aka, entity_type, num_tokens, name_length,
                expected_result, expected_result_rationale)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows_to_insert,
        )
        await db.commit()

    return summary
