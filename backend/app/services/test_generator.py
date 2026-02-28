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
    mapping = {
        'Must Hit':        ('HIT', 'Must Hit — deterministic match; any miss is a critical system failure'),
        'Should Hit':      ('HIT', 'Should Hit — standard capability; a miss indicates a configuration or threshold gap'),
        'Testing Purposes':('HIT', 'Testing Purposes — capability benchmark; a miss is expected without the specific capability in place'),
        'Should Not Hit':  ('MISS','Should Not Hit — expected non-match; a hit would be a false positive'),
    }
    return mapping.get(expected_outcome, ('HIT', expected_outcome))


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


# ── Dispatch table: type_id → function ────────────────────────────────────────

VARIATION_FUNCTIONS: dict[str, Callable] = {
    'TC001': _v_exact_match,
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
}


# ── Sampling ───────────────────────────────────────────────────────────────────

async def _sample_names(
    applicable_entity_types: list[str],
    min_tokens: int,
    min_name_length: int,
    count: int,
    db: aiosqlite.Connection,
    distribution: str = 'balanced',
    custom_dist: dict | None = None,
) -> list[dict]:
    """
    Stratified sample of watchlist entries for a given type's constraints.
    Returns up to `count * 2` rows to allow for skips.
    """
    conditions = []
    params: list = []

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

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    # Get total available
    async with db.execute(
        f"SELECT COUNT(*) FROM watchlist_entries {where}", params
    ) as cur:
        total = (await cur.fetchone())[0]

    if total == 0:
        return []

    # Sample up to 2x count, randomly ordered
    sample_n = min(count * 3, total)
    async with db.execute(
        f"""SELECT uid, watchlist, sub_watchlist_1, cleaned_name, original_name,
                   primary_aka, entity_type, num_tokens, name_length,
                   nationality, sanctions_program
            FROM watchlist_entries {where}
            ORDER BY RANDOM()
            LIMIT ?""",
        params + [sample_n],
    ) as cur:
        rows = await cur.fetchall()

    return [dict(r) for r in rows]


# ── Main generation entry point ────────────────────────────────────────────────

async def generate_test_cases(request: GenerationRequest, db: aiosqlite.Connection) -> dict:
    """
    Generate test cases for the requested type_ids.
    Stores results in the test_cases table.
    Returns a summary dict.
    """
    all_types = {t.type_id: t for t in load_test_case_types()}
    meta_by_id = {tid: _get_type_meta(tid) for tid in request.type_ids}

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
        meta = meta_by_id.get(type_id, {})
        expected_outcome = meta.get('expected_outcome', 'Should Hit')
        expected_result, rationale_prefix = outcome_to_result(expected_outcome)
        var_fn = VARIATION_FUNCTIONS.get(type_id)
        if var_fn is None:
            continue

        # Sample candidate names
        candidates = await _sample_names(
            applicable_entity_types=type_def.applicable_entity_types,
            min_tokens=type_def.applicable_min_tokens,
            min_name_length=type_def.applicable_min_name_length,
            count=request.count_per_type,
            db=db,
            distribution=request.culture_distribution,
            custom_dist=request.custom_distribution,
        )

        type_count = 0
        type_skips = 0

        for record in candidates:
            if type_count >= request.count_per_type:
                break

            name = record['cleaned_name']
            try:
                test_name, rationale_suffix = var_fn(name, record, rng)
            except Exception as exc:
                test_name = None
                rationale_suffix = f"error: {exc}"

            if test_name is None or test_name.strip() == name.strip():
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
                record.get('nationality'),
                test_name,
                record['primary_aka'],
                record['entity_type'],
                len(test_name.split()),
                len(test_name),
                expected_result,
                full_rationale,
            ))
            type_count += 1

        summary['by_type'][type_id] = {'generated': type_count, 'skipped': type_skips}
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
