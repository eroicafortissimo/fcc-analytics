"""
list_cleaner.py
Normalizes raw entry dicts from the parsers, then upserts into SQLite.
Also provides read helpers for the /entries and /summary endpoints.
"""
from __future__ import annotations

import re
import unicodedata
from datetime import date, datetime, timezone, timedelta
from typing import Any
import aiosqlite

from app.models.schemas import ListFilters, WatchlistSummary


# ── Culture Classification ─────────────────────────────────────────────────────
# Maps nationality adjective → (region, name_culture)

_CULTURE_MAP: dict[str, tuple[str, str]] = {
    # East Asian
    "Chinese":      ("East Asian", "Chinese"),
    "Taiwanese":    ("East Asian", "Chinese"),
    "Japanese":     ("East Asian", "Japanese"),
    "Korean":       ("East Asian", "Korean"),
    "North Korean": ("East Asian", "Korean"),
    "Vietnamese":   ("East Asian", "Vietnamese"),
    # South & Southeast Asian
    "Indian":       ("South & Southeast Asian", "Indian/South Asian"),
    "Sri Lankan":   ("South & Southeast Asian", "Indian/South Asian"),
    "Nepali":       ("South & Southeast Asian", "Indian/South Asian"),
    "Bangladeshi":  ("South & Southeast Asian", "Pakistani/Bangladeshi"),
    "Pakistani":    ("South & Southeast Asian", "Pakistani/Bangladeshi"),
    "Indonesian":   ("South & Southeast Asian", "Indonesian/Malay"),
    "Malaysian":    ("South & Southeast Asian", "Indonesian/Malay"),
    "Singaporean":  ("South & Southeast Asian", "Indonesian/Malay"),
    "Filipino":     ("South & Southeast Asian", "Filipino"),
    "Thai":         ("South & Southeast Asian", "Indonesian/Malay"),
    "Cambodian":    ("South & Southeast Asian", "Indonesian/Malay"),
    "Laotian":      ("South & Southeast Asian", "Indonesian/Malay"),
    "Burmese":      ("South & Southeast Asian", "Indian/South Asian"),
    "Timorese":     ("South & Southeast Asian", "Indonesian/Malay"),
    # Middle Eastern & North African
    "Iraqi":        ("Middle Eastern & North African", "Arabic"),
    "Syrian":       ("Middle Eastern & North African", "Arabic"),
    "Lebanese":     ("Middle Eastern & North African", "Arabic"),
    "Jordanian":    ("Middle Eastern & North African", "Arabic"),
    "Saudi":        ("Middle Eastern & North African", "Arabic"),
    "Emirati":      ("Middle Eastern & North African", "Arabic"),
    "Kuwaiti":      ("Middle Eastern & North African", "Arabic"),
    "Qatari":       ("Middle Eastern & North African", "Arabic"),
    "Bahraini":     ("Middle Eastern & North African", "Arabic"),
    "Omani":        ("Middle Eastern & North African", "Arabic"),
    "Yemeni":       ("Middle Eastern & North African", "Arabic"),
    "Egyptian":     ("Middle Eastern & North African", "Arabic"),
    "Libyan":       ("Middle Eastern & North African", "Arabic"),
    "Tunisian":     ("Middle Eastern & North African", "Arabic"),
    "Moroccan":     ("Middle Eastern & North African", "Arabic"),
    "Algerian":     ("Middle Eastern & North African", "Arabic"),
    "Sudanese":     ("Middle Eastern & North African", "Arabic"),
    "Palestinian":  ("Middle Eastern & North African", "Arabic"),
    "Iranian":      ("Middle Eastern & North African", "Persian/Farsi"),
    "Turkish":      ("Middle Eastern & North African", "Turkish"),
    "Israeli":      ("Middle Eastern & North African", "Hebrew/Israeli"),
    # Sub-Saharan African — West
    "Nigerian":       ("Sub-Saharan African", "West African"),
    "Ghanaian":       ("Sub-Saharan African", "West African"),
    "Senegalese":     ("Sub-Saharan African", "West African"),
    "Guinean":        ("Sub-Saharan African", "West African"),
    "Ivorian":        ("Sub-Saharan African", "West African"),
    "Malian":         ("Sub-Saharan African", "West African"),
    "Burkinabe":      ("Sub-Saharan African", "West African"),
    "Nigerien":       ("Sub-Saharan African", "West African"),
    "Cameroonian":    ("Sub-Saharan African", "West African"),
    "Togolese":       ("Sub-Saharan African", "West African"),
    "Beninese":       ("Sub-Saharan African", "West African"),
    "Sierra Leonean": ("Sub-Saharan African", "West African"),
    "Liberian":       ("Sub-Saharan African", "West African"),
    "Gambian":        ("Sub-Saharan African", "West African"),
    "Congolese":      ("Sub-Saharan African", "West African"),
    "Central African":("Sub-Saharan African", "West African"),
    "Chadian":        ("Sub-Saharan African", "West African"),
    # Sub-Saharan African — East
    "Kenyan":         ("Sub-Saharan African", "East African"),
    "Ethiopian":      ("Sub-Saharan African", "East African"),
    "Somali":         ("Sub-Saharan African", "East African"),
    "Tanzanian":      ("Sub-Saharan African", "East African"),
    "Ugandan":        ("Sub-Saharan African", "East African"),
    "Rwandan":        ("Sub-Saharan African", "East African"),
    "Burundian":      ("Sub-Saharan African", "East African"),
    "Eritrean":       ("Sub-Saharan African", "East African"),
    "Djiboutian":     ("Sub-Saharan African", "East African"),
    "Comorian":       ("Sub-Saharan African", "East African"),
    "Malagasy":       ("Sub-Saharan African", "East African"),
    "South Sudanese": ("Sub-Saharan African", "East African"),
    # Sub-Saharan African — Southern
    "South African":  ("Sub-Saharan African", "Southern African"),
    "Zimbabwean":     ("Sub-Saharan African", "Southern African"),
    "Zambian":        ("Sub-Saharan African", "Southern African"),
    "Malawian":       ("Sub-Saharan African", "Southern African"),
    "Mozambican":     ("Sub-Saharan African", "Southern African"),
    "Botswanan":      ("Sub-Saharan African", "Southern African"),
    "Namibian":       ("Sub-Saharan African", "Southern African"),
    "Angolan":        ("Sub-Saharan African", "Southern African"),
    # Western — Anglo/Germanic
    "Australian":  ("Western", "Anglo/Germanic"),
    "American":    ("Western", "Anglo/Germanic"),
    "British":     ("Western", "Anglo/Germanic"),
    "German":      ("Western", "Anglo/Germanic"),
    "Austrian":    ("Western", "Anglo/Germanic"),
    "Dutch":       ("Western", "Anglo/Germanic"),
    "Belgian":     ("Western", "Anglo/Germanic"),
    "Swiss":       ("Western", "Anglo/Germanic"),
    # Western — Hispanic/Latino
    "Mexican":     ("Western", "Hispanic/Latino"),
    "Colombian":   ("Western", "Hispanic/Latino"),
    "Venezuelan":  ("Western", "Hispanic/Latino"),
    "Ecuadorian":  ("Western", "Hispanic/Latino"),
    "Peruvian":    ("Western", "Hispanic/Latino"),
    "Bolivian":    ("Western", "Hispanic/Latino"),
    "Chilean":     ("Western", "Hispanic/Latino"),
    "Argentine":   ("Western", "Hispanic/Latino"),
    "Guatemalan":  ("Western", "Hispanic/Latino"),
    "Honduran":    ("Western", "Hispanic/Latino"),
    "Salvadoran":  ("Western", "Hispanic/Latino"),
    "Costa Rican": ("Western", "Hispanic/Latino"),
    "Nicaraguan":  ("Western", "Hispanic/Latino"),
    "Panamanian":  ("Western", "Hispanic/Latino"),
    "Cuban":       ("Western", "Hispanic/Latino"),
    "Dominican":   ("Western", "Hispanic/Latino"),
    "Paraguayan":  ("Western", "Hispanic/Latino"),
    "Uruguayan":   ("Western", "Hispanic/Latino"),
    "Belizean":    ("Western", "Hispanic/Latino"),
    # Western — Romance
    "French":     ("Western", "Romance"),
    "Italian":    ("Western", "Romance"),
    "Spanish":    ("Western", "Romance"),
    "Romanian":   ("Western", "Romance"),
    "Moldovan":   ("Western", "Romance"),
    "Brazilian":  ("Western", "Romance"),
    "Haitian":    ("Western", "Romance"),
    "Portuguese": ("Western", "Romance"),
    "Greek":      ("Western", "Romance"),
    "Cypriot":    ("Western", "Romance"),
    "Maltese":    ("Western", "Romance"),
    # Western — Slavic/Eastern European
    "Russian":         ("Western", "Slavic/Eastern European"),
    "Ukrainian":       ("Western", "Slavic/Eastern European"),
    "Belarusian":      ("Western", "Slavic/Eastern European"),
    "Polish":          ("Western", "Slavic/Eastern European"),
    "Czech":           ("Western", "Slavic/Eastern European"),
    "Serbian":         ("Western", "Slavic/Eastern European"),
    "Croatian":        ("Western", "Slavic/Eastern European"),
    "Bosnian":         ("Western", "Slavic/Eastern European"),
    "Bulgarian":       ("Western", "Slavic/Eastern European"),
    "Macedonian":      ("Western", "Slavic/Eastern European"),
    "Montenegrin":     ("Western", "Slavic/Eastern European"),
    "Albanian":        ("Western", "Slavic/Eastern European"),
    "Slovak":          ("Western", "Slavic/Eastern European"),
    "Kosovar":         ("Western", "Slavic/Eastern European"),
    "Hungarian":       ("Western", "Slavic/Eastern European"),
    "Lithuanian":      ("Western", "Slavic/Eastern European"),
    "Latvian":         ("Western", "Slavic/Eastern European"),
    # Other — Central Asian
    "Kazakhstani": ("Other", "Central Asian"),
    "Uzbek":       ("Other", "Central Asian"),
    "Kyrgyz":      ("Other", "Central Asian"),
    "Tajik":       ("Other", "Central Asian"),
    "Turkmen":     ("Other", "Central Asian"),
    "Afghan":      ("Other", "Central Asian"),
    "Azerbaijani": ("Other", "Central Asian"),
    "Armenian":    ("Other", "Central Asian"),
    "Georgian":    ("Other", "Central Asian"),
    "Mongolian":   ("Other", "Central Asian"),
    # Other — Nordic
    "Swedish":   ("Other", "Nordic"),
    "Norwegian": ("Other", "Nordic"),
    "Finnish":   ("Other", "Nordic"),
    "Danish":    ("Other", "Nordic"),
    "Icelandic": ("Other", "Nordic"),
    "Estonian":  ("Other", "Nordic"),
    # Other — South American Indigenous (mapped from rarer nationalities)
    "Guyanese":    ("Other", "South American Indigenous"),
    "Surinamese":  ("Other", "South American Indigenous"),
}


def _culture_from_nationality(nationality: str | None) -> tuple[str, str, str] | None:
    """Look up (region, name_culture, confidence) from nationality adjective."""
    if not nationality:
        return None
    key = nationality.strip()
    if key in _CULTURE_MAP:
        return (*_CULTURE_MAP[key], "High")
    # Case-insensitive fallback
    key_lower = key.lower()
    for k, v in _CULTURE_MAP.items():
        if k.lower() == key_lower:
            return (*v, "High")
    return None


def _culture_from_name(name: str) -> tuple[str, str, str] | None:
    """
    Heuristic: infer (region, name_culture, confidence) from name text.
    Checks Unicode script first, then Latin-script name patterns.
    Returns None only when truly ambiguous.
    """
    if not name:
        return None

    # ── Script detection ───────────────────────────────────────────────────────
    # Arabic script (U+0600–U+06FF)
    if any('\u0600' <= ch <= '\u06ff' for ch in name):
        return ("Middle Eastern & North African", "Arabic", "Medium")
    # Cyrillic (U+0400–U+04FF)
    if any('\u0400' <= ch <= '\u04ff' for ch in name):
        return ("Western", "Slavic/Eastern European", "Medium")
    # CJK Unified Ideographs — most likely Chinese but could be Japanese
    if any('\u4e00' <= ch <= '\u9fff' or '\u3400' <= ch <= '\u4dbf' for ch in name):
        return ("East Asian", "Chinese", "Medium")
    # Korean Hangul
    if any('\uac00' <= ch <= '\ud7af' or '\u1100' <= ch <= '\u11ff' for ch in name):
        return ("East Asian", "Korean", "Medium")
    # Japanese Hiragana / Katakana
    if any('\u3040' <= ch <= '\u30ff' for ch in name):
        return ("East Asian", "Japanese", "Medium")
    # Devanagari (Hindi, Nepali, Sanskrit)
    if any('\u0900' <= ch <= '\u097f' for ch in name):
        return ("South & Southeast Asian", "Indian/South Asian", "Medium")
    # Persian/Urdu extension (U+FB50–U+FDFF, U+FE70–U+FEFF)
    if any('\ufb50' <= ch <= '\ufdff' or '\ufe70' <= ch <= '\ufeff' for ch in name):
        return ("Middle Eastern & North African", "Persian/Farsi", "Medium")

    # ── Latin-script heuristics ────────────────────────────────────────────────
    n = name.strip().lower()
    words = n.split()

    # Arabic patterns: al-, abu-, abd-, bin, binte, ibn, umm, etc.
    arabic_triggers = {
        'al', 'abu', 'abd', 'abdal', 'abdu', 'abdul', 'ibn', 'bin', 'binte',
        'umm', 'um', 'haj', 'hajj', 'sheikh', 'shaikh', 'shaykh', 'sheik',
        'syed', 'sayyid',
        # Common Arabic given names
        'muhammad', 'mohammed', 'mohammad', 'mohamad', 'muhamad', 'muhammed',
        'ahmad', 'ahmed', 'ali', 'hassan', 'husayn', 'hussein', 'hasan',
        'omar', 'umar', 'osman', 'uthman', 'ibrahim', 'ismail', 'isma\'il',
        'nayif', 'nayef', 'talal', 'subhi', 'taysir', 'marwan', 'khaled',
        'khalid', 'yusuf', 'yousuf', 'saleh', 'salih', 'faris', 'samir',
        'mustafa', 'musab', 'jabril', 'jibril', 'zaydan', 'hawatma', 'fadlallah',
        'yassin', 'yassine', 'atef', 'atif', 'mullah', 'maulana',
    }
    if words and words[0] in arabic_triggers:
        return ("Middle Eastern & North African", "Arabic", "Medium")
    if any(w in arabic_triggers for w in words):
        return ("Middle Eastern & North African", "Arabic", "Medium")
    if any(n.startswith(pfx) for pfx in ('al-', 'abu ', 'abd ', 'ibn ', 'bin ')):
        return ("Middle Eastern & North African", "Arabic", "Medium")
    if any(sfx in n for sfx in ('ullah', 'uddin', 'ul-haq', 'al-haj', 'allah ')):
        return ("Middle Eastern & North African", "Arabic", "Medium")

    # South Asian: khan, begum, bibi, mirza, chaudhry/choudry, akhtar, etc.
    south_asian_words = {'khan', 'begum', 'bibi', 'mirza', 'chaudhry', 'choudry', 'akhtar',
                          'sheikh', 'malik', 'rana', 'raja', 'baig', 'beg'}
    if any(w in south_asian_words for w in words):
        return ("South & Southeast Asian", "Pakistani/Bangladeshi", "Medium")

    # Hispanic given names (common in Latin America)
    hispanic_given = {
        'jesus', 'jose', 'juan', 'carlos', 'luis', 'jorge', 'miguel', 'pedro',
        'antonio', 'francisco', 'manuel', 'rafael', 'alejandro', 'sergio',
        'roberto', 'fernando', 'hector', 'mario', 'victor', 'pablo', 'jesus',
        'maria', 'rosa', 'isabel', 'guadalupe', 'patricia', 'ana', 'carmen',
        'amezcua', 'contreras', 'hernandez', 'gonzalez', 'rodriguez', 'garcia',
        'martinez', 'lopez', 'ramirez', 'torres', 'flores', 'rivera', 'chavez',
        'moreno', 'romero', 'jimenez', 'sanchez', 'ruiz', 'gutierrez', 'perez',
    }
    if any(w in hispanic_given for w in words):
        return ("Western", "Hispanic/Latino", "Medium")

    # Slavic suffixes: -ov/-ev/-ova/-eva/-ski/-sky/-ski/-vich/-ich/-enko
    slavic_sfx = ('ov', 'ev', 'ova', 'eva', 'ski', 'sky', 'skiy', 'vich', 'vich',
                   'enko', 'chuk', 'yuk', 'iuk', 'uk', 'enko', 'ko')
    last_word = words[-1] if words else ''
    if any(last_word.endswith(s) for s in slavic_sfx):
        return ("Western", "Slavic/Eastern European", "Medium")

    # Romanian: -escu, -anu, -eanu, -aru
    if any(last_word.endswith(s) for s in ('escu', 'anu', 'eanu', 'aru')):
        return ("Western", "Romance", "Medium")

    # Armenian: -ian, -yan  (also common in Iranian)
    if last_word.endswith(('ian', 'yan', 'jan')):
        return ("Middle Eastern & North African", "Persian/Farsi", "Medium")

    # Central Asian: -baev, -ov, -ev already caught; also -zadeh, -pour/-pour
    if any(last_word.endswith(s) for s in ('zadeh', 'pour', 'poor', 'nejad', 'nezhad', 'far', 'rad')):
        return ("Middle Eastern & North African", "Persian/Farsi", "Medium")
    if any(last_word.endswith(s) for s in ('baev', 'bekov', 'bekova', 'bekov')):
        return ("Other", "Central Asian", "Medium")

    # Hispanic: de, del, la, el, don, doña, -ez, -ez ending
    hispanic_particles = {'de', 'del', 'la', 'el', 'don', 'dona', 'los', 'las'}
    if len(words) > 1 and any(w in hispanic_particles for w in words[:-1]):
        return ("Western", "Hispanic/Latino", "Medium")
    if last_word.endswith('ez') or last_word.endswith('es'):
        return ("Western", "Hispanic/Latino", "Medium")

    # East Asian romanized: common endings / patterns
    east_asian_sfx = ('jung', 'young', 'yang', 'kong', 'hong', 'ming', 'ping', 'ling',
                       'wei', 'fei', 'zhi', 'qing', 'bin', 'jun', 'lei', 'xia')
    if len(words) <= 3 and any(w in east_asian_sfx or w.endswith(sfx)
                                for w in words for sfx in east_asian_sfx):
        return ("East Asian", "Chinese", "Low")

    # African patterns: common West/East African name suffixes
    west_african_sfx = ('diallo', 'koné', 'kone', 'traoré', 'traore', 'coulibaly',
                         'ouédraogo', 'ouedraogo', 'bah', 'konaté', 'konate', 'keita',
                         'cissé', 'cisse', 'fofana', 'camara', 'toure', 'touré',
                         'diabate', 'diabaté', 'dembele', 'dembélé')
    if n in west_african_sfx or last_word in west_african_sfx or any(w in west_african_sfx for w in words):
        return ("Sub-Saharan African", "West African", "Medium")

    # ── Entity-name keyword matching ───────────────────────────────────────────
    # Legal entity suffixes that imply a country/region
    legal_sfx_map = {
        # Russian legal forms
        'ooo': ("Western", "Slavic/Eastern European"),
        'zao': ("Western", "Slavic/Eastern European"),
        'pao': ("Western", "Slavic/Eastern European"),
        # French legal forms
        'sarl': ("Western", "Romance"),
        'sas':  ("Western", "Romance"),
        'sca':  ("Western", "Romance"),
        # German legal forms
        'gmbh': ("Western", "Anglo/Germanic"),
        'ag':   ("Western", "Anglo/Germanic"),
        'kg':   ("Western", "Anglo/Germanic"),
        # Italian legal forms
        'spa':  ("Western", "Romance"),
        'srl':  ("Western", "Romance"),
        # Spanish/Portuguese legal forms
        'sa':   ("Western", "Hispanic/Latino"),
        'sl':   ("Western", "Hispanic/Latino"),
        'ltda': ("Western", "Hispanic/Latino"),
        # Nordic
        'ab':   ("Other", "Nordic"),
        'oy':   ("Other", "Nordic"),
        # UAE Free Zone
        'fze':  ("Middle Eastern & North African", "Arabic"),
        'fzc':  ("Middle Eastern & North African", "Arabic"),
        'fzco': ("Middle Eastern & North African", "Arabic"),
        'llc':  ("Middle Eastern & North African", "Arabic"),  # common in UAE/Gulf
    }
    if last_word.rstrip('.') in legal_sfx_map and len(words) >= 2:
        region, cult = legal_sfx_map[last_word.rstrip('.')]
        return (region, cult, "Low")

    # "Pvt" / "Pvt Ltd" → South Asian
    if 'pvt' in words or 'pvt.' in words:
        return ("South & Southeast Asian", "Pakistani/Bangladeshi", "Low")

    # Country/region keyword scan in the full name
    keyword_map: list[tuple[tuple, tuple[str, str]]] = [
        (('russia', 'russian', 'rossiya', ' rus ', 'mosco', 'russe'),
         ("Western", "Slavic/Eastern European")),
        (('china', 'chinese', 'sino', 'zhong', 'beijing', 'shanghai', 'shenzhen',
          'guangzhou', 'hong kong', 'taiwan'),
         ("East Asian", "Chinese")),
        (('korea', 'korean', 'dprk', 'north korea'),
         ("East Asian", "Korean")),
        (('japan', 'japanese', 'nippon', 'nikkei'),
         ("East Asian", "Japanese")),
        (('iran', 'iranian', 'persia', 'shahid', 'mohandesi', 'toseh', 'sepah',
          'sazeh', 'melli', 'mellat', 'tejarat', 'parsian', 'arya', 'keyhan'),
         ("Middle Eastern & North African", "Persian/Farsi")),
        (('iraq', 'iraqi', 'baghdad', 'basra'),
         ("Middle Eastern & North African", "Arabic")),
        (('syria', 'syrian', 'damascus'),
         ("Middle Eastern & North African", "Arabic")),
        (('turkey', 'turkish', 'turkiye', 'ankara', 'istanbul'),
         ("Middle Eastern & North African", "Turkish")),
        (('lebanon', 'lebanese', 'beirut', 'hizbullah', 'hezbollah'),
         ("Middle Eastern & North African", "Arabic")),
        (('israel', 'israeli', 'tel aviv'),
         ("Middle Eastern & North African", "Hebrew/Israeli")),
        (('india', 'indian', 'mumbai', 'delhi', 'bangalore', 'kolkata'),
         ("South & Southeast Asian", "Indian/South Asian")),
        (('pakistan', 'pakistani', 'karachi', 'lahore', 'islamabad'),
         ("South & Southeast Asian", "Pakistani/Bangladeshi")),
        (('bangladesh', 'bangladeshi', 'dhaka'),
         ("South & Southeast Asian", "Pakistani/Bangladeshi")),
        (('nigeria', 'nigerian', 'lagos', 'abuja'),
         ("Sub-Saharan African", "West African")),
        (('venezuela', 'venezuelan', 'caracas'),
         ("Western", "Hispanic/Latino")),
        (('colombia', 'colombian', 'bogota'),
         ("Western", "Hispanic/Latino")),
        (('mexico', 'mexican'),
         ("Western", "Hispanic/Latino")),
        (('france', 'french', 'paris'),
         ("Western", "Romance")),
        (('germany', 'german', 'berlin', 'munich'),
         ("Western", "Anglo/Germanic")),
        (('ukraine', 'ukrainian', 'kyiv', 'kharkiv'),
         ("Western", "Slavic/Eastern European")),
        (('belarus', 'belarusian', 'minsk'),
         ("Western", "Slavic/Eastern European")),
        (('middle east', 'gulf', 'arabian', 'khalij'),
         ("Middle Eastern & North African", "Arabic")),
        (('afghanistan', 'afghan', 'kabul'),
         ("Other", "Central Asian")),
        (('myanmar', 'burma', 'yangon'),
         ("South & Southeast Asian", "Indonesian/Malay")),
        (('vietnam', 'viet', 'hanoi', 'ho chi'),
         ("East Asian", "Vietnamese")),
    ]
    for keywords, (region, cult) in keyword_map:
        if any(kw in n for kw in keywords):
            return (region, cult, "Medium")

    return None


_PROGRAM_CULTURE: dict[str, tuple[str, str]] = {
    # OFAC program codes → (region, name_culture)
    "IRAN":       ("Middle Eastern & North African", "Persian/Farsi"),
    "IFSR":       ("Middle Eastern & North African", "Persian/Farsi"),  # Iran freedom
    "IRAN-TRA":   ("Middle Eastern & North African", "Persian/Farsi"),
    "IRAN-EO13599":("Middle Eastern & North African", "Persian/Farsi"),
    "NPWMD":      ("Middle Eastern & North African", "Persian/Farsi"),  # WMD mostly Iran
    "HRIT-IR":    ("Middle Eastern & North African", "Persian/Farsi"),
    "SYRIA":      ("Middle Eastern & North African", "Arabic"),
    "SYRIA2":     ("Middle Eastern & North African", "Arabic"),
    "IRQ":        ("Middle Eastern & North African", "Arabic"),
    "IRAQ2":      ("Middle Eastern & North African", "Arabic"),
    "IRAQ3":      ("Middle Eastern & North African", "Arabic"),
    "SDGT":       ("Middle Eastern & North African", "Arabic"),  # mostly terrorism/Middle East
    "DPRK":       ("East Asian", "Korean"),
    "DPRK2":      ("East Asian", "Korean"),
    "DPRK3":      ("East Asian", "Korean"),
    "DPRK4":      ("East Asian", "Korean"),
    "DPRK-EO13722":("East Asian", "Korean"),
    "CUBA":       ("Western", "Hispanic/Latino"),
    "VENEZUELA":  ("Western", "Hispanic/Latino"),
    "VENEZUELA2": ("Western", "Hispanic/Latino"),
    "RUSSIA-EO14024":("Western", "Slavic/Eastern European"),
    "UKRAINE-EO13662":("Western", "Slavic/Eastern European"),
    "UKRAINE-EO13685":("Western", "Slavic/Eastern European"),
    "BELARUS-EO14038":("Western", "Slavic/Eastern European"),
    "LIBYA":      ("Middle Eastern & North African", "Arabic"),
    "LIBYA2":     ("Middle Eastern & North African", "Arabic"),
    "SOMALIA":    ("Sub-Saharan African", "East African"),
    "AL-SHABAAB": ("Sub-Saharan African", "East African"),
    "MALI":       ("Sub-Saharan African", "West African"),
    "SUDAN":      ("Middle Eastern & North African", "Arabic"),
    "ZIMBABWE":   ("Sub-Saharan African", "Southern African"),
    "MYANMAR":    ("South & Southeast Asian", "Indonesian/Malay"),
    "BURMA":      ("South & Southeast Asian", "Indian/South Asian"),
    "SOUTH SUDAN":("Sub-Saharan African", "East African"),
    "NICARAGUA":  ("Western", "Hispanic/Latino"),
    "BELARUS":    ("Western", "Slavic/Eastern European"),
    # EU programme codes
    "IRQ":        ("Middle Eastern & North African", "Arabic"),
    "SY":         ("Middle Eastern & North African", "Arabic"),
    "RU":         ("Western", "Slavic/Eastern European"),
    "BY":         ("Western", "Slavic/Eastern European"),
    "KP":         ("East Asian", "Korean"),
    "IR":         ("Middle Eastern & North African", "Persian/Farsi"),
    "LY":         ("Middle Eastern & North African", "Arabic"),
    "TN":         ("Middle Eastern & North African", "Arabic"),
    "LB":         ("Middle Eastern & North African", "Arabic"),
    "YE":         ("Middle Eastern & North African", "Arabic"),
}


def _culture_from_program(program: str | None) -> tuple[str, str, str] | None:
    """Infer culture from OFAC/EU sanctions program code."""
    if not program:
        return None
    # Try each semicolon-separated sub-program
    for part in program.split(";"):
        p = part.strip().upper()
        if p in _PROGRAM_CULTURE:
            return (*_PROGRAM_CULTURE[p], "Medium")
        # Prefix match (e.g. "IRAN-EO13599" → starts with "IRAN")
        for key, val in _PROGRAM_CULTURE.items():
            if p.startswith(key):
                return (*val, "Medium")
    return None


def get_culture(
    nationality: str | None,
    name: str,
    sanctions_program: str | None = None,
) -> tuple[str | None, str | None, str | None]:
    """
    Returns (region, name_culture, culture_confidence).
    Tier 1: nationality lookup (High)
    Tier 2: name heuristic (Medium/Low)
    Tier 3: sanctions program code (Medium)
    Tier 4: None — caller should use Claude fallback
    """
    result = _culture_from_nationality(nationality)
    if result:
        return result
    result = _culture_from_name(name)
    if result:
        return result
    result = _culture_from_program(sanctions_program)
    if result:
        return result
    return None, None, None


# ── Cleaning Pipeline ──────────────────────────────────────────────────────────

def clean_name(raw: str) -> str:
    """
    Normalize a raw name string:
    1. Unicode NFC normalization
    2. Remove control characters
    3. Collapse multiple spaces
    4. Strip leading/trailing whitespace
    Case is preserved as-is from the source.
    """
    if not raw:
        return ""

    # NFC normalize (handles composed vs decomposed Unicode)
    name = unicodedata.normalize("NFC", raw)

    # Remove control characters except regular spaces
    name = "".join(ch for ch in name if unicodedata.category(ch) != "Cc" or ch == " ")

    # Collapse whitespace
    name = re.sub(r"\s+", " ", name).strip()

    return name


def count_tokens(name: str) -> int:
    return len(name.split()) if name else 0


def detect_recently_modified(date_str: str | None, days: int = 90) -> bool:
    if not date_str:
        return False
    try:
        listed = datetime.strptime(date_str[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - listed).days <= days
    except ValueError:
        return False


def normalize_entity_type(raw: str) -> str:
    mapping = {
        "individual": "individual",
        "person": "individual",
        "entity": "entity",
        "organisation": "entity",
        "organization": "entity",
        "company": "entity",
        "vessel": "vessel",
        "ship": "vessel",
        "aircraft": "aircraft",
        "plane": "aircraft",
        "country": "country",
    }
    return mapping.get((raw or "").lower().strip(), "unknown")


# ── Upsert ─────────────────────────────────────────────────────────────────────

async def clean_and_upsert(
    entries: list[dict[str, Any]],
    watchlist_key: str,
    db: aiosqlite.Connection,
) -> int:
    """
    Clean each raw entry dict and upsert into watchlist_entries table.
    Returns count of inserted/updated rows.
    """
    if not entries:
        return 0

    rows: list[tuple] = []
    for e in entries:
        uid = e.get("uid", "")
        original_name = (e.get("original_name") or "").strip()
        if not original_name or not uid:
            continue

        cleaned = clean_name(original_name)
        if not cleaned:
            continue

        entity_type = normalize_entity_type(e.get("entity_type", "unknown"))
        date_listed = e.get("date_listed")
        recently_mod = 1 if detect_recently_modified(date_listed) else 0

        src_nat = e.get("nationality") or None  # used only for culture lookup, not stored
        primary_aka = e.get("primary_aka", "primary")

        # Derive parent_uid for AKA entries when not explicitly provided
        parent_uid = e.get("parent_uid")
        if not parent_uid and primary_aka == "aka" and "_aka_" in uid:
            parent_uid = uid.split("_aka_")[0] + "_primary"

        region, name_culture, culture_confidence = get_culture(
            src_nat, cleaned, e.get("sanctions_program")
        )

        rows.append((
            uid,
            watchlist_key,
            e.get("sub_watchlist_1"),
            e.get("sub_watchlist_2"),
            cleaned,
            original_name,
            primary_aka,
            entity_type,
            count_tokens(cleaned),
            len(cleaned),
            date_listed,
            recently_mod,
            e.get("sanctions_program"),
            parent_uid,
            region,
            name_culture,
            culture_confidence,
        ))

    if not rows:
        return 0

    await db.executemany(
        """INSERT INTO watchlist_entries
           (uid, watchlist, sub_watchlist_1, sub_watchlist_2,
            cleaned_name, original_name, primary_aka, entity_type,
            num_tokens, name_length,
            date_listed, recently_modified, sanctions_program, parent_uid,
            region, name_culture, culture_confidence)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(uid) DO UPDATE SET
             cleaned_name       = excluded.cleaned_name,
             original_name      = excluded.original_name,
             sub_watchlist_1    = excluded.sub_watchlist_1,
             sub_watchlist_2    = excluded.sub_watchlist_2,
             primary_aka        = excluded.primary_aka,
             entity_type        = excluded.entity_type,
             num_tokens         = excluded.num_tokens,
             name_length        = excluded.name_length,
             date_listed        = excluded.date_listed,
             recently_modified  = excluded.recently_modified,
             sanctions_program  = excluded.sanctions_program,
             parent_uid         = excluded.parent_uid,
             region             = CASE WHEN excluded.region IS NOT NULL THEN excluded.region
                                       ELSE region END,
             name_culture       = CASE WHEN excluded.name_culture IS NOT NULL THEN excluded.name_culture
                                       ELSE name_culture END,
             culture_confidence = CASE WHEN excluded.culture_confidence IS NOT NULL THEN excluded.culture_confidence
                                       ELSE culture_confidence END
        """,
        rows,
    )
    await db.commit()
    return len(rows)


# ── Read Helpers ───────────────────────────────────────────────────────────────

def _build_where(filters: ListFilters) -> tuple[str, list[Any]]:
    """Return (WHERE clause string, params list) for the given filters."""
    conditions: list[str] = []
    params: list[Any] = []

    if filters.watchlists:
        placeholders = ", ".join("?" for _ in filters.watchlists)
        conditions.append(f"watchlist IN ({placeholders})")
        params.extend(filters.watchlists)

    if filters.entity_types:
        placeholders = ", ".join("?" for _ in filters.entity_types)
        conditions.append(f"entity_type IN ({placeholders})")
        params.extend(filters.entity_types)

    if filters.cultures:
        # Also match the DB typo variant "Slavic/Eastern Eastern European"
        expanded = list(filters.cultures)
        if "Slavic/Eastern European" in expanded and "Slavic/Eastern Eastern European" not in expanded:
            expanded.append("Slavic/Eastern Eastern European")
        placeholders = ", ".join("?" for _ in expanded)
        conditions.append(f"name_culture IN ({placeholders})")
        params.extend(expanded)

    if filters.programs:
        prog_conditions = [f"sanctions_program LIKE ?" for _ in filters.programs]
        conditions.append(f"({' OR '.join(prog_conditions)})")
        params.extend(f"%{p}%" for p in filters.programs)

    if filters.search:
        conditions.append("(cleaned_name LIKE ? OR original_name LIKE ?)")
        like = f"%{filters.search}%"
        params.extend([like, like])

    if filters.recently_modified_only:
        conditions.append("recently_modified = 1")

    if filters.min_tokens is not None:
        conditions.append("num_tokens >= ?")
        params.append(filters.min_tokens)

    if filters.max_tokens is not None:
        conditions.append("num_tokens <= ?")
        params.append(filters.max_tokens)

    if filters.min_length is not None:
        conditions.append("name_length >= ?")
        params.append(filters.min_length)

    if filters.max_length is not None:
        conditions.append("name_length <= ?")
        params.append(filters.max_length)

    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    return where, params


async def get_chart_data(filters: ListFilters, db: aiosqlite.Connection) -> dict:
    """Return all chart data for the current filter state."""
    where, params = _build_where(filters)

    async def query(sql: str, p: list = None) -> list:
        async with db.execute(sql, p if p is not None else params) as cur:
            return await cur.fetchall()

    # By watchlist — fixed display order
    _WL_ORDER = ["OFAC_SDN", "OFAC_NON_SDN", "BIS", "EU", "HMT", "JAPAN"]
    rows = await query(
        f"SELECT watchlist, COUNT(*) as count FROM watchlist_entries {where} GROUP BY watchlist"
    )
    wl_map = {r[0]: r[1] for r in rows}
    by_watchlist = [
        {"name": wl, "count": wl_map[wl]}
        for wl in _WL_ORDER if wl in wl_map
    ] + [
        {"name": wl, "count": cnt}
        for wl, cnt in wl_map.items() if wl not in _WL_ORDER
    ]

    # By entity type
    rows = await query(
        f"SELECT entity_type, COUNT(*) as count FROM watchlist_entries {where} GROUP BY entity_type ORDER BY count DESC"
    )
    by_entity_type = [{"name": r[0], "value": r[1]} for r in rows]

    # Name length histogram — single query with CASE bucketing
    rows = await query(f"""
        SELECT
            CASE
                WHEN name_length BETWEEN 1  AND 5  THEN '1-5'
                WHEN name_length BETWEEN 6  AND 10 THEN '6-10'
                WHEN name_length BETWEEN 11 AND 15 THEN '11-15'
                WHEN name_length BETWEEN 16 AND 20 THEN '16-20'
                WHEN name_length BETWEEN 21 AND 30 THEN '21-30'
                WHEN name_length BETWEEN 31 AND 40 THEN '31-40'
                WHEN name_length BETWEEN 41 AND 50 THEN '41-50'
                ELSE '51+'
            END as bucket,
            COUNT(*) as count
        FROM watchlist_entries {where}
        GROUP BY bucket
    """)
    bucket_order = ['1-5','6-10','11-15','16-20','21-30','31-40','41-50','51+']
    bucket_map = {r[0]: r[1] for r in rows}
    name_length_hist = [{"bucket": b, "count": bucket_map.get(b, 0)} for b in bucket_order]

    # Token count histogram — single query with CASE bucketing
    rows = await query(f"""
        SELECT
            CASE WHEN num_tokens > 10 THEN '11+' ELSE CAST(num_tokens AS TEXT) END as bucket,
            COUNT(*) as count
        FROM watchlist_entries {where}
        GROUP BY bucket
    """)
    token_order = [str(t) for t in range(1, 11)] + ['11+']
    token_map = {r[0]: r[1] for r in rows}
    token_hist = [{"tokens": t, "count": token_map.get(t, 0)} for t in token_order]

    # Recently modified + total in one query
    rows = await query(f"""
        SELECT COUNT(*), SUM(recently_modified) FROM watchlist_entries {where}
    """)
    total = rows[0][0]
    recently_modified_count = rows[0][1] or 0

    return {
        "total": total,
        "by_watchlist": by_watchlist,
        "by_entity_type": by_entity_type,
        "name_length_hist": name_length_hist,
        "token_count_hist": token_hist,
        "recently_modified_count": recently_modified_count,
    }


async def get_entries_from_db(filters: ListFilters, db: aiosqlite.Connection) -> dict:
    where, params = _build_where(filters)

    count_sql = f"SELECT COUNT(*) FROM watchlist_entries {where}"
    async with db.execute(count_sql, params) as cur:
        total = (await cur.fetchone())[0]

    # Paginated data
    offset = (filters.page - 1) * filters.page_size
    data_sql = f"""
        SELECT uid, watchlist, sub_watchlist_1, sub_watchlist_2,
               cleaned_name, original_name, primary_aka, entity_type,
               num_tokens, name_length, date_listed, recently_modified,
               sanctions_program, parent_uid, region, name_culture, culture_confidence
        FROM watchlist_entries
        {where}
        ORDER BY
            CASE watchlist
                WHEN 'OFAC_SDN'     THEN 1
                WHEN 'OFAC_NON_SDN' THEN 2
                WHEN 'BIS'          THEN 3
                WHEN 'EU'           THEN 4
                WHEN 'HMT'          THEN 5
                WHEN 'JAPAN'        THEN 6
                ELSE 7
            END,
            COALESCE(parent_uid, uid),
            CASE primary_aka WHEN 'primary' THEN 0 ELSE 1 END,
            cleaned_name
        LIMIT ? OFFSET ?
    """
    async with db.execute(data_sql, params + [filters.page_size, offset]) as cur:
        rows = await cur.fetchall()

    items = [_row_to_dict(r) for r in rows]

    # Batch also_on_lists lookup: one query for all names on this page
    if items:
        from collections import defaultdict
        page_names = list({e["cleaned_name"] for e in items})
        placeholders = ", ".join("?" * len(page_names))
        async with db.execute(
            f"SELECT DISTINCT cleaned_name, watchlist FROM watchlist_entries "
            f"WHERE cleaned_name IN ({placeholders}) AND primary_aka = 'primary'",
            page_names,
        ) as cur:
            overlap_rows = await cur.fetchall()
        name_to_lists: dict = defaultdict(set)
        for name, wl in overlap_rows:
            name_to_lists[name].add(wl)
        for e in items:
            e["also_on_lists"] = sorted(name_to_lists[e["cleaned_name"]] - {e["watchlist"]})

    return {
        "total": total,
        "page": filters.page,
        "page_size": filters.page_size,
        "items": items,
    }


def _compute_parent_uid(uid: str, primary_aka: str) -> str | None:
    """Return the parent (primary) UID for AKA entries where derivable."""
    if primary_aka != "aka":
        return None
    # OFAC pattern: OFAC_SDN_12345_aka_67890 → OFAC_SDN_12345_primary
    if "_aka_" in uid:
        entity_part = uid.split("_aka_")[0]
        return f"{entity_part}_primary"
    return None


def _row_to_dict(row: aiosqlite.Row) -> dict:
    d = dict(row)
    d["recently_modified"] = bool(d.get("recently_modified"))
    # Use stored parent_uid; fall back to computed value for legacy rows
    if not d.get("parent_uid"):
        d["parent_uid"] = _compute_parent_uid(d.get("uid", ""), d.get("primary_aka", "primary"))
    d.setdefault("also_on_lists", [])
    return d


async def get_summary(db: aiosqlite.Connection) -> WatchlistSummary:
    async with db.execute("SELECT COUNT(*) FROM watchlist_entries") as cur:
        total = (await cur.fetchone())[0]

    async with db.execute(
        "SELECT watchlist, COUNT(*) FROM watchlist_entries GROUP BY watchlist"
    ) as cur:
        by_watchlist = {r[0]: r[1] for r in await cur.fetchall()}

    async with db.execute(
        "SELECT entity_type, COUNT(*) FROM watchlist_entries GROUP BY entity_type"
    ) as cur:
        by_entity = {r[0]: r[1] for r in await cur.fetchall()}

    async with db.execute(
        "SELECT timestamp FROM download_log ORDER BY timestamp DESC LIMIT 1"
    ) as cur:
        row = await cur.fetchone()
    last_updated = datetime.fromisoformat(row[0]) if row else None

    return WatchlistSummary(
        total=total,
        by_watchlist=by_watchlist,
        by_entity_type=by_entity,
        last_updated=last_updated,
    )
