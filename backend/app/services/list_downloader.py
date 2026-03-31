"""
list_downloader.py
Downloads and parses each watchlist source, stores cleaned entries in SQLite.
Returns a DownloadStatus per list so the frontend can show partial success.
"""
from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone, timedelta
from typing import Any
import httpx
import aiosqlite

from app.models.schemas import DownloadStatus
from app.services.list_cleaner import clean_and_upsert

logger = logging.getLogger(__name__)

# ── Source Registry ────────────────────────────────────────────────────────────

WATCHLIST_SOURCES: dict[str, dict[str, Any]] = {
    "OFAC_SDN": {
        "url": "https://www.treasury.gov/ofac/downloads/sdn.xml",
        "format": "ofac_xml",
        "label": "OFAC SDN",
    },
    "OFAC_NON_SDN": {
        "url": "https://www.treasury.gov/ofac/downloads/consolidated/consolidated.xml",
        "format": "ofac_xml",
        "label": "OFAC Consolidated Non-SDN",
    },
    "EU": {
        "url": "https://webgate.ec.europa.eu/fsd/fsf/public/files/xmlFullSanctionsList_1_1/content?token=dG9rZW4tMjAxNw",
        "format": "eu_xml",
        "label": "EU Consolidated",
    },
    "HMT": {
        "url": "https://ofsistorage.blob.core.windows.net/publishlive/2022format/ConList.csv",
        "format": "hmt_csv",
        "label": "UK HMT",
    },
    "BIS": {
        "url": "https://data.trade.gov/downloadable_consolidated_screening_list/v1/consolidated.csv",
        "format": "bis_csv",
        "label": "BIS Entity List",
    },
    "JAPAN": {
        "url": "https://data.opensanctions.org/datasets/latest/jp_meti_eul/targets.simple.csv",
        "format": "opensanctions_csv",
        "label": "Japan METI",
    },
}

CACHE_TTL_HOURS = 24  # Re-download only if cache is older than this


# ── Entry Point ────────────────────────────────────────────────────────────────

async def download_all_lists(
    watchlist_keys: list[str],
    db: aiosqlite.Connection,
) -> list[DownloadStatus]:
    statuses: list[DownloadStatus] = []
    for key in watchlist_keys:
        if key not in WATCHLIST_SOURCES:
            statuses.append(
                DownloadStatus(
                    watchlist=key,
                    status="failed",
                    error=f"Unknown watchlist key: {key}",
                    timestamp=datetime.now(timezone.utc),
                )
            )
            continue
        status = await _download_one(key, WATCHLIST_SOURCES[key], db)
        statuses.append(status)
    return statuses


# ── Per-list download ──────────────────────────────────────────────────────────

async def _download_one(
    key: str,
    source: dict[str, Any],
    db: aiosqlite.Connection,
) -> DownloadStatus:
    label = source["label"]

    # Check cache freshness
    if await _is_cache_fresh(key, db):
        async with db.execute(
            "SELECT COUNT(*) FROM watchlist_entries WHERE watchlist = ?", (key,)
        ) as cur:
            count = (await cur.fetchone())[0]
        logger.info(f"{label}: using cached data ({count} entries)")
        return DownloadStatus(
            watchlist=key,
            status="cached",
            count=count,
            timestamp=datetime.now(timezone.utc),
        )

    try:
        raw = await _fetch_url(source["url"])
    except Exception as exc:
        logger.warning(f"{label}: download failed — {exc}")
        return DownloadStatus(
            watchlist=key,
            status="failed",
            error=str(exc),
            timestamp=datetime.now(timezone.utc),
        )

    try:
        fmt = source["format"]
        if fmt == "ofac_xml":
            entries = parse_ofac_xml(raw, key)
        elif fmt == "eu_xml":
            entries = parse_eu_xml(raw)
        elif fmt == "hmt_csv":
            entries = parse_hmt_csv(raw)
        elif fmt == "bis_csv":
            entries = parse_bis_csv(raw)
        elif fmt == "bis_html":
            entries = parse_bis_html(raw)
        elif fmt == "opensanctions_csv":
            entries = parse_opensanctions_csv(raw, key)
        elif fmt == "japan_html":
            entries = parse_japan_html(raw)
        else:
            raise ValueError(f"Unknown format: {fmt}")
    except Exception as exc:
        logger.warning(f"{label}: parse failed — {exc}")
        return DownloadStatus(
            watchlist=key,
            status="failed",
            error=f"Parse error: {exc}",
            timestamp=datetime.now(timezone.utc),
        )

    count = await clean_and_upsert(entries, key, db)
    await _record_download(key, "success", count, None, db)
    logger.info(f"{label}: loaded {count} entries")
    return DownloadStatus(
        watchlist=key,
        status="success",
        count=count,
        timestamp=datetime.now(timezone.utc),
    )


# ── HTTP fetch ─────────────────────────────────────────────────────────────────

async def _fetch_url(url: str) -> bytes:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }
    async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
        resp = await client.get(url, headers=headers)
        resp.raise_for_status()
        return resp.content


# ── Cache helpers ──────────────────────────────────────────────────────────────

async def _is_cache_fresh(key: str, db: aiosqlite.Connection) -> bool:
    # If there are no entries for this watchlist, never consider it cached
    async with db.execute(
        "SELECT COUNT(*) FROM watchlist_entries WHERE watchlist = ?", (key,)
    ) as cur:
        count = (await cur.fetchone())[0]
    if count == 0:
        return False

    async with db.execute(
        """SELECT timestamp FROM download_log
           WHERE watchlist = ? AND status = 'success'
           ORDER BY timestamp DESC LIMIT 1""",
        (key,),
    ) as cur:
        row = await cur.fetchone()
    if not row:
        return False
    last = datetime.fromisoformat(row[0])
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - last) < timedelta(hours=CACHE_TTL_HOURS)


async def _record_download(
    key: str, status: str, count: int, error: str | None, db: aiosqlite.Connection
):
    await db.execute(
        "INSERT INTO download_log (watchlist, status, count, error) VALUES (?, ?, ?, ?)",
        (key, status, count, error),
    )
    await db.commit()


# ── Country → Nationality adjective mapping ────────────────────────────────────

_COUNTRY_NATIONALITY: dict[str, str] = {
    # A
    "Afghanistan": "Afghan", "Albania": "Albanian", "Algeria": "Algerian",
    "Angola": "Angolan", "Argentina": "Argentine", "Armenia": "Armenian",
    "Australia": "Australian", "Austria": "Austrian", "Azerbaijan": "Azerbaijani",
    # B
    "Bahrain": "Bahraini", "Bangladesh": "Bangladeshi", "Belarus": "Belarusian",
    "Belgium": "Belgian", "Belize": "Belizean", "Benin": "Beninese",
    "Bolivia": "Bolivian", "Bosnia": "Bosnian",
    "Bosnia and Herzegovina": "Bosnian", "Botswana": "Botswanan",
    "Brazil": "Brazilian", "Bulgaria": "Bulgarian",
    "Burkina Faso": "Burkinabe", "Burma": "Burmese", "Burundi": "Burundian",
    # C
    "Cambodia": "Cambodian", "Cameroon": "Cameroonian",
    "Central African Republic": "Central African",
    "Chad": "Chadian", "Chile": "Chilean", "China": "Chinese",
    "Colombia": "Colombian", "Comoros": "Comorian",
    "Congo": "Congolese", "Republic of the Congo": "Congolese",
    "Democratic Republic of the Congo": "Congolese", "DRC": "Congolese",
    "Costa Rica": "Costa Rican", "Cote d'Ivoire": "Ivorian",
    "Ivory Coast": "Ivorian", "Croatia": "Croatian", "Cuba": "Cuban",
    "Cyprus": "Cypriot", "Czech Republic": "Czech",
    # D–E
    "Denmark": "Danish", "Djibouti": "Djiboutian",
    "Dominican Republic": "Dominican",
    "Ecuador": "Ecuadorian", "Egypt": "Egyptian",
    "El Salvador": "Salvadoran", "Eritrea": "Eritrean", "Ethiopia": "Ethiopian",
    # F–G
    "France": "French", "Gambia": "Gambian", "Georgia": "Georgian",
    "Germany": "German", "Ghana": "Ghanaian", "Greece": "Greek",
    "Guatemala": "Guatemalan", "Guinea": "Guinean",
    "Guinea-Bissau": "Guinean", "Guyana": "Guyanese",
    # H–I
    "Haiti": "Haitian", "Honduras": "Honduran",
    "Hong Kong": "Chinese", "Hungary": "Hungarian",
    "India": "Indian", "Indonesia": "Indonesian",
    "Iran": "Iranian", "Iraq": "Iraqi",
    "Israel": "Israeli", "Italy": "Italian",
    # J–K
    "Jamaica": "Jamaican", "Japan": "Japanese",
    "Jordan": "Jordanian", "Kazakhstan": "Kazakhstani", "Kenya": "Kenyan",
    "Korea": "Korean", "Kosovo": "Kosovar", "Kuwait": "Kuwaiti",
    "Kyrgyzstan": "Kyrgyz",
    # L
    "Laos": "Laotian", "Latvia": "Latvian", "Lebanon": "Lebanese",
    "Liberia": "Liberian", "Libya": "Libyan", "Lithuania": "Lithuanian",
    # M
    "Macedonia": "Macedonian", "North Macedonia": "Macedonian",
    "Madagascar": "Malagasy", "Malawi": "Malawian",
    "Malaysia": "Malaysian", "Mali": "Malian",
    "Malta": "Maltese", "Mauritania": "Mauritanian",
    "Mexico": "Mexican", "Moldova": "Moldovan",
    "Montenegro": "Montenegrin", "Morocco": "Moroccan",
    "Mozambique": "Mozambican", "Myanmar": "Burmese",
    # N
    "Namibia": "Namibian", "Nepal": "Nepali",
    "Netherlands": "Dutch", "Nicaragua": "Nicaraguan",
    "Niger": "Nigerien", "Nigeria": "Nigerian",
    "North Korea": "North Korean",
    "Democratic People's Republic of Korea": "North Korean",
    # O–P
    "Oman": "Omani", "Pakistan": "Pakistani",
    "Palestinian Territories": "Palestinian", "Palestine": "Palestinian",
    "Panama": "Panamanian", "Paraguay": "Paraguayan", "Peru": "Peruvian",
    "Philippines": "Filipino", "Poland": "Polish",
    # Q–R
    "Qatar": "Qatari", "Romania": "Romanian", "Russia": "Russian",
    "Russian Federation": "Russian", "Rwanda": "Rwandan",
    # S
    "Saudi Arabia": "Saudi", "Senegal": "Senegalese", "Serbia": "Serbian",
    "Sierra Leone": "Sierra Leonean", "Singapore": "Singaporean",
    "Somalia": "Somali", "Somaliland": "Somali",
    "South Africa": "South African", "South Sudan": "South Sudanese",
    "Spain": "Spanish", "Sri Lanka": "Sri Lankan", "Sudan": "Sudanese",
    "Suriname": "Surinamese", "Syria": "Syrian",
    # T
    "Taiwan": "Taiwanese", "Tajikistan": "Tajik",
    "Tanzania": "Tanzanian", "Thailand": "Thai",
    "Timor-Leste": "Timorese", "Togo": "Togolese",
    "Tunisia": "Tunisian", "Turkey": "Turkish", "Turkmenistan": "Turkmen",
    # U
    "Uganda": "Ugandan", "Ukraine": "Ukrainian",
    "United Arab Emirates": "Emirati", "UAE": "Emirati",
    "Uruguay": "Uruguayan", "Uzbekistan": "Uzbek",
    # V–Z
    "Venezuela": "Venezuelan", "Vietnam": "Vietnamese",
    "West Bank": "Palestinian", "Yemen": "Yemeni",
    "Zambia": "Zambian", "Zimbabwe": "Zimbabwean",
}

def _country_to_nationality(country: str) -> str | None:
    """Convert country name (noun form) to nationality adjective.
    Returns None if country is unrecognized — caller should keep the raw value.
    Case-insensitive: EU exports country names in ALL CAPS.
    """
    if not country:
        return None
    c = country.strip()
    # Direct match (case-insensitive)
    c_lower = c.lower()
    for key, val in _COUNTRY_NATIONALITY.items():
        if key.lower() == c_lower:
            return val
    # Prefix match for compound strings like "Iraq, Baghdad" or "Russia (Moscow)"
    for key, val in _COUNTRY_NATIONALITY.items():
        if c_lower.startswith(key.lower()):
            return val
    return None


def _nat_from_raw(raw: str) -> str:
    """Return nationality adjective from raw country string.
    If already an adjective (e.g. 'Russian'), _country_to_nationality returns None,
    so we fall back to raw itself — the value is already correct.
    """
    return _country_to_nationality(raw) or raw


# ── OFAC XML Parser ────────────────────────────────────────────────────────────

def parse_ofac_xml(raw: bytes, watchlist_key: str) -> list[dict]:
    """
    Parse OFAC SDN or Consolidated Non-SDN XML into a list of raw entry dicts.
    OFAC XML schema: https://home.treasury.gov/system/files/126/sdn_advanced_api.pdf
    The flat sdn.xml uses <sdnList><sdnEntry> elements.
    """
    from lxml import etree

    root = etree.fromstring(raw)
    # Strip namespace if present
    ns = ""
    tag = root.tag
    if tag.startswith("{"):
        ns = tag[1 : tag.index("}")]

    def t(name: str) -> str:
        return f"{{{ns}}}{name}" if ns else name

    # Extract global publish date from root (flat SDN XML has no per-entry dates)
    global_date_listed = None
    pub_info = root.find(t("publshInformation")) or root.find(t("publishInformation"))
    if pub_info is not None:
        pub_date_el = pub_info.find(t("Publish_Date"))
        if pub_date_el is not None and pub_date_el.text:
            try:
                global_date_listed = datetime.strptime(
                    pub_date_el.text.strip(), "%m/%d/%Y"
                ).strftime("%Y-%m-%d")
            except ValueError:
                pass

    entries: list[dict] = []
    for sdn in root.iter(t("sdnEntry")):
        uid_el = sdn.find(t("uid"))
        uid = uid_el.text.strip() if uid_el is not None and uid_el.text else ""

        # Entity type
        sdn_type_el = sdn.find(t("sdnType"))
        sdn_type = (sdn_type_el.text or "").strip() if sdn_type_el is not None else ""

        # Programs
        programs: list[str] = []
        prog_list = sdn.find(t("programList"))
        if prog_list is not None:
            for prog in prog_list.iter(t("program")):
                if prog.text:
                    programs.append(prog.text.strip())

        # Use global publish date (per-entry listing dates not in flat sdn.xml)
        date_listed = global_date_listed

        # ── 5-tier nationality extraction ────────────────────────────────────
        # Priority varies by entity type:
        #   Individuals: nationality → citizenship → place-of-birth → id-country
        #   Entities/Vessels/Aircraft: nationality → id-country → address-country
        # Address country is intentionally skipped for individuals because it
        # reflects residence, not nationality (e.g. AHMAD lives in Norway, is Iraqi).
        nat_country = None
        is_individual = sdn_type == "Individual"

        # Tier 1: nationalityList — OFAC stores this as adjective ("Russian"), convert anyway
        nat_list = sdn.find(t("nationalityList"))
        if nat_list is not None:
            for nat_el in nat_list.iter(t("nationality")):
                country_el = nat_el.find(t("country"))
                if country_el is not None and country_el.text:
                    nat_country = _nat_from_raw(country_el.text.strip())
                    break

        # Tier 2: citizenshipList — direct citizenship ("Iraq" → "Iraqi")
        if not nat_country:
            cit_list = sdn.find(t("citizenshipList"))
            if cit_list is not None:
                for cit_el in cit_list.iter(t("citizenship")):
                    country_el = cit_el.find(t("country"))
                    if country_el is not None and country_el.text:
                        nat_country = _nat_from_raw(country_el.text.strip())
                        break

        # Tier 3 (individuals only): placeOfBirthList country
        if not nat_country and is_individual:
            pob_list = sdn.find(t("placeOfBirthList"))
            if pob_list is not None:
                for pob_el in pob_list.iter(t("placeOfBirth")):
                    country_el = pob_el.find(t("country"))
                    if country_el is not None and country_el.text:
                        nat_country = _nat_from_raw(country_el.text.strip())
                        break

        # Tier 4: idList — document-issuing country (passport, NIT, cedula, etc.)
        # Reliable for both individuals (passport) and entities (registration number).
        if not nat_country:
            id_list = sdn.find(t("idList"))
            if id_list is not None:
                for id_el in id_list.iter(t("id")):
                    id_country_el = id_el.find(t("idCountry"))
                    if id_country_el is not None and id_country_el.text:
                        candidate = _nat_from_raw(id_country_el.text.strip())
                        # Skip vague/unhelpful country values
                        if candidate and candidate not in ("American", "United States"):
                            nat_country = candidate
                            break

        # Tier 5: addressList country — entities/vessels/aircraft only
        # Individuals' addresses reflect residence, not nationality.
        if not nat_country and not is_individual:
            addr_list = sdn.find(t("addressList"))
            if addr_list is not None:
                for addr in addr_list.iter(t("address")):
                    country_el = addr.find(t("country"))
                    if country_el is not None and country_el.text:
                        candidate = _country_to_nationality(country_el.text.strip())
                        if candidate:
                            nat_country = candidate
                            break

        # Primary name
        last_el = sdn.find(t("lastName"))
        first_el = sdn.find(t("firstName"))
        last = (last_el.text or "").strip() if last_el is not None else ""
        first = (first_el.text or "").strip() if first_el is not None else ""
        primary_name = f"{first} {last}".strip() if first else last

        primary_row_uid = f"{watchlist_key}_{uid}_primary"
        if primary_name:
            entries.append(
                {
                    "uid": primary_row_uid,
                    "watchlist": watchlist_key,
                    "sub_watchlist_1": programs[0] if programs else None,
                    "sub_watchlist_2": programs[1] if len(programs) > 1 else None,
                    "original_name": primary_name,
                    "primary_aka": "primary",
                    "entity_type": _map_sdn_type(sdn_type),
                    "date_listed": date_listed,
                    "sanctions_program": "; ".join(programs) if programs else None,
                    "parent_uid": None,
                    "nationality": nat_country,
                }
            )

        # AKAs
        aka_list = sdn.find(t("akaList"))
        if aka_list is not None:
            for aka in aka_list.iter(t("aka")):
                aka_uid_el = aka.find(t("uid"))
                aka_uid = (aka_uid_el.text or "").strip() if aka_uid_el is not None else ""
                aka_last_el = aka.find(t("lastName"))
                aka_first_el = aka.find(t("firstName"))
                aka_last = (aka_last_el.text or "").strip() if aka_last_el is not None else ""
                aka_first = (aka_first_el.text or "").strip() if aka_first_el is not None else ""
                aka_name = f"{aka_first} {aka_last}".strip() if aka_first else aka_last
                if aka_name:
                    entries.append(
                        {
                            "uid": f"{watchlist_key}_{uid}_aka_{aka_uid}",
                            "watchlist": watchlist_key,
                            "sub_watchlist_1": programs[0] if programs else None,
                            "sub_watchlist_2": programs[1] if len(programs) > 1 else None,
                            "original_name": aka_name,
                            "primary_aka": "aka",
                            "entity_type": _map_sdn_type(sdn_type),
                            "date_listed": date_listed,
                            "sanctions_program": "; ".join(programs) if programs else None,
                            "parent_uid": primary_row_uid,
                            "nationality": nat_country,
                        }
                    )

    return entries


def _map_sdn_type(sdn_type: str) -> str:
    mapping = {
        "Individual": "individual",
        "Entity": "entity",
        "Vessel": "vessel",
        "Aircraft": "aircraft",
    }
    return mapping.get(sdn_type, "unknown")


# ── EU XML Parser ──────────────────────────────────────────────────────────────

def parse_eu_xml(raw: bytes) -> list[dict]:
    """
    Parse EU Consolidated Sanctions XML.
    Root: <export> → <sanctionEntity> → <nameAlias>
    All nameAlias fields and regulation/citizenship attributes are XML attributes,
    not child elements.
    """
    from lxml import etree

    try:
        root = etree.fromstring(raw)
    except etree.XMLSyntaxError:
        raw = raw.lstrip(b"\xef\xbb\xbf")
        root = etree.fromstring(raw)

    entries: list[dict] = []

    for entry in root.iter("{*}sanctionEntity"):  # actual element name
        entry_uid = entry.get("logicalId", "")

        # classificationCode attribute on <subjectType> ("P"=person, "E"=entity, "S"=ship)
        subject_type_node = entry.find("{*}subjectType")
        subject_type = ""
        if subject_type_node is not None:
            subject_type = subject_type_node.get("classificationCode", "").strip()

        # programme and publicationDate are attributes on <regulation>
        programmes: list[str] = []
        date_listed = None
        for reg in entry.iter("{*}regulation"):
            prog = reg.get("programme", "").strip()
            if prog:
                programmes.append(prog)
            if date_listed is None:
                pub = reg.get("publicationDate", "").strip()
                if pub:
                    date_listed = pub[:10]

        # Nationality: countryDescription attribute on <citizenship>
        nat_country = None
        for cit in entry.iter("{*}citizenship"):
            country_desc = cit.get("countryDescription", "").strip()
            if country_desc:
                nat_country = _nat_from_raw(country_desc)
                break

        # Build alias entries; track primary UID for parent_uid assignment
        primary_entry_uid: str | None = None
        alias_entries: list[dict] = []

        for alias in entry.iter("{*}nameAlias"):
            alias_uid = alias.get("logicalId", "")
            # wholeName, firstName, lastName, middleName are XML attributes
            whole_name = alias.get("wholeName", "").strip()
            first_name = alias.get("firstName", "").strip()
            middle_name = alias.get("middleName", "").strip()
            last_name = alias.get("lastName", "").strip()

            name = whole_name or " ".join(
                p for p in [first_name, middle_name, last_name] if p
            )
            if not name:
                continue

            is_primary = alias.get("strong", "false").lower() == "true"
            full_uid = f"EU_{entry_uid}_alias_{alias_uid}"
            if is_primary and primary_entry_uid is None:
                primary_entry_uid = full_uid

            alias_entries.append(
                {
                    "uid": full_uid,
                    "watchlist": "EU",
                    "sub_watchlist_1": programmes[0] if programmes else None,
                    "sub_watchlist_2": None,
                    "original_name": name,
                    "primary_aka": "primary" if is_primary else "aka",
                    "entity_type": _map_eu_type(subject_type),
                    "date_listed": date_listed,
                    "sanctions_program": "; ".join(programmes) if programmes else None,
                    "nationality": nat_country,
                    "_is_primary": is_primary,
                }
            )

        for ae in alias_entries:
            is_prim = ae.pop("_is_primary")
            ae["parent_uid"] = None if is_prim else primary_entry_uid
            entries.append(ae)

    return entries


def _map_eu_type(code: str) -> str:
    mapping = {
        "P": "individual",
        "E": "entity",
        "I": "entity",  # fallback
        "S": "vessel",
    }
    return mapping.get(code.upper(), "unknown")


# ── HMT CSV Parser ─────────────────────────────────────────────────────────────

def parse_hmt_csv(raw: bytes) -> list[dict]:
    """
    Parse UK HMT Office of Financial Sanctions Implementation CSV (2022 format).
    Line 0 is metadata ("Last Updated,DD/MM/YYYY") — must be skipped.
    Line 1 is the real header row.
    Key columns: Name 6 (full name), Group ID (entity ID), Group Type,
                 Alias Type, Regime, Listed On, Nationality.
    """
    import csv
    import io
    from collections import defaultdict

    text = raw.decode("utf-8-sig", errors="replace")
    lines = text.splitlines()

    # Find the real header line (skip leading metadata lines like "Last Updated,...")
    start_idx = 0
    for i, line in enumerate(lines):
        if "Group ID" in line or "Name 6" in line or "Group Type" in line:
            start_idx = i
            break

    csv_text = "\n".join(lines[start_idx:])
    reader = csv.DictReader(io.StringIO(csv_text))

    # Collect all rows grouped by Group ID so we can assign parent_uid
    groups: dict[str, list[dict]] = defaultdict(list)

    for row in reader:
        group_id = row.get("Group ID", "").strip()
        name6 = row.get("Name 6", "").strip()
        name1 = row.get("Name 1", "").strip()
        name2 = row.get("Name 2", "").strip()
        name3 = row.get("Name 3", "").strip()
        name4 = row.get("Name 4", "").strip()
        name5 = row.get("Name 5", "").strip()

        full_name = name6 or " ".join(p for p in [name1, name2, name3, name4, name5] if p)
        if not full_name or not group_id:
            continue

        group_type = row.get("Group Type", "").strip()
        regime = row.get("Regime", "").strip()
        date_listed = row.get("Listed On", "").strip() or None
        if date_listed:
            try:
                from datetime import datetime as dt_
                date_listed = dt_.strptime(date_listed, "%d/%m/%Y").strftime("%Y-%m-%d")
            except ValueError:
                date_listed = None

        alias_type = row.get("Alias Type", "").strip().lower()
        # "Primary name variation" → primary; all others (AKA, etc.) → aka
        is_primary = alias_type in ("", "primary name", "primary", "primary name variation")

        nat_raw = row.get("Nationality", "").strip()
        nationality = _nat_from_raw(nat_raw) if nat_raw else None

        uid = f"HMT_{group_id}_{_short_hash(full_name)}"
        groups[group_id].append(
            {
                "uid": uid,
                "watchlist": "HMT",
                "sub_watchlist_1": regime or None,
                "sub_watchlist_2": None,
                "original_name": full_name,
                "primary_aka": "primary" if is_primary else "aka",
                "entity_type": _map_hmt_type(group_type),
                "date_listed": date_listed,
                "sanctions_program": regime or None,
                "nationality": nationality,
                "_is_primary": is_primary,
            }
        )

    # Assign parent_uid and deduplicate
    entries: list[dict] = []
    seen_uids: set[str] = set()
    for group_entries in groups.values():
        primary_uid: str | None = next(
            (e["uid"] for e in group_entries if e["_is_primary"]), None
        )
        for e in group_entries:
            is_prim = e.pop("_is_primary")
            e["parent_uid"] = None if is_prim else primary_uid
            if e["uid"] in seen_uids:
                continue
            seen_uids.add(e["uid"])
            entries.append(e)

    return entries


def _map_hmt_type(group_type: str) -> str:
    g = group_type.lower()
    if "individual" in g:
        return "individual"
    if "entity" in g or "organisation" in g or "company" in g:
        return "entity"
    if "ship" in g or "vessel" in g:
        return "vessel"
    if "aircraft" in g:
        return "aircraft"
    return "unknown"


# ── BIS CSV Parser (trade.gov Consolidated Screening List) ────────────────────

def parse_bis_csv(raw: bytes) -> list[dict]:
    """
    Parse the trade.gov Consolidated Screening List CSV, keeping only
    Bureau of Industry and Security – Entity List entries.
    CSV columns: id, source, entity_number, type, programs, name, title,
                 addresses, ..., alt_names, start_date, end_date, ...
    """
    import csv
    import io

    text = raw.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    entries: list[dict] = []
    seen: set[str] = set()

    BIS_SOURCES = {
        "Entity List (EL) - Bureau of Industry and Security",
        "Denied Persons List (DPL) - Bureau of Industry and Security",
        "Unverified List (UVL) - Bureau of Industry and Security",
        "Military End User (MEU) List - Bureau of Industry and Security",
    }

    for row in reader:
        source = (row.get("source") or "").strip().strip('"')
        if source not in BIS_SOURCES:
            continue

        name = (row.get("name") or "").strip().strip('"')
        if not name:
            continue

        row_id = (row.get("id") or "").strip().strip('"')
        uid = f"BIS_{_short_hash(row_id or name)}"
        if uid in seen:
            continue
        seen.add(uid)

        # Sub-list label (Entity List, DPL, etc.)
        sub = source.split(" - ")[0].strip()

        date_listed = (row.get("start_date") or "").strip().strip('"') or None
        if date_listed:
            # Format is already YYYY-MM-DD in trade.gov CSV
            date_listed = date_listed[:10]

        entity_type_raw = (row.get("type") or "").strip().strip('"').lower()
        if "individual" in entity_type_raw:
            entity_type = "individual"
        elif "vessel" in entity_type_raw or "ship" in entity_type_raw:
            entity_type = "vessel"
        else:
            entity_type = "entity"

        entries.append(
            {
                "uid": uid,
                "watchlist": "BIS",
                "sub_watchlist_1": sub,
                "sub_watchlist_2": None,
                "original_name": name,
                "primary_aka": "primary",
                "entity_type": entity_type,
                "date_listed": date_listed,
                "sanctions_program": sub,
            }
        )

        # Alt names
        alt_names_raw = (row.get("alt_names") or "").strip().strip('"')
        for alt in alt_names_raw.split(";") if alt_names_raw else []:
            alt = alt.strip()
            if not alt or alt == name:
                continue
            alt_uid = f"BIS_{_short_hash(alt + row_id)}"
            if alt_uid in seen:
                continue
            seen.add(alt_uid)
            entries.append(
                {
                    "uid": alt_uid,
                    "watchlist": "BIS",
                    "sub_watchlist_1": sub,
                    "sub_watchlist_2": None,
                    "original_name": alt,
                    "primary_aka": "aka",
                    "entity_type": entity_type,
                    "date_listed": date_listed,
                    "sanctions_program": sub,
                    "parent_uid": uid,
                }
            )

    return entries


# ── BIS HTML Parser (legacy fallback) ─────────────────────────────────────────

def parse_bis_html(raw: bytes) -> list[dict]:
    """
    Parse BIS Entity List. The page links to a downloadable CSV/Excel file.
    For now, parse the HTML table if a CSV isn't directly available.
    Returns a best-effort list; full CSV integration can be added later.
    """
    # BIS publishes a consolidated CSV at a known path — try to parse HTML table
    from lxml import etree, html as lxml_html

    entries: list[dict] = []
    try:
        doc = lxml_html.fromstring(raw)
        rows = doc.cssselect("table tr") or []
        headers: list[str] = []
        for row in rows:
            cells = [c.text_content().strip() for c in row.findall(".//td") or row.findall(".//th")]
            if not headers:
                headers = [c.lower() for c in cells]
                continue
            if not cells or len(cells) < 2:
                continue
            name_idx = next((i for i, h in enumerate(headers) if "name" in h), 0)
            name = cells[name_idx] if name_idx < len(cells) else ""
            if not name:
                continue
            uid = f"BIS_{_short_hash(name)}"
            entries.append(
                {
                    "uid": uid,
                    "watchlist": "BIS",
                    "sub_watchlist_1": "Entity List",
                    "sub_watchlist_2": None,
                    "original_name": name,
                    "primary_aka": "primary",
                    "entity_type": "entity",
                    "date_listed": None,
                    "sanctions_program": "BIS Entity List",
                }
            )
    except Exception as exc:
        logger.warning(f"BIS HTML parse error: {exc}")
    return entries


# ── OpenSanctions CSV Parser ───────────────────────────────────────────────────

def parse_opensanctions_csv(raw: bytes, watchlist_key: str) -> list[dict]:
    """
    Parse OpenSanctions targets.simple.csv format.
    Columns: id, schema, name, aliases, nationality, country, first_seen, last_seen, ...
    Used for Japan METI End-User List via the OpenSanctions mirror.
    """
    import csv
    import io

    text = raw.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    entries: list[dict] = []
    seen: set[str] = set()

    for row in reader:
        entity_id = (row.get("id") or "").strip()
        name = (row.get("name") or "").strip()
        if not name or not entity_id:
            continue

        schema = (row.get("schema") or "").strip()
        entity_type = _map_opensanctions_schema(schema)

        # Date: try multiple possible field names
        date_listed = None
        for date_field in ("listed_on", "first_seen", "start_date"):
            raw_date = (row.get(date_field) or "").strip()
            if raw_date:
                date_listed = raw_date[:10]
                break

        program = (row.get("datasets") or "METI End-User List").strip()

        # Nationality: try 'nationality' column first, then 'country'
        nationality = None
        for nat_field in ("nationality", "country"):
            raw_nat = (row.get(nat_field) or "").strip().split(";")[0].strip()
            if raw_nat:
                nationality = _nat_from_raw(raw_nat)
                break

        uid = f"{watchlist_key}_{_short_hash(entity_id)}"
        if uid in seen:
            continue
        seen.add(uid)

        entries.append(
            {
                "uid": uid,
                "watchlist": watchlist_key,
                "sub_watchlist_1": "End-User List",
                "sub_watchlist_2": None,
                "original_name": name,
                "primary_aka": "primary",
                "entity_type": entity_type,
                "date_listed": date_listed,
                "sanctions_program": program,
                "parent_uid": None,
                "nationality": nationality,
            }
        )

        # Aliases (semicolon-separated)
        aliases_raw = (row.get("aliases") or "").strip()
        for alias in (aliases_raw.split(";") if aliases_raw else []):
            alias = alias.strip()
            if not alias or alias == name:
                continue
            alias_uid = f"{watchlist_key}_{_short_hash(entity_id + alias)}"
            if alias_uid in seen:
                continue
            seen.add(alias_uid)
            entries.append(
                {
                    "uid": alias_uid,
                    "watchlist": watchlist_key,
                    "sub_watchlist_1": "End-User List",
                    "sub_watchlist_2": None,
                    "original_name": alias,
                    "primary_aka": "aka",
                    "entity_type": entity_type,
                    "date_listed": date_listed,
                    "sanctions_program": program,
                    "parent_uid": uid,
                }
            )

    return entries


def _map_opensanctions_schema(schema: str) -> str:
    mapping = {
        "Person": "individual",
        "Organization": "entity",
        "LegalEntity": "entity",
        "Company": "entity",
        "Vessel": "vessel",
        "Aircraft": "aircraft",
    }
    return mapping.get(schema, "entity")


# ── Japan METI HTML Parser ─────────────────────────────────────────────────────

def parse_japan_html(raw: bytes) -> list[dict]:
    """
    Parse Japan METI End-User List from HTML page.
    Best-effort; METI publishes PDFs; HTML page links to the list.
    """
    from lxml import html as lxml_html

    entries: list[dict] = []
    try:
        doc = lxml_html.fromstring(raw)
        rows = doc.cssselect("table tr")
        headers: list[str] = []
        for row in rows:
            cells = [c.text_content().strip() for c in row.findall(".//td") or row.findall(".//th")]
            if not headers:
                headers = [c.lower() for c in cells]
                continue
            if not cells:
                continue
            name_idx = next((i for i, h in enumerate(headers) if "name" in h), 0)
            name = cells[name_idx] if name_idx < len(cells) else cells[0] if cells else ""
            if not name or len(name) < 2:
                continue
            uid = f"JAPAN_{_short_hash(name)}"
            entries.append(
                {
                    "uid": uid,
                    "watchlist": "JAPAN",
                    "sub_watchlist_1": "End-User List",
                    "sub_watchlist_2": None,
                    "original_name": name,
                    "primary_aka": "primary",
                    "entity_type": "entity",
                    "date_listed": None,
                    "sanctions_program": "METI End-User List",
                }
            )
    except Exception as exc:
        logger.warning(f"Japan METI HTML parse error: {exc}")
    return entries


# ── Utility ────────────────────────────────────────────────────────────────────

def _short_hash(s: str) -> str:
    return hashlib.md5(s.encode()).hexdigest()[:8]
