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
        "url": "https://webgate.ec.europa.eu/fsd/fsf/public/files/xmlFullSanctionsList_1_1/content",
        "format": "eu_xml",
        "label": "EU Consolidated",
    },
    "HMT": {
        "url": "https://ofsistorage.blob.core.windows.net/publishlive/2022format/ConList.csv",
        "format": "hmt_csv",
        "label": "UK HMT",
    },
    "BIS": {
        "url": "https://www.bis.doc.gov/index.php/policy-guidance/lists-of-parties-of-concern/entity-list",
        "format": "bis_html",
        "label": "BIS Entity List",
    },
    "JAPAN": {
        "url": "https://www.meti.go.jp/policy/anpo/law_fuca.html",
        "format": "japan_html",
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
        elif fmt == "bis_html":
            entries = parse_bis_html(raw)
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
            "Mozilla/5.0 (compatible; ScreeningValidationBot/1.0; "
            "+https://github.com/screening-validation)"
        )
    }
    async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
        resp = await client.get(url, headers=headers)
        resp.raise_for_status()
        return resp.content


# ── Cache helpers ──────────────────────────────────────────────────────────────

async def _is_cache_fresh(key: str, db: aiosqlite.Connection) -> bool:
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

        # Date of listing (OFAC uses <publishInformation><Publish_Date> at root level)
        # Per-entry listing date not reliably present in flat XML; skip for now
        date_listed = None

        # Primary name
        last_el = sdn.find(t("lastName"))
        first_el = sdn.find(t("firstName"))
        last = (last_el.text or "").strip() if last_el is not None else ""
        first = (first_el.text or "").strip() if first_el is not None else ""
        primary_name = f"{first} {last}".strip() if first else last

        if primary_name:
            entries.append(
                {
                    "uid": f"{watchlist_key}_{uid}_primary",
                    "watchlist": watchlist_key,
                    "sub_watchlist_1": programs[0] if programs else None,
                    "sub_watchlist_2": programs[1] if len(programs) > 1 else None,
                    "original_name": primary_name,
                    "primary_aka": "primary",
                    "entity_type": _map_sdn_type(sdn_type),
                    "date_listed": date_listed,
                    "sanctions_program": "; ".join(programs) if programs else None,
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
    Schema: https://webgate.ec.europa.eu/fsd/fsf
    Root: <sanctionsList> → <sanctionsEntry> → <nameAlias>
    """
    from lxml import etree

    try:
        root = etree.fromstring(raw)
    except etree.XMLSyntaxError:
        # Some EU responses have a BOM or encoding declaration issues
        raw = raw.lstrip(b"\xef\xbb\xbf")
        root = etree.fromstring(raw)

    ns_map: dict = {}
    tag = root.tag
    if tag.startswith("{"):
        ns_uri = tag[1 : tag.index("}")]
        ns_map = {"eu": ns_uri}

    entries: list[dict] = []

    for entry in root.iter("{*}sanctionsEntry"):
        entry_uid = entry.get("logicalId", "")
        subject_type_node = entry.find("{*}subjectType")
        subject_type = ""
        if subject_type_node is not None:
            code_el = subject_type_node.find("{*}code")
            subject_type = (code_el.text or "").strip() if code_el is not None else ""

        # Programmes
        programmes: list[str] = []
        for reg in entry.iter("{*}regulation"):
            prog_el = reg.find("{*}programme")
            if prog_el is not None and prog_el.text:
                programmes.append(prog_el.text.strip())

        date_listed = None
        for reg in entry.iter("{*}regulation"):
            date_el = reg.find("{*}publicationDate")
            if date_el is not None and date_el.text:
                date_listed = date_el.text.strip()[:10]
                break

        for alias in entry.iter("{*}nameAlias"):
            alias_uid = alias.get("logicalId", "")
            full_name_el = alias.find("{*}wholeName")
            first_el = alias.find("{*}firstName")
            last_el = alias.find("{*}lastName")
            middle_el = alias.find("{*}middleName")

            if full_name_el is not None and full_name_el.text:
                name = full_name_el.text.strip()
            else:
                parts = [
                    (first_el.text or "").strip() if first_el is not None else "",
                    (middle_el.text or "").strip() if middle_el is not None else "",
                    (last_el.text or "").strip() if last_el is not None else "",
                ]
                name = " ".join(p for p in parts if p)

            if not name:
                continue

            is_primary = alias.get("strong", "false").lower() == "true"
            entries.append(
                {
                    "uid": f"EU_{entry_uid}_alias_{alias_uid}",
                    "watchlist": "EU",
                    "sub_watchlist_1": programmes[0] if programmes else None,
                    "sub_watchlist_2": None,
                    "original_name": name,
                    "primary_aka": "primary" if is_primary else "aka",
                    "entity_type": _map_eu_type(subject_type),
                    "date_listed": date_listed,
                    "sanctions_program": "; ".join(programmes) if programmes else None,
                }
            )

    return entries


def _map_eu_type(code: str) -> str:
    mapping = {
        "P": "individual",
        "I": "entity",
        "S": "vessel",
    }
    return mapping.get(code.upper(), "unknown")


# ── HMT CSV Parser ─────────────────────────────────────────────────────────────

def parse_hmt_csv(raw: bytes) -> list[dict]:
    """
    Parse UK HMT Office of Financial Sanctions Implementation CSV.
    2022 format CSV with headers.
    """
    import csv
    import io

    text = raw.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    entries: list[dict] = []
    seen_uids: set[str] = set()

    for row in reader:
        uid_raw = row.get("UniqueID", "").strip()
        name6 = row.get("Name 6", "").strip()  # Full name field in 2022 format

        # Build name from components
        name1 = row.get("Name 1", "").strip()
        name2 = row.get("Name 2", "").strip()
        name3 = row.get("Name 3", "").strip()
        name4 = row.get("Name 4", "").strip()
        name5 = row.get("Name 5", "").strip()

        full_name = name6 or " ".join(p for p in [name1, name2, name3, name4, name5] if p)
        if not full_name:
            continue

        group_type = row.get("Group Type", "").strip()
        regime = row.get("Regime Name", "").strip()
        date_listed = row.get("Listed On", "").strip() or None
        if date_listed:
            # Normalize date format
            try:
                from datetime import datetime as dt_
                date_listed = dt_.strptime(date_listed, "%d/%m/%Y").strftime("%Y-%m-%d")
            except ValueError:
                date_listed = None

        alias_type = row.get("Alias Type", "").strip()
        is_primary = alias_type.lower() in ("", "primary name", "primary")

        uid = f"HMT_{uid_raw}_{_short_hash(full_name)}"
        if uid in seen_uids:
            continue
        seen_uids.add(uid)

        entries.append(
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
            }
        )

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


# ── BIS HTML Parser ────────────────────────────────────────────────────────────

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
