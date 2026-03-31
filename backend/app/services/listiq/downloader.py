"""Download and parse OFAC SDN XML into ListIQ records."""
import hashlib
import json
import logging
from datetime import date, datetime
from xml.etree import ElementTree as ET

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.listiq_models import ListIQSnapshot, ListIQRecord

logger = logging.getLogger(__name__)

OFAC_SDN_URL = "https://sanctionslistservice.ofac.treas.gov/api/publicationpreview/exports/sdn.xml"
LIST_NAME = "OFAC_SDN"


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _parse_sdn_xml(xml_bytes: bytes) -> list[dict]:
    root = ET.fromstring(xml_bytes)
    ns = {"sdn": "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/XML"}
    # Try with namespace first, fall back to no namespace
    entries = root.findall("sdn:sdnEntry", ns) or root.findall("sdnEntry")
    if not entries:
        # Try wildcard namespace
        entries = list(root.iter("{*}sdnEntry"))

    def _find(el, tag):
        """Find a child element by tag, trying wildcard namespace first."""
        found = el.find(f"{{*}}{tag}")
        if found is not None:
            return found
        return el.find(tag)

    def _text(el, tag):
        child = _find(el, tag)
        return child.text.strip() if child is not None and child.text else ""

    records = []
    for entry in entries:
        uid = _text(entry, "uid")
        first = _text(entry, "firstName")
        last = _text(entry, "lastName")
        primary_name = f"{first} {last}".strip() if first else last
        sdn_type = _text(entry, "sdnType")

        akas = []
        for aka in entry.iter("{*}aka"):
            fn = _text(aka, "firstName")
            ln = _text(aka, "lastName")
            name = f"{fn} {ln}".strip() if fn else ln
            if name:
                akas.append(name)

        ids = []
        for id_el in entry.iter("{*}id"):
            id_type = _find(id_el, "idType")
            id_num = _find(id_el, "idNumber")
            if id_type is not None or id_num is not None:
                ids.append({
                    "type": id_type.text.strip() if id_type is not None and id_type.text else "",
                    "number": id_num.text.strip() if id_num is not None and id_num.text else "",
                })

        addresses = []
        for addr in entry.iter("{*}address"):
            parts = []
            for tag in ["address1", "address2", "address3", "city", "stateOrProvince", "postalCode", "country"]:
                el = _find(addr, tag)
                if el is not None and el.text:
                    parts.append(el.text.strip())
            if parts:
                addresses.append(", ".join(parts))

        programs = []
        for prog in list(entry.iter("{*}program")) + list(entry.findall("program")):
            if prog.text:
                programs.append(prog.text.strip())

        raw = {
            "uid": uid,
            "primary_name": primary_name,
            "record_type": sdn_type,
            "akas": akas,
            "ids": ids,
            "addresses": addresses,
            "programs": programs,
        }

        records.append({
            "record_uid": uid,
            "record_type": sdn_type,
            "primary_name": primary_name,
            "akas": json.dumps(akas),
            "ids": json.dumps(ids),
            "addresses": json.dumps(addresses),
            "programs": json.dumps(programs),
            "raw_data": json.dumps(raw),
        })

    return records


async def run_sync(db: AsyncSession) -> dict:
    """
    Full sync cycle: download → hash check → parse → store snapshot → diff → store changes.
    Returns a summary dict.
    """
    today = date.today()
    logger.info("ListIQ sync starting for %s", today)

    # Check if today's snapshot already exists
    existing = await db.execute(
        select(ListIQSnapshot).where(
            ListIQSnapshot.list_name == LIST_NAME,
            ListIQSnapshot.snapshot_date == today,
        )
    )
    if existing.scalars().first():
        logger.info("ListIQ: today's snapshot already exists, skipping")
        return {"status": "skipped", "reason": "already synced today"}

    # Download
    try:
        async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
            resp = await client.get(OFAC_SDN_URL)
            resp.raise_for_status()
            xml_bytes = resp.content
    except Exception as exc:
        logger.error("ListIQ download failed: %s", exc)
        return {"status": "error", "error": str(exc)}

    file_hash = _sha256(xml_bytes)

    # Check if hash matches the most recent snapshot (no changes)
    latest_snap = await db.execute(
        select(ListIQSnapshot)
        .where(ListIQSnapshot.list_name == LIST_NAME)
        .order_by(ListIQSnapshot.snapshot_date.desc())
        .limit(1)
    )
    latest_snap = latest_snap.scalars().first()
    if latest_snap and latest_snap.raw_file_hash == file_hash:
        logger.info("ListIQ: file hash unchanged, no changes detected")
        # Still record today's snapshot with the same hash
        snap = ListIQSnapshot(
            list_name=LIST_NAME,
            snapshot_date=today,
            raw_file_hash=file_hash,
            record_count=latest_snap.record_count,
        )
        db.add(snap)
        await db.commit()
        return {"status": "no_changes", "record_count": latest_snap.record_count}

    # Parse
    try:
        records = _parse_sdn_xml(xml_bytes)
    except Exception as exc:
        logger.error("ListIQ parse failed: %s", exc)
        return {"status": "error", "error": f"Parse failed: {exc}"}

    # Store snapshot
    snap = ListIQSnapshot(
        list_name=LIST_NAME,
        snapshot_date=today,
        raw_file_hash=file_hash,
        record_count=len(records),
    )
    db.add(snap)
    await db.flush()  # get snap.id

    # Store records
    for r in records:
        db.add(ListIQRecord(
            snapshot_id=snap.id,
            list_name=LIST_NAME,
            snapshot_date=today,
            **r,
        ))
    await db.commit()

    # Compute diff against the most recent snapshot that has records stored
    changes_summary = {"additions": 0, "deletions": 0, "modifications": 0}
    comparison_snap_result = await db.execute(
        select(ListIQSnapshot)
        .where(
            ListIQSnapshot.list_name == LIST_NAME,
            ListIQSnapshot.id.in_(select(ListIQRecord.snapshot_id).distinct()),
            ListIQSnapshot.id != snap.id,
        )
        .order_by(ListIQSnapshot.snapshot_date.desc())
        .limit(1)
    )
    comparison_snap = comparison_snap_result.scalars().first()
    if comparison_snap:
        changes_summary = await _compute_diff(db, comparison_snap, snap, today)

    logger.info(
        "ListIQ sync complete: %d records, +%d -%d ~%d",
        len(records),
        changes_summary["additions"],
        changes_summary["deletions"],
        changes_summary["modifications"],
    )
    return {
        "status": "success",
        "record_count": len(records),
        **changes_summary,
    }


async def _compute_diff(db: AsyncSession, yesterday_snap: ListIQSnapshot, today_snap: ListIQSnapshot, change_date: date) -> dict:
    from app.models.listiq_models import ListIQChange

    # Load yesterday's records as {uid: record_dict}
    y_rows = await db.execute(
        select(ListIQRecord).where(ListIQRecord.snapshot_id == yesterday_snap.id)
    )
    yesterday = {r.record_uid: r for r in y_rows.scalars()}

    # Load today's records
    t_rows = await db.execute(
        select(ListIQRecord).where(ListIQRecord.snapshot_id == today_snap.id)
    )
    today_map = {r.record_uid: r for r in t_rows.scalars()}

    y_uids = set(yesterday.keys())
    t_uids = set(today_map.keys())

    additions = t_uids - y_uids
    deletions = y_uids - t_uids
    common = y_uids & t_uids

    counts = {"additions": len(additions), "deletions": len(deletions), "modifications": 0}

    FIELD_PRIORITY = ["primary_name", "akas", "ids", "addresses", "programs"]

    for uid in additions:
        r = today_map[uid]
        db.add(ListIQChange(
            list_name=LIST_NAME,
            change_date=change_date,
            record_uid=uid,
            change_type="ADDITION",
            after_data=r.raw_data,
        ))

    for uid in deletions:
        r = yesterday[uid]
        db.add(ListIQChange(
            list_name=LIST_NAME,
            change_date=change_date,
            record_uid=uid,
            change_type="DELETION",
            before_data=r.raw_data,
        ))

    for uid in common:
        y = yesterday[uid]
        t = today_map[uid]
        changed_fields = []
        for field in FIELD_PRIORITY:
            if getattr(y, field) != getattr(t, field):
                changed_fields.append(field)
        if changed_fields:
            counts["modifications"] += 1
            db.add(ListIQChange(
                list_name=LIST_NAME,
                change_date=change_date,
                record_uid=uid,
                change_type="MODIFICATION",
                modification_fields=json.dumps(changed_fields),
                before_data=y.raw_data,
                after_data=t.raw_data,
            ))

    await db.commit()
    return counts
