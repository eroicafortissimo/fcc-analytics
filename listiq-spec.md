# ListIQ — Watchlist Change Intelligence
### Module Specification | Part of Illuminate AFC

---

## Overview

ListIQ automatically downloads, stores, and compares OFAC sanctions watchlists daily. It surfaces additions, deletions, and modifications in a compliance-focused UI so users can immediately understand what changed on any given day.

---

## Architecture

### Tech Stack
- **Backend:** FastAPI (Python) — consistent with ScreenIQ
- **Database:** SQLite via SQLAlchemy ORM — PostgreSQL-ready
- **Scheduler:** APScheduler with CronTrigger
- **Frontend:** React + Vite — shared with ScreenIQ
- **Data Source:** OFAC SDN XML

### Database Notes
- SQLAlchemy ORM only — no raw SQL
- All models database-agnostic (SQLite now, PostgreSQL later)
- Alembic migrations from day one
- Shared database with ScreenIQ: `backend/app/db/platform.db`
- Table prefix: `listiq_*`

---

## Watchlists

| List | Source | Format | Status |
|------|--------|--------|--------|
| OFAC SDN | US Treasury | XML | ✅ Phase 1 |
| OFAC Non-SDN | US Treasury | XML | 🔜 Phase 2 |
| EU Consolidated | European Union | XML | 🔜 Phase 2 |
| HMT | UK Treasury | CSV | 🔜 Phase 2 |
| Japan Economic Sanctions | Japanese Government | PDF | 🔜 Phase 3 |

---

## Data Model

### `listiq_snapshots`
One row per daily download.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto increment |
| list_name | TEXT | e.g. "OFAC_SDN" |
| snapshot_date | DATE | Date of download |
| raw_file_hash | TEXT | SHA256 of raw file |
| record_count | INTEGER | Total records in snapshot |
| created_at | DATETIME | Timestamp of ingestion |

### `listiq_records`
One row per record per snapshot.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto increment |
| snapshot_id | INTEGER FK | Links to listiq_snapshots |
| list_name | TEXT | e.g. "OFAC_SDN" |
| record_uid | TEXT | OFAC UID — primary key for diffing |
| record_type | TEXT | Individual, Entity, Vessel, Aircraft |
| primary_name | TEXT | Primary name on record |
| akas | TEXT | JSON array of AKA names |
| ids | TEXT | JSON array of ID documents |
| addresses | TEXT | JSON array of addresses |
| programs | TEXT | JSON array of sanctions programs |
| raw_data | TEXT | Full JSON of original record |
| snapshot_date | DATE | Denormalized for query performance |

### `listiq_changes`
One row per detected change.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto increment |
| list_name | TEXT | e.g. "OFAC_SDN" |
| change_date | DATE | Date change was detected |
| record_uid | TEXT | Record that changed |
| change_type | TEXT | ADDITION, DELETION, MODIFICATION |
| modification_fields | TEXT | JSON array of changed field names |
| before_data | TEXT | JSON of record before change (null for additions) |
| after_data | TEXT | JSON of record after change (null for deletions) |
| created_at | DATETIME | When diff was computed |

### `listiq_config`
Stores scheduler and app configuration.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto increment |
| key | TEXT | Config key |
| value | TEXT | Config value |
| updated_at | DATETIME | Last updated |

Default config entries:
- `sync_hour` = "6"
- `sync_minute` = "0"
- `sync_enabled` = "true"

---

## Backend API Endpoints

All routes prefixed with `/listiq/`

### Sync
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/listiq/sync/trigger` | Manually trigger sync now |
| GET | `/listiq/sync/status` | Last sync time, status, record count |
| GET | `/listiq/sync/schedule` | Get current schedule config |
| PUT | `/listiq/sync/schedule` | Update sync schedule |

### Changes
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/listiq/changes` | All changes with optional filters: date, type, list |
| GET | `/listiq/changes/{change_date}` | All changes for a specific date |
| GET | `/listiq/changes/summary/{change_date}` | Counts by type for a date |
| GET | `/listiq/changes/{id}` | Full detail for a single change |

### Records
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/listiq/records/{record_uid}/history` | Full change history for a record |
| GET | `/listiq/snapshots` | List all snapshots |

---

## Scheduler Logic

Use `APScheduler` with `BackgroundScheduler` and `CronTrigger`.

- **Default:** Daily at 6:00 AM server time
- **Configurable:** Via settings UI and PUT /listiq/sync/schedule
- **On app startup:** If today's snapshot doesn't exist, trigger sync immediately

### Sync Steps (in order):
1. Download OFAC SDN XML from Treasury URL
2. SHA256 hash the file
3. If hash matches yesterday's snapshot hash → skip, log "no changes detected"
4. Parse XML into structured records
5. Store new row in `listiq_snapshots`
6. Store all records in `listiq_records`
7. Query yesterday's records, compute diff against today's
8. Write all changes to `listiq_changes`
9. Log result with counts: X additions, Y deletions, Z modifications

### Error Handling:
- If download fails → log error, do NOT write snapshot, surface in UI sync status
- If parsing fails → same as above
- If diff fails → log error, snapshot is saved but changes may be incomplete — surface warning in UI

---

## Diff Logic

Compare today's `listiq_records` against yesterday's using `record_uid` as primary key.

### Change Type Detection:
1. **ADDITION** — `record_uid` in today's snapshot but not yesterday's
2. **DELETION** — `record_uid` in yesterday's snapshot but not today's
3. **MODIFICATION** — `record_uid` in both snapshots but one or more fields differ

### Modification Field Priority (store and display in this order):
1. Name (`primary_name`)
2. AKA (`akas`)
3. ID (`ids`)
4. Address (`addresses`)
5. Other (`programs`, `record_type`, etc.)

A single record modification can affect multiple fields — capture all of them in `modification_fields`.

---

## Frontend UI

### Screen 1: ListIQ Dashboard (`/listiq`)

**Summary Panel (top)**
- Date picker — defaults to today
- Three count cards:
  - 🟢 **Additions** (count) — green
  - 🔴 **Deletions** (count) — red
  - 🟡 **Modifications** (count) — amber
- Last sync timestamp + status indicator
- "Sync Now" button with loading spinner

**Change Log Table (below summary)**

| Column | Description |
|--------|-------------|
| Change Type | Color-coded badge: ADDITION / DELETION / MODIFICATION |
| Record UID | OFAC UID |
| Primary Name | Name on the record |
| Record Type | Individual / Entity / Vessel / Aircraft |
| Modified Fields | Comma list of changed fields (for modifications only) |
| Programs | Sanctions programs |
| Date | Change date |

- Rows color coded: green border for additions, red for deletions, amber for modifications
- Sortable columns
- Filter by: Change Type, Record Type, Date range
- Paginated (25 rows per page)
- Click any row → opens Change Detail View

### Screen 2: Change Detail View (modal or slide-over)

**For Additions:**
- Full record details displayed cleanly
- Header: "New Record Added" in green

**For Deletions:**
- Full record details as they existed before removal
- Header: "Record Removed" in red

**For Modifications:**
- Side by side diff view
- Left panel: "Yesterday" — record as of previous snapshot
- Right panel: "Today" — record as of current snapshot
- Changed fields highlighted in amber
- Unchanged fields shown in muted gray
- Modified fields shown in priority order: Name → AKA → ID → Address → Other
- Header: "Record Modified" in amber

### Screen 3: Settings (`/listiq/settings`)

- **Sync Schedule** — displayed as human readable: "Daily at 6:00 AM"
- Time picker to change hour and minute
- Toggle to enable/disable automatic sync
- Save button
- "Sync Now" button with status feedback
- Sync history log — last 10 syncs with timestamp, status, record count, change counts

---

## Implementation Notes for Claude Code

1. **Start with backend** — OFAC XML download, parsing, and DB models before touching frontend
2. **OFAC SDN XML key fields:** `<sdnEntry>`, `<uid>`, `<lastName>`, `<sdnType>`, `<akaList>`, `<idList>`, `<addressList>`, `<programList>`
3. **SQLAlchemy ORM only** — no raw SQL, all models in `backend/app/models/listiq_models.py`
4. **Alembic** — create migration for listiq tables before writing any DB code
5. **APScheduler** — initialize in `main.py` startup event, load schedule from `listiq_config` table
6. **Hash check** — always compare file hash before processing to avoid unnecessary diffs
7. **Prefix all routes** with `/listiq/` — no conflicts with ScreenIQ routes
8. **Frontend routes** — add `/listiq` and `/listiq/settings` to React Router in `App.jsx`
9. **Hub integration** — ListIQ sync status card on Hub page calls `/listiq/sync/status`
10. **Back navigation** — all ListIQ pages have "← Illuminate AFC" link back to hub

---

## Phase 2 (Future)
- OFAC Non-SDN list
- EU Consolidated list
- HMT list
- Email/Slack alerts on changes
- Export change log to CSV or PDF
- Full text search across snapshots
- Record timeline view — complete change history for a single entity over time
