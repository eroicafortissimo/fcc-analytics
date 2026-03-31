# Illuminate AFC — Platform Specification
### Full Product Suite

---

## Vision

Illuminate AFC is a suite of AI-powered compliance tools built for small and mid-size banks and fintechs. Each tool is a standalone module accessible from a central hub. The platform is built on a shared codebase, shared database, and shared design system.

**Tagline:** Intelligent sanctions and AML compliance tooling.

---

## Product Suite

| Module | Status | Description |
|--------|--------|-------------|
| **ScreenIQ** | ✅ Built | Sanctions screening validation — downloads watchlists, generates intelligent name variation test cases, interprets screening results |
| **ListIQ** | 🔜 In Progress | Watchlist change intelligence — daily diff of OFAC and global sanctions lists |
| **RuleIQ** | 🔜 Planned | Transaction monitoring rule validation and threshold calibration |
| **RiskIQ** | 🔜 Planned | Customer risk rating model validation and bias testing |
| **AlertIQ** | 🔜 Planned | False positive rate analysis and alert quality scoring |
| **SARIQ** | 🔜 Planned | AI-assisted SAR narrative generation |

---

## Platform Architecture

### Shared Tech Stack

| Layer | Technology |
|-------|------------|
| Frontend | React (Vite), React Router, Tailwind CSS, Recharts, Axios |
| Backend | Python / FastAPI |
| Database | SQLite (SQLAlchemy ORM) → PostgreSQL-ready |
| LLM | Claude API (Anthropic) via LangChain / LangGraph |
| Data Processing | Pandas, openpyxl |
| Scheduler | APScheduler |
| Migrations | Alembic |

### Database Strategy
- Single SQLite database file: `backend/app/db/platform.db`
- Each module uses prefixed tables: `screeniq_*`, `listiq_*`, `ruleiq_*` etc.
- SQLAlchemy ORM throughout — no raw SQL anywhere
- Alembic migrations from day one
- PostgreSQL swap requires only a config change (DATABASE_URL in .env)

### Project Structure
```
illuminate-afc/
├── frontend/
│   ├── src/
│   │   ├── pages/
│   │   │   ├── Hub.jsx              # Illuminate AFC landing page
│   │   │   ├── screeniq/            # ScreenIQ module pages
│   │   │   └── listiq/              # ListIQ module pages
│   │   ├── components/
│   │   │   ├── shared/              # Shared UI components
│   │   │   ├── screeniq/            # ScreenIQ-specific components
│   │   │   └── listiq/              # ListIQ-specific components
│   │   └── App.jsx                  # Router: / → Hub, /screeniq → ScreenIQ, /listiq → ListIQ
├── backend/
│   ├── app/
│   │   ├── main.py                  # FastAPI entry point
│   │   ├── routers/
│   │   │   ├── screeniq/            # lists.py, testcases.py, results.py
│   │   │   └── listiq/              # sync.py, changes.py, records.py
│   │   ├── services/
│   │   │   ├── screeniq/            # Existing ScreenIQ services
│   │   │   └── listiq/              # ListIQ services
│   │   ├── models/
│   │   │   ├── screeniq_models.py
│   │   │   └── listiq_models.py
│   │   ├── db/
│   │   │   └── platform.db          # Shared SQLite database
│   │   └── data/
│   │       └── test_case_types.csv  # ScreenIQ test case definitions
│   ├── alembic/                     # Database migrations
│   ├── requirements.txt
│   └── .env                         # API keys — never commit
├── CLAUDE.md                        # Claude Code context file
├── illuminate-afc-platform-spec.md  # This file
└── listiq-spec.md                   # ListIQ detailed spec
```

### Routing Convention

| Route | Module |
|-------|--------|
| `/` | Illuminate AFC Hub |
| `/screeniq` | ScreenIQ (previously `/`) |
| `/screeniq/lists` | List Explorer |
| `/screeniq/testcases` | Test Case Generator |
| `/screeniq/results` | Results Interpreter |
| `/listiq` | ListIQ Dashboard |
| `/listiq/settings` | ListIQ Settings |

### API Route Convention

| Prefix | Module |
|--------|--------|
| `/screeniq/*` | ScreenIQ endpoints |
| `/listiq/*` | ListIQ endpoints |
| `/health` | Platform health check |

---

## Illuminate AFC Hub (Landing Page)

### Design
- Clean, professional landing page
- Illuminate AFC logo and tagline
- Module cards in a grid layout
- Each card shows: module name, description, status, and a link

### Module Card — ScreenIQ
- **Name:** ScreenIQ
- **Description:** Validate your sanctions screening system. Download global watchlists, generate intelligent name variation test cases, and analyze your system's performance.
- **Status:** Active
- **Link:** /screeniq

### Module Card — ListIQ
- **Name:** ListIQ
- **Description:** Track daily changes across OFAC and global sanctions watchlists. Surface additions, deletions, and modifications the moment they happen.
- **Status:** Active
- **Last Sync:** Show timestamp and status from ListIQ sync API
- **Link:** /listiq

### Future Module Cards
- Display planned modules (RuleIQ, RiskIQ, etc.) as "Coming Soon" cards

---

## ScreenIQ Module (Existing)

ScreenIQ is fully built. See `OFAC_Screening_Validation_Platform_Spec.md` for full details.

### Summary of Features
1. **List Explorer** — Downloads and cleans OFAC SDN, OFAC Non-SDN, EU, HMT, BIS, Japan lists. Infers nationality via 3-tier LangGraph chain. Interactive table with filters and charts.
2. **Test Case Generator** — Generates name variation test cases (250 per type, 20+ variation types). Culture distribution controls. LangGraph chatbot for creating new test case types. Exports to Excel, SWIFT pacs.008/009, FUF format.
3. **Results Interpreter** — Uploads screening results, computes confusion matrix (TP/FP/TN/FN), visualizes performance, uses LangGraph miss analysis engine to explain false negatives.

### Migration Notes (Hub Integration)
- Move existing ScreenIQ frontend routes from `/` to `/screeniq`
- Prefix all backend API routes with `/screeniq/`
- Update Axios base URLs in frontend
- Add "← Back to Illuminate AFC" navigation link in ScreenIQ header

---

## ListIQ Module

See `listiq-spec.md` for full details.

### Summary
Daily automated download and diff of OFAC SDN watchlist (Phase 1). Surfaces additions, deletions, and modifications in a compliance-focused UI with summary dashboard, change log table, and side-by-side diff view.

---

## Shared Design System

### Colors
- **Primary:** Deep navy (#1a2744) — trust, compliance
- **Accent:** Electric blue (#3b82f6) — modern, AI-forward
- **Success/Addition:** Green (#22c55e)
- **Danger/Deletion:** Red (#ef4444)
- **Warning/Modification:** Amber (#f59e0b)
- **Background:** Light gray (#f8fafc)

### Typography
- Headers: Inter or similar clean sans-serif
- Body: Same family, regular weight
- Monospace: For record UIDs and diff views

### Components (shared across modules)
- `<PageHeader>` — module title, breadcrumb back to hub
- `<StatusBadge>` — color coded ADDITION / DELETION / MODIFICATION
- `<DataTable>` — sortable, filterable, paginated table
- `<SyncStatus>` — last sync time and trigger button
- `<LoadingSpinner>` — consistent loading states
- `<ErrorBanner>` — consistent error display

---

## CLAUDE.md (Master — for Claude Code Sessions)

```
# Illuminate AFC Platform

## What This Is
A suite of AI-powered AFC compliance tools for small and mid-size banks.
Current modules: ScreenIQ (screening validation), ListIQ (watchlist change tracking)

## Tech Stack
- Frontend: React (Vite), React Router, Tailwind CSS, Recharts, Axios
- Backend: Python/FastAPI
- LLM: Claude API via LangChain/LangGraph
- Database: SQLite via SQLAlchemy ORM (PostgreSQL-ready)
- Scheduler: APScheduler
- Migrations: Alembic

## Project Layout
illuminate-afc/
├── frontend/src/pages/Hub.jsx        # Landing page
├── frontend/src/pages/screeniq/      # ScreenIQ pages
├── frontend/src/pages/listiq/        # ListIQ pages
├── backend/app/routers/screeniq/     # ScreenIQ API routes
├── backend/app/routers/listiq/       # ListIQ API routes
├── backend/app/services/screeniq/    # ScreenIQ business logic
├── backend/app/services/listiq/      # ListIQ business logic
├── backend/app/db/platform.db        # Shared SQLite DB
└── backend/.env                      # API keys — never commit

## Key Conventions
- All API keys in backend/.env — never hardcode
- SQLAlchemy ORM only — no raw SQL
- Table prefixes: screeniq_*, listiq_*
- Route prefixes: /screeniq/*, /listiq/*
- Alembic for all DB migrations
- Cache LLM results to minimize API costs
- SWIFT messages must be valid ISO 20022 XML

## Dev Commands
- Backend: cd backend && uvicorn app.main:app --reload --port 8000
- Frontend: cd frontend && npm run dev -- --host
- DB migrations: cd backend && alembic upgrade head

## Watchlist Sources
| List | URL |
|------|-----|
| OFAC SDN | https://www.treasury.gov/ofac/downloads/sdn.xml |
| OFAC Non-SDN | https://www.treasury.gov/ofac/downloads/consolidated/consolidated.xml |
| EU Consolidated | https://webgate.ec.europa.eu/fsd/fsf/public/files/xmlFullSanctionsList_1_1/content |
| UK HMT | https://ofsistorage.blob.core.windows.net/publishlive/2022format/ConList.csv |
| BIS Entity List | https://www.bis.doc.gov/index.php/policy-guidance/lists-of-parties-of-concern/entity-list |
| Japan METI | https://www.meti.go.jp/policy/anpo/law_hufreelist.html |
```

---

## Build Order (Platform Migration + ListIQ)

1. **Restructure project** — rename folders, update routes, add Hub landing page
2. **Migrate ScreenIQ routes** — prefix frontend and backend routes with /screeniq
3. **Build ListIQ backend** — OFAC download, parsing, DB models, diff logic, scheduler
4. **Build ListIQ frontend** — dashboard, change log table, diff view, settings
5. **Polish Hub** — add ListIQ sync status card, wire up navigation
6. **Test end to end** — both modules accessible from hub, no broken routes
