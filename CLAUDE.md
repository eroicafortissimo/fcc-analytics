# Screening Validation Platform

## What This Is
A full-stack sanctions screening validation platform with three features:
1. List Explorer — downloads, cleans, and analyzes OFAC/EU/HMT/BIS/Japan sanctions lists
2. Test Case Generator — creates intelligent name variation test cases for screening system validation
3. Results Interpreter — analyzes screening results (TP/FP/TN/FN) and explains misses

## Tech Stack
- Frontend: React (Vite), React Router, Recharts, Tailwind CSS, Axios
- Backend: Python/FastAPI
- LLM: Claude API via LangChain/LangGraph
- Data: Pandas, SQLite (aiosqlite), openpyxl
- Charts: Recharts

## Project Layout
```
screening-validation-platform/
├── frontend/          # Vite + React SPA
├── backend/
│   ├── app/
│   │   ├── main.py              # FastAPI entry point
│   │   ├── routers/             # lists.py, testcases.py, results.py
│   │   ├── services/            # Business logic
│   │   ├── models/schemas.py    # Pydantic models
│   │   ├── data/                # test_case_types.csv, cached XMLs
│   │   └── db/                  # SQLite databases
│   ├── requirements.txt
│   └── .env                     # ANTHROPIC_API_KEY (never commit)
└── CLAUDE.md
```

## Agentic Components (LangGraph)
Three LangGraph workflows:
1. `nationality_chain.py` — 3-tier inference (data → heuristic → LLM)
2. `chatbot_agent.py` — natural language test case type creator with human-in-the-loop
3. `miss_analyzer.py` — FN analysis engine

## Key Conventions
- All API keys in backend/.env — never hardcode
- Test case types defined in backend/app/data/test_case_types.csv
- SWIFT messages must be valid ISO 20022 XML (pacs.008, pacs.009)
- Cache LLM results to minimize API costs (SQLite cache table)
- Use proper error handling for list download failures — partial success is OK
- SQLite db path: backend/app/db/platform.db

## Watchlist Sources
| List | URL |
|------|-----|
| OFAC SDN | https://www.treasury.gov/ofac/downloads/sdn.xml |
| OFAC Non-SDN | https://www.treasury.gov/ofac/downloads/consolidated/consolidated.xml |
| EU Consolidated | https://webgate.ec.europa.eu/fsd/fsf/public/files/xmlFullSanctionsList_1_1/content |
| UK HMT | https://ofsistorage.blob.core.windows.net/publishlive/2022format/ConList.csv |
| BIS Entity List | https://www.bis.doc.gov/index.php/policy-guidance/lists-of-parties-of-concern/entity-list |
| Japan METI | https://www.meti.go.jp/policy/anpo/law_hu\xfca.html |

## Dev Commands
- Backend: `cd backend && uvicorn app.main:app --reload --port 8000`
- Frontend: `cd frontend && npm run dev`
- Install backend: `pip install -r backend/requirements.txt`
- Install frontend: `cd frontend && npm install`
