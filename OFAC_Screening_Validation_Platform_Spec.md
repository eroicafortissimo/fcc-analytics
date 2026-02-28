# OFAC Screening Validation Platform — Project Specification

## Overview

Build a full-stack web application that serves as a **sanctions screening validation platform**. The tool downloads and cleans multiple international sanctions/watchlists, generates intelligent test cases with name variations, and interprets screening results returned by a bank after running the test data through their screening system.

This is a portfolio project built with **LangChain/LangGraph** for the agentic components and **Claude API** as the LLM. Use **Claude Code** to build it.

---

## Tech Stack

| Layer | Technology |
|---|---|
| **Frontend** | React (single-page app, 3 routes + home) |
| **Backend** | Python / FastAPI |
| **LLM Orchestration** | LangChain + LangGraph (agentic workflows with state management) |
| **LLM Provider** | Claude API (Anthropic) |
| **Data Processing** | Pandas |
| **Export** | openpyxl for Excel, custom formatters for SWIFT message formats |
| **Database** | SQLite for local persistence (test cases, results, list snapshots) |
| **Charts** | Recharts or Plotly (interactive, filterable) |

---

## Architecture Notes

- The app has three core features accessed from a home page: **List Explorer**, **Test Case Generator**, **Results Interpreter**
- Agentic components (powered by LangGraph) exist in three places:
  1. **Nationality/Country inference chain** in List Explorer (data → heuristic → LLM fallback)
  2. **Chatbot for creating new test case types** in Test Case Generator (natural language → structured test definition → human validation → generation)
  3. **Miss analysis engine** in Results Interpreter (LLM reasons about why expected hits didn't match)
- All other logic is deterministic Python (ETL, variation generation, export formatting, statistics)
- LangGraph workflows should have proper state management, checkpoints, and clear node/edge definitions
- Store API keys in environment variables, never hardcode

---

## PAGE 1: Home Page

A clean landing page with:
- App title: **"Screening Validation Platform"** (or similar)
- Brief description of what the tool does
- Three cards/buttons linking to:
  1. **List Explorer** — Browse and analyze global sanctions lists
  2. **Test Case Generator** — Create screening test data with intelligent name variations
  3. **Results Interpreter** — Analyze screening results and identify gaps

---

## PAGE 2: List Explorer

### Workflow

When the user navigates to this page, the system:

1. **Downloads** the latest lists from these sources:
   - **OFAC SDN** (Specially Designated Nationals) — XML from Treasury
   - **OFAC Consolidated Non-SDN** — XML from Treasury
   - **EU Consolidated Sanctions** — XML from EU
   - **UK HMT** (His Majesty's Treasury) — CSV/XML from UK gov
   - **BIS Entity List** (Bureau of Industry and Security) — from Commerce Dept
   - **Japan METI** (Ministry of Economy, Trade and Industry) — sanctions list

2. **Cleans** each list:
   - Standardizes name fields
   - Removes duplicates
   - Normalizes character encodings (handle Arabic, Cyrillic, CJK, etc.)
   - Parses structured fields (dates, IDs, addresses)

3. **Infers Nationality/Country/Region** using a three-tier agentic chain (BUILD THIS AS A LANGGRAPH WORKFLOW):
   - **Tier 1 — Data lookup:** Check if nationality, country, or citizenship is explicitly in the list data
   - **Tier 2 — Heuristic analysis:** If not present, analyze qualities of the name (script used, phonetic patterns, common name origins, associated addresses/documents in the record)
   - **Tier 3 — LLM fallback:** If heuristic is inconclusive, send the name and any available context to Claude and have the LLM determine the most likely nationality/region with a confidence score

4. **Displays** the consolidated data in an interactive table

### Table Columns

| Column | Description |
|---|---|
| Watchlist | OFAC SDN, OFAC Non-SDN, EU, HMT, BIS, Japan |
| Sub-Watchlist 1 | Program name (e.g., "SDGT", "IRAN", "CYBER2") |
| Sub-Watchlist 2 | Secondary classification if applicable (may be blank) |
| Cleaned Name | Standardized, cleaned version of the name |
| Original Name | Raw name as it appears in the source data |
| Primary/AKA | Whether this is a primary name or an alias |
| Entity Type | Individual, Entity, Country, Vessel, Aircraft |
| # of Tokens | Number of space-separated tokens in the cleaned name |
| Name Length | Character count of the cleaned name |
| Nationality/Country/Region | Determined via the 3-tier inference chain above |
| Date of Listing | When the entry was added to the list |
| Recently Modified | Flag if entry was added or modified in the last 90 days |
| Sanctions Program/Regime | The specific sanctions program (for prioritizing testing on high-risk programs) |

### Filters and Search

- Dropdown filters for: Watchlist, Sub-Watchlist, Entity Type, Nationality/Country/Region
- Free-text search bar that searches across Cleaned Name and Original Name
- All filters and search should be combinable

### Interactive Analytics

- Auto-updating charts that respond to the active filters:
  - **Distribution by Watchlist** (bar chart)
  - **Distribution by Entity Type** (pie/donut chart)
  - **Distribution by Nationality/Region** (horizontal bar chart, top 20)
  - **Name Length Distribution** (histogram)
  - **Token Count Distribution** (histogram)
  - **Recently Modified entries** (count + trend if historical data available)
- Charts should be interactive (hover for details, click to filter)

---

## PAGE 3: Test Case Generator

### Test Case Types

- Test case types are defined in a **CSV file** in the codebase (e.g., `test_case_types.csv`)
- Each row defines a test case type with fields like: `type_id`, `type_name`, `description`, `applicable_entity_types`, `applicable_min_tokens`, `applicable_min_name_length`, `variation_logic`
- Examples of test case types (non-exhaustive — build a comprehensive set of at least 20-25 types):
  - Exact match
  - First/last name transposition
  - Missing middle name
  - Truncation (first N characters)
  - Common misspelling / typo injection
  - Phonetic equivalent (Soundex/Metaphone-style)
  - Transliteration variant (Arabic → Latin, Cyrillic → Latin, etc.)
  - Character substitution (e.g., Mohammad → Muhammad → Mohammed)
  - Abbreviated first name (e.g., Robert → Rob, William → Wm)
  - Name with title/honorific added or removed
  - Name with punctuation variations (hyphens, apostrophes, periods)
  - Partial name match (first name only, last name only)
  - Additional token noise (extra middle names, suffixes)
  - Combined variations (e.g., transposition + misspelling)
  - Vessel/Aircraft specific: IMO number variation, flag state change
  - Entity-specific: abbreviation of company type (Ltd → Limited, Corp → Corporation)
  - Nickname substitution
  - Spacing variations (removed spaces, extra spaces, merged tokens)
  - Diacritical mark removal or addition
  - Script conversion (e.g., simplified vs traditional Chinese)
  - Initials only
  - Reversed word order for multi-part names

### Generation Logic

- Default: generate **250 test cases per test case type** (user can adjust this number via a control)
- When generating 250 (or N) test cases for a given type:
  - Pull names from the consolidated watchlist data (from List Explorer)
  - **Ensure wide distribution across**: short names, long names, many tokens, few tokens, diverse cultures/nationalities, different entity types, different watchlists
  - **Skip names that are not applicable** for the given test case type (e.g., "truncation" doesn't work well on 2-character names; "transliteration" requires names originally in non-Latin script). Track and surface which names were skipped and why.
  - Apply the variation logic to produce the test name
- For each generated test case, also create an **expected result**: should this test case generate a hit against the original name? Include a brief rationale (e.g., "Expected HIT — phonetic equivalent should match within 80% threshold" or "Expected MISS — truncation to 3 characters loses too much information for reliable matching")

### Culture Distribution Control

- The user can select or adjust the distribution of cultures/nationalities to include in the test set
- Options: balanced (equal representation), weighted by list composition, or custom percentages
- This ensures the test set exercises the screening system across diverse naming conventions

### Chatbot for New Test Case Types (LANGGRAPH AGENTIC WORKFLOW)

- A chat interface on this page where the user can describe a new test case type in plain English
- Example: "Create a test case where you take the first two letters of each token in a multi-token name and concatenate them"
- The agent should:
  1. Parse the user's natural language description
  2. Propose a structured test case definition (type_name, description, variation_logic as pseudocode/rules, applicable constraints)
  3. Show the user 3-5 example variations generated from real watchlist names
  4. Ask the user to validate or refine
  5. Once confirmed, add the new test case type to the active set and generate test cases for it
- Build this as a LangGraph workflow with a human-in-the-loop validation checkpoint

### Export Formats

The user selects an export format before generating:

1. **Names Only** — Simple export with test name and metadata (Excel)
2. **Raw SWIFT Message Format** — Test names embedded in properly formatted SWIFT messages:
   - **pacs.008** (Customer Credit Transfer) — place names in Debtor/Creditor name fields, also vary placement across other fields (address, account name)
   - **pacs.009** (Financial Institution Credit Transfer) — place names in ordering/beneficiary institution fields
   - Generate valid XML structure for ISO 20022 messages
3. **FUF Format (Fircosoft/Firco Universal Format)** — Firco-compatible message format equivalent of the pacs.008 and pacs.009 messages above
   - Match the field mappings that Fircosoft screening systems expect
   - Include proper FUF headers, field tags, and message structure

For formats 2 and 3: distribute test names across a variety of message fields (not just one field) to test that the screening system is scanning all relevant fields.

### Test Case Output Table

Display generated test cases in a table with these columns:

| Column | Description |
|---|---|
| Test Case ID | Unique sequential ID |
| Test Case Type | The variation type applied (embedded/shown in cell) |
| Watchlist | Source watchlist of the original name |
| Sub-Watchlist | Program/sub-list |
| Cleaned Original Name | The cleaned version of the original watchlist name |
| Original Original Name | The raw name from the source list |
| Culture/Nationality | Of the original name |
| Test Name | The generated variation |
| Primary/AKA | Whether the source was a primary name or alias |
| Entity Type | Individual, Entity, Vessel, etc. |
| # of Tokens | Token count of the test name |
| Name Length | Character length of the test name |
| Expected Result | HIT or MISS |
| Expected Result Rationale | Brief explanation of why hit/miss is expected |

---

## PAGE 4: Results Interpreter

### Input

- The user uploads a file (CSV/Excel) containing the screening results from their bank's system
- The uploaded data should contain: Test Case ID, Test Name, Expected Result (HIT/MISS), Actual Result (HIT/MISS), and optionally: match score, matched list entry, alert details
- The system joins the uploaded results with the generated test case data using Test Case ID

### High-Level Statistics Dashboard

- **Confusion matrix**: TP, FP, TN, FN counts and rates
- **Overall detection rate** (TP / (TP + FN))
- **False positive rate** (FP / (FP + TN))
- **Precision and recall**

### Trend/Breakdown Analytics (interactive, filterable charts)

- **By Test Case Type**: which variation types are being caught vs. missed?
- **By Culture/Nationality**: detection rate broken down by culture (critical for identifying bias or gaps in transliteration handling)
- **By Name Length**: does detection degrade for very short or very long names?
- **By Token Count**: performance across different token counts
- **By Entity Type**: Individual vs. Entity vs. Vessel performance
- **By Watchlist**: performance differences across OFAC vs. EU vs. HMT etc.

### Miss Analysis (LANGGRAPH AGENTIC WORKFLOW)

- For each **False Negative** (expected HIT that was missed):
  - The LLM analyzes the test name vs. the original name
  - Generates a description of **what probably went wrong and why the screening system didn't generate a hit**
  - Consider factors like: character-level similarity score, token overlap, phonetic distance, whether the variation type is known to challenge fuzzy matching algorithms, whether the name length fell below a likely minimum threshold
  - Output: a natural language explanation per missed hit, plus a summary of systemic patterns across all misses
- For each **False Positive** (expected MISS that hit):
  - Brief analysis of why the system may have over-matched (e.g., coincidental partial overlap with a different list entry)

### Export

- Full results table exportable to Excel
- Summary report with charts and miss analysis exportable to PDF or Word (for sharing with model validation teams or examiners)

---

## Project Structure

```
screening-validation-platform/
├── frontend/
│   ├── src/
│   │   ├── components/
│   │   │   ├── Home.jsx
│   │   │   ├── ListExplorer.jsx
│   │   │   ├── TestCaseGenerator.jsx
│   │   │   ├── ResultsInterpreter.jsx
│   │   │   ├── ChatBot.jsx
│   │   │   └── charts/
│   │   ├── App.jsx
│   │   └── index.js
│   └── package.json
├── backend/
│   ├── app/
│   │   ├── main.py                  # FastAPI app
│   │   ├── routers/
│   │   │   ├── lists.py             # List Explorer endpoints
│   │   │   ├── testcases.py         # Test Case Generator endpoints
│   │   │   └── results.py           # Results Interpreter endpoints
│   │   ├── services/
│   │   │   ├── list_downloader.py   # Downloads and cleans all watchlists
│   │   │   ├── list_cleaner.py      # Standardization and normalization
│   │   │   ├── nationality_chain.py # LangGraph: 3-tier nationality inference
│   │   │   ├── test_generator.py    # Test case generation logic
│   │   │   ├── chatbot_agent.py     # LangGraph: new test case type chatbot
│   │   │   ├── results_analyzer.py  # Statistics and confusion matrix
│   │   │   ├── miss_analyzer.py     # LangGraph: miss analysis engine
│   │   │   └── export_service.py    # Excel, SWIFT, FUF formatters
│   │   ├── models/
│   │   │   └── schemas.py           # Pydantic models
│   │   └── data/
│   │       └── test_case_types.csv  # Pre-defined test case type definitions
│   ├── requirements.txt
│   └── .env                         # ANTHROPIC_API_KEY
├── CLAUDE.md                        # Project context for Claude Code sessions
└── README.md
```

---

## LangGraph Workflow Definitions

### Workflow 1: Nationality Inference Chain (`nationality_chain.py`)

```
State: { name, record_context, nationality, confidence, method_used }

Nodes:
  [data_lookup]     → Check explicit nationality/country fields in record
  [heuristic]       → Analyze name script, phonetics, associated addresses
  [llm_inference]   → Send to Claude with context, get nationality + confidence
  [output]          → Return final result

Edges:
  data_lookup → output          (if nationality found in data, confidence: HIGH)
  data_lookup → heuristic       (if not found)
  heuristic → output            (if confidence > threshold)
  heuristic → llm_inference     (if inconclusive)
  llm_inference → output
```

### Workflow 2: Test Case Type Chatbot (`chatbot_agent.py`)

```
State: { user_message, proposed_definition, examples, user_confirmed, test_type_record }

Nodes:
  [parse_request]       → LLM interprets natural language description
  [generate_definition] → Create structured test case type definition
  [generate_examples]   → Apply definition to 3-5 real watchlist names
  [present_to_user]     → Show definition + examples, ask for validation
  [refine]              → If user requests changes, update definition
  [finalize]            → Add to active test case types, trigger generation

Edges:
  parse_request → generate_definition
  generate_definition → generate_examples
  generate_examples → present_to_user
  present_to_user → finalize           (if user confirms)
  present_to_user → refine             (if user requests changes)
  refine → generate_definition         (loop back)
```

### Workflow 3: Miss Analysis Engine (`miss_analyzer.py`)

```
State: { false_negatives[], analysis_results[], systemic_patterns }

Nodes:
  [analyze_individual] → For each FN: compare test name vs original, 
                          assess similarity metrics, identify likely failure mode
  [identify_patterns]  → Aggregate individual analyses, find systemic gaps
                          (e.g., "Arabic transliterations missed 73% of the time")
  [generate_report]    → Produce structured summary with recommendations

Edges:
  analyze_individual → identify_patterns
  identify_patterns → generate_report
```

---

## Key Implementation Notes

1. **Test case applicability**: Not all watchlist names work for all test case types. The generator must check applicability constraints (min tokens, min name length, applicable entity types, script requirements) BEFORE attempting to generate a variation. Track skipped names and expose this to the user.

2. **Culture distribution**: When sampling 250 names for a test case type, use stratified sampling across nationalities/cultures. If the user selected "balanced," aim for equal representation. If "weighted by list composition," mirror the actual distribution. If custom, follow user percentages.

3. **SWIFT message generation**: pacs.008 and pacs.009 messages must be valid ISO 20022 XML. Use realistic placeholder data for non-name fields (BIC codes, amounts, dates, account numbers). Vary which fields contain the test names across the batch.

4. **FUF format**: Research Fircosoft Universal Format structure. Include proper field tags, message headers, and delimiters. If exact FUF spec is unavailable, create a reasonable approximation based on publicly available documentation and note this in the README.

5. **Results upload**: The system should gracefully handle mismatches between generated test case IDs and uploaded results (e.g., if the bank only ran a subset of test cases).

6. **Performance**: List downloading and cleaning may take time. Show progress indicators. Consider caching cleaned lists in SQLite so the user doesn't re-download every session.

7. **Error handling**: Gracefully handle cases where list sources are temporarily unavailable. Show which lists loaded successfully and which failed.

8. **API costs**: The LLM calls (nationality inference, chatbot, miss analysis) use the Claude API. Be mindful of token usage. Cache nationality inferences so the same name isn't sent to the LLM twice. Batch where possible.

---

## CLAUDE.md (for Claude Code sessions)

When working on this project in Claude Code, use the following as your CLAUDE.md:

```
# Screening Validation Platform

## What This Is
A full-stack sanctions screening validation platform with three features:
1. List Explorer — downloads, cleans, and analyzes OFAC/EU/HMT/BIS/Japan sanctions lists
2. Test Case Generator — creates intelligent name variation test cases for screening system validation
3. Results Interpreter — analyzes screening results (TP/FP/TN/FN) and explains misses

## Tech Stack
- Frontend: React
- Backend: Python/FastAPI
- LLM: Claude API via LangChain/LangGraph
- Data: Pandas, SQLite, openpyxl
- Charts: Recharts or Plotly

## Agentic Components (LangGraph)
Three LangGraph workflows:
1. nationality_chain.py — 3-tier inference (data → heuristic → LLM)
2. chatbot_agent.py — natural language test case type creator with human-in-the-loop
3. miss_analyzer.py — FN analysis engine

## Key Conventions
- All API keys in .env
- Test case types defined in backend/app/data/test_case_types.csv
- SWIFT messages must be valid ISO 20022 XML
- Cache LLM results to minimize API costs
- Use proper error handling for list download failures
```

---

## Build Order (Suggested)

1. **Backend: List downloading and cleaning** — get OFAC SDN working first, then add other lists one at a time
2. **Backend: Nationality inference chain** — build the LangGraph workflow
3. **Frontend: List Explorer** — table, filters, charts
4. **Backend: Test case type CSV** — define all 20-25 types with applicability rules
5. **Backend: Test case generation logic** — sampling, variation application, expected results
6. **Frontend: Test Case Generator** — table, export controls, culture distribution
7. **Backend: Chatbot agent** — LangGraph workflow for new test case types
8. **Frontend: Chatbot component** — chat UI on Test Case Generator page
9. **Backend: Export formatters** — Excel, pacs.008/009, FUF
10. **Backend: Results analyzer** — statistics, confusion matrix
11. **Backend: Miss analysis engine** — LangGraph workflow
12. **Frontend: Results Interpreter** — upload, dashboard, charts, miss explanations
13. **Polish: Error handling, loading states, caching, README**
