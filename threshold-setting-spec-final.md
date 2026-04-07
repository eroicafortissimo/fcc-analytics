# Threshold Setting — Transaction Monitoring Rule Calibration
### Module Specification | Part of Illuminate AFC

---

## Overview

Threshold Setting is a module within the Illuminate AFC platform that enables compliance teams to upload transaction data, define AML detection scenarios, perform statistical analysis on filtered transactions, and calibrate monitoring thresholds using data-driven evidence.

The tool supports both manual rule definition and AI-assisted scenario creation via natural language prompts. Users can analyze transactions at the individual transaction level or in aggregate (by day, rolling week, or rolling month), choose any column as an analysis parameter, simulate alert volumes at multiple threshold values, and export results with a written summary report.

**Tagline:** Data-driven threshold calibration for transaction monitoring rules.

---

## Core Workflow

```
Upload Transactions → Define Scenario → Filter Matching Transactions → Choose Analysis Type → Statistical Analysis → Threshold Simulation → Export Report
```

### Step 1: Upload Transaction Data
- Accept CSV, Excel (.xlsx/.xls), or direct database connection
- User maps columns on upload (or system auto-detects common field names)
- Validate data quality: flag missing values, invalid dates, negative amounts
- Store uploaded dataset in session (not permanently unless user chooses to save)
- Display upload summary: row count, date range, transaction types found, column list

### Step 2: Define AML Detection Scenario
Three modes:

**Predefined Scenario Mode:**
- User selects from a library of pre-built AML detection scenarios (see Predefined Scenarios below)
- Each scenario has a configuration pane split into two sections:
  - **Tunable Parameters** — values the user adjusts to calibrate the rule (thresholds, time windows, counts). These are the parameters being tested.
  - **Non-Tunable Parameters** — fixed filters that define the scope of the scenario (transaction type, jurisdiction list). These are not subject to threshold analysis — they define WHAT transactions are in scope, not WHERE to set the threshold.
- User can modify tunable parameter starting values or leave defaults
- User can view but not change non-tunable parameters (greyed out with lock icon and explanation)
- "Customize" button copies the predefined scenario into Manual Mode for full editing

---

#### Predefined Scenarios

##### Scenario 1: High Value Transactions

**Description:** Identify individual transactions that exceed a high-dollar threshold. Used to detect large one-time transfers that may warrant enhanced review.

**Analysis Type:** Single Transaction

| Parameter | Type | Default Value | Description |
|-----------|------|---------------|-------------|
| Transaction Amount Threshold | Tunable | $10,000 | Minimum dollar amount to flag. This is the value being calibrated. |

No non-tunable parameters — this scenario applies to ALL transaction types, products, and channels. The analysis shows the full distribution of transaction amounts across the entire dataset so the user can determine where to set the cutoff.

**Analysis Parameters:** Amount only
**Distributional View:** 1D tranche table of transaction amounts with percentiles and applicable event counts per tranche.

---

##### Scenario 2: High Risk Jurisdiction Activity

**Description:** Identify customers transacting with high-risk jurisdictions at volumes or frequencies that warrant review. High-risk jurisdictions are defined by FATF grey/black lists and OFAC-sanctioned countries — these are fixed inputs, not thresholds to be tuned.

**Analysis Type:** Aggregate (per customer, rolling 30-day default)

| Parameter | Type | Default Value | Description |
|-----------|------|---------------|-------------|
| Aggregate Amount Threshold | Tunable | $5,000 | Total dollar volume per customer to/from high-risk jurisdictions within the aggregation period. This is the value being calibrated. |
| Aggregate Count Threshold | Tunable | 3 | Number of transactions per customer to/from high-risk jurisdictions within the aggregation period. This is the value being calibrated. |
| Aggregation Period | Tunable | Rolling 30-day | Time window for aggregation (daily, rolling 7-day, rolling 30-day, custom). |
| High-Risk Jurisdiction List | Non-Tunable | FATF Grey/Black + OFAC Sanctioned | Predefined list of countries classified as high-risk. Displayed for transparency but not editable in the scenario pane. User can view the full list. Includes: Iran, North Korea, Syria, Myanmar, etc. |
| Transaction Direction | Non-Tunable | Inbound + Outbound | Both directions included. Not a tunable parameter — the scenario captures all activity with listed jurisdictions regardless of direction. |

**Analysis Parameters:** Amount, Count (2D)
**Distributional View:** 2D cross-tabulation — amount tranches × transaction count tranches. Individual tabs for each parameter plus combined view. Summary stats per cell.

---

##### Scenario 3: Structuring (Cash Only)

**Description:** Identify customers who appear to be structuring cash transactions to avoid the $10,000 CTR filing threshold. Each individual transaction must be below $10,000, but the aggregate across the time window must exceed $10,000. Only cash transactions are in scope.

**Analysis Type:** Aggregate (per customer, rolling 1-day default)

| Parameter | Type | Default Value | Description |
|-----------|------|---------------|-------------|
| Aggregate Amount Threshold | Tunable | $10,000 | Total cash deposited/withdrawn per customer within the aggregation period must EXCEED this value. This is the value being calibrated. |
| Aggregate Count Threshold | Tunable | 2 | Minimum number of cash transactions per customer within the aggregation period. Must be 2+ for structuring (a single transaction cannot be structuring). This is the value being calibrated. |
| Individual Transaction Ceiling | Tunable | $10,000 | Each individual transaction must be BELOW this amount. Transactions at or above this value are excluded because they would trigger a CTR and are not structuring. This is the value being calibrated. |
| Aggregation Period | Tunable | Rolling 1-day | Time window for aggregation. Default is 1 day (same-day structuring) but can be extended to multi-day patterns. |
| Transaction Type | Non-Tunable | Cash only | Only cash transactions (deposits, withdrawals, currency exchanges) are in scope. Wire transfers, checks, ACH, and other non-cash instruments are excluded. Not tunable — structuring by definition involves cash. |
| Transaction Direction | Non-Tunable | Deposits + Withdrawals | Both cash-in and cash-out included. |

**Analysis Parameters:** Amount, Count (2D)
**Distributional View:** 2D cross-tabulation — aggregate amount tranches × aggregate count tranches. Shows how many customers fall into each bucket. Individual tabs for amount-only and count-only distributions plus combined view.

**Special Logic:** The filter engine must apply a two-pass filter:
1. **Pass 1 (Non-Tunable):** Keep only cash transactions below the individual ceiling
2. **Pass 2 (Aggregate):** Group by customer + aggregation period, compute SUM and COUNT, then keep only groups where SUM > aggregate amount threshold AND COUNT >= aggregate count threshold

---

#### Predefined Scenario UI Design

Each predefined scenario card in the scenario library shows:
- Scenario name and one-line description
- Icon indicating analysis type (single vs. aggregate)
- Number of tunable parameters

When a scenario is selected, the **configuration pane** appears with two clearly separated sections:

**Tunable Parameters Section (white background, editable):**
- Each parameter shown as a labeled input field with its default value
- Numeric inputs have +/- steppers and allow direct typing
- Dropdown selectors for period/function choices
- Helper text below each field explaining what it controls
- "Reset to Defaults" button

**Non-Tunable Parameters Section (light grey background, locked):**
- Each parameter shown with a lock icon (🔒) and its fixed value
- Not editable — clicking shows a tooltip explaining why it's fixed
- Example tooltip: "Transaction type is fixed to Cash because structuring by definition involves cash transactions. To analyze other transaction types, create a custom scenario."
- "View Full List" link for jurisdiction lists that expands to show all countries

**Bottom of pane:**
- "Run Analysis" button (primary action)
- "Customize" link (copies scenario to Manual Mode for full editing)

---

**Manual Mode:**
- User selects filtering criteria from dropdown menus built from the uploaded data
- Filter by any combination of columns (e.g., transaction type = "Wire", country ≠ "US", amount > 5000)
- Support AND/OR logic for combining filters
- Allow multiple filter groups

**AI Prompt Mode:**
- User describes the scenario in plain English
- Example: "Find all outgoing wire transfers over $10,000 to high-risk countries within a 30-day rolling period per customer"
- LLM (Claude API) interprets the prompt and generates the corresponding filter rules
- User reviews and confirms/edits the generated rules before applying
- LLM has access to the column names and sample values from the uploaded data for context

### Step 3: Filter Matching Transactions
- Apply the defined scenario rules to the uploaded dataset
- Display filtered results in an interactive table
- Show filter summary: X of Y transactions matched (Z%)
- Allow user to refine filters and re-run

### Step 4: Choose Analysis Type

**Single Transaction Analysis:**
- Each row analyzed independently
- Statistical summary of the filtered transactions as individual records
- Use case: "What does the distribution of individual wire amounts look like?"

**Aggregate Transaction Analysis:**
- Transactions grouped by a key (e.g., customer ID, account number)
- User specifies aggregation period:
  - **Daily:** Sum/count per customer per calendar day
  - **Rolling 7-day:** Sum/count per customer over a sliding 7-day window
  - **Rolling 30-day:** Sum/count per customer over a sliding 30-day window
  - **Custom rolling period:** User specifies number of days
- User specifies aggregation function: SUM, COUNT, AVG, MAX, MIN
- Use case: "What does the distribution of total monthly wire volume per customer look like?"

### Step 5: Choose Analysis Parameters
- User selects which column(s) to analyze from the filtered/aggregated data
- Fully customizable — any numeric or categorical column is available
- Common parameters: transaction amount, transaction count, country code, product type, channel
- For categorical columns: frequency distribution, top N values, concentration analysis
- For numeric columns: full statistical summary

### Step 6: Distributional Analysis Dashboard

The analysis dynamically adapts based on how many parameters the user selects. The core unit of analysis is the **"applicable event"** — a transaction (single mode) or group of transactions (aggregate mode) that meets the scenario's minimum logic and spirit.

---

#### Tranche Design (Applies to All Numeric Parameters)

**System-Suggested Tranches:**
- On first run, the system analyzes the data distribution and auto-generates sensible value buckets
- Bucket boundaries are based on natural breakpoints in the data (Jenks optimization), round numbers, and regulatory-relevant thresholds (e.g., $3,000, $5,000, $10,000 for cash)
- Example for transaction amount: $0–1K | $1K–3K | $3K–5K | $5K–10K | $10K–25K | $25K–50K | $50K–100K | $100K+

**User-Adjustable:**
- User can drag tranche boundaries, add/remove tranches, or type custom values
- Adjustments update all tables and charts in real time
- User can save custom tranche configurations for reuse

---

#### One Parameter Selected (1D Analysis)

When the user selects a single parameter (e.g., transaction amount):

**Distribution Table:**
| Tranche | Applicable Events | % of Total | Cumulative % | Cumulative Events |
|---------|-------------------|-----------|--------------|-------------------|
| $0 – $1,000 | 45,200 | 52.1% | 52.1% | 45,200 |
| $1,000 – $5,000 | 22,100 | 25.5% | 77.6% | 67,300 |
| $5,000 – $10,000 | 11,400 | 13.1% | 90.7% | 78,700 |
| $10,000 – $25,000 | 5,800 | 6.7% | 97.4% | 84,500 |
| $25,000 – $50,000 | 1,600 | 1.8% | 99.2% | 86,100 |
| $50,000+ | 680 | 0.8% | 100.0% | 86,780 |

**Summary Statistics Box:**
- Min, Max, Mean, Median (P50)
- Percentiles: P25, P50, P75, P85, P90, P95, P99
- Standard deviation, IQR
- Outlier count (beyond 1.5x IQR and 3x IQR)

**Visualizations:**
- Histogram (bar chart matching the tranche table)
- Box plot with percentile markers
- Cumulative distribution function (CDF) curve
- Time series chart (event volume over the analysis period)

---

#### Two Parameters Selected (2D Analysis)

When the user selects two parameters (e.g., transaction amount + country code):

**Tab Structure:**
- **Tab 1: Parameter A Only** — Full 1D analysis for Parameter A (same as above)
- **Tab 2: Parameter B Only** — Full 1D analysis for Parameter B (same as above)
- **Tab 3: Combined (A × B)** — Two-dimensional cross-tabulation

**Combined Tab — Cross-Tabulation Matrix:**

Rows = Parameter A tranches, Columns = Parameter B values (or tranches if numeric)

| Amount \ Country | US | UK | Iran | China | Other |
|------------------|-----|-----|------|-------|-------|
| $0 – $5K | 32,100 | 8,200 | 120 | 4,500 | 22,380 |
| $5K – $10K | 6,800 | 2,100 | 85 | 1,200 | 1,215 |
| $10K – $25K | 3,200 | 900 | 340 | 800 | 560 |
| $25K+ | 1,100 | 380 | 210 | 290 | 300 |

Each cell = count of applicable events meeting both criteria.

**Combined Tab Visualizations:**
- Heatmap (color intensity = event count or volume)
- Stacked bar chart (Parameter A tranches stacked by Parameter B values)
- Grouped bar chart

**Summary statistics** (P25, P50, P75, P85, P90, min, max) shown for each parameter within each cross-tab cell on hover or drill-down.

---

#### Three or More Parameters Selected (3D+ Analysis)

When the user selects three or more parameters, the system offers three view modes that the user can toggle between:

**View Mode 1: Pairwise Tabs**
- System generates a tab for every 2-parameter combination plus each parameter individually
- For parameters A, B, C: tabs are "A Only", "B Only", "C Only", "A × B", "A × C", "B × C"
- Each tab shows the same 1D or 2D analysis described above
- No attempt to show a 3D matrix — keeps it readable

**View Mode 2: Fix-and-Filter**
- User selects one parameter as a **fixed filter** via a dropdown
- User picks a specific value or tranche for that parameter (e.g., Country = "Iran")
- Remaining parameters are displayed as a 2D cross-tabulation, filtered to only events matching the fixed parameter
- User can change the fixed filter value and the matrix updates in real time
- Example: Fix Country = "Iran", then see Amount × Product Type matrix for Iran-only events

**View Mode 3: Individual + All Combined**
- Each parameter gets its own tab with full 1D analysis
- Final tab attempts to show all parameters together via:
  - A filterable multi-column table where each row is an applicable event with all parameter values shown
  - Interactive filters for each parameter (sliders for numeric, dropdowns for categorical)
  - Summary statistics update as filters change
  - Parallel coordinates chart connecting each event's values across all parameters

**Toggle Control:**
- Radio button or dropdown at top of results page: "Pairwise View | Fix-and-Filter View | Individual + Combined View"
- Default is Pairwise View (most intuitive for most users)

---

#### Categorical Parameter Handling

For categorical parameters (e.g., country code, product type, channel):

**As a standalone (1D) analysis:**
- Frequency distribution table: value, count, % of total
- Top N values with concentration ratio (% of volume from top 10)
- Bar chart and donut chart

**As one dimension in a 2D analysis:**
- Categorical values become columns (or rows) in the cross-tab matrix
- If more than 15 unique values, show top 15 + "Other" bucket
- User can expand "Other" to see all values

### Step 7: Threshold Simulation

**How It Works:**
- User enters multiple candidate threshold values (e.g., $10K, $25K, $50K, $100K)
- Or: system auto-suggests thresholds based on percentile breakpoints (e.g., 90th, 95th, 99th percentile)
- For each threshold, the system calculates:
  - Number of alerts that would fire
  - Number of unique customers/entities flagged
  - % of total transactions captured above threshold
  - % of total dollar volume captured above threshold
  - Estimated daily/weekly/monthly alert volume

**Comparison Table:**
| Threshold | Alerts | Unique Customers | % Txns Captured | % Volume Captured | Est. Monthly Alerts |
|-----------|--------|-----------------|-----------------|-------------------|---------------------|
| $10,000   | 5,200  | 1,840           | 12.3%           | 67.8%             | 1,733               |
| $25,000   | 1,100  | 620             | 2.6%            | 41.2%             | 367                 |
| $50,000   | 280    | 155             | 0.7%            | 22.5%             | 93                  |
| $100,000  | 45     | 32              | 0.1%            | 8.1%              | 15                  |

**Threshold Recommendation:**
- System highlights a recommended threshold based on:
  - Alert volume manageability (configurable target, e.g., "we can handle 500 alerts/month")
  - Risk coverage (minimum % of suspicious volume captured)
  - Statistical breakpoints (natural gaps in the distribution)
- Recommendation includes plain-English justification

**Visualization:**
- Alert volume curve: X-axis = threshold value, Y-axis = number of alerts
- Coverage curve: X-axis = threshold value, Y-axis = % of volume captured
- Both curves on same chart with dual Y-axes so user can see the tradeoff

### Step 8: Export and Report

**Export Options:**
- Filtered transaction data → Excel/CSV
- Statistical summary → Excel (formatted with headers, charts)
- Threshold comparison table → Excel
- Full analysis report → PDF

**AI-Generated Written Summary Report:**
- LLM generates a narrative report summarizing:
  - Dataset overview (date range, volume, transaction types)
  - Scenario definition (what was tested)
  - Key statistical findings
  - Threshold analysis results
  - Recommended threshold with justification
  - Methodology description (for regulatory documentation)
- Report is formatted for inclusion in model validation documents or regulatory submissions
- User can edit the generated report before exporting

---

## Data Model

### `threshold_datasets`
Stores metadata about uploaded transaction files.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto increment |
| name | TEXT | User-assigned dataset name |
| file_name | TEXT | Original uploaded file name |
| row_count | INTEGER | Number of transactions |
| column_list | TEXT | JSON array of column names |
| date_range_start | DATE | Earliest transaction date |
| date_range_end | DATE | Latest transaction date |
| uploaded_at | DATETIME | Upload timestamp |

### `threshold_scenarios`
Stores defined AML detection scenarios.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto increment |
| dataset_id | INTEGER FK | Links to threshold_datasets |
| name | TEXT | User-assigned scenario name |
| description | TEXT | Plain English description |
| filter_rules | TEXT | JSON object defining all filter criteria |
| analysis_type | TEXT | "single" or "aggregate" |
| aggregation_key | TEXT | Column to group by (e.g., customer_id) |
| aggregation_period | TEXT | "daily", "rolling_7", "rolling_30", "custom" |
| aggregation_days | INTEGER | Custom rolling period in days (if applicable) |
| aggregation_function | TEXT | "SUM", "COUNT", "AVG", "MAX", "MIN" |
| created_at | DATETIME | Creation timestamp |
| created_by_ai | BOOLEAN | Whether scenario was generated via AI prompt |

### `threshold_analyses`
Stores completed analysis results.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto increment |
| scenario_id | INTEGER FK | Links to threshold_scenarios |
| parameter_columns | TEXT | JSON array of columns analyzed |
| statistics | TEXT | JSON object with all computed stats |
| threshold_values | TEXT | JSON array of tested thresholds |
| threshold_results | TEXT | JSON object with simulation results per threshold |
| recommended_threshold | FLOAT | System-recommended threshold value |
| recommendation_reason | TEXT | Plain English justification |
| report_text | TEXT | AI-generated summary report |
| created_at | DATETIME | Analysis timestamp |

---

## Backend API Endpoints

### Datasets
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | /threshold/datasets/upload | Upload CSV or Excel file |
| POST | /threshold/datasets/connect | Connect to database |
| GET | /threshold/datasets | List all uploaded datasets |
| GET | /threshold/datasets/{id} | Get dataset metadata and column info |
| GET | /threshold/datasets/{id}/preview | Preview first 100 rows |
| DELETE | /threshold/datasets/{id} | Delete a dataset |

### Scenarios
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | /threshold/scenarios | Create scenario (manual mode) |
| POST | /threshold/scenarios/ai | Create scenario from AI prompt |
| GET | /threshold/scenarios | List all scenarios |
| GET | /threshold/scenarios/{id} | Get scenario details |
| PUT | /threshold/scenarios/{id} | Update scenario filters |
| DELETE | /threshold/scenarios/{id} | Delete scenario |
| POST | /threshold/scenarios/{id}/apply | Apply scenario filters, return matched transactions |

### Analysis
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | /threshold/analysis | Run statistical analysis on filtered data |
| POST | /threshold/analysis/simulate | Run threshold simulation with multiple values |
| POST | /threshold/analysis/auto-thresholds | Auto-suggest thresholds from percentiles |
| GET | /threshold/analysis/{id} | Get saved analysis results |

### Export
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | /threshold/export/excel | Export filtered data + stats to Excel |
| POST | /threshold/export/report | Generate AI-written summary report (PDF) |
| POST | /threshold/export/comparison | Export threshold comparison table |

---

## Frontend UI Screens

### 1. Dataset Upload Page (`/threshold`)
- Drag-and-drop file upload area (CSV, Excel)
- Database connection form (host, port, database, query)
- List of previously uploaded datasets with date range and row count
- Click dataset to proceed to scenario builder

### 2. Scenario Builder (`/threshold/scenario`)
- **Left panel:** Column list from uploaded data with data types and sample values
- **Center panel:** Filter builder
  - Dropdown for column selection
  - Operator selection (=, ≠, >, <, >=, <=, IN, NOT IN, CONTAINS, BETWEEN)
  - Value input (auto-suggests from data for categorical columns)
  - AND/OR toggle between filter groups
  - Add/remove filter rows
- **AI prompt bar** at top: text input where user can describe scenario in plain English
  - "Generate Rules" button sends prompt to Claude API
  - Generated rules populate the filter builder for user review
  - User can accept, edit, or regenerate
- **Right panel:** Live preview showing count of matching transactions as filters change
- "Run Analysis" button at bottom

### 3. Analysis Configuration (`/threshold/analysis`)
- Toggle: Single Transaction / Aggregate Analysis
- If Aggregate:
  - Aggregation key dropdown (e.g., customer_id, account_number)
  - Aggregation period selector (daily, rolling 7-day, rolling 30-day, custom)
  - Aggregation function selector (SUM, COUNT, AVG, MAX, MIN)
- Parameter selector: checkboxes for which columns to analyze
- "Run Analysis" button

### 4. Results Dashboard (`/threshold/results`)
- **Summary cards** at top: total records, matched records, match %, date range
- **Statistics panel:** full stats table for each selected parameter
- **Charts:** histogram, box plot, CDF, time series — each in tabs or side by side
- **Filters:** interactive filters on the dashboard to slice the data further
- All charts are interactive (hover for values, click to drill down)

### 5. Threshold Simulator (`/threshold/simulate`)
- **Input section:**
  - Manual entry: add multiple threshold values
  - Auto-suggest button: generates thresholds at key percentiles
  - Target alert volume input: "We can handle ___ alerts per month"
- **Comparison table:** shows alerts, unique customers, % captured for each threshold
- **Dual-axis chart:** alert volume curve + coverage curve
- **Recommendation box:** highlighted recommended threshold with AI-generated justification
- "Export Report" button

### 6. Report Preview (`/threshold/report`)
- AI-generated narrative report displayed in formatted view
- Editable text areas — user can modify any section
- Sections: Executive Summary, Dataset Overview, Methodology, Statistical Findings, Threshold Analysis, Recommendation, Appendix
- Export buttons: PDF, Word

---

## AI Integration (Claude API via LangChain/LangGraph)

### Scenario Generation from Natural Language
- **Input:** User prompt + column names + sample values from uploaded data
- **Output:** JSON filter rules that map to the filter builder format
- **System prompt** includes: column definitions, valid operators, data types, and example filter rule JSON
- Chain validates output against actual column names before returning

### Threshold Recommendation
- **Input:** Statistical summary + threshold simulation results + user's target alert volume
- **Output:** Recommended threshold value + plain English justification
- Considers: distributional properties, natural breakpoints, alert manageability, risk coverage

### Report Generation
- **Input:** Full analysis context — dataset summary, scenario description, statistics, threshold results, recommendation
- **Output:** Multi-section narrative report suitable for regulatory documentation
- Tone: professional, compliance-oriented, suitable for inclusion in model validation packages or MRA responses

---

## Tech Stack (Consistent with Illuminate AFC Platform)

| Layer | Technology |
|-------|------------|
| Frontend | React (Vite), React Router, Tailwind CSS, Recharts, Axios |
| Backend | Python / FastAPI |
| Database | SQLite (SQLAlchemy ORM) → PostgreSQL-ready |
| Data Processing | Pandas, NumPy, SciPy (for statistical calculations) |
| LLM | Claude API (Anthropic) via LangChain |
| File Processing | pandas (CSV/Excel), openpyxl (Excel export) |
| PDF Export | ReportLab or WeasyPrint |
| Charts | Recharts (frontend), Matplotlib (for PDF export charts) |

---

## Database Connection Support

For direct database connections, support:
- PostgreSQL
- MySQL
- SQL Server (ODBC)
- Oracle
- SQLite

User provides:
- Connection string or individual fields (host, port, database, username, password)
- SQL query or table name
- Connection is read-only — never write to external databases
- Results of query are loaded into memory as a pandas DataFrame, same as CSV/Excel upload

---

## Route Convention (Illuminate AFC)

| Route | Page |
|-------|------|
| `/threshold` | Dataset upload and management |
| `/threshold/scenario` | Scenario builder |
| `/threshold/analysis` | Analysis configuration |
| `/threshold/results` | Results dashboard |
| `/threshold/simulate` | Threshold simulator |
| `/threshold/report` | Report preview and export |

### API Route Prefix
All backend endpoints prefixed with `/threshold/`

### Database Table Prefix
All tables prefixed with `threshold_*`

---

## Implementation Notes for Claude Code

1. Start with backend — file upload, parsing, and column detection first
2. Build the filter engine before the AI scenario generator
3. Pandas is the core engine — all filtering, aggregation, and statistics happen in pandas
4. Rolling window aggregations use `pandas.DataFrame.rolling()` with `groupby()`
5. Threshold simulation is just filtering the aggregated data at each threshold and counting results
6. AI scenario generation needs careful prompt engineering — include column names, types, and sample values in the system prompt
7. Cache uploaded datasets in memory or temp files — don't re-parse on every request
8. Use SciPy for percentile calculations and distribution fitting
9. Recharts for all frontend charts — keep chart components reusable
10. Export: use openpyxl for Excel, ReportLab or WeasyPrint for PDF
11. Route prefix: `/threshold/`
12. Table prefix: `threshold_*`
13. Add "Threshold Setting" card to Illuminate AFC hub page

---

## Phase 2 (Future)
- Backtesting: upload historical alerts and compare against simulated alerts to measure improvement
- Multi-scenario comparison: run same data through multiple scenarios and compare coverage
- Customer segmentation: analyze thresholds by customer risk tier
- Time-based trending: show how distributions shift month over month
- Regulatory scenario library: pre-built scenarios based on FinCEN advisories and FFIEC exam manual
- Peer benchmarking: anonymous comparison of thresholds against similar-sized institutions
- Integration with AlertIQ: feed simulated alerts into alert quality analysis
