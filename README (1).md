# 🎬 Cinema Intelligence Engine: Automated Cloud ETL Pipeline & Market BI Platform

An end-to-end data engineering and business intelligence platform that automates the ingestion, transformation, deduplication, and financial simulation of global and regional Indian film data. The platform executes containerized daily cloud workflows to extract media metadata via REST APIs, refines it within a cloud data warehouse using advanced SQL, and provisions an automated live bridge to multi-page Looker Studio dashboards and an interactive Excel Investment Simulator for executive decision support.

---

## 🛠️ System Architecture & End-to-End Data Flow

```
[TMDB REST API] ────(Python Requests)────> [GitHub Actions Runner]
                                                      │
                                          (Daily Automated Ingestion)
                                                      ▼
[Looker Studio BI Dashboard] <──── [Google BigQuery Cloud Data Warehouse]
     (Live Cloud Sync)                                │ (Medallion SQL Architecture)
                                         ├──> movies_raw (Landing Layer)
                                         └──> dim_movies_cleaned (Production Layer)
                                                      │
                                                      ▼
                                         [Google Sheets API Bridge]
                                                      │
                                              (CSV / Web Endpoint)
                                                      ▼
                                           [Excel Power Query]
                                                      │
                                              (M-Language Logic)
                                                      ▼
                                         [Excel Investment Simulator]
```

### Architectural Pipeline Breakdown

1. **Orchestration & Ingestion:** Containerized headless environments trigger Python scripts to query multi-market endpoints, streaming data securely using masked repository secret keys.
2. **Cloud Data Warehousing:** Raw records are appended directly into an enterprise Google BigQuery landing table (`movies_raw`).
3. **Transformation Layer:** Advanced SQL queries execute inside BigQuery, implementing a Medallion data architecture to structure, token-expand, deduplicate, and store high-fidelity production tables (`dim_movies_cleaned`).
4. **Cloud BI Consumption:** Looker Studio mirrors production tables in real time to generate interactive market maps, KPI summaries, and distribution charts.
5. **Downstream Desktop Bridge:** Production tables are exposed via secure streaming endpoints to Google Sheets and fetched dynamically via Excel Power Query upon workbook initialization, bypassing local database drivers.

---

## 🚀 Core Pipeline Engineering & Scripts

The engine repository is structured into production-grade functional modules:

### 1. Data Ingestion & Backfill (`/scripts`)

- **`bulk_backfill.py`** — Handles historic partitioning and deep-scraping filters. Leverages programmatic rate-limiting and connection-safety overrides (`urllib3.disable_warnings`) to fetch deep historical windows (2000–2012 / 2013–2026) without encountering connection reset failures.
- **`full_pipeline.py`** — The core daily script that handles rolling delta loads. Scans recent rolling windows, checks for live changes, appends new entries to BigQuery, and performs a programmatic deduplication loop using an analytical window function (`ROW_NUMBER() OVER(PARTITION BY id ORDER BY ...)`).
- **`ingest_ids.py`** — Interacts with TMDB's compression exports (`files.tmdb.org/p/exports/`) to extract valid, unique production IDs directly out of daily gzip archives (`.json.gz`).

### 2. Cloud Data Warehouse Schema (`cinema_intelligence`)

Data transits through an optimized processing schema inside Google BigQuery to preserve data lineage:

- **`movies_raw`** — Schemaless ingestion layer storing raw unstructured metadata, programmatic API popularity markers, and runtime ingestion timestamps.
- **`dim_movies_cleaned`** — The enterprise production layer. Derived by running specialized data engineering logic (`cleaning_logic(Silver_Layer).sql`) to sanitize malformed release dates, enforce data types, map sparse properties, and isolate rows containing verified commercial footprints.

---

## 📝 SQL Query Matrix (`/sql_queries`)

The repository contains enterprise-grade analytical views built with BigQuery/PostgreSQL dialects to isolate high-value trends:

| File | Purpose |
|---|---|
| `01_Genre_ROI_and_Profit_Analysis.sql` | Ranks content genres based on financial capital returns and net operational yield metrics |
| `02_Budget_Tier_Investment_Efficiency.sql` | Segments projects by investment scale to isolate performance sweet spots between micro-budget indie films and mega-blockbusters |
| `03_Marketing_vs_Profit_Anomaly.sql` | Leverages statistical standard deviations to identify projects that suffered from bloated commercial footprints relative to total revenue |
| `04_Revenue_Driver_Correlation_Analysis.sql` | Correlates independent production variables against ultimate financial success indexes |
| `05_Seasonal_Release_ROI_Analysis.sql` | Utilizes advanced time-series parsing to extract high-yield release windows across the calendar year |
| `06_Studio_Capital_Efficiency.sql` | Evaluates legacy production houses against modern streamers by aggregating production frequency against mean ROI |
| `07_Market_Language_Efficiency.sql` | Conducts micro-segmentation across local Indian cinematic languages to map structural regional demand |

---

## 📊 Looker Studio Business Intelligence Platform

The cloud reporting layer consists of a comprehensive, multi-page, interactive interface called **Global Market Pulse** designed for corporate decision support.

### Page 1: Global Market Pulse (Volume & Saturation)
- **Metrics Tracked:** Total Movies Ingested (26,457 entries), Average Popularity Coefficient (3.29), Mean Viewer Rating (4.95)
- **Visualizations:** Multi-variable Language Performance quadrant mapping average popularity against user review scores; Production Volume breakdown isolating language density (English 40.8%, Hindi, Malayalam); localized tree map outlining market saturation across thematic genres

### Page 2: Financial Matrix & ROI Audit
- **Metrics Tracked:** Total Gross Global Revenue ($110.5B), Average Engine ROI (7.97x), Net Collective Portfolio Profit ($81.8B)
- **Visualizations:** Dual-axis scatter matrix analyzing production budgets against box office multipliers; categorical time-series heatmap sorting historical release days and calendar months; high-fidelity tabular index ranking legacy properties by gross velocity

### Page 3: Audience Engagement & Consumer Sentiment
- **Metrics Tracked:** Mean Global Audience Rating Score, Composite Vote Volume Registry, Aggregated Core Demand Index
- **Visualizations:** Comprehensive linear regression metric correlating consumer reception metrics against scaling popularity flags to separate active word-of-mouth phenomena from temporary marketing anomalies

---

## 🗃️ Downstream Data Engineering Bridge

To support downstream desktop analytics without breaking data warehouse separation, the daily pipeline hosts a dedicated programmatic synchronization task:

- Implements service account tokenization via secure GCP IAM keys (`excel_automation_key.json`)
- Connects dynamically via Google API Drive and Sheet scopes to instantly overwrite raw reference frames (`TMDB_Cleaned_Data`, `TMDB_Genre_Reference`, `TMDB_Country_Reference`, `TMDB_Company_Reference`)
- Bundles structural mapping arrays to consolidate fragmented corporate entities (e.g., standardizing *Fox Searchlight, 20th Century Fox, Fox 2000* into *20th Century Studios*), ensuring high schema integrity

---

## 📈 Excel Investment Decision Calculator

### Overview

While Looker Studio provides trend exploration and visual macro-analytics, stakeholders require the ability to simulate granular investment scenarios prior to greenlighting a film. The Excel Investment Decision Calculator acts as a tactical simulation tool — users input any combination of genre, language, release month, budget tier, and production studio to instantly project the expected financial outcome and risk profile for that exact scenario.

### Data Flow & Automated Sourcing

```
Google BigQuery (dim_movies_cleaned)
         ↓
Google Sheets API Bridge
(3 Live Sheets: movies_all | movies_genre | movies_finance)
         ↓
Excel Power Query Interface
(Configured: Auto-Refresh on File Open)
         ↓
[Data Sheets] ──> [Reference Sheet] ──> [Data_Validation] ──> [5 Calc Sheets] ──> [Dashboard]
```

The connection leverages Google Sheets as an intermediary delivery network because BigQuery's Connected Sheets architecture provides a stable, web-streamed API connection. Power Query is configured to bypass local system cache and update automatically on workbook initialization — every time a stakeholder opens the file, they view live backend warehouse data with zero manual engineering overhead.

---

### Workbook Architecture (12-Sheet Structural Layout)

The workbook is decoupled into 5 operational layers to isolate processing logic from presentation frames.

---

#### Layer 1 — Data Ingestion Sheets (3 Sheets)

| Sheet | BigQuery Source | Purpose |
|---|---|---|
| `Data` | `movies_all` | Full master dataset — 5,000+ movies, 21 columns. Primary client-side source of truth |
| `Data_Finance` | `movies_finance` | Financially complete subset — only records where budget, revenue, and ROI are fully populated (`data_quality = "Complete Financials"`). All calculator formulas scan this sheet for performance |
| `Data_Genre` | `movies_genre` | Genre-segmented subset of profitable films mapped to complete `roi_category` keys, optimized for fast array traversal |

> **Design Principle:** `AVERAGEIF` and `MEDIAN(IF())` formulas compute by scanning arrays sequentially. Forcing these expressions to traverse a 5,000-row, 21-column array slows calculation speed significantly. Isolating a filtered, column-restricted financial table allows the model's computation layer to run smoothly.

---

#### Layer 2 — Core Reference Sheet (1 Sheet)

**`Reference_Data`** — A centralized index containing clean, deduplicated entities across all global genres, production nations, and corporate studios alongside their baseline production volumes. Serves as the single source of truth feeding all downstream dropdown lists.

---

#### Layer 3 — Data Validation Plumbing (1 Sheet, Hidden Backend)

**`Data_Validation`** — A hidden processing sheet hosting 6 dynamically managed sorting vectors:

```
genre_sorted | country_sorted | language_sorted | year_sorted | budget_tier_sorted | company_sorted
```

Each vector is generated via an array expression:

```excel
=SORT(UNIQUE(Reference_Data[Column]))
```

Upon a Power Query refresh, newly introduced records stream directly into these structures, allowing dashboard dropdown components to expand dynamically without any manual maintenance.

---

#### Layer 4 — Multi-Dimensional Calculation Sheets (5 Sheets)

Each sheet isolates a dedicated computational scope, pre-aggregating core indicators to keep the user frontend light and fast.

**`Calc_Genre`**
Computes average ROI, gross profitability, net revenue yield, and baseline volume per genre. Employs a wildcard matching expression to account for multi-genre films stored as comma-separated arrays (e.g., *"Drama, Romance, Thriller"*):
```excel
=AVERAGEIF(movies_finance[genres], "*"&genre_input&"*", movies_finance[roi])
```
Includes a reliability signaling constraint:
```excel
=IF(movie_count >= 5, "✅ Reliable", "⚠️ Low Sample")
```

**`Calc_Language`**
Aggregates average consumer success index values, popularity scores, and viewer ratings per regional dialect. Pulls from the holistic master table (`movies_all`) to ensure linguistic analysis accounts for all properties, regardless of financial completeness.

**`Calc_Season`**
Compiles monthly trend cycles using calendar constraints:
```excel
=TEXT(DATE(2024, month_index, 1), "MMMM")
```
Isolates seasonal market entry anomalies — March and April emerge as the highest ROI release windows (the Sleeper Hit effect).

**`Calc_Budget`**
Audits capital efficiency tiers across four risk profiles:

| Tier | ROI Range | Avg ROI | Success Rate | Film Count |
|---|---|---|---|---|
| Loss | <1x | 0.30x | 0% | 121 |
| Low ROI | 1–2x | 1.49x | 96% | 57 |
| Medium ROI | 2–5x | 3.19x | 100% | 85 |
| High ROI | >5x | 17.40x | 100% | 57 |

**`Calc_Studio`**
Isolates corporate metrics across highly active entities via an array filtration threshold:
```excel
=FILTER(Reference_Data[company_name], Reference_Data[movie_count] >= 10)
```
Narrows 6,384 unorganized records down to ~80 useful production companies. Reliability flags (`⚠️ Low Sample`) guarantee analytical transparency for studios with insufficient financial data coverage.

---

#### Layer 5 — Interactive User Dashboard (1 Sheet Frontend)

The terminal user interface built for stakeholders. All calculation plumbing remains invisible to the end user.

**5 User-Facing Input Dropdowns:**
- Genre Selector
- Target Budget Tier
- Primary Language
- Target Release Month
- Executive Production Studio

**8 Dynamic Analytical Output Metrics:**
- Selected Genre ROI
- Average Budget-Tier ROI
- Expected Target Revenue
- Projected Net Profit
- Modeled Success Rate
- Language Popularity Coefficient
- Peak-Season Revenue Benchmark
- Studio Average Operational Yield

**3 Cross-Filtering Visualizations:**
- Avg ROI by Genre (Horizontal Bar)
- Risk Distribution by ROI Tier (Horizontal Bar)
- Success Index by Regional Language (Horizontal Bar)

---

### Production Exceptions & Engineering Remediation

#### Bug 1 — Calc_Budget Table Schema Disconnect (`#REF!`) — Critical

**Root Cause:** Computational formulas in `Calc_Budget` originally targeted a column labeled `budget_tier` expected from the cloud data pipeline but absent from the final warehouse production export. This schema mismatch caused all downstream array operations to throw fatal `#REF!` failures, returning zeros across all rows.

**Remediation:** Re-mapped all references to target the verified production column `movies_finance[roi_category]`. Modified row category labels to match production schema boundaries: *Loss (<1x), Low (1-2x), Medium (2-5x), High ROI (>5x)*, restoring full model continuity.

**Lesson:** Column references in Excel Table formulas must match the exact column names in the Power Query output. A schema mismatch between BigQuery and Excel silently breaks everything downstream.

---

#### Bug 2 — Cascading Dropdown Breakdown in Data Validation — Medium

**Root Cause:** The `budget_tier_sorted` dynamic column inside the hidden plumbing sheet inherited the missing schema reference (`=SORT(UNIQUE(#REF!))`). This cascaded into the Budget Tier dropdown on the dashboard, causing user interface inputs to fail silently.

**Remediation:** Updated the source array parameter to evaluate the verified field:
```excel
=SORT(UNIQUE(movies_finance[roi_category]))
```
This immediately restored input rendering across the dashboard interface.

---

#### Bug 3 — Sparse Matrix Inflation inside Studio Calculations — Design Issue

**Root Cause:** The studio processor ingested all 6,384 production companies from raw metadata strings. The vast majority represented independent entities with single entries lacking long-term financial reporting, inflating the calculation sheet with empty rows and reducing analytical value to near zero.

**Remediation:** Implemented an array filtration layer to restrict calculation boundaries to statistically valid entities:
```excel
=FILTER(Reference_Data[company_name], Reference_Data[movie_count] >= 10)
```
Compressed the matrix to ~80 highly active studios and added automated data quality warnings (`⚠️ Low Sample`) to guarantee analytical transparency.

---

#### Bug 4 — Google Sheets Intermediary Header Shift (`Column1`) — Minor

**Root Cause:** Due to a structural export variation in the Connected Sheets bridge, Power Query maps a primary identifier field as an unassigned header string (`Column1`) rather than mapping the database schema attribute directly.

**Status:** Not breaking current functionality. Planned fix: introduce an explicit M-language table schema rename step inside the Power Query compilation block to enforce structural consistency regardless of source transport shifts.

---

### Key Operational Insights Generated by the Model

| Dimension | Analytical Finding | Strategic Business Value |
|---|---|---|
| **Most Capital-Efficient Genre** | Horror — average **25.5x ROI** | Allocates micro-budgets to high-multiplier genre templates to mitigate portfolio risk |
| **Optimal Release Window** | March / April — maximum structural ROI, lowest competition | Optimizes theatrical launch scheduling, capturing the Sleeper Hit market phenomenon |
| **Capital Scale vs Efficiency** | Low-budget films (<$10M) outpace blockbusters by **270%** on net yield | Challenges conventional scale assumptions; models optimal funding limits to protect liquidity |
| **High ROI Configurations** | Premium brackets preserve **100% profitability floor**, avg **17.4x return** | Establishes baseline financial targets for future script greenlighting workflows |
| **Loss-Making Baselines** | **121 consistent underperformers** averaging **0.30x ROI** | Serves as an automated negative-filtering boundary to reject unviable investment pitches |

---

### Platform Comparison: Cloud BI vs Desktop Modeling

| Feature Dimension | Looker Studio Analytics Platform | Excel Investment Simulator |
|---|---|---|
| **Core Business Purpose** | Macro trend discovery and multi-market portfolio exploration | Micro scenario modeling and tactical risk simulation |
| **User Interaction Model** | Multi-dimensional dashboard interaction and global cross-filtering | Granular discrete inputs → calculated output |
| **Primary Analytical Output** | Visual trend maps and data distribution profiles | Hard accounting numbers, specific margins, median indicators |
| **Target Enterprise User** | Market Researchers, Business Analysts, Media Strategists | Executive Decision Makers, Greenlight Committees, CFOs |

---

### Future Improvements

| Phase | Enhancement | Rationale |
|---|---|---|
| **Next** | Engineer `budget_tier` as a `CASE WHEN` column in BigQuery SQL | Enables budget analysis using actual production budgets instead of ROI proxies |
| **Next** | Fuzzy matching consolidation table for studio name variants | Merges fragmented entities like *20th Century Fox*, *20th Century Studios* into one |
| **Future** | Combined multi-filter `MEDIAN(IF())` calculator | Genre + Budget + Season simultaneously in one output — true scenario simulation |

---

## ⚙️ CI/CD Deployment & Automation

```yaml
Orchestration Platform : GitHub Actions Runner (Ubuntu Linux)
Execution Triggers     : Daily CRON Schedule + Manual Repository Dispatch
Data Security          : GCP IAM Keys and TMDB API Keys masked via GitHub Secrets
Runtime Efficiency     : Full pipeline execution completes within 2m 49s
```

---

## 📂 Project Structure

```
├── scripts/
│   ├── bulk_backfill.py          # Historic deep-scraping and partitioned backfill
│   ├── full_pipeline.py          # Core daily delta ingestion script
│   └── ingest_ids.py             # TMDB gzip ID extraction
├── sql_queries/
│   ├── 01_Genre_ROI_and_Profit_Analysis.sql
│   ├── 02_Budget_Tier_Investment_Efficiency.sql
│   ├── 03_Marketing_vs_Profit_Anomaly.sql
│   ├── 04_Revenue_Driver_Correlation_Analysis.sql
│   ├── 05_Seasonal_Release_ROI_Analysis.sql
│   ├── 06_Studio_Capital_Efficiency.sql
│   ├── 07_Market_Language_Efficiency.sql
│   └── cleaning_logic(Silver_Layer).sql
├── excel_dashboard/
│   └── TMBD_Movie_Dashboard.xlsx  # 12-sheet live investment calculator
├── .github/
│   └── workflows/
│       └── daily_update.yml       # GitHub Actions automation workflow
├── .env                           # API credentials (hidden)
└── README.md
```

---

## 🔮 Roadmap

| Phase | What | Tech |
|---|---|---|
| **Phase 5** | Python EDA — distributions, outliers, correlation heatmaps | Matplotlib, Seaborn, Plotly |
| **Phase 6** | NLP Sentiment Analysis on movie reviews | NLTK, TextBlob, HuggingFace |
| **Phase 7** | Predictive ROI Model — forecast returns before production begins | Scikit-learn regression |
```
