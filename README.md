# 🎬 Cinema Intelligence Engine

An end-to-end cloud data pipeline, data warehouse, and multi-tier business intelligence platform that extracts, transforms, and analyzes global movie data from the TMDB API. The system delivers executive market visibility in Looker Studio and interactive financial investment scenario modeling in Excel/Google Sheets.

---

## ⚡ Quick Access & Project Assets

- 📥 **Interactive Excel Simulator**: [Download `TMDB_Movie_Dashboard.xlsx`](./dashboardS/TMBD_Movie_Dashboard.xlsx)
- 🌐 **Master Google Sheet**: [TMDB_Cleaned_Data (Read-Only)](https://docs.google.com/spreadsheets/d/1A8SCVBQWOFAvr1N6UEcnnr9E4kGHGxOOeiBjdO-c2Eo/edit?usp=sharing)
- 📊 **Looker Studio Dashboard**: [View "Global Market Pulse" Report](https://datastudio.google.com/reporting/a64d606c-38aa-44be-aacc-9cdb960710d0)
- 🗄️ **Data Warehouse & Queries**: [BigQuery Analytics Pipeline](./analysis/BIGQUERY_ANALYTICS_PIPELINE.md)

---

## 🏗️ End-to-End System Architecture

```
                              [ TMDB API ]
                                   │
                     (Python Ingestion & Backfill)
                                   ▼
                     ┌───────────────────────────┐
                     │   GCP BigQuery Warehouse  │
                     │   • movies_raw (Bronze)   │
                     └─────────────┬─────────────┘
                                   │
                        (SQL Medallion Logic)
                                   ▼
                     ┌───────────────────────────┐
                     │   GCP BigQuery Warehouse  │
                     │  • dim_movies_cleaned     │
                     │    (Silver Layer View)    │
                     └──────┬─────────────┬──────┘
                            │             │
        ┌───────────────────┘             └───────────────────┐
        ▼                                                     ▼
┌────────────────────────────────┐                 ┌───────────────────────────┐
│   Looker Studio BI Dashboard   │                 │    Google Sheets Sync     │
│   "Global Market Pulse"        │                 │   (Cloud Export Layer)    │
│   • Market Engagement          │                 └─────────────┬─────────────┘
│   • Financial Metrics & Yield  │                               │
│   • Sentiment & Acclaim        │                    (Power Query / Connection)
└────────────────────────────────┘                               ▼
                                                     ┌───────────────────────────┐
                                                     │     Excel Interactive    │
                                                     │   Investment Simulator   │
                                                     │  • Budget Scenario Engine│
                                                     └───────────────────────────┘
```

---

## 🚀 System Components & Repository Layout

The repository is structured into modular documentation, automated ETL scripts, warehouse transformation views, and analytical models:

```text
TMDB_CINEMA_ENGINE/
├── README.md                           <-- Executive Master Overview (this file)
├── dashboards/                         <-- Interactive Excel Dashboard Assets
│   └── TMDB_Movie_Dashboard.xlsx       <-- Excel Investment Simulator File
├── analysis/                           <-- Comprehensive Architecture & BI Documentation
│   ├── BIGQUERY_ANALYTICS_PIPELINE.md  <-- Data warehouse schema, views & 11 SQL queries
│   ├── LOOKER_STUDIO_INSIGHTS.md       <-- 3-page executive dashboard walkthrough & metrics
│   ├── EXCEL_DASHBOARD_SETUP.md        <-- Google Sheets sync & Excel scenario engine
│   └── assets/                         <-- High-resolution architecture & UI screenshots
│       ├── bigquery/
│       ├── looker/
│       └── excel/
├── scripts/                            <-- Ingestion, backfill, and orchestration scripts
│   ├── bulk_backfill.py
│   ├── full_pipeline.py
│   └── ingest_ids.py
└── sql_queries/                        <-- Saved BigQuery SQL scripts & analytical views
    ├── 01_Genre_ROI_and_Profit_Analysis.sql
    ├── 02_Budget_Tier_Investment_Efficiency.sql
    ├── 03_Marketing_vs_Profit_Anomaly.sql
    ├── 04_Revenue_Driver_Correlation_Analysis.sql
    ├── 05_Seasonal_Release_ROI_Analysis.sql
    ├── 06_Studio_Capital_Efficiency.sql
    ├── 07_Market_Language_Efficiency.sql
    ├── 08_Excel_Dashboard_Raw_Export.sql
    ├── 09_Genre_Reference_List.sql
    ├── 10_Country_Reference_List.sql
    ├── 11_Company_Reference_List.sql
    ├── cleaning_logic(Silver_Layer).sql
    └── automation_scorecard_view.sql
```

---

## 📊 Analytical Deep Dives & Module Links

For detailed technical specifications, schema breakdowns, and visualization guides, refer to the dedicated sub-documentation:

### 🗄️ [BigQuery Warehouse & Transformation Pipeline](./analysis/BIGQUERY_ANALYTICS_PIPELINE.md)
- Details the Bronze-to-Silver Medallion data engineering workflow.
- Enforces deduplication using `QUALIFY ROW_NUMBER() OVER (PARTITION BY id ...)`.
- Complete code and documentation for the 11-query SQL suite and the live operational scorecard view (`v_automation_scorecard`).

### 📈 [Looker Studio Executive Dashboard — "Global Market Pulse"](./analysis/LOOKER_STUDIO_INSIGHTS.md)
- **Page 1 (Market Engagement):** Global market sizing across 26,073 movies, language reach, and genre saturation treemaps.
- **Page 2 (Financial Metrics):** Financial analysis across $111.43B revenue, $82.16B net profit, and an average 6.95x ROI.
- **Page 3 (Critical Response):** Sentiment correlation analysis protecting against low-acclaim commercial downside risk.

### 📑 [Excel & Google Sheets Financial Simulator](./analysis/EXCEL_DASHBOARD_SETUP.md)
- Direct BigQuery-to-Google Sheets automated data connector setup.
- Power Query M-language transformation layer with 6 connected data sources.
- Interactive Excel simulator modeling expected revenues, net profits, and historical success rates based on budget inputs, genre, language, and studio choices.

---

## 💡 Business & Financial Impact

- **Capital Efficiency Slicing:** Identifies $10M–$50M mid-budget films as the optimal risk-adjusted yield window compared to diminishing returns on $150M+ mega-blockbusters.
- **Release Window Optimization:** Quantifies seasonal release calendar spikes (e.g., April/October windows driving peak ROI multipliers).
- **Automated Data Quality:** Eliminates duplicate record ingestion via window-function deduplication and protects KPI aggregations with zero-budget checks (`SAFE_DIVIDE`).

---

## 🛠️ Tech Stack

- **Data Engineering:** Python, TMDB API, BigQuery SQL (`QUALIFY`, `UNNEST`, `SAFE_CAST`, `SAFE_DIVIDE`)
- **Cloud & Warehouse:** Google Cloud Platform (BigQuery Datasets & Views)
- **Business Intelligence:** Looker Studio, Google Sheets API
- **Financial Modeling:** Microsoft Excel, Power Query (M-Language), Data Validation, Dynamic Arrays
