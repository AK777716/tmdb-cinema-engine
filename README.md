This is a professional, high-impact `README.md` structure designed for a Data Analyst portfolio. It connects your technical work (Python/SQL) to the business value you've created (Looker Studio Insights).

Copy and paste this directly into your **README.md** file in VS Code:

---

# 🎬 Cinema Intelligence: End-to-End Data Pipeline & Analytics

## 🚀 Project Overview

This project is a full-stack data solution that automates the extraction, transformation, and visualization of global movie data. The goal is to provide film studio executives with data-backed insights into **ROI efficiency**, **market timing**, and **competitive dominance**.

### 🛠️ The Tech Stack

* **Data Ingestion:** Python (Requests, Pandas) & TMDB API
* **Cloud Data Warehouse:** Google BigQuery
* **Data Transformation:** SQL (DDL/DML for Silver Layer modeling)
* **Business Intelligence:** Looker Studio

---

## 🏗️ Data Architecture (The Pipeline)

1. **Bronze Layer (Raw):** Automated Python scripts fetch 5,000+ movie records from the TMDB API and load them into BigQuery.
2. **Silver Layer (Cleaned):** Developed a SQL view to handle null values, cast data types, and engineer financial metrics like **Net Profit** and **ROI**.
3. **Gold Layer (Insights):** Connected the cleaned data to Looker Studio for executive-level reporting.

---

## 📊 Phase 2: Key Analytical Insights (SQL)

Using advanced SQL queries, I identified four critical business trends:

* **Genre Efficiency:** **Horror** and **Comedy/Crime** are the most capital-efficient genres, with ROI multipliers of **25.5x** and **37.7x** respectively.
* **Budget Paradox:** Low-budget films (<$10M) outperformed Mega-Blockbusters in ROI by over **270%**, proving that higher spending has diminishing returns.
* **Market Timing:** While Summer months yield the highest total revenue, **March and April** represent the most efficient "Sleeper Hit" windows with double the average ROI.
* **Popularity vs. Profit:** Discovered that high "Vanity Metrics" (Popularity) often correlate with massive financial losses (e.g., *Awake*, *Lolita*).

---

## 🖥️ Phase 3: Executive Dashboard (BI)

I engineered an interactive Looker Studio dashboard focusing on four strategic views:

1. **Investment Efficiency:** A breakdown of ROI by genre to guide production greenlighting.
2. **Risk vs. Reward:** A scatter plot (filtered for Market Leaders with Popularity ≥ 20) to visualize the correlation between budget and revenue.
3. **Historical Growth:** A yearly time-series analysis showing industry revenue expansion since 1927.
4. **Market Share:** A competitive analysis of Tier-1 Studios (Revenue > $1B) to identify market dominance.

---

## 📂 Project Structure

```text
├── python_scripts/
│   ├── full_pipeline.py      # Main ingestion script
│   └── ingest_ids.py         # ID discovery script
├── sql_queries/
│   ├── 01_cleaning_logic.sql # Silver Layer View creation
│   ├── 02_roi_analysis.sql   # Genre performance queries
│   └── 03_market_share.sql   # Studio dominance logic
├── .env                      # API Credentials (Hidden)
└── README.md                 # Project Documentation

```

---

## 🏁 Conclusion & Future Scaling

This project demonstrates a scalable framework for market intelligence. By automating the data flow from API to Dashboard, I've reduced the manual reporting time for these insights to zero. Future iterations will include **Sentiment Analysis** of movie reviews using Natural Language Processing (NLP).

---

## Excel Decision Support Calculator

### What It Does
An interactive Investment Decision Calculator built in Excel, 
connected live to the BigQuery pipeline via Power Query.

Stakeholders can filter across 5 dimensions simultaneously:
- Genre
- Production Country  
- Language
- Release Year
- Budget Tier

The dashboard dynamically recalculates:
- Median ROI
- Median Revenue
- Median Net Profit
- Movie Count

### Why Excel (Not Just Looker Studio)
Looker Studio provides static visual analytics.
The Excel calculator provides dynamic What-If simulation —
stakeholders can test specific investment combinations
and get precise median metrics for that exact scenario.

### Technical Approach
- Multi-criteria MEDIAN(IF()) array formulas
- Dynamic chart highlighting using NA() trick
- Named ranges for clean formula readability
- Power Query auto-refresh on file open
- Data Validation dropdowns from clean reference lists