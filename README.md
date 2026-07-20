🎬 Cinema Intelligence: End-to-End Data Pipeline & Analytics
🚀 Project Overview
This project is a full-stack data solution that automates the extraction, transformation, and visualization of global movie data. The goal is to provide film studio executives with data-backed insights into ROI efficiency, market timing, and competitive dominance.
🛠️ The Tech Stack

Data Ingestion: Python (Requests, Pandas) & TMDB API
Automation: GitHub Actions (Daily scheduled pipeline)
Cloud Data Warehouse: Google BigQuery
Data Transformation: SQL (DDL/DML for Silver Layer modeling)
Business Intelligence: Looker Studio + Microsoft Excel


🏗️ Data Architecture (The Pipeline)

Bronze Layer (Raw): Automated Python scripts fetch 5,000+ movie records from the TMDB API and load them into BigQuery.
Silver Layer (Cleaned): Developed a SQL view to handle null values, cast data types, and engineer financial metrics like Net Profit and ROI.
Gold Layer (Insights): Connected the cleaned data to Looker Studio for executive-level reporting, and to Excel via Power Query for dynamic investment simulation.


📊 Phase 2: Key Analytical Insights (SQL)
Using advanced SQL queries, I identified four critical business trends:

Genre Efficiency: Horror and Comedy/Crime are the most capital-efficient genres, with ROI multipliers of 25.5x and 37.7x respectively.
Budget Paradox: Low-budget films (<$10M) outperformed Mega-Blockbusters in ROI by over 270%, proving that higher spending has diminishing returns.
Market Timing: While Summer months yield the highest total revenue, March and April represent the most efficient "Sleeper Hit" windows with double the average ROI.
Popularity vs. Profit: Discovered that high "Vanity Metrics" (Popularity) often correlate with massive financial losses (e.g., Awake, Lolita).


🖥️ Phase 3: Executive Dashboard (Looker Studio)
I engineered an interactive Looker Studio dashboard focusing on four strategic views:

Investment Efficiency: A breakdown of ROI by genre to guide production greenlighting.
Risk vs. Reward: A scatter plot (filtered for Market Leaders with Popularity ≥ 20) to visualize the correlation between budget and revenue.
Historical Growth: A yearly time-series analysis showing industry revenue expansion since 1927.
Market Share: A competitive analysis of Tier-1 Studios (Revenue > $1B) to identify market dominance.


💹 Phase 4: Excel Investment Decision Calculator
What It Does
While Looker Studio provides visual analytics for broad trends, stakeholders often need to simulate specific investment scenarios before greenlighting a film. This Excel calculator solves that.
An interactive What-If Investment Calculator connected live to the BigQuery pipeline via Power Query, allowing stakeholders to filter across 5 dimensions simultaneously and instantly see the expected financial outcome for that exact combination.
Filter DimensionExample Use🎭 GenreHorror, Action, Drama🌍 Production CountryUSA, UK, India🗣️ LanguageEnglish, French, Hindi📅 Release Year2010 – 2024💰 Budget TierLow (<$10M), Mid, Mega
Output Metrics (Dynamic)

📈 Median ROI
💵 Median Revenue
💹 Median Net Profit
🎬 Movie Count (sample size for confidence)

Why Excel Alongside Looker Studio?
Looker StudioExcel CalculatorPurposeTrend explorationInvestment simulationUserAnalyst / ExecutiveDecision makerInteractionFilter & browseInput scenario → get answerOutputVisual patternsPrecise median metrics
Technical Implementation

MEDIAN(IF()) array formulas for multi-criteria filtering
Dynamic chart highlighting using the NA() trick to isolate selected scenario
Named Ranges for clean, readable formula logic
Power Query auto-refresh on file open — always live data
Data Validation dropdowns sourced from clean reference lists in a dedicated sheet


📂 Project Structure
text├── python_scripts/
│   ├── full_pipeline.py      # Main ingestion script
│   └── ingest_ids.py         # ID discovery script
├── sql_queries/
│   ├── 01_cleaning_logic.sql # Silver Layer View creation
│   ├── 02_roi_analysis.sql   # Genre performance queries
│   └── 03_market_share.sql   # Studio dominance logic
├── excel_dashboard/
│   └── investment_calculator.xlsx  # Live Excel decision tool
├── .env                      # API Credentials (Hidden)
└── README.md                 # Project Documentation

🏁 Conclusion & Future Roadmap
This project demonstrates a scalable framework for market intelligence. By automating the data flow from API to Dashboard, I've reduced the manual reporting time for these insights to zero.
🔮 Planned Phases
PhaseWhatWhyPhase 5Python EDA (Matplotlib, Seaborn, Plotly)Deep statistical exploration — distributions, outliers, correlation heatmaps — directly in notebooks for transparencyPhase 6NLP Sentiment Analysis on movie reviewsQuantify audience perception and correlate review sentiment with actual box office performancePhase 7Predictive ROI Model (Scikit-learn)Train a regression model on historical data to forecast a film's ROI before production begins