# Excel Dashboard Setup Guide

## Power Query Connection URL
https://docs.google.com/spreadsheets/d/1eQYEU1ZHxVgPMMneD9pGSfEsKOSW_VPNwoYZb5hspn8/export?format=csv&gid=502841687

## How to Refresh Data
1. Open TMDB_Movie_Dashboard.xlsx
2. Go to Data tab → Click "Refresh All"
3. Data pulls latest from TMDB_Cleaned_Data Google Sheet
4. Google Sheet is auto-updated every morning from BigQuery

## Connection Chain
TMDB API → movies_raw (BigQuery) 
         → dim_movies_cleaned (BigQuery View) 
         → 08_Excel_Dashboard_Raw_Export.sql (Query)
         → TMDB_Cleaned_Data (Google Sheets)
         → Power Query Connection
         → TMDB_Movie_Dashboard.xlsx (Excel)



         ## Sheet Architecture

### Sheet 1: Data
- Source: Power Query connection to TMDB_Cleaned_Data Google Sheet
- Table name: movies
- Auto-refreshes on file open

### Sheet 2: Data_Validation
- Purpose: Unique sorted lists powering all dropdown filters
- Columns:
  - A: genre_sorted        → =SORT(UNIQUE(movies[genres]))
  - B: country_sorted      → =SORT(UNIQUE(movies[production_countries]))
  - C: language_sorted     → =SORT(UNIQUE(movies[language_name]))
  - D: year_sorted         → =SORT(UNIQUE(movies[release_year]),,,-1)
  - E: budget_tier_sorted  → =SORT(UNIQUE(movies[budget_tier]))
  - F: company_sorted      → =SORT(UNIQUE(movies[production_companies]))

  ### Sheet 3: Genre
- A: genre_name     → =Data_Validation!A2#
- B: median_roi     → =MEDIAN(IF(ISNUMBER(SEARCH()))) array formula
- C: avg_profit     → =AVERAGEIF with wildcard
- D: movie_count    → =COUNTIF with wildcard

### Sheet 4: Country
- A: country_name   → =Data_Validation!B2#
- B: median_revenue → =MEDIAN(IF(ISNUMBER(SEARCH()))) array formula
- C: movie_count    → =COUNTIF with wildcard

### Sheet 5: Language
- A: language_name  → =Data_Validation!C2#
- B: median_roi     → =MEDIAN(IF()) array formula
- C: avg_profit     → =AVERAGEIF
- D: movie_count    → =COUNTIF

### Sheet 6: Budget_Tier
- A: budget_tier    → =Data_Validation!E2#
- B: median_roi     → =MEDIAN(IF()) array formula
- C: avg_revenue    → =AVERAGEIF
- D: movie_count    → =COUNTIF

## Reference Data Architecture

### Why Three Separate Reference Queries Exist
genres, production_countries, and production_companies in 
dim_movies_cleaned store multiple values as comma-separated 
strings per row (e.g. "Action, Comedy, Drama").

For Excel dropdowns to show clean individual values, three 
separate BigQuery queries explode these columns using UNNEST 
and SPLIT, then save results to dedicated Google Sheets.

### Reference Tables
| Query File                    | Google Sheet           | Excel Table  |
|-------------------------------|------------------------|--------------|
| 09_Genre_Reference_List.sql   | TMDB_Genre_Reference   | genre_ref    |
| 10_Country_Reference_List.sql | TMDB_Country_Reference | country_ref  |
| 11_Company_Reference_List.sql | TMDB_Company_Reference | company_ref  |

### Filtering Logic
Dropdowns use clean reference lists.
KPI formulas use ISNUMBER(SEARCH()) wildcard matching against
the original comma-separated column in movies table.
This preserves full genre/country information per movie while
giving stakeholders clean dropdown options.

## Reference Google Sheets (Public - Read Only)

| Reference Sheet      | URL                                                                 |
|----------------------|---------------------------------------------------------------------|
| TMDB_Genre_Reference | https://docs.google.com/spreadsheets/d/1IFYxm_sYsUzEcChCLGfx-tldYQ4SkE0Q6ka5bgRsVnc |
| TMDB_Country_Reference | https://docs.google.com/spreadsheets/d/1owXBUKGbkzGOtUJn5FmnwfL-0u6uPKSOKce5YQnCHII |
| TMDB_Company_Reference | https://docs.google.com/spreadsheets/d/1mFYalgNIEre6RDtD65cbL2jRRRdA_gxySS40XVgWm-Q |

Note: These are dropdown reference lists only.
Filtering logic uses ISNUMBER(SEARCH()) against 
the main movies table, not these reference sheets.