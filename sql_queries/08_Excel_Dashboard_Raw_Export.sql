-- ============================================
-- Query 08: Excel Dashboard Raw Export
-- Purpose: Exports cleaned row-level movie data
-- to Google Sheets (TMDB_Cleaned_Data) which 
-- connects to Excel dashboard via Power Query.
-- Refresh: Manual re-export when needed.
-- ============================================

SELECT
  id,
  title,
  CAST(release_date AS DATE) AS release_date,
  EXTRACT(YEAR FROM release_date) AS release_year,
  EXTRACT(MONTH FROM release_date) AS release_month,
  budget,
  revenue,
  net_profit,
  roi,
  popularity,
  original_language,
  language_name,
  genres,
  production_companies,
  production_countries,
  is_adult_content,
  vote_average,
  vote_count,
  CASE 
    WHEN budget < 10000000 THEN 'Low (<$10M)'
    WHEN budget BETWEEN 10000000 AND 50000000 THEN 'Mid ($10M-$50M)'
    WHEN budget BETWEEN 50000000 AND 150000000 THEN 'High ($50M-$150M)'
    ELSE 'Blockbuster (>$150M)'
  END AS budget_tier
FROM `cinema-intelligence-engine.cinema_intelligence.dim_movies_cleaned`
ORDER BY release_date DESC