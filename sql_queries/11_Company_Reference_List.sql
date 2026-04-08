-- ============================================
-- Query 11: Clean Company Reference List
-- Purpose: Explodes comma-separated companies
-- into individual rows for Excel dropdown only.
-- Filtered to companies with 5+ movies to keep
-- dropdown manageable.
-- ============================================

SELECT DISTINCT
  TRIM(company) AS company_name,
  COUNT(*) AS movie_count
FROM `cinema-intelligence-engine.cinema_intelligence.dim_movies_cleaned`,
UNNEST(SPLIT(production_companies, ',')) AS company
WHERE production_companies IS NOT NULL
GROUP BY company_name
HAVING movie_count >= 5
ORDER BY movie_count DESC