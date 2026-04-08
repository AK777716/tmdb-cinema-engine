-- ============================================
-- Query 10: Clean Country Reference List  
-- Purpose: Explodes comma-separated countries
-- into individual rows for Excel dropdown only.
-- ============================================

SELECT DISTINCT
  TRIM(country) AS country_name
FROM `cinema-intelligence-engine.cinema_intelligence.dim_movies_cleaned`,
UNNEST(SPLIT(production_countries, ',')) AS country
WHERE production_countries IS NOT NULL
ORDER BY country_name ASC


