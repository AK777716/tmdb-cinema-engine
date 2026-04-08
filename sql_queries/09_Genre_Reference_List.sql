-- ============================================
-- Query 09: Clean Genre Reference List
-- Purpose: Explodes comma-separated genres into
-- individual rows for Excel dropdown use only.
-- Not used for movie-level analysis.
-- ============================================

SELECT DISTINCT
  TRIM(genre) AS genre_name
FROM `cinema-intelligence-engine.cinema_intelligence.dim_movies_cleaned`,
UNNEST(SPLIT(genres, ',')) AS genre
WHERE genres IS NOT NULL
ORDER BY genre_name ASC



