SELECT
  CORR(popularity, revenue) AS popularity_revenue_correlation,
  CORR(vote_average, revenue) AS rating_revenue_correlation
FROM `cinema-intelligence-engine.cinema_intelligence.dim_movies_cleaned`
WHERE revenue > 0;