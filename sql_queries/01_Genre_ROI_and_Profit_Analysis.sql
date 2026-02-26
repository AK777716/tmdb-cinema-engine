SELECT
  genres,
  COUNT(*) AS total_movies,
  ROUND(AVG(budget),2) AS avg_budget,
  ROUND(AVG(revenue),2) AS avg_revenue,
  ROUND(AVG(net_profit),2) AS avg_profit,
  ROUND(AVG(roi),2) AS avg_roi
FROM `cinema-intelligence-engine.cinema_intelligence.dim_movies_cleaned`
WHERE budget > 0 
  AND revenue > 0
GROUP BY genres
HAVING total_movies > 20
ORDER BY avg_profit DESC;