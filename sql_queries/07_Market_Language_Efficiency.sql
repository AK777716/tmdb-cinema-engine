SELECT
  language_name,
  COUNT(*) AS total_movies,
  ROUND(AVG(net_profit),2) AS avg_profit,
  ROUND(AVG(roi),2) AS avg_roi
FROM `cinema-intelligence-engine.cinema_intelligence.dim_movies_cleaned`
WHERE budget > 0
GROUP BY language_name
HAVING total_movies > 10
ORDER BY avg_profit DESC;