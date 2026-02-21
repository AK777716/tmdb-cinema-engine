SELECT 
    title,
    popularity,
    net_profit,
    roi
FROM `cinema_intelligence.dim_movies_cleaned`
WHERE popularity > (SELECT AVG(popularity) FROM `cinema_intelligence.dim_movies_cleaned`)
ORDER BY net_profit ASC
LIMIT 10;