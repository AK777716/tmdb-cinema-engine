SELECT 
    genres,
    COUNT(*) as total_movies,
    ROUND(AVG(budget), 2) as avg_investment,
    ROUND(AVG(roi), 2) as avg_return_multiplier,
    SUM(net_profit) as total_profit_contribution
FROM `cinema_intelligence.dim_movies_cleaned`
GROUP BY 1
HAVING total_movies > 5
ORDER BY avg_return_multiplier DESC;