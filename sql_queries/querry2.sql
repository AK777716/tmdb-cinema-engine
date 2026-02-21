SELECT 
    CASE 
        WHEN budget < 10000000 THEN 'Low Budget (<$10M)'
        WHEN budget BETWEEN 10000000 AND 50000000 THEN 'Mid Budget ($10M-$50M)'
        WHEN budget BETWEEN 50000000 AND 150000000 THEN 'High Budget ($50M-$150M)'
        ELSE 'Mega Blockbuster (>$150M)'
    END as budget_tier,
    COUNT(*) as movie_count,
    ROUND(AVG(roi), 2) as avg_roi,
    ROUND(AVG(popularity), 2) as avg_popularity_score
FROM `cinema_intelligence.dim_movies_cleaned`
GROUP BY 1
ORDER BY avg_roi DESC;