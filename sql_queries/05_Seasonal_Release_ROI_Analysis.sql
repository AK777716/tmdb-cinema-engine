SELECT 
    EXTRACT(MONTH FROM release_date) as release_month,
    COUNT(*) as release_count,
    ROUND(AVG(revenue), 2) as avg_revenue,
    ROUND(AVG(roi), 2) as avg_roi
FROM `cinema_intelligence.dim_movies_cleaned`
GROUP BY 1
ORDER BY avg_revenue DESC;