SELECT 
    id,
    title,
    release_date,
    EXTRACT(YEAR FROM CAST(release_date AS DATE)) as release_year,
    -- Handling 0s by marking them as NULL (standard practice)
    NULLIF(budget, 0) as budget,
    NULLIF(revenue, 0) as revenue,
    -- Financial Metrics (calculated only when data exists)
    CASE WHEN budget > 0 AND revenue > 0 THEN (revenue - budget) ELSE NULL END as net_profit,
    CASE WHEN budget > 0 AND revenue > 0 THEN (revenue / budget) ELSE NULL END as roi,
    genres,
    popularity
FROM 
    `cinema_intelligence.movies_raw`
WHERE 
    -- Filtering for movies that actually have the data we need for ROI
    budget > 0 
    AND revenue > 0
    AND release_date IS NOT NULL