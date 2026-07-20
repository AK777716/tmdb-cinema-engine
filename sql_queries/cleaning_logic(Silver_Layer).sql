CREATE OR REPLACE VIEW `cinema-intelligence-engine.cinema_intelligence.dim_movies_cleaned` AS
SELECT 
    *,
    CASE 
        WHEN original_language = 'hi' THEN 'Hindi'
        WHEN original_language = 'ml' THEN 'Malayalam'
        WHEN original_language = 'ta' THEN 'Tamil'
        WHEN original_language = 'te' THEN 'Telugu'
        WHEN original_language = 'kn' THEN 'Kannada'
        WHEN original_language = 'en' THEN 'English'
        ELSE 'Other International' 
    END AS language_name
FROM (
    SELECT 
        id, 
        title, 
        SAFE_CAST(release_date AS DATE) as release_date,
        COALESCE(budget, 0) as budget, 
        COALESCE(revenue, 0) as revenue, 
        (COALESCE(revenue, 0) - COALESCE(budget, 0)) as net_profit,
        
        -- ✅ Optimized: True financial ROI definition: (Revenue - Budget) / Budget
        -- Returns NULL if budget is 0 to protect dashboard average aggregations
        CASE 
            WHEN COALESCE(budget, 0) > 0 THEN SAFE_DIVIDE((COALESCE(revenue, 0) - COALESCE(budget, 0)), COALESCE(budget, 0))
            ELSE NULL 
        END AS roi,
        
        popularity, 
        original_language, 
        ingested_at, 
        IFNULL(CAST(genres AS STRING), 'Unknown') as genres,
        IFNULL(CAST(production_companies AS STRING), 'Unknown') as production_companies,
        IFNULL(CAST(production_countries AS STRING), 'Unknown') as production_countries, 
        IFNULL(CAST(adult AS STRING), 'false') as is_adult_content, 
        vote_average,
        vote_count
    FROM `cinema-intelligence-engine.cinema_intelligence.movies_raw`
    WHERE title IS NOT NULL 
      AND release_date IS NOT NULL
      AND release_date != '' 
    
    -- ✅ Optimized: Perfectly match your Python pipeline deduplication sequence
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY (COALESCE(budget, 0) + COALESCE(revenue, 0)) DESC, ingested_at DESC) = 1
);