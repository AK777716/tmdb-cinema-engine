CREATE OR REPLACE VIEW `cinema_intelligence.dim_movies_cleaned` AS
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
        CAST(release_date AS DATE) as release_date,
        budget, 
        revenue, 
        (revenue - budget) as net_profit,
        SAFE_DIVIDE(revenue, budget) as roi,
        popularity, 
        original_language, 
        ingested_at, 
        genres,
        production_companies,
        -- ✅ NEW ADDITIONS (Added carefully without touching previous logic)
        production_countries, 
        adult as is_adult_content, 
        vote_average,
        vote_count
    FROM `cinema_intelligence.movies_raw`
    WHERE budget > 0 AND revenue > 0 AND release_date IS NOT NULL
      AND status = 'Released'
);