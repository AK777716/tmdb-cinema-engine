CREATE OR REPLACE VIEW `cinema-intelligence-engine.cinema_intelligence.v_automation_scorecard` AS
SELECT 
    -- 1. Total lifetime volume tracked across warehouse history
    (SELECT COUNT(DISTINCT id) FROM `cinema_intelligence.dim_movies_cleaned`) as total_movies_tracked,
    
    -- 2. Cleaned delta load count converting the string to a timestamp with Indian Standard Time (IST)
    (SELECT COUNT(*) 
     FROM `cinema_intelligence.movies_raw` 
     WHERE DATE(SAFE_CAST(ingested_at AS TIMESTAMP), 'Asia/Kolkata') = CURRENT_DATE('Asia/Kolkata')) as added_today,
     
    -- 3. The highest performing language cohort pulled from today's active ingest window
    COALESCE(
        (SELECT language_name 
         FROM `cinema_intelligence.dim_movies_cleaned` 
         WHERE DATE(SAFE_CAST(ingested_at AS TIMESTAMP), 'Asia/Kolkata') = CURRENT_DATE('Asia/Kolkata')
           AND roi IS NOT NULL
         ORDER BY roi DESC 
         LIMIT 1), 
        'No New Ingest Data'
    ) as top_roi_language_today;