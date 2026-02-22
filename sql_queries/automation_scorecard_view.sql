CREATE OR REPLACE VIEW `cinema_intelligence.v_automation_scorecard` AS
SELECT 
    (SELECT COUNT(*) FROM `cinema_intelligence.movies_raw`) as total_movies_tracked,
    (SELECT COUNT(*) FROM `cinema_intelligence.movies_raw` WHERE DATE(ingested_at) = CURRENT_DATE()) as added_today,
    (SELECT language_name 
     FROM `cinema_intelligence.dim_movies_cleaned` 
     WHERE DATE(ingested_at) = CURRENT_DATE() 
     ORDER BY roi DESC LIMIT 1) as top_roi_language_today;