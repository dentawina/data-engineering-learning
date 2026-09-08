/*
  Gold USGS earthquake daily summary.
  Source: silver.usgs_earthquakes
  Grain: one row per event_date.
*/
SELECT
    event_time::date AS event_date,
    COUNT(*)::integer AS earthquake_count,
    ROUND(AVG(magnitude)::numeric, 2) AS avg_magnitude,
    MAX(magnitude) AS max_magnitude,
    ROUND(AVG(depth_km)::numeric, 2) AS avg_depth_km,
    MIN(depth_km) AS min_depth_km,
    COUNT(*) FILTER (WHERE magnitude >= 5)::integer AS magnitude_5_plus_count,
    COUNT(*) FILTER (WHERE tsunami = 1)::integer AS tsunami_flag_count,
    now() AS processed_at
FROM silver.usgs_earthquakes
WHERE event_time IS NOT NULL
GROUP BY event_time::date
ORDER BY event_date;
