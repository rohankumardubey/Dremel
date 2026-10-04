SELECT event_id, MIN(score) OVER (PARTITION BY country) AS country_min, MAX(score) OVER (PARTITION BY country) AS country_max FROM events WHERE event_id <= 100000 ORDER BY event_id ASC LIMIT 1000;
