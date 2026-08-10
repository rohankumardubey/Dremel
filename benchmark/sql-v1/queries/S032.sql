SELECT event_id, COUNT(*) OVER (PARTITION BY country) AS country_events, AVG(score) OVER (PARTITION BY country) AS country_score FROM events WHERE event_id <= 100000 ORDER BY event_id ASC LIMIT 1000;
