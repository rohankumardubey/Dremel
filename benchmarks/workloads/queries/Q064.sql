SELECT country, event_type, AVG(duration_ms) AS avg_duration FROM events GROUP BY country, event_type ORDER BY avg_duration DESC LIMIT 10;
