SELECT device, AVG(duration_ms) AS avg_duration FROM events GROUP BY device ORDER BY avg_duration ASC;
