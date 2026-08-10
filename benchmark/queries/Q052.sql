SELECT device, COUNT(*) AS cnt, SUM(duration_ms) AS total_duration, MAX(score) AS max_score FROM events WHERE score > 50.0 GROUP BY device;
