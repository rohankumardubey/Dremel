SELECT event_type, COUNT(*) AS cnt, SUM(bytes) AS total_bytes, SUM(duration_ms) AS total_duration, AVG(score) AS avg_score FROM events GROUP BY event_type;
