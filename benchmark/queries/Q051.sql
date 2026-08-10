SELECT event_type, COUNT(*) AS cnt, SUM(bytes) AS total_bytes, AVG(duration_ms) AS avg_duration FROM events WHERE duration_ms > 1000 GROUP BY event_type;
