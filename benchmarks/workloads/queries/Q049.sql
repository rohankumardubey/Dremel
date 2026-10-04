SELECT device, COUNT(*) AS cnt, AVG(bytes) AS avg_bytes, MIN(duration_ms) AS min_duration, MAX(duration_ms) AS max_duration FROM events GROUP BY device;
