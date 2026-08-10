SELECT country, COUNT(*) AS cnt, SUM(bytes) AS total_bytes, AVG(duration_ms) AS avg_duration, MIN(score) AS min_score, MAX(score) AS max_score FROM events GROUP BY country;
