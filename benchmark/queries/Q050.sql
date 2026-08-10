SELECT country, COUNT(*) AS cnt, SUM(bytes) AS total_bytes, AVG(score) AS avg_score FROM events WHERE success = true GROUP BY country;
