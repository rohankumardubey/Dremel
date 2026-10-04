SELECT country, SUM(bytes + duration_ms) AS total_work FROM events GROUP BY country ORDER BY total_work DESC LIMIT 5;
