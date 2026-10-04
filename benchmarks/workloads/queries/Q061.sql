SELECT country, SUM(bytes) AS total_bytes FROM events GROUP BY country ORDER BY total_bytes DESC LIMIT 5;
