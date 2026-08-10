SELECT country, SUM(bytes) AS total_bytes FROM events GROUP BY country;
