WITH country_totals AS (SELECT country, SUM(bytes) AS total_bytes FROM events GROUP BY country) SELECT country, total_bytes FROM country_totals WHERE total_bytes > 0 ORDER BY country ASC;
