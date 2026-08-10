SELECT d.country, d.total FROM (SELECT country, COUNT(*) AS total FROM events GROUP BY country) AS d WHERE d.total > 0 ORDER BY country ASC;
