SELECT country, COUNT(*) AS total FROM events WHERE score >= 90.0 GROUP BY country HAVING COUNT(*) > 0 ORDER BY total DESC;
