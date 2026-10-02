SELECT country, COUNT(*), SUM(bytes) FROM events WHERE event_id <= 100000 GROUP BY country ORDER BY country;
