SELECT country, device, COUNT(*) AS cnt, AVG(score) AS avg_score FROM events WHERE success = true AND score > 25.0 GROUP BY country, device ORDER BY cnt DESC LIMIT 20;
