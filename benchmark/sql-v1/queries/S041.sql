WITH high_scores AS (SELECT event_id, country FROM events WHERE score >= 90.0) SELECT country, COUNT(*) AS total FROM high_scores GROUP BY country HAVING COUNT(*) > 0 ORDER BY country ASC;
