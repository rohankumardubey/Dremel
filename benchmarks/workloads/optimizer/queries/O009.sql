SELECT event_id, ROW_NUMBER() OVER (PARTITION BY country ORDER BY score DESC, event_id ASC) AS rn FROM events WHERE event_id <= 100000 ORDER BY event_id ASC LIMIT 1000;
