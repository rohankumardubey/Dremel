SELECT event_id, country, ROW_NUMBER() OVER (PARTITION BY country ORDER BY score DESC, event_id ASC) AS row_number FROM events WHERE event_id <= 100000 ORDER BY event_id ASC LIMIT 1000;
