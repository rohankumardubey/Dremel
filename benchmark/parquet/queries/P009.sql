SELECT event_id, ROW_NUMBER() OVER (PARTITION BY country ORDER BY score DESC, event_id ASC) FROM events WHERE event_id <= 500 ORDER BY event_id;
