SELECT event_id, score FROM events WHERE event_id <= 100000 ORDER BY score DESC, event_id ASC LIMIT 1000 OFFSET 100
