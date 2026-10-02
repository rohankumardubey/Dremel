SELECT event_id, country, score FROM events WHERE event_id BETWEEN 200000 AND 201000 ORDER BY score DESC, event_id ASC LIMIT 25;
