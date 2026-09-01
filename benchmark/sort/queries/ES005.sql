SELECT event_id, country, event_type, score FROM events WHERE success = true AND event_type = 'purchase' ORDER BY country ASC, score DESC, event_id ASC
