SELECT event_id, score * 1.5 AS weighted_score FROM events WHERE success = true AND country = 'DE' LIMIT 1000;
