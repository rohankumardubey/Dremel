SELECT event_id, duration_ms FROM events WHERE event_id <= 100000 AND score >= 99.5 ORDER BY event_id ASC LIMIT 50;
