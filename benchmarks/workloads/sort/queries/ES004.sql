SELECT event_id, bytes + duration_ms AS activity_cost, country FROM events WHERE event_id <= 50000 ORDER BY activity_cost DESC, event_id ASC
