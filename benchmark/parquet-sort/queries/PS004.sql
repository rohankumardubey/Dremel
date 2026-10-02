SELECT event_id, bytes + duration_ms AS activity_cost FROM events WHERE event_id >= 900000 AND event_id < 920000 ORDER BY activity_cost DESC, event_id ASC
