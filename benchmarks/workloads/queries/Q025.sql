SELECT event_id, bytes + duration_ms AS combined_metric FROM events WHERE device = 'desktop' LIMIT 1000;
