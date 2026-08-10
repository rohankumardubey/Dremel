SELECT event_id, event_type, bytes FROM events WHERE event_type = 'download' AND bytes > 500000 LIMIT 1000;
