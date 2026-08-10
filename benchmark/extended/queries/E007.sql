SELECT event_id, bytes / (duration_ms + 1) AS transfer_rate FROM events WHERE success = true LIMIT 1000;
