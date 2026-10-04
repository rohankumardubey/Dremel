SELECT event_id, user_id, country, device, event_type, duration_ms, bytes, score, success, campaign_id FROM events WHERE score > 99.0 LIMIT 1000;
