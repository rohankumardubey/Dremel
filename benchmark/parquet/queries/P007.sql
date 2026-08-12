SELECT SUM(bytes), AVG(score), MIN(duration_ms), MAX(timestamp), COUNT(campaign_id) FROM events WHERE event_id <= 100000;
