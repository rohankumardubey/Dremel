SELECT event_id, DATE_TRUNC('month', timestamp) AS event_month FROM events WHERE event_id <= 1000 ORDER BY event_id ASC;
