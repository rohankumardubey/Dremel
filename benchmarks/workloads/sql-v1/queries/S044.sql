SELECT event_id FROM events WHERE event_id <= 1000 AND campaign_id IN (SELECT campaign_id FROM campaigns) ORDER BY event_id ASC;
