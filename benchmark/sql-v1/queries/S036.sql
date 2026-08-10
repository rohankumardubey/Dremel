SELECT campaign_id AS id FROM campaigns WHERE campaign_id <= 1000 UNION SELECT campaign_id AS id FROM events WHERE event_id <= 1000;
