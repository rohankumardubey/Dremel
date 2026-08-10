SELECT event_id FROM events e WHERE event_id <= 1000 AND NOT EXISTS (SELECT campaign_id FROM campaigns c WHERE c.campaign_id = e.campaign_id) ORDER BY event_id ASC;
