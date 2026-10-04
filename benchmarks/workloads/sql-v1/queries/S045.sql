SELECT event_id, (SELECT MAX(budget) FROM campaigns c WHERE c.campaign_id = e.campaign_id) AS campaign_budget FROM events e WHERE event_id <= 1000 ORDER BY event_id ASC;
