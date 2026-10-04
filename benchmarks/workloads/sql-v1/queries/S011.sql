SELECT event_id, COALESCE(campaign_id, 0) AS campaign FROM events LIMIT 1000;
