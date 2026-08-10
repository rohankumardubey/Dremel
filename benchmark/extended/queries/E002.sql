SELECT event_id, NOT (campaign_id > 2500) AS low_campaign FROM events LIMIT 1000;
