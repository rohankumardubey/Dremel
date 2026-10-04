SELECT event_id, campaign_id FROM events WHERE event_id <= 1000 ORDER BY campaign_id ASC NULLS LAST, event_id DESC LIMIT 100;
