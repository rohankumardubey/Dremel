SELECT event_id, campaign_id, country FROM events WHERE event_id <= 100000 ORDER BY campaign_id ASC NULLS LAST, country DESC, event_id DESC LIMIT 1000
