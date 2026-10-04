SELECT event_id, NULLIF(device, 'mobile') AS non_mobile FROM events LIMIT 1000;
