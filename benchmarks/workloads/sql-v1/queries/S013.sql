SELECT event_id, LOWER(country) AS lower_country, UPPER(device) AS upper_device, LENGTH(event_type) AS event_length FROM events LIMIT 1000;
