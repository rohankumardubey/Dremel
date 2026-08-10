SELECT event_id, CONCAT(SUBSTRING(event_type, 1, 3), '-', CAST(user_id AS varchar)) AS label FROM events LIMIT 1000;
