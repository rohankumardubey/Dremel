SELECT event_id, CAST(score AS bigint) AS whole_score, CAST(event_id AS varchar) AS event_text FROM events LIMIT 1000;
