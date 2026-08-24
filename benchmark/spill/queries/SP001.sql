SELECT event_id, COUNT(*) AS cnt FROM events GROUP BY event_id ORDER BY event_id DESC LIMIT 100
