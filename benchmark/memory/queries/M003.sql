SELECT user_id, COUNT(*) FROM events WHERE event_id <= 100000 GROUP BY user_id
