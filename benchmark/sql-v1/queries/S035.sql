SELECT country AS value FROM events WHERE event_id <= 1000 UNION SELECT region AS value FROM users WHERE user_id <= 1000;
