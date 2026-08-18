SELECT u.segment, AVG(e.score) FROM events e JOIN users u ON e.user_id = u.user_id WHERE e.event_id <= 100000 GROUP BY u.segment ORDER BY u.segment;
