SELECT e.event_id, u.segment FROM events e JOIN users u ON e.user_id = u.user_id WHERE e.event_id <= 1000 ORDER BY e.event_id DESC LIMIT 20 OFFSET 5;
