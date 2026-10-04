SELECT u.segment, COUNT(*) AS event_count FROM events e JOIN users u ON e.user_id = u.user_id GROUP BY u.segment HAVING COUNT(*) > 1000 ORDER BY u.segment ASC;
