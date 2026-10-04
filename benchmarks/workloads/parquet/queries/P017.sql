SELECT AVG(e.score), AVG(e.duration_ms), COUNT(e.score) FROM events e JOIN users u ON e.user_id = u.user_id WHERE e.event_id <= 100000;
