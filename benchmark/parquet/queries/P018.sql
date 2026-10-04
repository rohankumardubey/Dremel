SELECT AVG(e.score), COUNT(e.score) FROM events e JOIN users u ON e.user_id = u.user_id WHERE e.event_id < 0;
