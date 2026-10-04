SELECT event_id, bytes + duration_ms AS activity_cost, score * 2 AS doubled_score, success, country FROM events WHERE event_id <= 100000
