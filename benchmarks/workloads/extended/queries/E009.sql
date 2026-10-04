SELECT event_type, SUM(bytes + duration_ms) AS total_work, AVG(score * 1.5) AS weighted_score FROM events GROUP BY event_type;
