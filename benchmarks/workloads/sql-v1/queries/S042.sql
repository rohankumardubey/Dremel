SELECT event_id, (SELECT MAX(budget) FROM campaigns) AS max_budget FROM events WHERE event_id <= 100 ORDER BY event_id ASC;
