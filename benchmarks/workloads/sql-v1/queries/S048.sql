SELECT event_id, EXTRACT(year FROM timestamp) AS event_year, EXTRACT(month FROM timestamp) AS event_month FROM events WHERE event_id <= 1000 ORDER BY event_id ASC;
