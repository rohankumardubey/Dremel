SELECT event_type, SUM(bytes) AS total_bytes FROM events GROUP BY event_type;
