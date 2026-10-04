SELECT country, device, event_type, COUNT(*) AS cnt FROM events GROUP BY country, device, event_type;
