SELECT country, event_type, COUNT(*) AS cnt FROM events GROUP BY country, event_type;
