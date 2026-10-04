SELECT device, event_type, COUNT(*) AS cnt FROM events GROUP BY device, event_type;
