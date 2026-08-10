SELECT country, device, COUNT(*) AS cnt FROM events GROUP BY country, device ORDER BY cnt DESC LIMIT 10;
