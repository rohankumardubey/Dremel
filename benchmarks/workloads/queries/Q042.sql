SELECT country, success, COUNT(*) AS cnt FROM events GROUP BY country, success;
