SELECT user_id, country, COUNT(*) AS events, SUM(bytes) AS total_bytes FROM events GROUP BY user_id, country ORDER BY total_bytes DESC, user_id ASC, country ASC LIMIT 100
