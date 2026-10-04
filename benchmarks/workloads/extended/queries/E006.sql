SELECT country, COUNT(*) AS cnt, AVG(campaign_id) AS avg_campaign FROM events WHERE campaign_id > 2500 GROUP BY country ORDER BY country ASC;
