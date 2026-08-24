SELECT campaign_id, COUNT(*) AS events, AVG(score) AS avg_score FROM events GROUP BY campaign_id ORDER BY events DESC, campaign_id ASC NULLS FIRST LIMIT 50
