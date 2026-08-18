SELECT c.channel, COUNT(*) FROM events e LEFT JOIN campaigns c ON e.campaign_id = c.campaign_id WHERE e.event_id <= 100000 GROUP BY c.channel ORDER BY c.channel NULLS FIRST;
