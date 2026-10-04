SELECT COUNT(campaign_id) AS campaign_count, SUM(campaign_id) AS campaign_sum, AVG(campaign_id) AS campaign_avg, MIN(campaign_id) AS campaign_min, MAX(campaign_id) AS campaign_max FROM events;
