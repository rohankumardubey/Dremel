SELECT campaign_id, budget + CAST('0.05' AS DECIMAL(18,2)) AS adjusted_budget FROM campaigns WHERE campaign_id <= 100 ORDER BY campaign_id ASC;
