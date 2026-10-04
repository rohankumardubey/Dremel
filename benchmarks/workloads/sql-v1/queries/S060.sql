WITH selected_campaigns AS (SELECT campaign_id FROM campaigns WHERE campaign_id <= 100) SELECT COUNT(s.campaign_id) FROM campaigns c LEFT JOIN selected_campaigns s ON c.campaign_id = s.campaign_id;
