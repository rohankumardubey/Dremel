WITH selected_campaigns AS (SELECT campaign_id FROM campaigns WHERE campaign_id <= 100) SELECT COUNT(*) FROM selected_campaigns s JOIN campaigns c ON s.campaign_id = c.campaign_id;
