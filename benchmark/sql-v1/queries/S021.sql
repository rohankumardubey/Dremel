SELECT COUNT(c.campaign_id) AS matched_campaigns FROM events e LEFT JOIN campaigns c ON e.campaign_id = c.campaign_id;
