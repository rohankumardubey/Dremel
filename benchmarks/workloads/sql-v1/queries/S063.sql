SELECT COUNT(*) FROM (SELECT campaign_id FROM campaigns WHERE campaign_id <= 5) c, (SELECT user_id FROM users WHERE user_id <= 10) u;
