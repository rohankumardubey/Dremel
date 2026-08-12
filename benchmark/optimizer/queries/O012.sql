SELECT COUNT(*) FROM events e JOIN campaigns c ON e.campaign_id = c.campaign_id JOIN users u ON e.user_id = u.user_id WHERE u.user_id <= 100;
