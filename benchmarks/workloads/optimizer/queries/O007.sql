SELECT COUNT(*) FROM events e JOIN users u ON e.user_id = u.user_id JOIN campaigns c ON e.campaign_id = c.campaign_id;
