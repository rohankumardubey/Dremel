SELECT SUM(CASE WHEN success = true THEN bytes ELSE 0 END) AS successful_bytes FROM events;
