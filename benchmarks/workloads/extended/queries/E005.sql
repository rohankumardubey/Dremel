SELECT COUNT(*) FROM events WHERE NOT success = false AND (score >= 80.0 OR bytes > 950000);
