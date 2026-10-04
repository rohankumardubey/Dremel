SELECT event_id, CASE WHEN score >= 90.0 THEN 'high' WHEN score >= 50.0 THEN 'medium' ELSE 'low' END AS score_band FROM events LIMIT 1000;
