WITH recent_events AS (SELECT event_id, country, score FROM events WHERE event_id <= 100000) SELECT event_id, country FROM recent_events WHERE score >= 99.0 ORDER BY event_id ASC LIMIT 1000;
