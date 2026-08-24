SELECT timestamp, COUNT(*) AS events, SUM(bytes) AS total_bytes FROM events WHERE success = true GROUP BY timestamp ORDER BY events DESC, timestamp DESC LIMIT 100 OFFSET 10
