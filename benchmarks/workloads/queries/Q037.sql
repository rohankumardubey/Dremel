SELECT device, SUM(duration_ms) AS total_duration FROM events GROUP BY device;
