SELECT country, device, SUM(bytes) AS total_bytes FROM events GROUP BY country, device;
