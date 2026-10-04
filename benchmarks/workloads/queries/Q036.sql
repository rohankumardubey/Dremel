SELECT country, MIN(duration_ms) AS min_duration FROM events GROUP BY country;
