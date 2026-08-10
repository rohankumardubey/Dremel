SELECT country, MAX(score) AS max_score FROM events GROUP BY country ORDER BY max_score DESC;
