SELECT device, success, AVG(score) AS avg_score FROM events GROUP BY device, success;
