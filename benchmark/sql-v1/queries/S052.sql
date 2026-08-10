SELECT user_id, EXTRACT(year FROM signup_date) AS signup_year FROM users WHERE user_id <= 100 ORDER BY user_id ASC;
