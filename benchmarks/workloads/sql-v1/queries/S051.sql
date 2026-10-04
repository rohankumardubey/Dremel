SELECT user_id, CAST('123.456' AS DECIMAL(18,2)) AS rounded_decimal, CAST(lifetime_value AS DECIMAL(18,2)) AS value FROM users WHERE user_id <= 100 ORDER BY user_id ASC;
