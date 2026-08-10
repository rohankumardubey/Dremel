SELECT ABS(-9223372036854775807) AS magnitude, 1 / 0 AS divide_by_zero, CAST('not-an-int' AS BIGINT) AS invalid_cast FROM events LIMIT 1;
