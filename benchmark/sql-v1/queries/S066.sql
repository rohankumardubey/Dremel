SELECT campaign_id, budget + 1 AS decimal_plus_int, budget + 0.5 AS double_plus_decimal FROM campaigns WHERE campaign_id <= 100 ORDER BY campaign_id ASC;
