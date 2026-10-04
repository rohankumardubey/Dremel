#!/usr/bin/env python3
"""Create the additive production-v1 SQL capability workload."""

import json
from pathlib import Path

QUERIES = [
    (
        "qualified_names",
        "SELECT events.event_id, events.country FROM events WHERE events.event_id <= 1000 ORDER BY event_id ASC;",
    ),
    (
        "searched_case",
        "SELECT event_id, CASE WHEN score >= 90.0 THEN 'high' WHEN score >= 50.0 THEN 'medium' ELSE 'low' END AS score_band FROM events LIMIT 1000;",
    ),
    (
        "case_aggregate",
        "SELECT SUM(CASE WHEN success = true THEN bytes ELSE 0 END) AS successful_bytes FROM events;",
    ),
    ("in", "SELECT COUNT(*) FROM events WHERE country IN ('IN', 'US', 'SG');"),
    (
        "not_in_null",
        "SELECT COUNT(*) FROM events WHERE campaign_id NOT IN (1, 2, 3, NULL);",
    ),
    ("between", "SELECT COUNT(*) FROM events WHERE score BETWEEN 25.0 AND 75.0;"),
    (
        "not_between",
        "SELECT COUNT(*) FROM events WHERE duration_ms NOT BETWEEN 100 AND 9000;",
    ),
    ("like", "SELECT COUNT(*) FROM events WHERE event_type LIKE 'cl_ck';"),
    ("not_like", "SELECT COUNT(*) FROM events WHERE device NOT LIKE '%ile';"),
    (
        "cast",
        "SELECT event_id, CAST(score AS bigint) AS whole_score, CAST(event_id AS varchar) AS event_text FROM events LIMIT 1000;",
    ),
    (
        "coalesce",
        "SELECT event_id, COALESCE(campaign_id, 0) AS campaign FROM events LIMIT 1000;",
    ),
    (
        "nullif",
        "SELECT event_id, NULLIF(device, 'mobile') AS non_mobile FROM events LIMIT 1000;",
    ),
    (
        "string_case",
        "SELECT event_id, LOWER(country) AS lower_country, UPPER(device) AS upper_device, LENGTH(event_type) AS event_length FROM events LIMIT 1000;",
    ),
    (
        "string_compose",
        "SELECT event_id, CONCAT(SUBSTRING(event_type, 1, 3), '-', CAST(user_id AS varchar)) AS label FROM events LIMIT 1000;",
    ),
    ("escaped_literal", "SELECT 'Dremel''s SQL' AS label FROM events LIMIT 1;"),
    ("alternate_not_equal", "SELECT COUNT(*) FROM events WHERE country <> 'IN';"),
    ("users_scan", "SELECT COUNT(*) FROM users;"),
    ("campaigns_scan", "SELECT COUNT(*) FROM campaigns;"),
    (
        "inner_hash_join",
        "SELECT COUNT(*) FROM events e INNER JOIN users u ON e.user_id = u.user_id;",
    ),
    (
        "join_group",
        "SELECT u.segment, COUNT(*) AS event_count FROM events e JOIN users u ON e.user_id = u.user_id GROUP BY u.segment ORDER BY u.segment ASC;",
    ),
    (
        "left_hash_join",
        "SELECT COUNT(c.campaign_id) AS matched_campaigns FROM events e LEFT JOIN campaigns c ON e.campaign_id = c.campaign_id;",
    ),
    (
        "multi_join",
        "SELECT u.region, c.channel, COUNT(*) AS event_count FROM events e JOIN users u ON e.user_id = u.user_id LEFT JOIN campaigns c ON e.campaign_id = c.campaign_id WHERE u.active = true GROUP BY u.region, c.channel ORDER BY event_count DESC LIMIT 20;",
    ),
    (
        "right_hash_join",
        "SELECT COUNT(*) FROM events e RIGHT JOIN campaigns c ON e.campaign_id = c.campaign_id;",
    ),
    (
        "full_hash_join",
        "SELECT COUNT(*) FROM campaigns c FULL JOIN events e ON c.campaign_id = e.campaign_id;",
    ),
    (
        "having",
        "SELECT u.segment, COUNT(*) AS event_count FROM events e JOIN users u ON e.user_id = u.user_id GROUP BY u.segment HAVING COUNT(*) > 1000 ORDER BY u.segment ASC;",
    ),
    (
        "distinct",
        "SELECT DISTINCT country, device FROM events ORDER BY country ASC, device DESC;",
    ),
    (
        "multi_order_nulls",
        "SELECT event_id, campaign_id FROM events WHERE event_id <= 1000 ORDER BY campaign_id ASC NULLS LAST, event_id DESC LIMIT 100;",
    ),
    (
        "offset",
        "SELECT event_id, score FROM events ORDER BY event_id ASC LIMIT 100 OFFSET 500;",
    ),
    (
        "window_row_number",
        "SELECT event_id, country, ROW_NUMBER() OVER (PARTITION BY country ORDER BY score DESC, event_id ASC) AS row_number FROM events WHERE event_id <= 100000 ORDER BY event_id ASC LIMIT 1000;",
    ),
    (
        "window_ranks",
        "SELECT event_id, RANK() OVER (PARTITION BY device ORDER BY duration_ms DESC) AS rank_value, DENSE_RANK() OVER (PARTITION BY device ORDER BY duration_ms DESC) AS dense_rank_value FROM events WHERE event_id <= 100000 ORDER BY event_id ASC LIMIT 1000;",
    ),
    (
        "window_lag_lead",
        "SELECT event_id, LAG(bytes, 1, 0) OVER (PARTITION BY user_id ORDER BY timestamp ASC) AS previous_bytes, LEAD(bytes, 1, 0) OVER (PARTITION BY user_id ORDER BY timestamp ASC) AS next_bytes FROM events WHERE event_id <= 100000 ORDER BY event_id ASC LIMIT 1000;",
    ),
    (
        "window_partition_aggregate",
        "SELECT event_id, COUNT(*) OVER (PARTITION BY country) AS country_events, AVG(score) OVER (PARTITION BY country) AS country_score FROM events WHERE event_id <= 100000 ORDER BY event_id ASC LIMIT 1000;",
    ),
    (
        "window_running_frame",
        "SELECT event_id, SUM(bytes) OVER (PARTITION BY country ORDER BY event_id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_bytes FROM events WHERE event_id <= 100000 ORDER BY event_id ASC LIMIT 1000;",
    ),
    (
        "union_all",
        "SELECT country AS value FROM events WHERE event_id <= 1000 UNION ALL SELECT region AS value FROM users WHERE user_id <= 1000;",
    ),
    (
        "union_distinct",
        "SELECT country AS value FROM events WHERE event_id <= 1000 UNION SELECT region AS value FROM users WHERE user_id <= 1000;",
    ),
    (
        "union_numeric",
        "SELECT campaign_id AS id FROM campaigns WHERE campaign_id <= 1000 UNION SELECT campaign_id AS id FROM events WHERE event_id <= 1000;",
    ),
    (
        "cte_aggregate",
        "WITH country_totals AS (SELECT country, SUM(bytes) AS total_bytes FROM events GROUP BY country) SELECT country, total_bytes FROM country_totals WHERE total_bytes > 0 ORDER BY country ASC;",
    ),
    (
        "cte_filter_project",
        "WITH recent_events AS (SELECT event_id, country, score FROM events WHERE event_id <= 100000) SELECT event_id, country FROM recent_events WHERE score >= 99.0 ORDER BY event_id ASC LIMIT 1000;",
    ),
    (
        "cte_join_aggregate",
        "WITH segment_events AS (SELECT u.segment, COUNT(*) AS event_count FROM events e JOIN users u ON e.user_id = u.user_id GROUP BY u.segment) SELECT segment, event_count FROM segment_events WHERE event_count > 1000 ORDER BY segment ASC;",
    ),
    (
        "multiple_ctes",
        "WITH country_totals AS (SELECT country, COUNT(*) AS total FROM events GROUP BY country), region_totals AS (SELECT region, COUNT(*) AS total FROM users GROUP BY region) SELECT region, total FROM region_totals ORDER BY region ASC;",
    ),
    (
        "cte_outer_aggregate",
        "WITH high_scores AS (SELECT event_id, country FROM events WHERE score >= 90.0) SELECT country, COUNT(*) AS total FROM high_scores GROUP BY country HAVING COUNT(*) > 0 ORDER BY country ASC;",
    ),
    (
        "scalar_subquery",
        "SELECT event_id, (SELECT MAX(budget) FROM campaigns) AS max_budget FROM events WHERE event_id <= 100 ORDER BY event_id ASC;",
    ),
    (
        "exists_correlated",
        "SELECT event_id FROM events e WHERE event_id <= 1000 AND EXISTS (SELECT campaign_id FROM campaigns c WHERE c.campaign_id = e.campaign_id) ORDER BY event_id ASC;",
    ),
    (
        "in_subquery",
        "SELECT event_id FROM events WHERE event_id <= 1000 AND campaign_id IN (SELECT campaign_id FROM campaigns) ORDER BY event_id ASC;",
    ),
    (
        "correlated_scalar",
        "SELECT event_id, (SELECT MAX(budget) FROM campaigns c WHERE c.campaign_id = e.campaign_id) AS campaign_budget FROM events e WHERE event_id <= 1000 ORDER BY event_id ASC;",
    ),
    (
        "not_exists_correlated",
        "SELECT event_id FROM events e WHERE event_id <= 1000 AND NOT EXISTS (SELECT campaign_id FROM campaigns c WHERE c.campaign_id = e.campaign_id) ORDER BY event_id ASC;",
    ),
    (
        "date_timestamp_literals",
        "SELECT DATE '2024-02-29' AS leap_day, TIMESTAMP '2024-02-29T12:34:56Z' AS moment FROM events LIMIT 1;",
    ),
    (
        "extract_timestamp",
        "SELECT event_id, EXTRACT(year FROM timestamp) AS event_year, EXTRACT(month FROM timestamp) AS event_month FROM events WHERE event_id <= 1000 ORDER BY event_id ASC;",
    ),
    (
        "date_trunc",
        "SELECT event_id, DATE_TRUNC('month', timestamp) AS event_month FROM events WHERE event_id <= 1000 ORDER BY event_id ASC;",
    ),
    (
        "date_interval_arithmetic",
        "SELECT DATE '2024-01-31' + INTERVAL '2 days' AS shifted_date FROM events LIMIT 1;",
    ),
    (
        "decimal_cast",
        "SELECT user_id, CAST('123.456' AS DECIMAL(18,2)) AS rounded_decimal, CAST(lifetime_value AS DECIMAL(18,2)) AS value FROM users WHERE user_id <= 100 ORDER BY user_id ASC;",
    ),
    (
        "date_column_extract",
        "SELECT user_id, EXTRACT(year FROM signup_date) AS signup_year FROM users WHERE user_id <= 100 ORDER BY user_id ASC;",
    ),
    (
        "checked_overflow",
        "SELECT 9223372036854775807 + 1 AS overflow_value FROM events LIMIT 1;",
    ),
    (
        "derived_table",
        "SELECT d.country, d.total FROM (SELECT country, COUNT(*) AS total FROM events GROUP BY country) AS d WHERE d.total > 0 ORDER BY country ASC;",
    ),
    (
        "chained_ctes",
        "WITH first_totals AS (SELECT country, COUNT(*) AS total FROM events GROUP BY country), filtered_totals AS (SELECT country, total FROM first_totals WHERE total > 0) SELECT country, total FROM filtered_totals ORDER BY country ASC;",
    ),
    (
        "cte_to_cte_join",
        "WITH event_counts AS (SELECT country, COUNT(*) AS total FROM events GROUP BY country), event_bytes AS (SELECT country, SUM(bytes) AS total_bytes FROM events GROUP BY country) SELECT a.country, a.total, b.total_bytes FROM event_counts a JOIN event_bytes b ON a.country = b.country ORDER BY a.country ASC;",
    ),
    (
        "derived_table_join",
        "SELECT a.country, a.total, b.total_bytes FROM (SELECT country, COUNT(*) AS total FROM events GROUP BY country) a JOIN (SELECT country, SUM(bytes) AS total_bytes FROM events GROUP BY country) b ON a.country = b.country ORDER BY a.country ASC;",
    ),
    (
        "materialized_full_join",
        "WITH event_counts AS (SELECT country, COUNT(*) AS total FROM events GROUP BY country), region_counts AS (SELECT region, COUNT(*) AS total FROM users GROUP BY region) SELECT a.country, b.region FROM event_counts a FULL JOIN region_counts b ON a.country = b.region ORDER BY a.country ASC NULLS LAST, b.region ASC NULLS LAST;",
    ),
    (
        "cte_base_join",
        "WITH selected_campaigns AS (SELECT campaign_id FROM campaigns WHERE campaign_id <= 100) SELECT COUNT(*) FROM selected_campaigns s JOIN campaigns c ON s.campaign_id = c.campaign_id;",
    ),
    (
        "base_cte_left_join",
        "WITH selected_campaigns AS (SELECT campaign_id FROM campaigns WHERE campaign_id <= 100) SELECT COUNT(s.campaign_id) FROM campaigns c LEFT JOIN selected_campaigns s ON c.campaign_id = s.campaign_id;",
    ),
    (
        "exact_decimal_aggregate",
        "SELECT SUM(lifetime_value) AS total_value, MIN(lifetime_value) AS min_value, MAX(lifetime_value) AS max_value FROM users;",
    ),
    (
        "exact_decimal_arithmetic",
        "SELECT campaign_id, budget + CAST('0.05' AS DECIMAL(18,2)) AS adjusted_budget FROM campaigns WHERE campaign_id <= 100 ORDER BY campaign_id ASC;",
    ),
    (
        "comma_cross_join",
        "SELECT COUNT(*) FROM (SELECT campaign_id FROM campaigns WHERE campaign_id <= 5) c, (SELECT user_id FROM users WHERE user_id <= 10) u;",
    ),
    (
        "explicit_cross_join",
        "SELECT COUNT(*) FROM (SELECT campaign_id FROM campaigns WHERE campaign_id <= 5) c CROSS JOIN (SELECT user_id FROM users WHERE user_id <= 10) u;",
    ),
    (
        "scalar_error_semantics",
        "SELECT ABS(-9223372036854775807) AS magnitude, 1 / 0 AS divide_by_zero, CAST('not-an-int' AS BIGINT) AS invalid_cast FROM events LIMIT 1;",
    ),
    (
        "numeric_widening",
        "SELECT campaign_id, budget + 1 AS decimal_plus_int, budget + 0.5 AS double_plus_decimal FROM campaigns WHERE campaign_id <= 100 ORDER BY campaign_id ASC;",
    ),
    (
        "window_min_max",
        "SELECT event_id, MIN(score) OVER (PARTITION BY country) AS country_min, MAX(score) OVER (PARTITION BY country) AS country_max FROM events WHERE event_id <= 100000 ORDER BY event_id ASC LIMIT 1000;",
    ),
    (
        "timestamp_interval_subtract",
        "SELECT TIMESTAMP '2024-02-29T12:34:56Z' - INTERVAL '90 seconds' AS shifted_timestamp FROM events LIMIT 1;",
    ),
]


root = Path(__file__).resolve().parents[2]
base = root / "benchmarks" / "workloads" / "sql-v1"
queries = base / "queries"
queries.mkdir(parents=True, exist_ok=True)
manifest = []
for index, (category, sql) in enumerate(QUERIES, 1):
    query_id = f"S{index:03d}"
    (queries / f"{query_id}.sql").write_text(sql + "\n")
    manifest.append(
        {
            "query_id": query_id,
            "category": category,
            "file": f"queries/{query_id}.sql",
            "description": sql.rstrip(";"),
            "feature_status": "implemented",
        }
    )
(base / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
print(f"created {len(manifest)} production-v1 SQL benchmark queries")
