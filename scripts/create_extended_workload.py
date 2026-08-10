#!/usr/bin/env python3
"""Create the versioned production-hardening benchmark workload."""

import json
from pathlib import Path

QUERIES = [
    (
        "nullable_expression",
        "SELECT event_id, campaign_id > 2500 AS high_campaign FROM events LIMIT 1000;",
    ),
    (
        "nullable_expression",
        "SELECT event_id, NOT (campaign_id > 2500) AS low_campaign FROM events LIMIT 1000;",
    ),
    (
        "arithmetic_aggregate",
        "SELECT SUM(bytes + duration_ms) AS total_work FROM events;",
    ),
    (
        "arithmetic_aggregate",
        "SELECT AVG(bytes / (duration_ms + 1)) AS transfer_rate FROM events;",
    ),
    (
        "complex_predicate",
        "SELECT COUNT(*) FROM events WHERE NOT success = false AND (score >= 80.0 OR bytes > 950000);",
    ),
    (
        "nullable_group",
        "SELECT country, COUNT(*) AS cnt, AVG(campaign_id) AS avg_campaign FROM events WHERE campaign_id > 2500 GROUP BY country ORDER BY country ASC;",
    ),
    (
        "arithmetic_projection",
        "SELECT event_id, bytes / (duration_ms + 1) AS transfer_rate FROM events WHERE success = true LIMIT 1000;",
    ),
    (
        "three_valued_filter",
        "SELECT COUNT(*) FROM events WHERE campaign_id > 4500 OR success = true;",
    ),
    (
        "heavy_expression_group",
        "SELECT event_type, SUM(bytes + duration_ms) AS total_work, AVG(score * 1.5) AS weighted_score FROM events GROUP BY event_type;",
    ),
    (
        "ordered_expression_group",
        "SELECT country, SUM(bytes + duration_ms) AS total_work FROM events GROUP BY country ORDER BY total_work DESC LIMIT 5;",
    ),
    (
        "negated_predicate",
        "SELECT COUNT(*) FROM events WHERE NOT (country = 'IN' OR device = 'mobile');",
    ),
    ("arithmetic_min", "SELECT MIN(bytes - duration_ms) AS minimum_delta FROM events;"),
    (
        "arithmetic_max",
        "SELECT MAX(score * duration_ms) AS maximum_weighted_duration FROM events;",
    ),
    (
        "complex_multi_group",
        "SELECT country, device, COUNT(*) AS cnt, AVG(score) AS avg_score FROM events WHERE success = true AND score > 25.0 GROUP BY country, device ORDER BY cnt DESC LIMIT 20;",
    ),
    (
        "nullable_aggregates",
        "SELECT COUNT(campaign_id) AS campaign_count, SUM(campaign_id) AS campaign_sum, AVG(campaign_id) AS campaign_avg, MIN(campaign_id) AS campaign_min, MAX(campaign_id) AS campaign_max FROM events;",
    ),
    (
        "wide_projection",
        "SELECT event_id, user_id, country, device, event_type, duration_ms, bytes, score, success, campaign_id FROM events WHERE score > 99.0 LIMIT 1000;",
    ),
]

root = Path(__file__).resolve().parents[1]
base = root / "benchmark" / "extended"
out = base / "queries"
out.mkdir(parents=True, exist_ok=True)
manifest = []
for index, (category, sql) in enumerate(QUERIES, 1):
    query_id = f"E{index:03d}"
    (out / f"{query_id}.sql").write_text(sql + "\n")
    manifest.append(
        {
            "query_id": query_id,
            "category": category,
            "file": f"queries/{query_id}.sql",
            "description": sql.rstrip(";"),
        }
    )
(base / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
print(f"created {len(QUERIES)} extended benchmark queries")
