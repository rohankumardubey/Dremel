#!/usr/bin/env python3
"""Materialize the fixed 64-query benchmark corpus and manifest."""

import json
from pathlib import Path

GROUPS = [
    (
        "full_scan_aggregate",
        [
            "SELECT COUNT(*) FROM events;",
            "SELECT COUNT(user_id) FROM events;",
            "SELECT SUM(bytes) FROM events;",
            "SELECT AVG(duration_ms) FROM events;",
            "SELECT MIN(score) FROM events;",
            "SELECT MAX(score) FROM events;",
            "SELECT SUM(duration_ms) FROM events;",
            "SELECT AVG(score) FROM events;",
        ],
    ),
    (
        "filtered_scan",
        [
            "SELECT COUNT(*) FROM events WHERE success = true;",
            "SELECT COUNT(*) FROM events WHERE success = false;",
            "SELECT COUNT(*) FROM events WHERE duration_ms > 500;",
            "SELECT COUNT(*) FROM events WHERE bytes >= 500000;",
            "SELECT COUNT(*) FROM events WHERE score >= 75.0;",
            "SELECT COUNT(*) FROM events WHERE country = 'IN';",
            "SELECT COUNT(*) FROM events WHERE event_type = 'purchase';",
            "SELECT COUNT(*) FROM events WHERE device = 'mobile';",
            "SELECT COUNT(*) FROM events WHERE country = 'US' AND success = true;",
            "SELECT COUNT(*) FROM events WHERE duration_ms < 100 OR bytes > 900000;",
        ],
    ),
    (
        "projection_filter",
        [
            "SELECT event_id, user_id, country FROM events WHERE country = 'IN' AND success = true LIMIT 1000;",
            "SELECT event_id, device, duration_ms FROM events WHERE duration_ms > 9000 LIMIT 1000;",
            "SELECT event_id, event_type, bytes FROM events WHERE event_type = 'download' AND bytes > 500000 LIMIT 1000;",
            "SELECT user_id, score, success FROM events WHERE score > 95.0 LIMIT 1000;",
            "SELECT event_id, campaign_id FROM events WHERE campaign_id IS NOT NULL LIMIT 1000;",
            "SELECT event_id, duration_ms * 2 AS doubled_duration FROM events WHERE country = 'JP' LIMIT 1000;",
            "SELECT event_id, bytes + duration_ms AS combined_metric FROM events WHERE device = 'desktop' LIMIT 1000;",
            "SELECT event_id, score * 1.5 AS weighted_score FROM events WHERE success = true AND country = 'DE' LIMIT 1000;",
        ],
    ),
    (
        "single_group_by",
        [
            "SELECT country, COUNT(*) AS cnt FROM events GROUP BY country;",
            "SELECT device, COUNT(*) AS cnt FROM events GROUP BY device;",
            "SELECT event_type, COUNT(*) AS cnt FROM events GROUP BY event_type;",
            "SELECT country, SUM(bytes) AS total_bytes FROM events GROUP BY country;",
            "SELECT country, AVG(duration_ms) AS avg_duration FROM events GROUP BY country;",
            "SELECT device, AVG(score) AS avg_score FROM events GROUP BY device;",
            "SELECT event_type, SUM(bytes) AS total_bytes FROM events GROUP BY event_type;",
            "SELECT success, COUNT(*) AS cnt FROM events GROUP BY success;",
            "SELECT country, MAX(score) AS max_score FROM events GROUP BY country;",
            "SELECT country, MIN(duration_ms) AS min_duration FROM events GROUP BY country;",
            "SELECT device, SUM(duration_ms) AS total_duration FROM events GROUP BY device;",
            "SELECT event_type, AVG(score) AS avg_score FROM events GROUP BY event_type;",
        ],
    ),
    (
        "multi_group_by",
        [
            "SELECT country, device, COUNT(*) AS cnt FROM events GROUP BY country, device;",
            "SELECT country, event_type, COUNT(*) AS cnt FROM events GROUP BY country, event_type;",
            "SELECT device, event_type, COUNT(*) AS cnt FROM events GROUP BY device, event_type;",
            "SELECT country, success, COUNT(*) AS cnt FROM events GROUP BY country, success;",
            "SELECT country, device, SUM(bytes) AS total_bytes FROM events GROUP BY country, device;",
            "SELECT country, event_type, AVG(duration_ms) AS avg_duration FROM events GROUP BY country, event_type;",
            "SELECT device, success, AVG(score) AS avg_score FROM events GROUP BY device, success;",
            "SELECT country, device, event_type, COUNT(*) AS cnt FROM events GROUP BY country, device, event_type;",
        ],
    ),
    (
        "aggregate_heavy",
        [
            "SELECT country, COUNT(*) AS cnt, SUM(bytes) AS total_bytes, AVG(duration_ms) AS avg_duration, MIN(score) AS min_score, MAX(score) AS max_score FROM events GROUP BY country;",
            "SELECT event_type, COUNT(*) AS cnt, SUM(bytes) AS total_bytes, SUM(duration_ms) AS total_duration, AVG(score) AS avg_score FROM events GROUP BY event_type;",
            "SELECT device, COUNT(*) AS cnt, AVG(bytes) AS avg_bytes, MIN(duration_ms) AS min_duration, MAX(duration_ms) AS max_duration FROM events GROUP BY device;",
            "SELECT country, COUNT(*) AS cnt, SUM(bytes) AS total_bytes, AVG(score) AS avg_score FROM events WHERE success = true GROUP BY country;",
            "SELECT event_type, COUNT(*) AS cnt, SUM(bytes) AS total_bytes, AVG(duration_ms) AS avg_duration FROM events WHERE duration_ms > 1000 GROUP BY event_type;",
            "SELECT device, COUNT(*) AS cnt, SUM(duration_ms) AS total_duration, MAX(score) AS max_score FROM events WHERE score > 50.0 GROUP BY device;",
        ],
    ),
    (
        "definition_levels",
        [
            "SELECT COUNT(campaign_id) FROM events;",
            "SELECT COUNT(*) FROM events WHERE campaign_id IS NULL;",
            "SELECT COUNT(*) FROM events WHERE campaign_id IS NOT NULL;",
            "SELECT SUM(campaign_id) FROM events WHERE campaign_id IS NOT NULL;",
        ],
    ),
    (
        "order_by",
        [
            "SELECT country, COUNT(*) AS cnt FROM events GROUP BY country ORDER BY country ASC;",
            "SELECT event_type, SUM(bytes) AS total_bytes FROM events GROUP BY event_type ORDER BY total_bytes DESC;",
            "SELECT device, AVG(duration_ms) AS avg_duration FROM events GROUP BY device ORDER BY avg_duration ASC;",
            "SELECT country, MAX(score) AS max_score FROM events GROUP BY country ORDER BY max_score DESC;",
        ],
    ),
    (
        "order_by_limit",
        [
            "SELECT country, SUM(bytes) AS total_bytes FROM events GROUP BY country ORDER BY total_bytes DESC LIMIT 5;",
            "SELECT event_type, COUNT(*) AS cnt FROM events GROUP BY event_type ORDER BY cnt DESC LIMIT 5;",
            "SELECT country, device, COUNT(*) AS cnt FROM events GROUP BY country, device ORDER BY cnt DESC LIMIT 10;",
            "SELECT country, event_type, AVG(duration_ms) AS avg_duration FROM events GROUP BY country, event_type ORDER BY avg_duration DESC LIMIT 10;",
        ],
    ),
]

root = Path(__file__).resolve().parents[2]
out = root / "benchmarks" / "workloads" / "queries"
out.mkdir(parents=True, exist_ok=True)
manifest = []
i = 0
for category, queries in GROUPS:
    for sql in queries:
        i += 1
        qid = f"Q{i:03d}"
        (out / f"{qid}.sql").write_text(sql + "\n")
        manifest.append(
            {
                "query_id": qid,
                "category": category,
                "file": f"queries/{qid}.sql",
                "description": sql.rstrip(";"),
            }
        )
assert i == 64
(root / "benchmarks" / "workloads" / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
print("created 64 benchmark queries")
