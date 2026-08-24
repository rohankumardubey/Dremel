#!/usr/bin/env python3
"""Create deterministic spill-to-disk aggregation benchmarks."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
QUERIES = [
    (
        "SP001",
        "high_cardinality_count",
        "SELECT event_id, COUNT(*) AS cnt FROM events GROUP BY event_id ORDER BY event_id DESC LIMIT 100",
    ),
    (
        "SP002",
        "multi_aggregate",
        "SELECT user_id, SUM(bytes) AS total_bytes, AVG(score) AS avg_score, MIN(duration_ms) AS min_duration, MAX(duration_ms) AS max_duration, COUNT(campaign_id) AS campaigns FROM events GROUP BY user_id ORDER BY total_bytes DESC, user_id ASC LIMIT 100",
    ),
    (
        "SP003",
        "filtered_high_cardinality",
        "SELECT timestamp, COUNT(*) AS events, SUM(bytes) AS total_bytes FROM events WHERE success = true GROUP BY timestamp ORDER BY events DESC, timestamp DESC LIMIT 100 OFFSET 10",
    ),
    (
        "SP004",
        "multi_key_aggregate",
        "SELECT user_id, country, COUNT(*) AS events, SUM(bytes) AS total_bytes FROM events GROUP BY user_id, country ORDER BY total_bytes DESC, user_id ASC, country ASC LIMIT 100",
    ),
    (
        "SP005",
        "nullable_group_key",
        "SELECT campaign_id, COUNT(*) AS events, AVG(score) AS avg_score FROM events GROUP BY campaign_id ORDER BY events DESC, campaign_id ASC NULLS FIRST LIMIT 50",
    ),
]

base = ROOT / "benchmark" / "spill"
queries = base / "queries"
queries.mkdir(parents=True, exist_ok=True)
manifest = []
for query_id, category, sql in QUERIES:
    file = f"queries/{query_id}.sql"
    (base / file).write_text(sql + "\n")
    manifest.append(
        {
            "query_id": query_id,
            "category": category,
            "file": file,
            "description": sql,
        }
    )
(base / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
