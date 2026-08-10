#!/usr/bin/env python3
"""Create the deterministic concurrent-workload benchmark manifest."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
QUERIES = [
    ("C001", "selective_count", "SELECT COUNT(*) FROM events WHERE country = 'IN'"),
    (
        "C002",
        "group_aggregate",
        "SELECT country, COUNT(*) AS total FROM events GROUP BY country ORDER BY country ASC",
    ),
    (
        "C003",
        "hash_join",
        "SELECT COUNT(*) FROM events e JOIN campaigns c ON e.campaign_id = c.campaign_id",
    ),
    (
        "C004",
        "top_k",
        "SELECT event_id, score FROM events ORDER BY score DESC, event_id ASC LIMIT 25",
    ),
    (
        "C005",
        "filtered_projection",
        "SELECT event_id, duration_ms FROM events WHERE score >= 99.5 ORDER BY event_id ASC LIMIT 50",
    ),
]
base = ROOT / "benchmark" / "concurrency"
queries = base / "queries"
queries.mkdir(parents=True, exist_ok=True)
items = []
for query_id, category, sql in QUERIES:
    (queries / f"{query_id}.sql").write_text(sql + ";\n")
    items.append(
        {
            "query_id": query_id,
            "category": category,
            "file": f"queries/{query_id}.sql",
            "description": sql,
        }
    )
manifest = {
    "version": 1,
    "scheduler": {"max_active_queries": 4, "queue_capacity": 64, "memory_mb": 512},
    "queries": items,
    "mix": {
        "request_count": 30,
        "priorities": [2, 0, 1, 2, 1, 0, 2, 1, 2, 0],
        "groups": ["interactive", "batch", "tenant_c"],
        "normal_deadline_ms": 15000,
        "short_deadline_every": 11,
        "short_deadline_ms": 1,
        "cancel_every": 13,
        "reservation_mb": 4,
    },
}
(base / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
print(
    f"created {len(items)} concurrency queries and a {manifest['mix']['request_count']}-request mix"
)
