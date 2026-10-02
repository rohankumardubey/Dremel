#!/usr/bin/env python3
"""Create a mixed asynchronous workload over a Parquet fact table."""

import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument("--rows", type=int, default=1_000_000)
args = parser.parse_args()
if args.rows < 1_000:
    parser.error("--rows must be at least 1000")

prefix = min(100_000, args.rows)
selective_start = max(1, args.rows // 5)
selective_end = min(args.rows, selective_start + max(1_000, args.rows // 1_000))
queries = [
    ("CP001", "metadata_count", "SELECT COUNT(*) FROM events"),
    (
        "CP002", "pruned_aggregate",
        f"SELECT country, COUNT(*), SUM(bytes) FROM events WHERE event_id <= {prefix} GROUP BY country ORDER BY country",
    ),
    (
        "CP003", "selective_top_k",
        f"SELECT event_id, country, score FROM events WHERE event_id BETWEEN {selective_start} AND {selective_end} ORDER BY score DESC, event_id ASC LIMIT 25",
    ),
    (
        "CP004", "dimension_join",
        f"SELECT u.segment, COUNT(*) FROM events e JOIN users u ON e.user_id = u.user_id WHERE e.event_id <= {prefix} GROUP BY u.segment ORDER BY u.segment",
    ),
    (
        "CP005", "filtered_projection",
        f"SELECT event_id, duration_ms FROM events WHERE event_id <= {prefix} AND score >= 99.5 ORDER BY event_id ASC LIMIT 50",
    ),
]

base = ROOT / "benchmark/parquet-concurrency"
directory = base / "queries"
directory.mkdir(parents=True, exist_ok=True)
items = []
for query_id, category, sql in queries:
    filename = f"queries/{query_id}.sql"
    (base / filename).write_text(sql + ";\n")
    items.append({"query_id": query_id, "category": category,
                  "file": filename, "description": sql})
manifest = {
    "version": 1,
    "scheduler": {"max_active_queries": 4, "queue_capacity": 64, "memory_mb": 1024},
    "queries": items,
    "mix": {
        "request_count": 30,
        "priorities": [2, 0, 1, 2, 1, 0, 2, 1, 2, 0],
        "groups": ["interactive", "batch", "tenant_c"],
        "normal_deadline_ms": 15000,
        "short_deadline_every": 11,
        "short_deadline_ms": 1,
        "cancel_every": 13,
        "reservation_mb": 32,
    },
}
(base / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
