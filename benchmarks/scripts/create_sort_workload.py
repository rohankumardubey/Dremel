#!/usr/bin/env python3
"""Write the deterministic external-sort benchmark workload."""

import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
parser = argparse.ArgumentParser()
parser.add_argument("--rows", type=int, default=1_000_000)
args = parser.parse_args()
if args.rows < 1_000:
    parser.error("--rows must be at least 1000")

large = min(100_000, args.rows)
medium = min(50_000, args.rows)
WORKLOAD = [
    (
        "ES001",
        "integer_sort",
        f"SELECT event_id FROM events WHERE event_id <= {large} ORDER BY event_id DESC",
        f"Sort {large:,} integer identifiers in descending order",
    ),
    (
        "ES002",
        "multi_key_nullable",
        f"SELECT event_id, campaign_id, country, score FROM events WHERE event_id <= {large} ORDER BY campaign_id ASC NULLS LAST, country DESC, event_id DESC",
        "Sort nullable and dictionary values with three ordering keys",
    ),
    (
        "ES003",
        "limit_offset",
        "SELECT event_id, user_id, score FROM events ORDER BY score DESC, event_id ASC LIMIT 1000 OFFSET 100",
        "Use bounded fan-in passes and merge only the requested output page",
    ),
    (
        "ES004",
        "computed_key",
        f"SELECT event_id, bytes + duration_ms AS activity_cost, country FROM events WHERE event_id <= {medium} ORDER BY activity_cost DESC, event_id ASC",
        "Sort a computed numeric projection and a deterministic tie breaker",
    ),
    (
        "ES005",
        "selective_filter",
        "SELECT event_id, country, event_type, score FROM events WHERE success = true AND event_type = 'purchase' ORDER BY country ASC, score DESC, event_id ASC",
        "Sort rows retained by a selective dictionary and boolean filter",
    ),
]

directory = ROOT / "benchmarks/workloads/sort"
(directory / "queries").mkdir(parents=True, exist_ok=True)
manifest = []
for query_id, category, sql, description in WORKLOAD:
    relative = f"queries/{query_id}.sql"
    (directory / relative).write_text(sql + "\n")
    manifest.append(
        {
            "query_id": query_id,
            "category": category,
            "file": relative,
            "description": description,
        }
    )
(directory / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
