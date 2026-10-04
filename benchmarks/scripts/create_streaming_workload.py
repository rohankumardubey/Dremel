#!/usr/bin/env python3
"""Write the deterministic streaming-result benchmark workload."""

import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
parser = argparse.ArgumentParser()
parser.add_argument("--rows", type=int, default=1_000_000)
args = parser.parse_args()
if args.rows < 1_000:
    parser.error("--rows must be at least 1000")

prefix_end = min(100_000, args.rows)
wide_end = min(50_000, args.rows)
nullable_start = args.rows // 5 + 1
nullable_end = min(args.rows, nullable_start + 99_999)
WORKLOAD = [
    (
        "ST001",
        "narrow_projection",
        f"SELECT event_id FROM events WHERE event_id <= {prefix_end}",
        f"Stream {prefix_end:,} integer identifiers",
    ),
    (
        "ST002",
        "wide_projection",
        f"SELECT event_id, user_id, timestamp, country, device, event_type, campaign_id, bytes, duration_ms, success, score FROM events WHERE event_id <= {wide_end}",
        f"Stream {wide_end:,} rows containing numeric, dictionary, nullable, and string values",
    ),
    (
        "ST003",
        "selective_filter",
        "SELECT event_id, user_id, country, bytes, score FROM events WHERE success = true AND event_type = 'purchase'",
        "Stream successful purchase events through a selective filter",
    ),
    (
        "ST004",
        "nullable_projection",
        f"SELECT event_id, campaign_id, score FROM events WHERE event_id BETWEEN {nullable_start} AND {nullable_end}",
        f"Stream {nullable_end - nullable_start + 1:,} rows with nullable campaign identifiers",
    ),
    (
        "ST005",
        "scalar_expression",
        f"SELECT event_id, bytes + duration_ms AS activity_cost, score * 2 AS doubled_score, success, country FROM events WHERE event_id <= {prefix_end}",
        "Stream computed scalar expressions without result materialization",
    ),
]

directory = ROOT / "benchmarks/workloads/streaming"
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
