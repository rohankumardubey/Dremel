#!/usr/bin/env python3
"""Write ordered Parquet streaming cases for both engines."""

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
late = max(1, args.rows * 90 // 100)
late_end = max(late + 1, args.rows * 92 // 100)
cases = [
    (
        "PS001", "leading_row_groups",
        f"SELECT event_id, score FROM events WHERE event_id <= {prefix} ORDER BY score DESC, event_id ASC LIMIT 1000 OFFSET 100",
        "Sort projected rows from leading Parquet row groups",
    ),
    (
        "PS002", "nullable_dictionary",
        f"SELECT event_id, campaign_id, country FROM events WHERE event_id <= {prefix} ORDER BY campaign_id ASC NULLS LAST, country DESC, event_id DESC LIMIT 1000",
        "Sort nullable and dictionary values with explicit null placement",
    ),
    (
        "PS003", "multipass",
        "SELECT event_id, user_id, score FROM events ORDER BY score DESC, event_id ASC LIMIT 1000 OFFSET 100",
        "Generate bounded runs across all row groups and merge an output page",
    ),
    (
        "PS004", "late_row_groups",
        f"SELECT event_id, bytes + duration_ms AS activity_cost FROM events WHERE event_id >= {late} AND event_id < {late_end} ORDER BY activity_cost DESC, event_id ASC",
        "Prune to late row groups before sorting a computed projection",
    ),
    (
        "PS005", "selective_filter",
        "SELECT event_id, country, event_type, score FROM events WHERE success = true AND event_type = 'purchase' ORDER BY country ASC, score DESC, event_id ASC LIMIT 1000",
        "Sort selectively filtered rows from dictionary and boolean columns",
    ),
]

directory = ROOT / "benchmark/parquet-sort"
(directory / "queries").mkdir(parents=True, exist_ok=True)
manifest = []
for query_id, category, sql, description in cases:
    filename = f"queries/{query_id}.sql"
    (directory / filename).write_text(sql + "\n")
    manifest.append({"query_id": query_id, "category": category,
                     "file": filename, "description": description})
(directory / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
