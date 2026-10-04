#!/usr/bin/env python3
"""Create Parquet projection, pruning, and streaming benchmarks."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument("--rows", type=int, default=1_000_000)
args = parser.parse_args()
if args.rows < 1_000:
    parser.error("--rows must be at least 1000")

prefix_end = min(100_000, args.rows)
short_prefix_end = min(1_000, args.rows)
window_end = min(500, args.rows)
selective_start = max(1, args.rows // 5)
selective_end = max(selective_start, args.rows * 26 // 100)
late_start = max(1, args.rows * 90 // 100)
late_end = max(late_start + 1, args.rows * 92 // 100)
top_k_start = max(1, args.rows // 5)
top_k_end = min(args.rows, top_k_start + max(1_000, args.rows // 1_000))
QUERIES = [
    (
        "P001",
        "metadata_count",
        "SELECT COUNT(*) FROM events",
        0,
        16,
    ),
    (
        "P002",
        "leading_row_groups",
        f"SELECT COUNT(*) FROM events WHERE event_id <= {prefix_end}",
        1,
        2,
    ),
    (
        "P003",
        "projected_selective_aggregate",
        f"SELECT SUM(bytes) FROM events WHERE event_id BETWEEN {selective_start} AND {selective_end}",
        2,
        2,
    ),
    (
        "P004",
        "dictionary_projection",
        "SELECT country, COUNT(*) FROM events WHERE country = 'IN' GROUP BY country",
        1,
        16,
    ),
    (
        "P005",
        "late_row_groups",
        f"SELECT AVG(score) FROM events WHERE event_id >= {late_start} AND event_id < {late_end}",
        2,
        2,
    ),
    (
        "P006",
        "nullable_projection",
        "SELECT COUNT(campaign_id) FROM events WHERE campaign_id IS NULL",
        1,
        16,
    ),
    (
        "P007",
        "wide_projection",
        f"SELECT SUM(bytes), AVG(score), MIN(duration_ms), MAX(timestamp), COUNT(campaign_id) FROM events WHERE event_id <= {prefix_end}",
        6,
        2,
    ),
    (
        "P008",
        "dimension_join",
        f"SELECT u.segment, COUNT(*) FROM events e JOIN users u ON e.user_id = u.user_id WHERE e.event_id <= {prefix_end} GROUP BY u.segment ORDER BY u.segment",
        2,
        2,
    ),
    (
        "P009",
        "window_scan",
        f"SELECT event_id, ROW_NUMBER() OVER (PARTITION BY country ORDER BY score DESC, event_id ASC) FROM events WHERE event_id <= {window_end} ORDER BY event_id",
        3,
        1,
    ),
    (
        "P010",
        "streaming_top_k",
        f"SELECT event_id, country, score FROM events WHERE event_id BETWEEN {top_k_start} AND {top_k_end} ORDER BY score DESC, event_id ASC LIMIT 25 OFFSET 5",
        3,
        1,
    ),
    (
        "P011",
        "streaming_distinct",
        "SELECT DISTINCT country FROM events ORDER BY country",
        1,
        16,
    ),
    (
        "P012",
        "streaming_join_top_k",
        f"SELECT e.event_id, u.segment FROM events e JOIN users u ON e.user_id = u.user_id WHERE e.event_id <= {short_prefix_end} ORDER BY e.event_id DESC LIMIT 20 OFFSET 5",
        2,
        1,
    ),
    (
        "P013",
        "streaming_join_aggregates",
        f"SELECT u.segment, SUM(e.bytes), MIN(e.duration_ms), MAX(e.score), COUNT(*) FROM events e JOIN users u ON e.user_id = u.user_id WHERE e.event_id <= {prefix_end} GROUP BY u.segment ORDER BY u.segment",
        5,
        2,
    ),
    (
        "P014",
        "streaming_left_join",
        f"SELECT c.channel, COUNT(*) FROM events e LEFT JOIN campaigns c ON e.campaign_id = c.campaign_id WHERE e.event_id <= {prefix_end} GROUP BY c.channel ORDER BY c.channel NULLS FIRST",
        2,
        2,
    ),
    (
        "P015",
        "streaming_join_avg",
        f"SELECT u.segment, AVG(e.score) FROM events e JOIN users u ON e.user_id = u.user_id WHERE e.event_id <= {prefix_end} GROUP BY u.segment ORDER BY u.segment",
        3,
        2,
    ),
    (
        "P016",
        "streaming_left_join_nullable_avg",
        f"SELECT c.channel, AVG(e.campaign_id), COUNT(e.campaign_id) FROM events e LEFT JOIN campaigns c ON e.campaign_id = c.campaign_id WHERE e.event_id <= {prefix_end} GROUP BY c.channel ORDER BY c.channel NULLS FIRST",
        2,
        2,
    ),
    (
        "P017",
        "streaming_join_multiple_avg",
        f"SELECT AVG(e.score), AVG(e.duration_ms), COUNT(e.score) FROM events e JOIN users u ON e.user_id = u.user_id WHERE e.event_id <= {prefix_end}",
        4,
        2,
    ),
    (
        "P018",
        "streaming_join_empty_avg",
        "SELECT AVG(e.score), COUNT(e.score) FROM events e JOIN users u ON e.user_id = u.user_id WHERE e.event_id < 0",
        3,
        0,
    ),
    (
        "P019",
        "streaming_join_decimal_avg",
        f"SELECT u.segment, AVG(u.lifetime_value) FROM events e JOIN users u ON e.user_id = u.user_id WHERE e.event_id <= {prefix_end} GROUP BY u.segment ORDER BY u.segment",
        2,
        2,
    ),
]

base = ROOT / "benchmark" / "parquet"
queries = base / "queries"
queries.mkdir(parents=True, exist_ok=True)
manifest = []
for query_id, category, sql, expected_columns, maximum_row_groups in QUERIES:
    (queries / f"{query_id}.sql").write_text(sql + ";\n")
    manifest.append(
        {
            "query_id": query_id,
            "category": category,
            "file": f"queries/{query_id}.sql",
            "description": sql,
            "expected_columns_read": expected_columns,
            "maximum_row_groups_read": maximum_row_groups,
            "expected_streaming_fallback": query_id == "P009",
        }
    )
(base / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
print(f"created {len(manifest)} Parquet benchmark queries")
