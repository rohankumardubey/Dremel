#!/usr/bin/env python3
"""Create Parquet projection, pruning, and streaming benchmarks."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
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
        "SELECT COUNT(*) FROM events WHERE event_id <= 100000",
        1,
        2,
    ),
    (
        "P003",
        "projected_selective_aggregate",
        "SELECT SUM(bytes) FROM events WHERE event_id BETWEEN 200000 AND 260000",
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
        "SELECT AVG(score) FROM events WHERE event_id >= 900000 AND event_id < 920000",
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
        "SELECT SUM(bytes), AVG(score), MIN(duration_ms), MAX(timestamp), COUNT(campaign_id) FROM events WHERE event_id <= 100000",
        6,
        2,
    ),
    (
        "P008",
        "dimension_join",
        "SELECT u.segment, COUNT(*) FROM events e JOIN users u ON e.user_id = u.user_id WHERE e.event_id <= 100000 GROUP BY u.segment ORDER BY u.segment",
        2,
        2,
    ),
    (
        "P009",
        "window_scan",
        "SELECT event_id, ROW_NUMBER() OVER (PARTITION BY country ORDER BY score DESC, event_id ASC) FROM events WHERE event_id <= 500 ORDER BY event_id",
        3,
        1,
    ),
    (
        "P010",
        "streaming_top_k",
        "SELECT event_id, country, score FROM events WHERE event_id BETWEEN 200000 AND 201000 ORDER BY score DESC, event_id ASC LIMIT 25 OFFSET 5",
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
        "SELECT e.event_id, u.segment FROM events e JOIN users u ON e.user_id = u.user_id WHERE e.event_id <= 1000 ORDER BY e.event_id DESC LIMIT 20 OFFSET 5",
        2,
        1,
    ),
    (
        "P013",
        "streaming_join_aggregates",
        "SELECT u.segment, SUM(e.bytes), MIN(e.duration_ms), MAX(e.score), COUNT(*) FROM events e JOIN users u ON e.user_id = u.user_id WHERE e.event_id <= 100000 GROUP BY u.segment ORDER BY u.segment",
        5,
        2,
    ),
    (
        "P014",
        "streaming_left_join",
        "SELECT c.channel, COUNT(*) FROM events e LEFT JOIN campaigns c ON e.campaign_id = c.campaign_id WHERE e.event_id <= 100000 GROUP BY c.channel ORDER BY c.channel NULLS FIRST",
        2,
        2,
    ),
    (
        "P015",
        "join_avg_fallback",
        "SELECT u.segment, AVG(e.score) FROM events e JOIN users u ON e.user_id = u.user_id WHERE e.event_id <= 100000 GROUP BY u.segment ORDER BY u.segment",
        3,
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
            "expected_streaming_fallback": query_id in ("P009", "P015"),
        }
    )
(base / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
print(f"created {len(manifest)} Parquet benchmark queries")
