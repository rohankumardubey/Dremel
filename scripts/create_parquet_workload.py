#!/usr/bin/env python3
"""Create direct Parquet projection and row-group pruning benchmarks."""

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
        }
    )
(base / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
print(f"created {len(manifest)} direct Parquet benchmark queries")
