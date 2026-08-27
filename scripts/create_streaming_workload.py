#!/usr/bin/env python3
"""Write the deterministic streaming-result benchmark workload."""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKLOAD = [
    (
        "ST001",
        "narrow_projection",
        "SELECT event_id FROM events WHERE event_id <= 100000",
        "Stream 100,000 integer identifiers",
    ),
    (
        "ST002",
        "wide_projection",
        "SELECT event_id, user_id, timestamp, country, device, event_type, campaign_id, bytes, duration_ms, success, score FROM events WHERE event_id <= 50000",
        "Stream 50,000 rows containing numeric, dictionary, nullable, and string values",
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
        "SELECT event_id, campaign_id, score FROM events WHERE event_id BETWEEN 200001 AND 300000",
        "Stream 100,000 rows with nullable campaign identifiers",
    ),
    (
        "ST005",
        "scalar_expression",
        "SELECT event_id, bytes + duration_ms AS activity_cost, score * 2 AS doubled_score, success, country FROM events WHERE event_id <= 100000",
        "Stream computed scalar expressions without result materialization",
    ),
]

directory = ROOT / "benchmark/streaming"
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
