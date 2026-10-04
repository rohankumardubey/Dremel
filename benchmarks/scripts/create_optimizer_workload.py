#!/usr/bin/env python3
"""Create the isolated optimizer benchmark and plan-assertion workload."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
QUERIES = [
    (
        "O001",
        "constant_folding",
        "SELECT COUNT(*) FROM events WHERE (1 + 2) = 3 AND country = 'IN'",
        ["OptimizerExec"],
        True,
    ),
    (
        "O002",
        "three_valued_logic",
        "SELECT COUNT(*) FROM events WHERE true AND (success = true OR false)",
        ["OptimizerExec"],
        True,
    ),
    (
        "O003",
        "projection_pruning",
        "SELECT event_id FROM events WHERE country = 'US' LIMIT 1000",
        ["ScanExec", "OptimizerExec"],
        True,
    ),
    (
        "O004",
        "top_k",
        "SELECT event_id, score FROM events ORDER BY score DESC, event_id ASC LIMIT 100",
        ["TopKExec"],
        True,
    ),
    (
        "O005",
        "top_k_offset",
        "SELECT event_id, duration_ms FROM events ORDER BY duration_ms DESC, event_id ASC LIMIT 250 OFFSET 500",
        ["TopKExec"],
        True,
    ),
    (
        "O006",
        "hash_join_runtime_filter",
        "SELECT COUNT(*) FROM events e JOIN campaigns c ON e.campaign_id = c.campaign_id",
        ["HashJoinExec"],
        False,
    ),
    (
        "O007",
        "join_ordering",
        "SELECT COUNT(*) FROM events e JOIN users u ON e.user_id = u.user_id JOIN campaigns c ON e.campaign_id = c.campaign_id",
        ["HashJoinExec", "OptimizerExec"],
        False,
    ),
    (
        "O008",
        "aggregate_filter_pushdown",
        "SELECT country, COUNT(*) AS total FROM events WHERE score >= 90.0 GROUP BY country HAVING COUNT(*) > 0 ORDER BY total DESC",
        ["FilterExec", "PartialAggregateExec"],
        True,
    ),
    (
        "O009",
        "window_scan_pushdown",
        "SELECT event_id, ROW_NUMBER() OVER (PARTITION BY country ORDER BY score DESC, event_id ASC) AS rn FROM events WHERE event_id <= 100000 ORDER BY event_id ASC LIMIT 1000",
        ["WindowExec", "ScanFilterExec"],
        True,
    ),
    (
        "O010",
        "range_contradiction",
        "SELECT COUNT(*) FROM events WHERE event_id > 900000 AND event_id < 1000",
        ["EmptyScanExec", "OptimizerExec"],
        True,
    ),
    (
        "O011",
        "filter_ordering",
        "SELECT COUNT(*) FROM events WHERE LOWER(country) = 'in' AND event_id <= 100",
        ["ScanFilterExec", "OptimizerExec"],
        True,
    ),
    (
        "O012",
        "transitive_predicate_join_order",
        "SELECT COUNT(*) FROM events e JOIN campaigns c ON e.campaign_id = c.campaign_id JOIN users u ON e.user_id = u.user_id WHERE u.user_id <= 100",
        ["HashJoinExec", "ScanFilterExec", "OptimizerExec"],
        False,
    ),
]

ASSERTIONS = {
    "constant_folding": {"minimum_rewrites": 1},
    "three_valued_logic": {"minimum_rewrites": 1},
    "projection_pruning": {
        "required_plan_fragments": ["ScanExec(columns=[country,event_id])"]
    },
    "top_k": {"required_plan_fragments": ["TopKExec(k=100)"]},
    "top_k_offset": {"required_plan_fragments": ["TopKExec(k=750)"]},
    "hash_join_runtime_filter": {
        "required_plan_fragments": ["build=right", "runtime_filter=true"],
        "expected_disabled_operator": "NestedLoopJoinExec",
    },
    "join_ordering": {
        "minimum_rewrites": 1,
        "ordered_plan_fragments": ["table=campaigns", "table=users"],
        "expected_disabled_operator": "NestedLoopJoinExec",
    },
    "aggregate_filter_pushdown": {
        "ordered_operators": ["FilterExec", "PartialAggregateExec"]
    },
    "window_scan_pushdown": {
        "required_plan_fragments": [
            "ScanFilterExec(table=events;predicates=1;derived=0)"
        ]
    },
    "range_contradiction": {
        "minimum_rewrites": 1,
        "required_plan_fragments": ["EmptyScanExec(reason=contradiction)"],
    },
    "filter_ordering": {
        "minimum_rewrites": 1,
        "required_plan_fragments": [
            "ScanFilterExec(table=events;predicates=2;derived=0)"
        ],
    },
    "transitive_predicate_join_order": {
        "minimum_rewrites": 1,
        "ordered_plan_fragments": ["table=users", "table=campaigns"],
        "required_plan_fragments": [
            "ScanFilterExec(table=events;predicates=1;derived=1)",
            "ScanFilterExec(table=users;predicates=1;derived=0)",
        ],
        "expected_disabled_operator": "NestedLoopJoinExec",
    },
}

base = ROOT / "benchmarks" / "workloads" / "optimizer"
queries = base / "queries"
queries.mkdir(parents=True, exist_ok=True)
manifest = []
for query_id, category, sql, expected, execute_disabled in QUERIES:
    (queries / f"{query_id}.sql").write_text(sql + ";\n")
    record = {
        "query_id": query_id,
        "category": category,
        "file": f"queries/{query_id}.sql",
        "description": sql,
        "expected_optimized_operators": expected,
        "expected_disabled_operator": "SortExec"
        if category.startswith("top_k")
        else None,
        "execute_disabled": execute_disabled,
    }
    record.update(ASSERTIONS.get(category, {}))
    manifest.append(record)
(base / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
print(f"created {len(manifest)} optimizer benchmark queries")
