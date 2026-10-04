#!/usr/bin/env python3
"""Benchmark optimizer rules with optimized/disabled execution and plan assertions."""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
from pathlib import Path

from run_benchmark import ROOT, Server, compare_rows, result_hash, stats


def operator_names(plan: list[str]) -> list[str]:
    return [entry.split("(", 1)[0] for entry in plan]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--rust", default=str(ROOT / "target/release/dremel")
    )
    parser.add_argument("--cpp", default=str(ROOT / "benchmarks/cpp/build/dremel-cpp"))
    parser.add_argument("--data", default=str(ROOT / "data/events.dremel"))
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--batch-size", type=int, default=4096)
    parser.add_argument("--warmup", type=int, default=int(os.getenv("WARMUP", "3")))
    parser.add_argument(
        "--iterations", type=int, default=int(os.getenv("ITERATIONS", "20"))
    )
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument(
        "--manifest", type=Path, default=ROOT / "benchmarks/workloads/optimizer/manifest.json"
    )
    parser.add_argument("--results-dir", type=Path, default=ROOT / "results/optimizer")
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text())
    command = [
        "bench-server",
        "--data",
        args.data,
        "--threads",
        str(args.threads),
        "--batch-size",
        str(args.batch_size),
    ]
    enabled_env = os.environ.copy()
    enabled_env.pop("DREMEL_DISABLE_OPTIMIZER", None)
    disabled_env = os.environ.copy()
    disabled_env["DREMEL_DISABLE_OPTIMIZER"] = "1"
    servers: list[Server] = []
    try:
        rust_on = Server("Rust optimized", [args.rust, *command], enabled_env)
        servers.append(rust_on)
        cpp_on = Server("C++ optimized", [args.cpp, *command], enabled_env)
        servers.append(cpp_on)
        rust_off = Server(
            "Rust optimizer-disabled", [args.rust, *command], disabled_env
        )
        servers.append(rust_off)
        cpp_off = Server("C++ optimizer-disabled", [args.cpp, *command], disabled_env)
        servers.append(cpp_off)
        output = []
        for item in manifest:
            qid = item["query_id"]
            sql = (
                (args.manifest.parent / item["file"])
                .read_text()
                .strip()
                .replace("\n", " ")
            )
            for server in servers:
                server.prepare(qid, sql)
            plans = {
                "rust_optimized": rust_on.explain(qid),
                "cpp_optimized": cpp_on.explain(qid),
                "rust_disabled": rust_off.explain(qid),
                "cpp_disabled": cpp_off.explain(qid),
            }
            if plans["rust_optimized"] != plans["cpp_optimized"]:
                raise RuntimeError(f"{qid}: optimized plan mismatch: {plans}")
            if plans["rust_disabled"] != plans["cpp_disabled"]:
                raise RuntimeError(f"{qid}: disabled plan mismatch: {plans}")
            optimized_names = operator_names(plans["rust_optimized"])
            for expected in item["expected_optimized_operators"]:
                if expected not in optimized_names:
                    raise RuntimeError(
                        f"{qid}: expected {expected} in {plans['rust_optimized']}"
                    )
            expected_disabled = item.get("expected_disabled_operator")
            if expected_disabled and expected_disabled not in operator_names(
                plans["rust_disabled"]
            ):
                raise RuntimeError(f"{qid}: expected disabled {expected_disabled}")
            plan_text = "\n".join(plans["rust_optimized"])
            for fragment in item.get("required_plan_fragments", []):
                if fragment not in plan_text:
                    raise RuntimeError(f"{qid}: expected plan fragment {fragment!r}")
            fragments = item.get("ordered_plan_fragments", [])
            positions = [plan_text.find(fragment) for fragment in fragments]
            if any(position < 0 for position in positions) or positions != sorted(
                positions
            ):
                raise RuntimeError(
                    f"{qid}: plan fragments missing/out of order: {fragments}"
                )
            ordered_operators = item.get("ordered_operators", [])
            indexes = [
                optimized_names.index(name) if name in optimized_names else -1
                for name in ordered_operators
            ]
            if any(index < 0 for index in indexes) or indexes != sorted(indexes):
                raise RuntimeError(
                    f"{qid}: operators missing/out of order: {ordered_operators}"
                )
            minimum_rewrites = item.get("minimum_rewrites", 0)
            if minimum_rewrites:
                optimizer_line = next(
                    (
                        line
                        for line in plans["rust_optimized"]
                        if line.startswith("OptimizerExec(")
                    ),
                    "",
                )
                match = re.search(r"rewrites=(\d+)", optimizer_line)
                if not match or int(match.group(1)) < minimum_rewrites:
                    raise RuntimeError(
                        f"{qid}: expected at least {minimum_rewrites} rewrite(s)"
                    )
            _, rust_rows = rust_on.execute(qid, True)
            rust_optimized_memory = dict(rust_on.last_query_memory)
            _, cpp_rows = cpp_on.execute(qid, True)
            cpp_optimized_memory = dict(cpp_on.last_query_memory)
            ordered = "ORDER BY" in sql.upper()
            correct, detail = compare_rows(rust_rows, cpp_rows, ordered)
            if not correct:
                raise RuntimeError(f"{qid}: cross-engine result mismatch: {detail}")
            disabled_verified = bool(item["execute_disabled"])
            memory_accounting = {
                "rust_optimized": rust_optimized_memory,
                "cpp_optimized": cpp_optimized_memory,
            }
            if disabled_verified:
                _, rust_disabled_rows = rust_off.execute(qid, True)
                memory_accounting["rust_disabled"] = dict(
                    rust_off.last_query_memory
                )
                _, cpp_disabled_rows = cpp_off.execute(qid, True)
                memory_accounting["cpp_disabled"] = dict(cpp_off.last_query_memory)
                for name, rows in (
                    ("Rust disabled", rust_disabled_rows),
                    ("C++ disabled", cpp_disabled_rows),
                ):
                    correct, detail = compare_rows(rust_rows, rows, ordered)
                    if not correct:
                        raise RuntimeError(f"{qid}: {name} changed semantics: {detail}")
            record = {
                "query_id": qid,
                "category": item["category"],
                "correct": True,
                "disabled_execution_verified": disabled_verified,
                "result_hash": result_hash(rust_rows, ordered),
                "row_count": len(rust_rows),
                "plans": plans,
                "memory_accounting": memory_accounting,
            }
            if not args.validate_only:
                timing = {}
                pairs = [("rust_optimized", rust_on), ("cpp_optimized", cpp_on)]
                if disabled_verified:
                    pairs += [("rust_disabled", rust_off), ("cpp_disabled", cpp_off)]
                for label, server in pairs:
                    for _ in range(args.warmup):
                        server.execute(qid)
                    timing[label] = stats(
                        [server.execute(qid)[0] for _ in range(args.iterations)]
                    )
                record["timing"] = timing
                if disabled_verified:
                    record["rust_speedup"] = (
                        timing["rust_disabled"]["median_ns"]
                        / timing["rust_optimized"]["median_ns"]
                    )
                    record["cpp_speedup"] = (
                        timing["cpp_disabled"]["median_ns"]
                        / timing["cpp_optimized"]["median_ns"]
                    )
            output.append(record)
            print(
                f"{qid} MATCH plans=PASS disabled_execution={'PASS' if disabled_verified else 'plan-only'}",
                flush=True,
            )
        out = args.results_dir.resolve()
        out.mkdir(parents=True, exist_ok=True)
        document = {
            "warmup": args.warmup,
            "iterations": args.iterations,
            "validate_only": args.validate_only,
            "queries": output,
        }
        (out / "optimizer.json").write_text(json.dumps(document, indent=2) + "\n")
        flat = []
        for row in output:
            flat.append(
                {
                    "query_id": row["query_id"],
                    "category": row["category"],
                    "correct": row["correct"],
                    "disabled_execution_verified": row["disabled_execution_verified"],
                    "rust_optimized_median_ms": row.get("timing", {})
                    .get("rust_optimized", {})
                    .get("median_ns", 0)
                    / 1e6,
                    "cpp_optimized_median_ms": row.get("timing", {})
                    .get("cpp_optimized", {})
                    .get("median_ns", 0)
                    / 1e6,
                    "rust_speedup": row.get("rust_speedup", "plan-only"),
                    "cpp_speedup": row.get("cpp_speedup", "plan-only"),
                    "rust_optimized_accounted_bytes": row["memory_accounting"][
                        "rust_optimized"
                    ]["accounted_bytes"],
                    "cpp_optimized_accounted_bytes": row["memory_accounting"][
                        "cpp_optimized"
                    ]["accounted_bytes"],
                    "rust_memory_reduction": (
                        row["memory_accounting"]["rust_disabled"][
                            "accounted_bytes"
                        ]
                        / max(
                            1,
                            row["memory_accounting"]["rust_optimized"][
                                "accounted_bytes"
                            ],
                        )
                        if row["disabled_execution_verified"]
                        else "plan-only"
                    ),
                    "cpp_memory_reduction": (
                        row["memory_accounting"]["cpp_disabled"][
                            "accounted_bytes"
                        ]
                        / max(
                            1,
                            row["memory_accounting"]["cpp_optimized"][
                                "accounted_bytes"
                            ],
                        )
                        if row["disabled_execution_verified"]
                        else "plan-only"
                    ),
                }
            )
        with (out / "optimizer.csv").open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=flat[0].keys())
            writer.writeheader()
            writer.writerows(flat)
        report = [
            "OPTIMIZER BENCHMARK",
            f"Queries: {len(output)} / {len(output)} correct",
            "Plan assertions: PASS",
        ]
        for row in flat:
            rust_speedup = (
                f"{row['rust_speedup']:.3f}x"
                if isinstance(row["rust_speedup"], (int, float))
                else row["rust_speedup"]
            )
            cpp_speedup = (
                f"{row['cpp_speedup']:.3f}x"
                if isinstance(row["cpp_speedup"], (int, float))
                else row["cpp_speedup"]
            )
            rust_memory = (
                f"{row['rust_memory_reduction']:.3f}x"
                if isinstance(row["rust_memory_reduction"], (int, float))
                else row["rust_memory_reduction"]
            )
            cpp_memory = (
                f"{row['cpp_memory_reduction']:.3f}x"
                if isinstance(row["cpp_memory_reduction"], (int, float))
                else row["cpp_memory_reduction"]
            )
            report.append(
                f"{row['query_id']} {row['category']}: "
                f"Rust {rust_speedup}, memory {rust_memory}; "
                f"C++ {cpp_speedup}, memory {cpp_memory}"
            )
        (out / "report.txt").write_text("\n".join(report) + "\n")
        print(
            f"Optimizer correctness and plan assertions: {len(output)} / {len(output)} PASS"
        )
        return 0
    finally:
        for server in reversed(servers):
            server.close()


if __name__ == "__main__":
    raise SystemExit(main())
