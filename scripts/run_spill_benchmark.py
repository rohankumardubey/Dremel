#!/usr/bin/env python3
"""Benchmark bounded spill aggregation in the Rust and C++ engines."""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import tempfile
from pathlib import Path

from run_benchmark import ROOT, Server, compare_rows, result_hash, stats


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--rust", default=str(ROOT / "dremel-rs/target/release/dremel-rs")
    )
    parser.add_argument("--cpp", default=str(ROOT / "dremel-cpp/build/dremel-cpp"))
    parser.add_argument("--data", default=str(ROOT / "data/events.dremel"))
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--batch-size", type=int, default=4096)
    parser.add_argument("--memory-limit-mb", type=int, default=4)
    parser.add_argument("--warmup", type=int, default=int(os.getenv("WARMUP", "3")))
    parser.add_argument(
        "--iterations", type=int, default=int(os.getenv("ITERATIONS", "20"))
    )
    parser.add_argument("--tie-threshold", type=float, default=1.0)
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument(
        "--manifest", type=Path, default=ROOT / "benchmark/spill/manifest.json"
    )
    parser.add_argument(
        "--results-dir", type=Path, default=ROOT / "results/spill"
    )
    args = parser.parse_args()
    if (
        args.memory_limit_mb < 2
        or args.warmup < 0
        or args.iterations < 1
        or args.tie_threshold < 0
    ):
        parser.error("memory limit must be at least 2 MiB and counts cannot be negative")

    manifest = json.loads(args.manifest.read_text())
    servers: list[Server] = []
    with tempfile.TemporaryDirectory(prefix="dremel-spill-benchmark-") as temp:
        temp_root = Path(temp)
        common = [
            "bench-server",
            "--data",
            args.data,
            "--threads",
            str(args.threads),
            "--batch-size",
            str(args.batch_size),
        ]
        try:
            rust_spill = Server(
                "Rust spill",
                [
                    args.rust,
                    *common,
                    "--query-memory-limit-mb",
                    str(args.memory_limit_mb),
                    "--spill-dir",
                    str(temp_root / "rust"),
                ],
            )
            cpp_spill = Server(
                "C++ spill",
                [
                    args.cpp,
                    *common,
                    "--query-memory-limit-mb",
                    str(args.memory_limit_mb),
                    "--spill-dir",
                    str(temp_root / "cpp"),
                ],
            )
            rust_control = Server("Rust unlimited", [args.rust, *common])
            cpp_control = Server("C++ unlimited", [args.cpp, *common])
            servers = [rust_spill, cpp_spill, rust_control, cpp_control]
            peak_rss_kib = {
                "rust_spill": rust_spill.rss_kib(),
                "cpp_spill": cpp_spill.rss_kib(),
                "rust_unlimited": rust_control.rss_kib(),
                "cpp_unlimited": cpp_control.rss_kib(),
            }
            output = []
            for item in manifest:
                query_id = item["query_id"]
                sql = (args.manifest.parent / item["file"]).read_text().strip()
                for server in servers:
                    server.prepare(query_id, sql)
                plans = {
                    "rust_spill": rust_spill.explain(query_id),
                    "cpp_spill": cpp_spill.explain(query_id),
                    "rust_unlimited": rust_control.explain(query_id),
                    "cpp_unlimited": cpp_control.explain(query_id),
                }
                if plans["rust_spill"] != plans["cpp_spill"]:
                    raise RuntimeError(f"{query_id}: spill plans differ: {plans}")
                if plans["rust_unlimited"] != plans["cpp_unlimited"]:
                    raise RuntimeError(f"{query_id}: control plans differ: {plans}")
                spill_operators = [
                    operator
                    for operator in plans["rust_spill"]
                    if not operator.startswith("SpillAggregateExec")
                ]
                if spill_operators != plans["rust_unlimited"]:
                    raise RuntimeError(
                        f"{query_id}: spill and control plans differ beyond spill: {plans}"
                    )

                results = {}
                for label, server in (
                    ("rust_spill", rust_spill),
                    ("cpp_spill", cpp_spill),
                    ("rust_unlimited", rust_control),
                    ("cpp_unlimited", cpp_control),
                ):
                    _, results[label] = server.execute(query_id, True)
                reference = results["rust_unlimited"]
                for label, rows in results.items():
                    correct, detail = compare_rows(reference, rows, ordered=True)
                    if not correct:
                        raise RuntimeError(f"{query_id}: {label} mismatch: {detail}")

                rust_metrics = dict(rust_spill.last_spill_metrics)
                cpp_metrics = dict(cpp_spill.last_spill_metrics)
                comparable = (
                    "files_created",
                    "partitions",
                    "bytes_written",
                    "bytes_read",
                    "passes",
                    "spilled",
                )
                if any(rust_metrics[key] != cpp_metrics[key] for key in comparable):
                    raise RuntimeError(
                        f"{query_id}: cross-engine spill metrics differ: "
                        f"Rust={rust_metrics}, C++={cpp_metrics}"
                    )
                if (
                    not rust_metrics["spilled"]
                    or rust_metrics["bytes_written"] == 0
                    or rust_metrics["bytes_written"] != rust_metrics["bytes_read"]
                ):
                    raise RuntimeError(f"{query_id}: invalid spill metrics: {rust_metrics}")
                for root in (temp_root / "rust", temp_root / "cpp"):
                    if root.exists() and any(root.iterdir()):
                        raise RuntimeError(f"{query_id}: spill files were not cleaned: {root}")

                record = {
                    "query_id": query_id,
                    "category": item["category"],
                    "correct": True,
                    "row_count": len(reference),
                    "result_hash": result_hash(reference, ordered=True),
                    "plans": plans,
                    "spill": rust_metrics,
                    "rust_memory": dict(rust_spill.last_query_memory),
                    "cpp_memory": dict(cpp_spill.last_query_memory),
                }
                if not args.validate_only:
                    for iteration in range(args.warmup):
                        order = (
                            (cpp_spill, rust_spill)
                            if iteration % 2 == 0
                            else (rust_spill, cpp_spill)
                        )
                        for server in order:
                            server.execute(query_id)
                    samples = {"rust": [], "cpp": []}
                    for iteration in range(args.iterations):
                        order = (
                            (("cpp", cpp_spill), ("rust", rust_spill))
                            if iteration % 2 == 0
                            else (("rust", rust_spill), ("cpp", cpp_spill))
                        )
                        for language, server in order:
                            samples[language].append(server.execute(query_id)[0])
                    record["timing"] = {
                        "rust_spill": stats(samples["rust"]),
                        "cpp_spill": stats(samples["cpp"]),
                    }
                    peak_rss_kib["rust_spill"] = max(
                        peak_rss_kib["rust_spill"], rust_spill.rss_kib()
                    )
                    peak_rss_kib["cpp_spill"] = max(
                        peak_rss_kib["cpp_spill"], cpp_spill.rss_kib()
                    )
                output.append(record)
                print(
                    f"{query_id} MATCH partitions={rust_metrics['partitions']} "
                    f"files={rust_metrics['files_created']} "
                    f"written={rust_metrics['bytes_written']} bytes",
                    flush=True,
                )

            ratios = []
            rust_wins = cpp_wins = ties = 0
            for row in output:
                timing = row.get("timing", {})
                rust_ns = timing.get("rust_spill", {}).get("median_ns", 0)
                cpp_ns = timing.get("cpp_spill", {}).get("median_ns", 0)
                if not rust_ns or not cpp_ns:
                    continue
                ratio = rust_ns / cpp_ns
                ratios.append(ratio)
                difference = abs(ratio - 1) * 100
                if difference <= args.tie_threshold:
                    ties += 1
                elif ratio < 1:
                    rust_wins += 1
                else:
                    cpp_wins += 1
            geometric_mean = (
                math.exp(sum(math.log(ratio) for ratio in ratios) / len(ratios))
                if ratios
                else None
            )
            document = {
                "configuration": {
                    "threads": args.threads,
                    "batch_size": args.batch_size,
                    "query_memory_limit_mb": args.memory_limit_mb,
                    "warmup": args.warmup,
                    "iterations": args.iterations,
                    "tie_threshold_pct": args.tie_threshold,
                },
                "validate_only": args.validate_only,
                "correct": len(output),
                "query_count": len(output),
                "rust_wins": rust_wins,
                "cpp_wins": cpp_wins,
                "ties": ties,
                "rust_cpp_geometric_mean": geometric_mean,
                "peak_rss_kib_observed": peak_rss_kib,
                "queries": output,
            }
            out = args.results_dir.resolve()
            out.mkdir(parents=True, exist_ok=True)
            (out / "spill.json").write_text(json.dumps(document, indent=2) + "\n")
            flat = []
            for row in output:
                timing = row.get("timing", {})
                flat.append(
                    {
                        "query_id": row["query_id"],
                        "category": row["category"],
                        "correct": row["correct"],
                        "partitions": row["spill"]["partitions"],
                        "files_created": row["spill"]["files_created"],
                        "bytes_written": row["spill"]["bytes_written"],
                        "rust_peak_memory_bytes": row["rust_memory"]["peak_bytes"],
                        "cpp_peak_memory_bytes": row["cpp_memory"]["peak_bytes"],
                        "rust_spill_median_ms": timing.get("rust_spill", {}).get(
                            "median_ns", 0
                        )
                        / 1e6,
                        "cpp_spill_median_ms": timing.get("cpp_spill", {}).get(
                            "median_ns", 0
                        )
                        / 1e6,
                    }
                )
            with (out / "spill.csv").open("w", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=flat[0].keys())
                writer.writeheader()
                writer.writerows(flat)
            report = [
                "SPILL-TO-DISK BENCHMARK",
                f"Queries: {len(output)} / {len(output)} correct",
                f"Query memory limit: {args.memory_limit_mb} MiB",
            ]
            if geometric_mean is not None:
                report.extend(
                    (
                        f"Rust wins: {rust_wins}; C++ wins: {cpp_wins}; ties: {ties}",
                        f"Rust/C++ geometric mean: {geometric_mean:.4f}x",
                    )
                )
            for row in flat:
                report.append(
                    f"{row['query_id']} {row['category']}: "
                    f"partitions {row['partitions']}, files {row['files_created']}, "
                    f"written {row['bytes_written']} bytes; "
                    f"Rust {row['rust_spill_median_ms']:.3f} ms, "
                    f"C++ {row['cpp_spill_median_ms']:.3f} ms"
                )
            (out / "report.txt").write_text("\n".join(report) + "\n")
            print(f"Spill correctness: {len(output)} / {len(output)} PASS")
            return 0
        finally:
            for server in reversed(servers):
                server.close()


if __name__ == "__main__":
    raise SystemExit(main())
