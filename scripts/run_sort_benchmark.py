#!/usr/bin/env python3
"""Benchmark memory-bounded external merge sort in Rust and C++."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import statistics
import subprocess
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def percentile(values: list[int], fraction: float) -> int:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def timing(samples: list[int]) -> dict:
    return {
        "samples_ns": samples,
        "sample_count": len(samples),
        "min_ns": min(samples),
        "median_ns": statistics.median(samples),
        "mean_ns": statistics.mean(samples),
        "p95_ns": percentile(samples, 0.95),
        "stddev_ns": statistics.pstdev(samples),
    }


def process_rss_kib(pid: int) -> int:
    result = subprocess.run(
        ["ps", "-o", "rss=", "-p", str(pid)],
        text=True,
        capture_output=True,
        check=False,
    )
    try:
        return int(result.stdout.strip() or 0)
    except ValueError:
        return 0


def run_query(
    engine: str,
    data: str,
    sql: str,
    batch_size: int,
    memory_limit_mb: int | None,
    external: bool,
) -> dict:
    command = [
        engine,
        "query",
        "--data",
        data,
        "--batch-size",
        str(batch_size),
        "--sql",
        sql,
        "--stats",
    ]
    temporary = None
    if memory_limit_mb is not None:
        command.extend(("--query-memory-limit-mb", str(memory_limit_mb)))
    if external:
        temporary = tempfile.TemporaryDirectory(prefix="dremel-sort-bench-")
        command.extend(("--stream-results", "--spill-dir", temporary.name))
    started = time.perf_counter_ns()
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert process.stdout and process.stderr
    peak_rss_kib = 0
    stop = threading.Event()

    def sample_rss() -> None:
        nonlocal peak_rss_kib
        while not stop.is_set():
            peak_rss_kib = max(peak_rss_kib, process_rss_kib(process.pid))
            stop.wait(0.02)

    sampler = threading.Thread(target=sample_rss, daemon=True)
    sampler.start()
    digest = hashlib.sha256()
    rows = output_bytes = 0
    while chunk := process.stdout.read(1024 * 1024):
        digest.update(chunk)
        rows += chunk.count(b"\n")
        output_bytes += len(chunk)
    stderr = process.stderr.read().decode("utf-8", errors="replace")
    returncode = process.wait()
    wall_ns = time.perf_counter_ns() - started
    stop.set()
    sampler.join(timeout=1)
    leftovers = []
    if temporary is not None:
        leftovers = list(Path(temporary.name).iterdir())
        temporary.cleanup()
    stats = {}
    for line in reversed(stderr.splitlines()):
        if line.startswith("{"):
            stats = json.loads(line)
            break
    return {
        "returncode": returncode,
        "wall_ns": wall_ns,
        "rows": rows,
        "output_bytes": output_bytes,
        "sha256": digest.hexdigest(),
        "peak_rss_kib": peak_rss_kib,
        "stats": stats,
        "stderr": stderr,
        "spill_leftovers": [str(path) for path in leftovers],
    }


def explain(
    engine: str, data: str, sql: str, batch_size: int, memory_limit_mb: int
) -> list[str]:
    with tempfile.TemporaryDirectory(prefix="dremel-sort-plan-") as directory:
        result = subprocess.run(
            [
                engine,
                "query",
                "--data",
                data,
                "--batch-size",
                str(batch_size),
                "--query-memory-limit-mb",
                str(memory_limit_mb),
                "--spill-dir",
                directory,
                "--stream-results",
                "--sql",
                sql,
                "--explain",
            ],
            text=True,
            capture_output=True,
            check=False,
        )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip())
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--rust", default=str(ROOT / "dremel-rs/target/release/dremel-rs")
    )
    parser.add_argument("--cpp", default=str(ROOT / "dremel-cpp/build/dremel-cpp"))
    parser.add_argument("--data", default=str(ROOT / "data/events.dremel"))
    parser.add_argument("--batch-size", type=int, default=4096)
    parser.add_argument("--memory-limit-mb", type=int, default=2)
    parser.add_argument("--warmup", type=int, default=int(os.getenv("WARMUP", "3")))
    parser.add_argument(
        "--iterations", type=int, default=int(os.getenv("ITERATIONS", "20"))
    )
    parser.add_argument("--tie-threshold", type=float, default=1.0)
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument(
        "--manifest", type=Path, default=ROOT / "benchmark/sort/manifest.json"
    )
    parser.add_argument("--results-dir", type=Path, default=ROOT / "results/sort")
    args = parser.parse_args()
    if (
        args.memory_limit_mb < 2
        or args.batch_size < 1
        or args.warmup < 0
        or args.iterations < 1
        or args.tie_threshold < 0
    ):
        parser.error("memory limit must be at least 2 MiB; other values must be valid")

    manifest = json.loads(args.manifest.read_text())
    records = []
    rust_wins = cpp_wins = ties = 0
    ratios = []
    for item in manifest:
        query_id = item["query_id"]
        sql = (args.manifest.parent / item["file"]).read_text().strip()
        rust_plan = explain(
            args.rust, args.data, sql, args.batch_size, args.memory_limit_mb
        )
        cpp_plan = explain(
            args.cpp, args.data, sql, args.batch_size, args.memory_limit_mb
        )
        if rust_plan != cpp_plan or not any(
            operator.startswith("ExternalMergeSortExec") for operator in rust_plan
        ):
            raise RuntimeError(
                f"{query_id}: external-sort plans differ: Rust={rust_plan}, C++={cpp_plan}"
            )

        modes = {}
        for label, engine, limit, external in (
            ("rust_external", args.rust, args.memory_limit_mb, True),
            ("cpp_external", args.cpp, args.memory_limit_mb, True),
            ("rust_materialized", args.rust, None, False),
            ("cpp_materialized", args.cpp, None, False),
        ):
            modes[label] = run_query(
                engine, args.data, sql, args.batch_size, limit, external
            )
        if any(result["returncode"] != 0 for result in modes.values()):
            raise RuntimeError(f"{query_id}: correctness mode failed: {modes}")
        signatures = {
            (result["rows"], result["output_bytes"], result["sha256"])
            for result in modes.values()
        }
        if len(signatures) != 1:
            raise RuntimeError(f"{query_id}: external/materialized mismatch: {modes}")
        expected = next(iter(signatures))
        for label in ("rust_external", "cpp_external"):
            result = modes[label]
            stats = result["stats"]
            if (
                not stats.get("spilled")
                or not stats.get("result_streamed")
                or stats.get("spill_files_created", 0) < 1
                or stats.get("query_memory_peak_bytes", 2**63)
                > args.memory_limit_mb * 1024 * 1024
                or result["spill_leftovers"]
            ):
                raise RuntimeError(f"{query_id}: invalid external-sort stats: {result}")
        limited_rejections = {}
        for label, engine in (
            ("rust_limited_materialized", args.rust),
            ("cpp_limited_materialized", args.cpp),
        ):
            result = run_query(
                engine,
                args.data,
                sql,
                args.batch_size,
                args.memory_limit_mb,
                False,
            )
            rejected = result["returncode"] != 0
            if rejected and "RESOURCE_EXHAUSTED" not in result["stderr"]:
                raise RuntimeError(f"{query_id}: unexpected limited failure: {result}")
            if not rejected and (
                result["rows"], result["output_bytes"], result["sha256"]
            ) != expected:
                raise RuntimeError(f"{query_id}: limited materialized mismatch: {result}")
            limited_rejections[label] = rejected

        record = {
            "query_id": query_id,
            "category": item["category"],
            "description": item["description"],
            "correct": True,
            "row_count": modes["rust_external"]["rows"],
            "output_bytes": modes["rust_external"]["output_bytes"],
            "result_sha256": modes["rust_external"]["sha256"],
            "plan": rust_plan,
            "rust_stats": modes["rust_external"]["stats"],
            "cpp_stats": modes["cpp_external"]["stats"],
            "peak_rss_kib": {
                key: modes[key]["peak_rss_kib"]
                for key in modes
            },
            "materialized_control_wall_ns": {
                "rust": modes["rust_materialized"]["wall_ns"],
                "cpp": modes["cpp_materialized"]["wall_ns"],
            },
            "materialized_limit_rejected": limited_rejections,
        }
        if not args.validate_only:
            for iteration in range(args.warmup):
                order = (
                    ((args.cpp, "cpp"), (args.rust, "rust"))
                    if iteration % 2 == 0
                    else ((args.rust, "rust"), (args.cpp, "cpp"))
                )
                for engine, _ in order:
                    run_query(
                        engine,
                        args.data,
                        sql,
                        args.batch_size,
                        args.memory_limit_mb,
                        True,
                    )
            samples = {"rust": [], "cpp": []}
            for iteration in range(args.iterations):
                order = (
                    ((args.cpp, "cpp"), (args.rust, "rust"))
                    if iteration % 2 == 0
                    else ((args.rust, "rust"), (args.cpp, "cpp"))
                )
                for engine, label in order:
                    result = run_query(
                        engine,
                        args.data,
                        sql,
                        args.batch_size,
                        args.memory_limit_mb,
                        True,
                    )
                    actual = (
                        result["rows"],
                        result["output_bytes"],
                        result["sha256"],
                    )
                    if result["returncode"] != 0 or actual != expected:
                        raise RuntimeError(f"{query_id}: timed {label} run failed")
                    samples[label].append(result["wall_ns"])
                    record["peak_rss_kib"][f"{label}_external"] = max(
                        record["peak_rss_kib"][f"{label}_external"],
                        result["peak_rss_kib"],
                    )
            rust_timing = timing(samples["rust"])
            cpp_timing = timing(samples["cpp"])
            ratio = rust_timing["median_ns"] / cpp_timing["median_ns"]
            difference = abs(ratio - 1) * 100
            winner = (
                "TIE"
                if difference <= args.tie_threshold
                else "Rust"
                if ratio < 1
                else "C++"
            )
            rust_wins += winner == "Rust"
            cpp_wins += winner == "C++"
            ties += winner == "TIE"
            ratios.append(ratio)
            record["timing"] = {
                "rust_external": rust_timing,
                "cpp_external": cpp_timing,
                "rust_cpp_ratio": ratio,
                "difference_pct": difference,
                "winner": winner,
            }
        records.append(record)
        print(
            f"{query_id} MATCH rows={record['row_count']} "
            f"runs={record['rust_stats']['spill_partitions']}/"
            f"{record['cpp_stats']['spill_partitions']}"
        )

    geometric_mean = (
        math.exp(sum(math.log(value) for value in ratios) / len(ratios))
        if ratios
        else None
    )
    output = {
        "configuration": {
            "batch_size": args.batch_size,
            "query_memory_limit_mb": args.memory_limit_mb,
            "warmup": args.warmup,
            "iterations": args.iterations,
            "tie_threshold_pct": args.tie_threshold,
        },
        "validate_only": args.validate_only,
        "correct": len(records),
        "query_count": len(records),
        "rust_wins": rust_wins,
        "cpp_wins": cpp_wins,
        "ties": ties,
        "rust_cpp_geometric_mean": geometric_mean,
        "queries": records,
    }
    args.results_dir.mkdir(parents=True, exist_ok=True)
    (args.results_dir / "sort.json").write_text(json.dumps(output, indent=2) + "\n")
    with (args.results_dir / "sort.csv").open("w", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=(
                "query_id",
                "category",
                "rows",
                "rust_runs",
                "cpp_runs",
                "rust_median_ms",
                "cpp_median_ms",
                "rust_cpp_ratio",
                "winner",
            ),
        )
        writer.writeheader()
        for record in records:
            values = record.get("timing", {})
            writer.writerow(
                {
                    "query_id": record["query_id"],
                    "category": record["category"],
                    "rows": record["row_count"],
                    "rust_runs": record["rust_stats"]["spill_partitions"],
                    "cpp_runs": record["cpp_stats"]["spill_partitions"],
                    "rust_median_ms": values.get("rust_external", {}).get(
                        "median_ns", 0
                    )
                    / 1e6,
                    "cpp_median_ms": values.get("cpp_external", {}).get(
                        "median_ns", 0
                    )
                    / 1e6,
                    "rust_cpp_ratio": values.get("rust_cpp_ratio", ""),
                    "winner": values.get("winner", ""),
                }
            )
    lines = [
        "EXTERNAL MERGE SORT BENCHMARK",
        f"Queries: {len(records)} / {len(records)} correct",
        f"Query memory limit: {args.memory_limit_mb} MiB",
    ]
    if geometric_mean is not None:
        lines.extend(
            (
                f"Rust wins: {rust_wins}; C++ wins: {cpp_wins}; ties: {ties}",
                f"Rust/C++ geometric mean: {geometric_mean:.4f}x",
            )
        )
    (args.results_dir / "report.txt").write_text("\n".join(lines) + "\n")
    print(f"External merge sort correctness: {len(records)} / {len(records)} PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
